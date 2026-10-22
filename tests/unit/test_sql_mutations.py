from __future__ import annotations

import sqlite3
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable

from brickhouse.sql import SqlStatementService, UnsupportedSqlStatementError
from brickhouse.tables import (
    DeltaRsTableService,
    DeltaTableProviderError,
    OperationKeyReuseError,
    StaleTableSnapshotError,
    TableHandle,
    TableOperationState,
    WriteMode,
)
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace


def _orders() -> pa.Table:
    return pa.table(
        {
            "order_id": pa.array([1, 2, 3, 4], type=pa.int64()),
            "customer": pa.array(["alice", "alice", "bob", "bob"]),
            "amount": pa.array(
                [Decimal("10.50"), Decimal("5.25"), None, Decimal("20.00")],
                type=pa.decimal128(12, 2),
            ),
            "active": pa.array([True, True, True, False]),
        }
    )


def _seed(context: WorkspaceContext) -> tuple[TableHandle, pa.Table]:
    data = _orders()
    registration = context.tables.create_managed("main", "default", "orders")
    table = TableHandle.from_registration(registration)
    DeltaRsTableService(context).write_arrow(
        table,
        data,
        WriteMode.CREATE,
        operation_id="seed-orders",
    )
    return table, data


def test_sql_update_delete_commit_through_delta_and_are_dataframe_visible(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        table, original = _seed(context)
        sql = SqlStatementService(context)
        table_service = DeltaRsTableService(context)

        updated = sql.execute(
            "UPDATE main.default.orders SET customer = 'carol', amount = amount + 1 "
            "WHERE order_id = 1 AND amount IS NOT NULL",
            operation_id="update-order-1",
        )
        assert updated.rows_affected == 1
        assert updated.commit is not None
        assert updated.commit.commit_version == 1
        assert updated.commit.snapshot.schema == table_service.read_snapshot(table).schema
        update_history = DeltaTable(str(table.location)).history(limit=1)[0]
        assert update_history["operation"] == "UPDATE"
        assert update_history["brickhouse.operation_id"] == "update-order-1"
        assert update_history["operationMetrics"]["num_updated_rows"] == 1

        update_read = table_service.read_table(table)
        assert update_read.snapshot.version == 1
        assert update_read.data.schema == original.schema
        assert update_read.data.to_pylist()[0] == {
            "order_id": 1,
            "customer": "carol",
            "amount": Decimal("11.50"),
            "active": True,
        }
        sparkless_read = sql.execute(
            "SELECT order_id, customer, amount FROM orders ORDER BY order_id"
        ).data
        assert sparkless_read.to_pylist()[0] == {
            "order_id": 1,
            "customer": "carol",
            "amount": Decimal("11.50"),
        }

        deleted = sql.execute(
            "DELETE FROM orders WHERE amount IS NULL",
            operation_id="delete-null-amount",
        )
        assert deleted.rows_affected == 1
        assert deleted.commit is not None
        assert deleted.commit.commit_version == 2
        delete_history = DeltaTable(str(table.location)).history(limit=1)[0]
        assert delete_history["operation"] == "DELETE"
        assert delete_history["brickhouse.operation_id"] == "delete-null-amount"
        assert delete_history["operationMetrics"]["num_deleted_rows"] == 1

        final_read = table_service.read_table(table)
        assert final_read.snapshot.version == 2
        assert final_read.data.schema == original.schema
        assert final_read.data.num_rows == 3
        assert all(row["amount"] is not None for row in final_read.data.to_pylist())
        assert sql.execute("SELECT COUNT(*) AS n FROM orders").data.to_pylist() == [{"n": 3}]

        update_record = context.table_operations.get(table.table_id, "update-order-1")
        delete_record = context.table_operations.get(table.table_id, "delete-null-amount")
        assert update_record is not None and update_record.state is TableOperationState.COMMITTED
        assert delete_record is not None and delete_record.state is TableOperationState.COMMITTED
        assert (update_record.transaction_version, update_record.commit_version) == (1, 1)
        assert (delete_record.transaction_version, delete_record.commit_version) == (2, 2)


def test_sql_zero_row_mutations_are_idempotent_and_do_not_advance_delta_version(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        table, original = _seed(context)
        sql = SqlStatementService(context)
        table_service = DeltaRsTableService(context)

        no_update = sql.execute(
            "UPDATE orders SET customer = 'unused' WHERE order_id = 99",
            operation_id="zero-row-update",
        )
        assert no_update.rows_affected == 0
        assert no_update.no_op
        assert no_update.commit is not None
        assert no_update.commit.commit_version is None
        assert no_update.commit.version == 0

        no_delete = sql.execute(
            "DELETE FROM orders WHERE order_id = 99",
            operation_id="zero-row-delete",
        )
        assert no_delete.rows_affected == 0
        assert no_delete.no_op
        assert no_delete.commit is not None
        assert no_delete.commit.version == 0

        replay = sql.execute(
            "UPDATE orders SET customer = 'unused' WHERE order_id = 99",
            operation_id="zero-row-update",
        )
        assert replay.rows_affected == 0
        assert replay.no_op
        assert DeltaTable(str(table.location)).version() == 0
        assert table_service.read_table(table).data.equals(original)
        with pytest.raises(OperationKeyReuseError):
            sql.execute(
                "UPDATE orders SET customer = 'different' WHERE order_id = 99",
                operation_id="zero-row-update",
            )

        committed = sql.execute(
            "UPDATE orders SET active = false WHERE order_id = 4",
            operation_id="after-noop-update",
        )
        assert committed.rows_affected == 1
        assert committed.commit is not None
        assert committed.commit.commit_version == 1
        record = context.table_operations.get(table.table_id, "after-noop-update")
        assert record is not None and record.transaction_version == 1
        assert DeltaTable(str(table.location)).version() == 1


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE orders SET customer = 'x'",
        "UPDATE orders SET missing_column = 'x' WHERE order_id = 1",
        "DELETE FROM orders",
        "DELETE FROM orders WHERE missing_column = 1",
        "UPDATE orders SET customer = 'x' WHERE EXISTS (SELECT 1)",
        "DELETE FROM orders USING other WHERE orders.order_id = other.order_id",
    ],
)
def test_unsupported_or_unbound_mutations_have_no_side_effects(
    tmp_path: Path,
    statement: str,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        table, original = _seed(context)
        sql = SqlStatementService(context)

        with pytest.raises(UnsupportedSqlStatementError):
            sql.execute(statement, operation_id="rejected-mutation")

        assert DeltaTable(str(table.location)).version() == 0
        assert DeltaRsTableService(context).read_table(table).data.equals(original)
        assert context.table_operations.get(table.table_id, "rejected-mutation") is None


def test_provider_type_rejection_preserves_rows_and_releases_transaction_sequence(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        table, original = _seed(context)
        sql = SqlStatementService(context)

        with pytest.raises(DeltaTableProviderError, match="rejected SQL UPDATE"):
            sql.execute(
                "UPDATE orders SET amount = active WHERE order_id = 1",
                operation_id="bad-type-update",
            )

        failed = context.table_operations.get(table.table_id, "bad-type-update")
        assert failed is not None and failed.state is TableOperationState.FAILED
        assert failed.transaction_version is None
        assert DeltaTable(str(table.location)).version() == 0
        assert DeltaRsTableService(context).read_table(table).data.equals(original)

        committed = sql.execute(
            "UPDATE orders SET amount = 12.00 WHERE order_id = 1",
            operation_id="valid-update-after-failure",
        )
        assert committed.rows_affected == 1
        assert committed.commit is not None and committed.commit.commit_version == 1


def test_sql_mutation_rejects_a_stale_snapshot_before_provider_dispatch(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        table, _ = _seed(context)
        sql = SqlStatementService(context)
        sql.execute(
            "UPDATE orders SET active = false WHERE order_id = 1",
            operation_id="first-update",
        )
        service = DeltaRsTableService(context)

        with pytest.raises(StaleTableSnapshotError):
            service.delete_rows(
                table,
                "order_id = 2",
                operation_id="stale-delete",
                expected_version=0,
            )

        assert DeltaTable(str(table.location)).version() == 1
        assert service.read_table(table).data.num_rows == 4
        assert context.table_operations.get(table.table_id, "stale-delete") is None


def test_uncertain_sql_mutation_recovers_exact_history_metrics_on_replay(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        table, _ = _seed(context)
        sql = SqlStatementService(context)
        service = DeltaRsTableService(context)
        original_finalize = context.table_operations.finalize_committed

        def fail_finalize(*args: object, **kwargs: object) -> object:
            raise sqlite3.OperationalError("injected after native Delta commit")

        context.table_operations.finalize_committed = fail_finalize  # type: ignore[method-assign]
        with pytest.raises(Exception, match="uncertain"):
            sql.execute(
                "UPDATE orders SET customer = 'recovered' WHERE order_id = 1",
                operation_id="recover-update",
            )
        context.table_operations.finalize_committed = original_finalize  # type: ignore[method-assign]

        recovered = sql.execute(
            "UPDATE orders SET customer = 'recovered' WHERE order_id = 1",
            operation_id="recover-update",
        )
        assert recovered.rows_affected == 1
        assert recovered.commit is not None and recovered.commit.commit_version == 1
        assert context.table_operations.get(table.table_id, "recover-update").state is (
            TableOperationState.COMMITTED
        )
        assert DeltaTable(str(table.location)).version() == 1
        assert service.read_table(table).data.to_pylist()[0]["customer"] == "recovered"
