from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable

from brickhouse.catalog.repository import TableNotFoundError
from brickhouse.sql import SqlStatementService, UnsupportedSqlStatementError
from brickhouse.tables import (
    DeltaRsTableService,
    DuplicateMergeSourceKeyError,
    TableHandle,
    TableOperationState,
    WriteMode,
)
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace


def _rows(
    order_ids: list[int | None],
    customers: list[str],
    amounts: list[str | None],
    active: list[bool],
) -> pa.Table:
    return pa.table(
        {
            "order_id": pa.array(order_ids, type=pa.int64()),
            "customer": pa.array(customers, type=pa.string()),
            "amount": pa.array(
                [None if value is None else Decimal(value) for value in amounts],
                type=pa.decimal128(12, 2),
            ),
            "active": pa.array(active, type=pa.bool_()),
        }
    )


def _register_table(
    context: WorkspaceContext,
    name: str,
    data: pa.Table,
) -> TableHandle:
    registration = context.tables.create_managed("main", "default", name)
    table = TableHandle.from_registration(registration)
    DeltaRsTableService(context).write_arrow(
        table,
        data,
        WriteMode.CREATE,
        operation_id=f"seed-{name}",
    )
    return table


def _merge_sql(target: str = "orders") -> str:
    return (
        f"MERGE INTO main.default.{target} AS t "
        "USING main.default.changes AS s ON t.order_id = s.order_id "
        "WHEN MATCHED THEN UPDATE SET customer = s.customer, amount = s.amount, active = s.active "
        "WHEN NOT MATCHED THEN INSERT (order_id, customer, amount, active) "
        "VALUES (s.order_id, s.customer, s.amount, s.active)"
    )


def test_single_key_merge_commits_once_and_is_visible_through_sparkless(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    target_data = _rows(
        [1, 2, 3],
        ["alice", "bob", "carol"],
        ["10.00", "20.00", None],
        [True, True, False],
    )
    source_data = _rows(
        [2, 4],
        ["bobby", "dana"],
        ["21.50", "40.00"],
        [False, True],
    )
    with WorkspaceContext.local(workspace.path) as context:
        target = _register_table(context, "orders", target_data)
        source = _register_table(context, "changes", source_data)
        sql = SqlStatementService(context)
        tables = DeltaRsTableService(context)

        result = sql.execute(_merge_sql(), operation_id="merge-orders-2-4")

        assert result.rows_affected == 2
        assert not result.no_op
        assert result.commit is not None
        assert result.commit.mode == "sql_merge"
        assert result.commit.commit_version == 1
        assert result.commit.snapshot.schema == tables.read_snapshot(target).schema
        assert DeltaTable(str(target.location)).version() == 1
        assert DeltaTable(str(source.location)).version() == 0

        history = DeltaTable(str(target.location)).history(limit=1)[0]
        assert history["operation"] == "MERGE"
        assert history["brickhouse.operation_id"] == "merge-orders-2-4"
        assert history["operationMetrics"]["num_target_rows_updated"] == 1
        assert history["operationMetrics"]["num_target_rows_inserted"] == 1

        expected = [
            {
                "order_id": 1,
                "customer": "alice",
                "amount": Decimal("10.00"),
                "active": True,
            },
            {
                "order_id": 2,
                "customer": "bobby",
                "amount": Decimal("21.50"),
                "active": False,
            },
            {
                "order_id": 3,
                "customer": "carol",
                "amount": None,
                "active": False,
            },
            {
                "order_id": 4,
                "customer": "dana",
                "amount": Decimal("40.00"),
                "active": True,
            },
        ]
        provider_read = tables.read_table(target)
        assert provider_read.snapshot.version == 1
        assert provider_read.data.schema == target_data.schema
        assert sorted(provider_read.data.to_pylist(), key=lambda row: row["order_id"]) == expected
        sparkless_read = sql.execute(
            "SELECT order_id, customer, amount, active FROM orders ORDER BY order_id"
        ).data
        assert sparkless_read.schema == target_data.schema
        assert sparkless_read.to_pylist() == expected

        record = context.table_operations.get(target.table_id, "merge-orders-2-4")
        assert record is not None and record.state is TableOperationState.COMMITTED
        assert record.transaction_version == 1
        assert record.commit_version == 1
        assert record.row_count == 2


@pytest.mark.parametrize(
    "statement",
    [
        _merge_sql().replace(
            "WHEN MATCHED THEN UPDATE SET customer = s.customer, amount = s.amount, "
            "active = s.active",
            "WHEN MATCHED THEN DELETE",
        ),
        _merge_sql().replace(
            "WHEN NOT MATCHED THEN INSERT",
            "WHEN NOT MATCHED BY SOURCE THEN DELETE WHEN NOT MATCHED THEN INSERT",
        ),
        _merge_sql().replace(
            "ON t.order_id = s.order_id",
            "ON t.order_id = s.order_id AND t.active = s.active",
        ),
        _merge_sql().replace(
            "customer = s.customer",
            "missing_column = s.customer",
        ),
        _merge_sql().replace(
            "(order_id, customer, amount, active)",
            "(order_id, customer, amount)",
        ),
    ],
)
def test_unsupported_or_invalid_merge_has_no_target_side_effects(
    tmp_path: Path,
    statement: str,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    target_data = _rows([1, 2], ["alice", "bob"], ["10.00", "20.00"], [True, True])
    source_data = _rows([2, 3], ["bobby", "cara"], ["21.00", "30.00"], [False, True])
    with WorkspaceContext.local(workspace.path) as context:
        target = _register_table(context, "orders", target_data)
        _register_table(context, "changes", source_data)
        with pytest.raises(UnsupportedSqlStatementError):
            SqlStatementService(context).execute(statement, operation_id="rejected-merge")

        assert DeltaTable(str(target.location)).version() == 0
        assert DeltaRsTableService(context).read_table(target).data.equals(target_data)
        assert context.table_operations.get(target.table_id, "rejected-merge") is None


def test_merge_rejects_duplicate_source_keys_before_target_journaling(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    target_data = _rows([1, 2], ["alice", "bob"], ["10.00", "20.00"], [True, True])
    duplicate_source = _rows([2, 2], ["bobby", "robert"], ["21.00", "22.00"], [False, True])
    with WorkspaceContext.local(workspace.path) as context:
        target = _register_table(context, "orders", target_data)
        source = _register_table(context, "changes", duplicate_source)
        with pytest.raises(DuplicateMergeSourceKeyError, match="duplicate key"):
            SqlStatementService(context).execute(_merge_sql(), operation_id="duplicate-merge")

        assert DeltaTable(str(target.location)).version() == 0
        assert DeltaTable(str(source.location)).version() == 0
        assert DeltaRsTableService(context).read_table(target).data.equals(target_data)
        assert context.table_operations.get(target.table_id, "duplicate-merge") is None


def test_merge_rejects_null_source_keys_before_target_journaling(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    target_data = _rows([1, 2], ["alice", "bob"], ["10.00", "20.00"], [True, True])
    null_source = _rows([None], ["unknown"], ["21.00"], [False])
    with WorkspaceContext.local(workspace.path) as context:
        target = _register_table(context, "orders", target_data)
        source = _register_table(context, "changes", null_source)
        with pytest.raises(DuplicateMergeSourceKeyError, match="cannot contain NULL"):
            SqlStatementService(context).execute(_merge_sql(), operation_id="null-key-merge")

        assert DeltaTable(str(target.location)).version() == 0
        assert DeltaTable(str(source.location)).version() == 0
        assert DeltaRsTableService(context).read_table(target).data.equals(target_data)
        assert context.table_operations.get(target.table_id, "null-key-merge") is None


def test_merge_rejects_missing_target_before_provider_dispatch(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    source_data = _rows([3], ["cara"], ["30.00"], [True])
    with WorkspaceContext.local(workspace.path) as context:
        _register_table(context, "changes", source_data)
        with pytest.raises(TableNotFoundError):
            SqlStatementService(context).execute(
                _merge_sql(target="missing"),
                operation_id="missing-target-merge",
            )
