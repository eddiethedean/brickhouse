from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable

from brickhouse.catalog.repository import TableNotFoundError
from brickhouse.sql import SqlParseError, SqlStatementService, UnsupportedSqlStatementError
from brickhouse.tables import DeltaRsTableService, TableHandle, WriteMode
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace


def _rows(
    order_ids: list[int], customers: list[str], amounts: list[str], active: list[bool]
) -> pa.Table:
    return pa.table(
        {
            "order_id": pa.array(order_ids, type=pa.int64()),
            "customer": pa.array(customers, type=pa.string()),
            "amount": pa.array(
                [Decimal(value) for value in amounts],
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


def _string_map(value: object) -> dict[str, str] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return {str(key): str(item) for key, item in value.items()}
    assert isinstance(value, (list, tuple))
    return {str(key): str(item) for key, item in value}


def test_describe_table_returns_registered_provider_schema(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        table = _register_table(
            context,
            "orders",
            _rows([1], ["alice"], ["10.50"], [True]),
        )

        result = SqlStatementService(context).execute("DESCRIBE TABLE main.default.orders")

        assert result.object_name == "main.default.orders"
        assert result.data.schema == pa.schema(
            [
                pa.field("col_name", pa.string(), nullable=False),
                pa.field("data_type", pa.string(), nullable=False),
                pa.field("comment", pa.string(), nullable=True),
            ]
        )
        assert result.data.to_pylist() == [
            {"col_name": "order_id", "data_type": "bigint", "comment": None},
            {"col_name": "customer", "data_type": "string", "comment": None},
            {"col_name": "amount", "data_type": "decimal(12,2)", "comment": None},
            {"col_name": "active", "data_type": "boolean", "comment": None},
        ]
        assert DeltaRsTableService(context).read_snapshot(table).version == 0


def test_describe_table_formats_nested_delta_types(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    data = pa.Table.from_pylist(
        [{"item_ids": [1, 2], "profile": {"city": "Boston", "level": 3}}],
        schema=pa.schema(
            [
                pa.field("item_ids", pa.list_(pa.int64())),
                pa.field(
                    "profile",
                    pa.struct(
                        [
                            pa.field("city", pa.string()),
                            pa.field("level", pa.int32()),
                        ]
                    ),
                ),
            ]
        ),
    )
    with WorkspaceContext.local(workspace.path) as context:
        _register_table(context, "nested", data)

        result = SqlStatementService(context).execute("DESCRIBE TABLE nested")

        assert result.data.to_pylist() == [
            {"col_name": "item_ids", "data_type": "array<bigint>", "comment": None},
            {
                "col_name": "profile",
                "data_type": "struct<city:string,level:int>",
                "comment": None,
            },
        ]


def test_describe_history_uses_committed_delta_history_after_mutations(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    target_data = _rows(
        [1, 2, 3],
        ["alice", "bob", "carol"],
        ["10.00", "20.00", "30.00"],
        [True, True, False],
    )
    source_data = _rows(
        [2, 4],
        ["robert", "dana"],
        ["21.50", "40.00"],
        [False, True],
    )
    with WorkspaceContext.local(workspace.path) as context:
        target = _register_table(context, "orders", target_data)
        _register_table(context, "changes", source_data)
        service = SqlStatementService(context)
        service.execute(
            "UPDATE orders SET customer = 'bobby' WHERE order_id = 2",
            operation_id="describe-history-update",
        )
        service.execute(
            "DELETE FROM orders WHERE order_id = 1",
            operation_id="describe-history-delete",
        )
        service.execute(
            "MERGE INTO main.default.orders AS t "
            "USING main.default.changes AS s ON t.order_id = s.order_id "
            "WHEN MATCHED THEN UPDATE SET customer = s.customer, amount = s.amount, "
            "active = s.active "
            "WHEN NOT MATCHED THEN INSERT (order_id, customer, amount, active) "
            "VALUES (s.order_id, s.customer, s.amount, s.active)",
            operation_id="describe-history-merge",
        )

        result = service.execute("DESCRIBE HISTORY main.default.orders")

        string_map = pa.map_(pa.string(), pa.string())
        assert result.object_name == "main.default.orders"
        assert result.data.schema == pa.schema(
            [
                pa.field("version", pa.int64(), nullable=False),
                pa.field("timestamp", pa.timestamp("ms", tz="UTC"), nullable=False),
                pa.field("operation", pa.string(), nullable=False),
                pa.field("operationParameters", string_map, nullable=True),
                pa.field("readVersion", pa.int64(), nullable=True),
                pa.field("operationMetrics", string_map, nullable=True),
                pa.field("engineInfo", pa.string(), nullable=True),
            ]
        )
        rows = result.data.to_pylist()
        assert [(row["version"], row["operation"]) for row in rows] == [
            (3, "MERGE"),
            (2, "DELETE"),
            (1, "UPDATE"),
            (0, "WRITE"),
        ]

        provider_history = DeltaTable(str(target.location)).history(limit=100)
        assert len(rows) == len(provider_history)
        for actual, provider in zip(rows, provider_history, strict=True):
            assert actual["version"] == provider["version"]
            assert actual["timestamp"].timestamp() == pytest.approx(provider["timestamp"] / 1000)
            assert actual["operation"] == provider["operation"]
            assert _string_map(actual["operationParameters"]) == _string_map(
                [[key, value] for key, value in (provider.get("operationParameters") or {}).items()]
            )
            assert actual["readVersion"] == provider.get("readVersion")
            assert _string_map(actual["operationMetrics"]) == _string_map(
                [[key, value] for key, value in (provider.get("operationMetrics") or {}).items()]
            )
            assert actual["engineInfo"] == provider.get("engineInfo")

        merge_metrics = _string_map(rows[0]["operationMetrics"])
        delete_metrics = _string_map(rows[1]["operationMetrics"])
        update_metrics = _string_map(rows[2]["operationMetrics"])
        assert merge_metrics is not None
        assert merge_metrics["num_target_rows_updated"] == "1"
        assert merge_metrics["num_target_rows_inserted"] == "1"
        assert delete_metrics is not None and delete_metrics["num_deleted_rows"] == "1"
        assert update_metrics is not None and update_metrics["num_updated_rows"] == "1"

        described = service.execute("DESCRIBE TABLE orders").data
        assert described.schema.names == ["col_name", "data_type", "comment"]
        assert service.execute(
            "SELECT order_id, customer, amount, active FROM orders ORDER BY order_id"
        ).data.to_pylist() == [
            {
                "order_id": 2,
                "customer": "robert",
                "amount": Decimal("21.50"),
                "active": False,
            },
            {
                "order_id": 3,
                "customer": "carol",
                "amount": Decimal("30.00"),
                "active": False,
            },
            {
                "order_id": 4,
                "customer": "dana",
                "amount": Decimal("40.00"),
                "active": True,
            },
        ]


@pytest.mark.parametrize(
    "statement",
    [
        "DESCRIBE EXTENDED orders",
        "DESCRIBE DETAIL orders",
        "DESCRIBE HISTORY orders LIMIT 1",
    ],
)
def test_describe_rejects_unselected_variants_without_side_effects(
    tmp_path: Path,
    statement: str,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        _register_table(context, "orders", _rows([1], ["alice"], ["10.50"], [True]))

        with pytest.raises((SqlParseError, UnsupportedSqlStatementError)):
            SqlStatementService(context).execute(statement)

        assert {table.name for table in context.tables.list("main", "default")} == {"orders"}


def test_describe_rejects_missing_registered_table(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        with pytest.raises(TableNotFoundError):
            SqlStatementService(context).execute("DESCRIBE HISTORY missing")
