from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable

from brickhouse.engines.sparkless import SparklessSessionFactory
from brickhouse.sql import (
    SqlParseError,
    SqlStatementPlanner,
    SqlStatementService,
    UnsupportedSqlStatementError,
)
from brickhouse.tables import (
    DeltaRsTableService,
    SnapshotSelector,
    SnapshotUnavailableError,
    TableHandle,
    WriteMode,
)
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace


def _data(ids: list[int], values: list[str]) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "value": pa.array(values, type=pa.string()),
        }
    )


def _register_table(context: WorkspaceContext, data: pa.Table) -> TableHandle:
    registration = context.tables.create_managed("main", "default", "orders")
    table = TableHandle.from_registration(registration)
    DeltaRsTableService(context).write_arrow(
        table,
        data,
        WriteMode.CREATE,
        operation_id="seed-time-travel-orders",
    )
    return table


def test_sql_version_as_of_reads_one_pinned_snapshot_through_sparkless(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    initial = _data([1], ["initial"])
    appended = _data([2], ["later"])
    with WorkspaceContext.local(workspace.path) as context:
        table = _register_table(context, initial)
        tables = DeltaRsTableService(context)
        tables.write_arrow(
            table,
            appended,
            WriteMode.APPEND,
            operation_id="append-time-travel-orders",
        )
        planner = SqlStatementPlanner(context)
        plan = planner.plan("SELECT id, value FROM orders VERSION AS OF 0 ORDER BY id")
        assert plan.source_tables[0].table.table_id == table.table_id
        assert plan.source_tables[0].snapshot_version == 0
        assert plan.rendered_sql is not None
        assert "VERSION AS OF" not in plan.rendered_sql

        historical = (
            SqlStatementService(context)
            .execute("SELECT id, value FROM orders VERSION AS OF 0 ORDER BY id")
            .data
        )
        direct = tables.read_table(table, SnapshotSelector(version=0))
        with SparklessSessionFactory(context).open(
            snapshot_versions={table.table_id: 0}
        ) as session:
            sparkless = session.table("orders").toArrow()
        latest = (
            SqlStatementService(context).execute("SELECT id, value FROM orders ORDER BY id").data
        )

        assert direct.snapshot.version == 0
        assert historical.schema == direct.data.schema == sparkless.schema
        assert (
            historical.to_pylist()
            == direct.data.to_pylist()
            == sparkless.to_pylist()
            == [{"id": 1, "value": "initial"}]
        )
        assert latest.to_pylist() == [
            {"id": 1, "value": "initial"},
            {"id": 2, "value": "later"},
        ]


@pytest.mark.parametrize(
    "statement",
    [
        "SELECT * FROM orders TIMESTAMP AS OF '2026-10-21 00:00:00'",
        "SELECT * FROM orders VERSION AS OF -1",
        "SELECT * FROM orders VERSION AS OF 1.5",
        "SELECT * FROM orders VERSION AS OF '0'",
        "SELECT * FROM orders VERSION BETWEEN 0 AND 1",
    ],
)
def test_unsupported_snapshot_selectors_reject_during_planning(
    tmp_path: Path,
    statement: str,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        _register_table(context, _data([1], ["latest"]))

        with pytest.raises((SqlParseError, UnsupportedSqlStatementError)):
            SqlStatementService(context).execute(statement)

        assert SqlStatementService(context).execute("SELECT * FROM orders").data.to_pylist() == [
            {"id": 1, "value": "latest"}
        ]


def test_future_version_as_of_raises_without_falling_back_to_latest(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        _register_table(context, _data([1], ["latest"]))
        service = SqlStatementService(context)

        with pytest.raises(SnapshotUnavailableError):
            service.execute("SELECT * FROM orders VERSION AS OF 99")

        assert service.execute("SELECT * FROM orders").data.to_pylist() == [
            {"id": 1, "value": "latest"}
        ]


def test_expired_snapshot_raises_without_falling_back_to_latest(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        table = _register_table(context, _data([1], ["old"]))
        tables = DeltaRsTableService(context)
        tables.write_arrow(
            table,
            _data([2], ["latest"]),
            WriteMode.OVERWRITE,
            operation_id="overwrite-time-travel-orders",
        )
        DeltaTable(str(table.location)).vacuum(
            retention_hours=0,
            dry_run=False,
            enforce_retention_duration=False,
        )
        service = SqlStatementService(context)

        with pytest.raises(SnapshotUnavailableError):
            service.execute("SELECT * FROM orders VERSION AS OF 0")

        assert service.execute("SELECT * FROM orders").data.to_pylist() == [
            {"id": 2, "value": "latest"}
        ]
