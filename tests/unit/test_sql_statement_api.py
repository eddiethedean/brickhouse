from __future__ import annotations

import importlib.metadata
import json
import sqlite3
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from threading import Event, Thread

import pyarrow as pa
import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InvalidParameterValue, ResourceConflict, ResourceDoesNotExist
from databricks.sdk.service.sql import (
    ColumnInfoTypeName,
    CreateWarehouseRequestWarehouseType,
    Disposition,
    ExecuteStatementRequestOnWaitTimeout,
    Format,
    StatementParameterListItem,
    StatementState,
)

from brickhouse.api.statements import SqlStatementHttpApi
from brickhouse.compute import MAX_QUEUED_STATEMENTS_PER_WAREHOUSE
from brickhouse.diagnostic_server import create_diagnostic_server
from brickhouse.sql.parameters import SqlStatementParameter
from brickhouse.sql.planner import SqlOperation
from brickhouse.sql.service import SqlExecutionResult, SqlStatementService
from brickhouse.sql.statements import SqlStatementExecutionService
from brickhouse.tables import DeltaRsTableService, TableHandle, WriteMode
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace, open_workspace


@contextmanager
def local_statement_sdk_client(
    path: Path,
) -> Iterator[tuple[WorkspaceClient, Path, WorkspaceContext]]:
    if not path.exists():
        initialize_workspace(path)
    descriptor = open_workspace(path)
    context = WorkspaceContext.local(descriptor.path)
    context.__enter__()
    server = create_diagnostic_server(context, "127.0.0.1", 0)
    thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    client = WorkspaceClient(
        host=f"http://127.0.0.1:{server.server_port}",
        token="brickhouse-local-test-token",
        auth_type="pat",
    )
    try:
        yield client, descriptor.path, context
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
        context.close()


def _warehouse_id(client: WorkspaceClient, *, name: str = "statement-tests") -> str:
    warehouse = client.warehouses.create(
        name=name,
        warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
        enable_serverless_compute=True,
        cluster_size="2X-Small",
        auto_stop_mins=0,
    ).result()
    assert warehouse.id is not None
    return warehouse.id


def _seed_orders(context: WorkspaceContext) -> None:
    registration = context.tables.create_managed("main", "default", "orders")
    table = TableHandle.from_registration(registration)
    DeltaRsTableService(context).write_arrow(
        table,
        pa.table(
            {
                "order_id": pa.array([1, 2, 3], type=pa.int64()),
                "amount": pa.array(
                    [Decimal("10.50"), Decimal("5.25"), None],
                    type=pa.decimal128(12, 2),
                ),
                "active": pa.array([True, True, False], type=pa.bool_()),
            }
        ),
        WriteMode.CREATE,
        operation_id="statement-api-seed-orders",
    )


def _get_until_terminal(
    client: WorkspaceClient,
    statement_id: str,
    *,
    timeout_seconds: float = 5.0,
):
    deadline = time.monotonic() + timeout_seconds
    while True:
        result = client.statement_execution.get_statement(statement_id)
        assert result.status is not None
        if result.status.state not in {StatementState.PENDING, StatementState.RUNNING}:
            return result
        if time.monotonic() >= deadline:
            pytest.fail(f"statement did not finish within {timeout_seconds} seconds")
        time.sleep(0.02)


def test_pinned_sdk_statement_json_schema_polling_and_persistent_results(tmp_path: Path) -> None:
    assert importlib.metadata.version("databricks-sdk") == "0.153.0"
    query = "SELECT order_id, amount, active FROM orders WHERE amount IS NOT NULL ORDER BY order_id"

    with local_statement_sdk_client(tmp_path / "workspace") as (client, workspace_path, context):
        warehouse_id = _warehouse_id(client)
        _seed_orders(context)

        synchronous = client.statement_execution.execute_statement(
            query,
            warehouse_id,
            catalog="main",
            schema="default",
        )
        assert synchronous.statement_id is not None
        assert synchronous.status is not None
        assert synchronous.status.state is StatementState.SUCCEEDED
        assert synchronous.manifest is not None
        assert synchronous.manifest.schema is not None
        assert synchronous.manifest.schema.column_count == 3
        assert synchronous.manifest.schema.columns is not None
        assert [column.name for column in synchronous.manifest.schema.columns] == [
            "order_id",
            "amount",
            "active",
        ]
        assert [column.type_name for column in synchronous.manifest.schema.columns] == [
            ColumnInfoTypeName.LONG,
            ColumnInfoTypeName.DECIMAL,
            ColumnInfoTypeName.BOOLEAN,
        ]
        assert synchronous.manifest.schema.columns[1].type_precision == 12
        assert synchronous.manifest.schema.columns[1].type_scale == 2
        assert synchronous.result is not None
        assert synchronous.result.data_array == [
            ["1", "10.50", "true"],
            ["2", "5.25", "true"],
        ]

        asynchronous = client.statement_execution.execute_statement(
            "SELECT order_id FROM orders ORDER BY order_id",
            warehouse_id,
            wait_timeout="0s",
        )
        assert asynchronous.statement_id is not None
        assert asynchronous.status is not None
        assert asynchronous.status.state is StatementState.PENDING
        polled = _get_until_terminal(client, asynchronous.statement_id)
        assert polled.status is not None
        assert polled.status.state is StatementState.SUCCEEDED
        chunk = client.statement_execution.get_statement_result_chunk_n(
            asynchronous.statement_id,
            0,
        )
        assert chunk.data_array == [["1"], ["2"], ["3"]]

        failed = client.statement_execution.execute_statement(
            "SELECT * FROM absent_table",
            warehouse_id,
        )
        assert failed.statement_id is not None
        assert failed.status is not None
        assert failed.status.state is StatementState.FAILED
        assert failed.status.error is not None
        assert failed.status.error.error_code is not None
        assert failed.status.error.error_code.value == "BAD_REQUEST"
        failed_id = failed.statement_id

        with pytest.raises(InvalidParameterValue, match="format=JSON_ARRAY"):
            client.statement_execution.execute_statement(
                "SELECT 1",
                warehouse_id,
                format=Format.ARROW_STREAM,
            )
        with pytest.raises(InvalidParameterValue, match="format=JSON_ARRAY"):
            client.statement_execution.execute_statement(
                "SELECT 1",
                warehouse_id,
                format=Format.CSV,
            )
        with pytest.raises(InvalidParameterValue, match="disposition=INLINE"):
            client.statement_execution.execute_statement(
                "SELECT 1",
                warehouse_id,
                disposition=Disposition.EXTERNAL_LINKS,
            )
        with pytest.raises(InvalidParameterValue, match="row_limit"):
            client.statement_execution.execute_statement(
                "SELECT 1",
                warehouse_id,
                row_limit=0,
            )
        with pytest.raises(InvalidParameterValue, match="byte_limit"):
            client.statement_execution.execute_statement(
                "SELECT 1",
                warehouse_id,
                byte_limit=(8 * 1024 * 1024) + 1,
            )

        with sqlite3.connect(workspace_path / "metadata.sqlite") as connection:
            assert connection.execute(
                "SELECT state, result_row_count, result_byte_count "
                "FROM sql_statements WHERE statement_id = ?",
                (synchronous.statement_id,),
            ).fetchone() == ("SUCCEEDED", 2, 42)
            assert connection.execute(
                "SELECT state, error_code FROM sql_statements WHERE statement_id = ?",
                (failed_id,),
            ).fetchone() == ("FAILED", "BAD_REQUEST")

    with local_statement_sdk_client(workspace_path) as (reopened_client, _path, _context):
        reopened = reopened_client.statement_execution.get_statement(synchronous.statement_id)
        assert reopened.status is not None
        assert reopened.status.state is StatementState.SUCCEEDED
        assert reopened.result is not None
        assert reopened.result.data_array == [["1", "10.50", "true"], ["2", "5.25", "true"]]


def test_sdk_statement_row_and_byte_limits_return_truncated_prefixes(tmp_path: Path) -> None:
    with local_statement_sdk_client(tmp_path / "workspace") as (client, _path, context):
        warehouse_id = _warehouse_id(client)
        _seed_orders(context)

        row_limited = client.statement_execution.execute_statement(
            "SELECT order_id FROM orders ORDER BY order_id",
            warehouse_id,
            row_limit=2,
        )
        assert row_limited.status is not None
        assert row_limited.status.state is StatementState.SUCCEEDED
        assert row_limited.manifest is not None
        assert row_limited.manifest.total_row_count == 3
        assert row_limited.manifest.truncated is True
        assert row_limited.result is not None
        assert row_limited.result.data_array == [["1"], ["2"]]

        byte_limited = client.statement_execution.execute_statement(
            "SELECT order_id FROM orders ORDER BY order_id",
            warehouse_id,
            byte_limit=8,
        )
        assert byte_limited.status is not None
        assert byte_limited.status.state is StatementState.SUCCEEDED
        assert byte_limited.manifest is not None
        assert byte_limited.manifest.total_row_count == 3
        assert byte_limited.manifest.truncated is True
        assert byte_limited.result is not None
        assert byte_limited.result.data_array == [["1"]]


def test_statement_result_chunks_are_bounded_and_oldest_results_are_evicted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def execute(_service, _statement: str, **_kwargs: object) -> SqlExecutionResult:
        return SqlExecutionResult(
            operation=SqlOperation.SELECT,
            data=pa.table({"value": pa.array(range(1001), type=pa.int64())}),
        )

    monkeypatch.setattr(SqlStatementService, "execute", execute)
    with local_statement_sdk_client(tmp_path / "workspace") as (client, workspace_path, _context):
        warehouse_id = _warehouse_id(client)
        first = client.statement_execution.execute_statement("SELECT value", warehouse_id)
        assert first.statement_id is not None
        assert first.status is not None
        assert first.status.state is StatementState.SUCCEEDED
        assert first.manifest is not None
        assert first.manifest.total_chunk_count == 2
        assert first.result is not None
        assert len(first.result.data_array) == 1000
        second_chunk = client.statement_execution.get_statement_result_chunk_n(
            first.statement_id,
            1,
        )
        assert second_chunk.chunk_index == 1
        assert second_chunk.row_offset == 1000
        assert second_chunk.data_array == [["1000"]]

        with sqlite3.connect(workspace_path / "metadata.sqlite") as connection:
            retained_bytes = connection.execute(
                "SELECT stored_bytes FROM sql_statement_results WHERE statement_id = ?",
                (first.statement_id,),
            ).fetchone()[0]
        monkeypatch.setattr(
            "brickhouse.sql.statements.MAX_RETAINED_SQL_RESULT_BYTES",
            retained_bytes + max(1, retained_bytes // 2),
        )

        second = client.statement_execution.execute_statement("SELECT value", warehouse_id)
        assert second.statement_id is not None
        assert second.status is not None
        assert second.status.state is StatementState.SUCCEEDED
        closed = client.statement_execution.get_statement(first.statement_id)
        assert closed.status is not None
        assert closed.status.state is StatementState.CLOSED
        assert closed.manifest is None
        with pytest.raises(ResourceConflict):
            client.statement_execution.get_statement_result_chunk_n(first.statement_id, 0)


def test_oversized_inline_result_fails_without_retaining_partial_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def execute(_service, _statement: str, **_kwargs: object) -> SqlExecutionResult:
        return SqlExecutionResult(
            operation=SqlOperation.SELECT,
            data=pa.table({"value": ["x" * 32]}),
        )

    monkeypatch.setattr(SqlStatementService, "execute", execute)
    monkeypatch.setattr("brickhouse.sql.results.MAX_INLINE_RESULT_BYTES", 16)
    with local_statement_sdk_client(tmp_path / "workspace") as (client, workspace_path, _context):
        warehouse_id = _warehouse_id(client)
        result = client.statement_execution.execute_statement("SELECT value", warehouse_id)
        assert result.statement_id is not None
        assert result.status is not None
        assert result.status.state is StatementState.FAILED
        assert result.status.error is not None
        assert result.status.error.error_code is not None
        assert result.status.error.error_code.value == "RESOURCE_EXHAUSTED"
        assert result.manifest is None
        assert result.result is None
        with sqlite3.connect(workspace_path / "metadata.sqlite") as connection:
            assert connection.execute(
                "SELECT count(*) FROM sql_statement_results WHERE statement_id = ?",
                (result.statement_id,),
            ).fetchone() == (0,)


def test_expired_statement_result_closes_but_keeps_statement_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with local_statement_sdk_client(tmp_path / "workspace") as (client, workspace_path, context):
        warehouse_id = _warehouse_id(client)
        _seed_orders(context)
        result = client.statement_execution.execute_statement(
            "SELECT order_id FROM orders ORDER BY order_id",
            warehouse_id,
        )
        assert result.statement_id is not None
        monkeypatch.setattr(
            "brickhouse.sql.statements._utc_now",
            lambda: "2100-01-01T00:00:00+00:00",
        )

        closed = client.statement_execution.get_statement(result.statement_id)
        assert closed.status is not None
        assert closed.status.state is StatementState.CLOSED
        assert closed.result is None
        with sqlite3.connect(workspace_path / "metadata.sqlite") as connection:
            assert connection.execute(
                "SELECT state, sql_text FROM sql_statements WHERE statement_id = ?",
                (result.statement_id,),
            ).fetchone() == ("SUCCEEDED", "SELECT order_id FROM orders ORDER BY order_id")
        with pytest.raises(ResourceConflict):
            client.statement_execution.get_statement_result_chunk_n(result.statement_id, 0)


def test_v16_statement_results_migrate_to_bounded_v17_chunks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def execute(_service, _statement: str, **_kwargs: object) -> SqlExecutionResult:
        return SqlExecutionResult(
            operation=SqlOperation.SELECT,
            data=pa.table({"value": pa.array(range(1001), type=pa.int64())}),
        )

    monkeypatch.setattr(SqlStatementService, "execute", execute)
    workspace_path = tmp_path / "workspace"
    with local_statement_sdk_client(workspace_path) as (client, _path, _context):
        warehouse_id = _warehouse_id(client)
        result = client.statement_execution.execute_statement("SELECT value", warehouse_id)
        assert result.statement_id is not None

        with sqlite3.connect(workspace_path / "metadata.sqlite") as connection:
            (manifest_json,) = connection.execute(
                "SELECT manifest_json FROM sql_statement_results WHERE statement_id = ?",
                (result.statement_id,),
            ).fetchone()
            manifest = json.loads(manifest_json)
            rows: list[list[str | None]] = []
            for (chunk_json,) in connection.execute(
                "SELECT result_json FROM sql_statement_result_chunks "
                "WHERE statement_id = ? ORDER BY chunk_index",
                (result.statement_id,),
            ):
                rows.extend(json.loads(chunk_json)["data_array"])
            legacy_manifest = {
                **manifest,
                "chunks": [{"chunk_index": 0, "row_count": len(rows), "row_offset": 0}],
                "total_chunk_count": 1,
            }
            legacy_result = {
                "chunk_index": 0,
                "data_array": rows,
                "row_count": len(rows),
                "row_offset": 0,
            }
            connection.execute("PRAGMA foreign_keys = OFF")
            for trigger in (
                "job_attempt_outputs_are_immutable",
                "sql_statement_result_identity_is_immutable",
                "sql_statement_result_close_is_one_way",
                "sql_statement_result_chunk_update_is_forbidden",
                "sql_statement_result_chunk_delete_requires_close",
                "sql_terminal_statements_are_immutable",
                "sql_statement_identity_is_immutable",
            ):
                connection.execute(f'DROP TRIGGER "{trigger}"')
            connection.execute("DROP INDEX sql_statement_results_expiry_idx")
            connection.execute("DROP TABLE sql_statement_result_chunks")
            connection.execute("DROP TABLE sql_statement_results")
            connection.execute("ALTER TABLE job_task_attempt_outputs DROP COLUMN expired_at")
            connection.execute(
                "CREATE TRIGGER job_attempt_outputs_are_immutable "
                "BEFORE UPDATE ON job_task_attempt_outputs "
                "BEGIN SELECT RAISE(ABORT, 'job task attempt output is immutable'); END"
            )
            connection.execute(
                "UPDATE sql_statements SET manifest_json = ?, result_json = ? "
                "WHERE statement_id = ?",
                (
                    json.dumps(legacy_manifest, separators=(",", ":")),
                    json.dumps(legacy_result, separators=(",", ":")),
                    result.statement_id,
                ),
            )
            connection.execute("ALTER TABLE sql_statements DROP COLUMN byte_limit")
            connection.execute("ALTER TABLE sql_statements DROP COLUMN row_limit")
            connection.execute("DROP INDEX IF EXISTS job_runs_admission_state_idx")
            connection.execute("ALTER TABLE job_runs DROP COLUMN dispatched_at")
            connection.execute("ALTER TABLE job_runs DROP COLUMN admission_key")
            connection.execute("DELETE FROM schema_migrations WHERE version = 17")
            connection.execute("DELETE FROM schema_migrations WHERE version = 18")
            connection.execute("DELETE FROM schema_migrations WHERE version = 19")
            connection.execute("DELETE FROM schema_migrations WHERE version = 20")
            connection.execute("DELETE FROM schema_migrations WHERE version = 21")
            connection.execute("PRAGMA user_version = 16")
            connection.execute(
                "CREATE TRIGGER sql_statement_identity_is_immutable "
                "BEFORE UPDATE OF statement_id, warehouse_id, sql_text, parameters_json, "
                "catalog_name, schema_name, created_at ON sql_statements "
                "BEGIN SELECT RAISE(ABORT, 'SQL statement identity is immutable'); END"
            )
            connection.execute(
                "CREATE TRIGGER sql_terminal_statements_are_immutable "
                "BEFORE UPDATE ON sql_statements "
                "WHEN OLD.state IN ('SUCCEEDED', 'FAILED', 'CANCELED') "
                "BEGIN SELECT RAISE(ABORT, 'terminal SQL statements are immutable'); END"
            )
            connection.commit()
            connection.execute("PRAGMA foreign_keys = ON")

    with local_statement_sdk_client(workspace_path) as (upgraded_client, _path, _context):
        migrated = upgraded_client.statement_execution.get_statement(result.statement_id)
        assert migrated.status is not None
        assert migrated.status.state is StatementState.SUCCEEDED
        assert migrated.manifest is not None
        assert migrated.manifest.total_chunk_count == 2
        assert migrated.result is not None
        assert len(migrated.result.data_array) == 1000
        final_chunk = upgraded_client.statement_execution.get_statement_result_chunk_n(
            result.statement_id,
            1,
        )
        assert final_chunk.data_array == [["1000"]]


def test_sdk_statements_bind_to_all_persistent_warehouse_profiles(tmp_path: Path) -> None:
    with local_statement_sdk_client(tmp_path / "workspace") as (client, _path, context):
        _seed_orders(context)
        profiles = (
            ("serverless", CreateWarehouseRequestWarehouseType.PRO, True),
            ("pro", CreateWarehouseRequestWarehouseType.PRO, False),
            ("classic", CreateWarehouseRequestWarehouseType.CLASSIC, False),
        )
        for name, warehouse_type, serverless in profiles:
            warehouse = client.warehouses.create(
                name=name,
                warehouse_type=warehouse_type,
                enable_serverless_compute=serverless,
                cluster_size="2X-Small",
                auto_stop_mins=0,
            ).result()
            assert warehouse.id is not None
            result = client.statement_execution.execute_statement(
                "SELECT order_id FROM orders ORDER BY order_id",
                warehouse.id,
                catalog="main",
                schema="default",
            )
            assert result.status is not None
            assert result.status.state is StatementState.SUCCEEDED
            assert result.result is not None
            assert result.result.data_array == [["1"], ["2"], ["3"]]
            lease = context.compute.warehouses.get_admission(result.statement_id)
            assert lease.warehouse_id == warehouse.id
            assert lease.state == "RELEASED"


def test_sdk_statement_executes_named_typed_parameters_as_data(tmp_path: Path) -> None:
    payload = "x'); DROP TABLE orders; --"
    workspace_path = tmp_path / "workspace"
    with local_statement_sdk_client(workspace_path) as (client, _path, context):
        warehouse_id = _warehouse_id(client)
        _seed_orders(context)

        filtered = client.statement_execution.execute_statement(
            "SELECT order_id FROM orders "
            "WHERE amount > :minimum AND active = :active ORDER BY order_id",
            warehouse_id,
            parameters=[
                StatementParameterListItem(name="minimum", type="DECIMAL(12,2)", value="5.25"),
                StatementParameterListItem(name="active", type="BOOLEAN", value="true"),
            ],
        )
        assert filtered.status is not None
        assert filtered.status.state is StatementState.SUCCEEDED
        assert filtered.result is not None
        assert filtered.result.data_array == [["1"]]

        literal = client.statement_execution.execute_statement(
            "SELECT :payload AS value",
            warehouse_id,
            parameters=[StatementParameterListItem(name="payload", value=payload)],
        )
        assert literal.status is not None
        assert literal.status.state is StatementState.SUCCEEDED
        assert literal.result is not None
        assert literal.result.data_array == [[payload]]
        assert context.tables.get("main", "default", "orders").name == "orders"
        with sqlite3.connect(workspace_path / "metadata.sqlite") as connection:
            stored_sql, stored_parameters = connection.execute(
                "SELECT sql_text, parameters_json FROM sql_statements WHERE statement_id = ?",
                (literal.statement_id,),
            ).fetchone()
        assert stored_sql == "SELECT :payload AS value"
        assert json.loads(stored_parameters) == [
            {"name": "payload", "type": None, "value": payload}
        ]

        null_value = client.statement_execution.execute_statement(
            "SELECT :missing AS value",
            warehouse_id,
            parameters=[StatementParameterListItem(name="missing")],
        )
        assert null_value.status is not None
        assert null_value.status.state is StatementState.SUCCEEDED
        assert null_value.result is not None
        assert null_value.result.data_array == [[None]]

        typed_values = client.statement_execution.execute_statement(
            "SELECT :date_value AS date_value, :timestamp_value AS timestamp_value, "
            ":double_value AS double_value",
            warehouse_id,
            parameters=[
                StatementParameterListItem(name="date_value", type="DATE", value="2026-10-20"),
                StatementParameterListItem(
                    name="timestamp_value", type="TIMESTAMP", value="2026-10-20T11:30:00Z"
                ),
                StatementParameterListItem(name="double_value", type="DOUBLE", value="1.25"),
            ],
        )
        assert typed_values.status is not None
        assert typed_values.status.state is StatementState.SUCCEEDED
        assert typed_values.result is not None
        assert typed_values.result.data_array == [
            ["2026-10-20", "2026-10-20T11:30:00.000Z", "1.25"]
        ]

        durable_pending = SqlStatementExecutionService(context).submit(
            sql_text="SELECT :payload AS value",
            warehouse_id=warehouse_id,
            catalog=None,
            schema=None,
            asynchronous=True,
            parameters=(SqlStatementParameter(name="payload", value=payload),),
        )
        pending_id = str(durable_pending["statement_id"])

        pending = client.statement_execution.execute_statement(
            "SELECT :payload AS value",
            warehouse_id,
            wait_timeout="0s",
            parameters=[StatementParameterListItem(name="payload", value=payload)],
        )
        assert pending.statement_id is not None
        assert pending.status is not None
        assert pending.status.state is StatementState.PENDING

    with local_statement_sdk_client(workspace_path) as (reopened_client, _path, _context):
        restored = _get_until_terminal(reopened_client, pending_id)
        assert restored.status is not None
        assert restored.status.state is StatementState.SUCCEEDED
        assert restored.result is not None
        assert restored.result.data_array == [[payload]]


def test_sdk_statement_executes_named_parameter_insert_into_registered_delta_table(
    tmp_path: Path,
) -> None:
    with local_statement_sdk_client(tmp_path / "workspace") as (client, _path, context):
        warehouse_id = _warehouse_id(client)
        registration = context.tables.create_managed("main", "default", "insert_target")
        table = TableHandle.from_registration(registration)
        table_service = DeltaRsTableService(context)
        target_schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("label", pa.string(), nullable=False),
            ]
        )
        table_service.write_arrow(
            table,
            pa.Table.from_batches([], schema=target_schema),
            WriteMode.CREATE,
            operation_id="statement-api-insert-create",
        )

        result = client.statement_execution.execute_statement(
            "INSERT INTO insert_target VALUES (:id, :label)",
            warehouse_id,
            parameters=[
                StatementParameterListItem(name="id", type="BIGINT", value="11"),
                StatementParameterListItem(name="label", type="STRING", value="twelve"),
            ],
        )

        assert result.status is not None
        assert result.status.state is StatementState.SUCCEEDED
        assert table_service.read_table(table).data.to_pylist() == [{"id": 11, "label": "twelve"}]
        assert table_service.read_snapshot(table).version == 1


@pytest.mark.parametrize(
    ("statement", "parameters", "message"),
    [
        ("SELECT :missing AS value", [], "missing SQL parameter"),
        (
            "SELECT :value AS value",
            [
                StatementParameterListItem(name="value", value="x"),
                StatementParameterListItem(name="VALUE", value="y"),
            ],
            "duplicate",
        ),
        (
            "SELECT :value AS value",
            [StatementParameterListItem(name="value", value="x", type="ARRAY<INT>")],
            "unsupported",
        ),
        ("SELECT ? AS value", [], "positional"),
    ],
)
def test_invalid_sdk_parameters_fail_before_statement_creation(
    tmp_path: Path,
    statement: str,
    parameters: list[StatementParameterListItem],
    message: str,
) -> None:
    with local_statement_sdk_client(tmp_path / "workspace") as (client, workspace_path, _context):
        warehouse_id = _warehouse_id(client)
        with pytest.raises(InvalidParameterValue, match=message):
            client.statement_execution.execute_statement(
                statement,
                warehouse_id,
                parameters=parameters,
            )
        with sqlite3.connect(workspace_path / "metadata.sqlite") as connection:
            assert connection.execute("SELECT count(*) FROM sql_statements").fetchone()[0] == 0


def test_wait_timeout_validation_precedes_statement_admission(tmp_path: Path) -> None:
    with local_statement_sdk_client(tmp_path / "workspace") as (client, workspace_path, context):
        warehouse_id = _warehouse_id(client)
        _seed_orders(context)
        api = SqlStatementHttpApi(context)
        for wait_timeout in ("1s", "4s", "51s", "soon"):
            response = api.dispatch(
                "POST",
                "/api/2.0/sql/statements",
                {
                    "statement": "SELECT order_id FROM orders",
                    "warehouse_id": warehouse_id,
                    "wait_timeout": wait_timeout,
                },
            )
            assert response is not None
            assert response.status == 400

        unsupported = api.dispatch(
            "POST",
            "/api/2.0/sql/statements",
            {
                "statement": "SELECT order_id FROM orders",
                "warehouse_id": warehouse_id,
                "wait_timeout": "50s",
                "on_wait_timeout": "WAIT",
            },
        )
        assert unsupported is not None
        assert unsupported.status == 400

        cancellation = api.dispatch(
            "POST",
            "/api/2.0/sql/statements",
            {
                "statement": "SELECT order_id FROM orders",
                "warehouse_id": warehouse_id,
                "wait_timeout": "5s",
                "on_wait_timeout": "CANCEL",
            },
        )
        assert cancellation is not None
        assert cancellation.status == 200
        assert isinstance(cancellation.payload, dict)
        assert cancellation.payload["status"] == {"state": "SUCCEEDED"}
        with sqlite3.connect(workspace_path / "metadata.sqlite") as connection:
            assert connection.execute("SELECT count(*) FROM sql_statements").fetchone()[0] == 1
            assert (
                connection.execute(
                    "SELECT count(*) FROM sql_statement_admission_leases WHERE state = 'ACTIVE'"
                ).fetchone()[0]
                == 0
            )

        valid = api.dispatch(
            "POST",
            "/api/2.0/sql/statements",
            {
                "statement": "SELECT order_id FROM orders",
                "warehouse_id": warehouse_id,
                "wait_timeout": "50s",
                "on_wait_timeout": "CONTINUE",
            },
        )
        assert valid is not None
        assert valid.status == 200
        assert isinstance(valid.payload, dict)
        assert valid.payload["status"] == {"state": "SUCCEEDED"}
        api.close()


def test_wait_timeout_cancel_waits_for_safe_pre_execution_cancellation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from brickhouse.sql.service import SqlStatementService

    with local_statement_sdk_client(tmp_path / "workspace") as (client, _path, context):
        warehouse_id = _warehouse_id(client)
        entered = Event()
        active_statement: dict[str, str] = {}
        active_lease_during_cancel: list[str] = []
        original_execute = SqlStatementService.execute

        def wait_for_timeout_cancel(
            service: SqlStatementService,
            statement: str,
            *,
            operation_id: str | None = None,
            parameters=(),
            cancellation=None,
            row_limit: int | None = None,
            byte_limit: int | None = None,
        ):
            assert operation_id is not None
            statement_id = operation_id.removeprefix("statement-")
            active_statement["id"] = statement_id
            entered.set()
            deadline = time.monotonic() + 8
            while not cancellation.cancellation_requested:
                if time.monotonic() >= deadline:
                    raise TimeoutError("wait-timeout cancellation was not requested")
                time.sleep(0.005)
            active_lease_during_cancel.append(
                context.compute.warehouses.get_admission(statement_id).state
            )
            return original_execute(
                service,
                statement,
                operation_id=operation_id,
                parameters=parameters,
                cancellation=cancellation,
                row_limit=row_limit,
                byte_limit=byte_limit,
            )

        monkeypatch.setattr(SqlStatementService, "execute", wait_for_timeout_cancel)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                client.statement_execution.execute_statement,
                "SELECT 1 AS id",
                warehouse_id,
                wait_timeout="5s",
                on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CANCEL,
            )
            assert entered.wait(timeout=3)
            result = future.result(timeout=7)

        assert result.statement_id == active_statement["id"]
        assert result.status is not None
        assert result.status.state is StatementState.CANCELED
        assert active_lease_during_cancel == ["ACTIVE"]
        assert context.compute.warehouses.get_admission(result.statement_id).state == "RELEASED"


def test_wait_timeout_returns_running_then_continues_exactly_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with local_statement_sdk_client(tmp_path / "workspace") as (client, _path, context):
        warehouse_id = _warehouse_id(client)
        _seed_orders(context)
        entered = Event()
        resume = Event()
        executions: list[str] = []
        from brickhouse.sql.service import SqlStatementService

        original_execute = SqlStatementService.execute

        def pause_execute(
            service: SqlStatementService,
            statement: str,
            *,
            operation_id: str | None = None,
            parameters=(),
            cancellation=None,
            row_limit: int | None = None,
            byte_limit: int | None = None,
        ):
            executions.append(statement)
            entered.set()
            if not resume.wait(timeout=8):
                raise TimeoutError("test did not release the SQL execution barrier")
            return original_execute(
                service,
                statement,
                operation_id=operation_id,
                parameters=parameters,
                cancellation=cancellation,
                row_limit=row_limit,
                byte_limit=byte_limit,
            )

        monkeypatch.setattr(SqlStatementService, "execute", pause_execute)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                client.statement_execution.execute_statement,
                "SELECT order_id FROM orders",
                warehouse_id,
                wait_timeout="5s",
                on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
            )
            if not entered.wait(timeout=3):
                response = future.result(timeout=4)
                pytest.fail(f"statement did not enter the engine: {response!r}")
            try:
                timed_out = future.result(timeout=7)
                assert timed_out.statement_id is not None
                assert timed_out.status is not None
                assert timed_out.status.state is StatementState.RUNNING
            finally:
                resume.set()

        assert timed_out.statement_id is not None
        completed = _get_until_terminal(client, timed_out.statement_id)
        assert completed.status is not None
        assert completed.status.state is StatementState.SUCCEEDED
        assert len(executions) == 1


def test_queued_sdk_statement_dispatches_after_fifo_admission_promotion(tmp_path: Path) -> None:
    with local_statement_sdk_client(tmp_path / "workspace") as (client, _path, context):
        warehouse_id = _warehouse_id(client)
        _seed_orders(context)
        warehouses = context.compute.warehouses
        active = warehouses.admit_statement(warehouse_id, "concurrent-running-statement")
        assert active.is_active

        pending = client.statement_execution.execute_statement(
            "SELECT order_id FROM orders ORDER BY order_id",
            warehouse_id,
            wait_timeout="0s",
        )
        assert pending.statement_id is not None
        assert pending.status is not None
        assert pending.status.state is StatementState.PENDING
        lease = warehouses.get_admission(pending.statement_id)
        assert lease.is_queued

        warehouses.release_statement(active.statement_id)
        assert warehouses.get_admission(pending.statement_id).is_active
        completed = _get_until_terminal(client, pending.statement_id)
        assert completed.status is not None
        assert completed.status.state is StatementState.SUCCEEDED
        assert completed.result is not None
        assert completed.result.data_array == [["1"], ["2"], ["3"]]
        assert warehouses.get_admission(pending.statement_id).state == "RELEASED"
        assert warehouses.count_active_statements(warehouse_id) == 0


def test_cancel_queued_sdk_statement_releases_only_its_admission(tmp_path: Path) -> None:
    with local_statement_sdk_client(tmp_path / "workspace") as (client, _path, context):
        warehouse_id = _warehouse_id(client)
        warehouses = context.compute.warehouses
        active = warehouses.admit_statement(warehouse_id, "cancel-queued-active")
        pending = client.statement_execution.execute_statement(
            "SELECT 1 AS id",
            warehouse_id,
            wait_timeout="0s",
        )
        assert pending.statement_id is not None
        assert pending.status is not None
        assert pending.status.state is StatementState.PENDING
        assert warehouses.get_admission(pending.statement_id).is_queued

        client.statement_execution.cancel_execution(pending.statement_id)

        canceled = client.statement_execution.get_statement(pending.statement_id)
        assert canceled.status is not None
        assert canceled.status.state is StatementState.CANCELED
        assert canceled.result is None
        assert warehouses.get_admission(pending.statement_id).state == "RELEASED"
        assert warehouses.get_admission(active.statement_id).state == "ACTIVE"
        warehouses.release_statement(active.statement_id)


@pytest.mark.parametrize("cancel_by", ["client", "warehouse_stop"])
def test_cancel_active_sparkless_statement_waits_for_engine_terminal_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cancel_by: str,
) -> None:
    from brickhouse.engines.sparkless import (
        _SparklessDataFrameFacade,
        _SparklessSessionFacade,
    )

    with local_statement_sdk_client(tmp_path / "workspace") as (client, _path, context):
        warehouse_id = _warehouse_id(client)
        _seed_orders(context)
        warehouses = context.compute.warehouses
        action_entered = Event()
        original_to_batches = _SparklessDataFrameFacade.toArrowBatches

        def large_range(self, query: str, *args, **kwargs):
            self._owner._require_open_owner()
            return _SparklessDataFrameFacade(
                self._owner._native_session.range(10_000_000),
                self._owner,
            )

        def signal_action(self, *args, **kwargs):
            action_entered.set()
            return original_to_batches(self, *args, **kwargs)

        monkeypatch.setattr(_SparklessSessionFacade, "sql", large_range)
        monkeypatch.setattr(_SparklessDataFrameFacade, "toArrowBatches", signal_action)
        pending = client.statement_execution.execute_statement(
            "SELECT order_id FROM orders",
            warehouse_id,
            wait_timeout="0s",
        )
        assert pending.statement_id is not None
        assert action_entered.wait(timeout=5)

        if cancel_by == "client":
            client.statement_execution.cancel_execution(pending.statement_id)
        else:
            client.warehouses.stop(warehouse_id).result()

        terminal = _get_until_terminal(client, pending.statement_id, timeout_seconds=8)
        assert terminal.status is not None
        assert terminal.status.state is StatementState.CANCELED
        assert terminal.status.error is not None
        assert terminal.status.error.error_code.value == "CANCELLED"
        assert terminal.result is None
        assert warehouses.get_admission(pending.statement_id).state == "RELEASED"
        assert warehouses.count_active_statements(warehouse_id) == 0


def test_cancel_after_select_finishes_reports_the_real_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from brickhouse.sql.service import SqlStatementService

    with local_statement_sdk_client(tmp_path / "workspace") as (client, _path, context):
        warehouse_id = _warehouse_id(client)
        _seed_orders(context)
        action_finished = Event()
        resume = Event()
        original_execute = SqlStatementService.execute

        def finish_then_pause(
            service: SqlStatementService,
            statement: str,
            *,
            operation_id: str | None = None,
            parameters=(),
            cancellation=None,
            row_limit: int | None = None,
            byte_limit: int | None = None,
        ):
            result = original_execute(
                service,
                statement,
                operation_id=operation_id,
                parameters=parameters,
                cancellation=cancellation,
                row_limit=row_limit,
                byte_limit=byte_limit,
            )
            action_finished.set()
            if not resume.wait(timeout=5):
                raise TimeoutError("test did not release the completed SQL action")
            return result

        monkeypatch.setattr(SqlStatementService, "execute", finish_then_pause)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                client.statement_execution.execute_statement,
                "SELECT order_id FROM orders ORDER BY order_id",
                warehouse_id,
            )
            assert action_finished.wait(timeout=5)
            with sqlite3.connect(context.workspace.metadata_path) as connection:
                row = connection.execute(
                    "SELECT statement_id FROM sql_statements WHERE state = 'RUNNING'"
                ).fetchone()
            assert row is not None
            statement_id = str(row[0])
            assert context.compute.warehouses.get_admission(statement_id).state == "ACTIVE"
            client.statement_execution.cancel_execution(statement_id)
            assert context.compute.warehouses.get_admission(statement_id).state == "ACTIVE"
            resume.set()
            result = future.result(timeout=5)

        assert result.status is not None
        assert result.status.state is StatementState.SUCCEEDED
        assert result.result is not None
        assert result.result.data_array == [["1"], ["2"], ["3"]]


def test_cancel_after_ctas_commit_preserves_committed_write_and_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with local_statement_sdk_client(tmp_path / "workspace") as (client, _path, context):
        warehouse_id = _warehouse_id(client)
        _seed_orders(context)
        committed = Event()
        resume = Event()
        original_write = DeltaRsTableService.write_arrow

        def commit_then_pause(
            service: DeltaRsTableService,
            *args,
            **kwargs,
        ):
            result = original_write(service, *args, **kwargs)
            committed.set()
            if not resume.wait(timeout=5):
                raise TimeoutError("test did not release the committed CTAS")
            return result

        monkeypatch.setattr(DeltaRsTableService, "write_arrow", commit_then_pause)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                client.statement_execution.execute_statement,
                "CREATE TABLE copied_orders USING DELTA AS SELECT order_id FROM orders",
                warehouse_id,
            )
            assert committed.wait(timeout=5)
            with sqlite3.connect(context.workspace.metadata_path) as connection:
                statement_id = str(
                    connection.execute(
                        "SELECT statement_id FROM sql_statements WHERE state = 'RUNNING'"
                    ).fetchone()[0]
                )
            assert context.compute.warehouses.get_admission(statement_id).state == "ACTIVE"
            client.statement_execution.cancel_execution(statement_id)
            assert context.compute.warehouses.get_admission(statement_id).state == "ACTIVE"
            resume.set()
            result = future.result(timeout=5)

        assert result.status is not None
        assert result.status.state is StatementState.SUCCEEDED
        registration = context.tables.get("main", "default", "copied_orders")
        copied = DeltaRsTableService(context).read_table(
            TableHandle.from_registration(registration)
        )
        assert copied.snapshot.version == 0
        assert copied.data.to_pydict() == {"order_id": [1, 2, 3]}
        assert context.compute.warehouses.get_admission(statement_id).state == "RELEASED"


def test_real_sdk_statement_waits_for_workspace_wide_admission_slot(tmp_path: Path) -> None:
    with local_statement_sdk_client(tmp_path / "workspace") as (client, _path, context):
        first = _warehouse_id(client)
        second_record = client.warehouses.create(
            name="second-warehouse",
            warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
            enable_serverless_compute=False,
            cluster_size="2X-Small",
            auto_stop_mins=0,
        ).result()
        third_record = client.warehouses.create(
            name="third-warehouse",
            warehouse_type=CreateWarehouseRequestWarehouseType.CLASSIC,
            cluster_size="2X-Small",
            auto_stop_mins=0,
        ).result()
        assert second_record.id is not None
        assert third_record.id is not None
        _seed_orders(context)

        warehouses = context.compute.warehouses
        first_active = warehouses.admit_statement(first, "workspace-active-1")
        second_active = warehouses.admit_statement(second_record.id, "workspace-active-2")
        queued = client.statement_execution.execute_statement(
            "SELECT order_id FROM orders ORDER BY order_id",
            third_record.id,
            wait_timeout="0s",
        )
        assert queued.statement_id is not None
        assert queued.status is not None
        assert queued.status.state is StatementState.PENDING
        assert warehouses.get_admission(queued.statement_id).is_queued

        warehouses.release_statement(first_active.statement_id)
        assert warehouses.get_admission(queued.statement_id).is_active
        result = _get_until_terminal(client, queued.statement_id)
        assert result.status is not None
        assert result.status.state is StatementState.SUCCEEDED
        assert result.result is not None
        assert result.result.data_array == [["1"], ["2"], ["3"]]
        assert warehouses.get_admission(queued.statement_id).state == "RELEASED"
        warehouses.release_statement(second_active.statement_id)


def test_sdk_overload_fails_without_dispatching_engine_work(tmp_path: Path) -> None:
    with local_statement_sdk_client(tmp_path / "workspace") as (client, workspace_path, context):
        warehouse_id = _warehouse_id(client)
        warehouses = context.compute.warehouses
        warehouses.admit_statement(warehouse_id, "overload-active")
        for index in range(MAX_QUEUED_STATEMENTS_PER_WAREHOUSE):
            warehouses.admit_statement(warehouse_id, f"overload-queued-{index}")

        result = client.statement_execution.execute_statement(
            "SELECT * FROM missing_table",
            warehouse_id,
            wait_timeout="0s",
        )
        assert result.statement_id is not None
        assert result.status is not None
        assert result.status.state is StatementState.FAILED
        assert result.status.error is not None
        with sqlite3.connect(workspace_path / "metadata.sqlite") as connection:
            state, error_code = connection.execute(
                "SELECT state, error_code FROM sql_statements WHERE statement_id = ?",
                (result.statement_id,),
            ).fetchone()
        assert (state, error_code) == ("FAILED", "RESOURCE_EXHAUSTED")


def test_interrupted_running_statement_fails_and_releases_admission(tmp_path: Path) -> None:
    from brickhouse.sql.statements import SqlStatementExecutionService

    with local_statement_sdk_client(tmp_path / "workspace") as (client, _path, context):
        warehouse_id = _warehouse_id(client)
        service = SqlStatementExecutionService(context)
        response = service.submit(
            sql_text="SELECT 1",
            warehouse_id=warehouse_id,
            catalog=None,
            schema=None,
            asynchronous=True,
        )
        statement_id = str(response["statement_id"])
        _record, claimed = service._repository.claim(statement_id)
        assert claimed
        assert context.compute.warehouses.get_admission(statement_id).is_active

        reopened_service = SqlStatementExecutionService(context)
        terminal = reopened_service.get(statement_id)
        assert terminal["status"]["state"] == "FAILED"
        assert context.compute.warehouses.get_admission(statement_id).state == "RELEASED"


def test_missing_stopped_and_deleted_warehouse_ids_reject_before_statement_creation(
    tmp_path: Path,
) -> None:
    with local_statement_sdk_client(tmp_path / "workspace") as (client, workspace_path, _context):
        active = _warehouse_id(client, name="active")
        stopped = _warehouse_id(client, name="stopped")
        deleted = _warehouse_id(client, name="deleted")
        client.warehouses.stop(stopped).result()
        client.warehouses.delete(deleted)

        for invalid_id, error_type in (
            ("missing-warehouse", ResourceDoesNotExist),
            (stopped, ResourceConflict),
            (deleted, ResourceDoesNotExist),
        ):
            with pytest.raises(error_type):
                client.statement_execution.execute_statement("SELECT 1", invalid_id)

        with sqlite3.connect(workspace_path / "metadata.sqlite") as connection:
            count = connection.execute("SELECT count(*) FROM sql_statements").fetchone()[0]
            active_count = connection.execute(
                "SELECT count(*) FROM sql_statement_admission_leases "
                "WHERE warehouse_id = ? AND state = 'ACTIVE'",
                (active,),
            ).fetchone()[0]
        assert count == 0
        assert active_count == 0


def test_stopping_warehouse_during_real_query_cancels_result_and_keeps_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with local_statement_sdk_client(tmp_path / "workspace") as (client, _path, context):
        warehouse_id = _warehouse_id(client)
        warehouses = context.compute.warehouses
        _seed_orders(context)
        table = context.tables.get("main", "default", "orders")
        table_handle = TableHandle.from_registration(table)
        delta = DeltaRsTableService(context)
        committed_version = delta.read_snapshot(table_handle).version

        entered = Event()
        resume = Event()
        active_statement: dict[str, str] = {}
        from brickhouse.sql.service import SqlStatementService

        original_execute = SqlStatementService.execute

        def finish_then_pause(
            service: SqlStatementService,
            statement: str,
            *,
            operation_id: str | None = None,
            parameters=(),
            cancellation=None,
            row_limit: int | None = None,
            byte_limit: int | None = None,
        ):
            result = original_execute(
                service,
                statement,
                operation_id=operation_id,
                parameters=parameters,
                cancellation=cancellation,
                row_limit=row_limit,
                byte_limit=byte_limit,
            )
            assert operation_id is not None
            active_statement["id"] = operation_id.removeprefix("statement-")
            entered.set()
            if not resume.wait(timeout=5):
                raise TimeoutError("test did not release the SQL execution barrier")
            return result

        monkeypatch.setattr(SqlStatementService, "execute", finish_then_pause)
        stop_client = WorkspaceClient(
            host=client.config.host,
            token="brickhouse-local-test-token",
            auth_type="pat",
        )
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                client.statement_execution.execute_statement,
                "SELECT count(*) AS total FROM orders",
                warehouse_id,
            )
            assert entered.wait(timeout=5)
            stop_client.warehouses.stop(warehouse_id).result()
            assert warehouses.get_admission(active_statement["id"]).state == "ACTIVE"
            resume.set()
            canceled = future.result(timeout=5)

        assert canceled.status is not None
        assert canceled.status.state is StatementState.CANCELED
        assert canceled.result is None
        assert warehouses.get_admission(canceled.statement_id).state == "RELEASED"
        assert delta.read_snapshot(table_handle).version == committed_version
        retained = delta.read_table(table_handle)
        assert retained.snapshot.version == committed_version
        assert retained.data.to_pydict()["order_id"] == [1, 2, 3]


def test_connector_session_lifecycle_binds_warehouse_namespace_and_limits(tmp_path: Path) -> None:
    with local_statement_sdk_client(tmp_path / "workspace") as (client, workspace_path, context):
        warehouse_id = _warehouse_id(client)
        context.catalogs.create("analytics")
        context.schemas.create("analytics", "dashboards")
        api = SqlStatementHttpApi(context)

        valid = api.dispatch(
            "POST",
            "/api/2.0/sql/sessions",
            {"warehouse_id": warehouse_id},
        )
        assert valid is not None
        assert valid.status == 200
        assert isinstance(valid.payload, dict)
        default_session_id = valid.payload["session_id"]
        default_session = api._sessions.get(default_session_id, warehouse_id=warehouse_id)
        assert default_session.catalog == "main"
        assert default_session.schema == "default"
        assert default_session.execution_mode == "FAST"

        analytics = api.dispatch(
            "POST",
            "/api/2.0/sql/sessions",
            {
                "execution_mode": "FAST",
                "warehouse_id": warehouse_id,
                "catalog": "analytics",
                "schema": "dashboards",
            },
        )
        assert analytics is not None and analytics.status == 200
        assert isinstance(analytics.payload, dict)
        analytics_session_id = analytics.payload["session_id"]
        analytics_session = api._sessions.get(analytics_session_id, warehouse_id=warehouse_id)
        assert analytics_session.catalog == "analytics"
        assert analytics_session.schema == "dashboards"
        assert default_session.catalog == "main"
        assert default_session.schema == "default"
        assert default_session_id != analytics_session_id

        statement = api.dispatch(
            "POST",
            "/api/2.0/sql/statements",
            {
                "warehouse_id": warehouse_id,
                "session_id": analytics_session_id,
                "statement": "SELECT :answer AS answer",
                "parameters": [{"name": "answer", "type": "INT", "value": "42"}],
                "disposition": "INLINE",
                "format": "JSON_ARRAY",
            },
        )
        assert statement is not None and statement.status == 200
        assert isinstance(statement.payload, dict)
        statement_id = statement.payload["statement_id"]
        assert statement.payload["status"] == {"state": "SUCCEEDED"}
        assert statement.payload["manifest"]["schema"]["columns"][0]["name"] == "answer"
        assert statement.payload["result"]["data_array"] == [["42"]]
        with sqlite3.connect(workspace_path / "metadata.sqlite") as connection:
            assert connection.execute(
                "SELECT catalog_name, schema_name FROM sql_statements WHERE statement_id = ?",
                (statement_id,),
            ).fetchone() == ("analytics", "dashboards")

        wrong_warehouse_statement = api.dispatch(
            "POST",
            "/api/2.0/sql/statements",
            {
                "warehouse_id": "other-warehouse",
                "session_id": analytics_session_id,
                "statement": "SELECT 1",
            },
        )
        assert wrong_warehouse_statement is not None
        assert wrong_warehouse_statement.status == 400
        unknown_session_statement = api.dispatch(
            "POST",
            "/api/2.0/sql/statements",
            {
                "warehouse_id": warehouse_id,
                "session_id": "not-a-live-session",
                "statement": "SELECT 1",
            },
        )
        assert unknown_session_statement is not None
        assert unknown_session_statement.status == 404

        closed_statement = api.dispatch(
            "DELETE",
            f"/api/2.0/sql/statements/{statement_id}",
            {"statement_id": statement_id},
        )
        assert closed_statement is not None and closed_statement.status == 200
        closed_statement_state = api.dispatch("GET", f"/api/2.0/sql/statements/{statement_id}")
        assert closed_statement_state is not None
        assert closed_statement_state.payload == {
            "statement_id": statement_id,
            "status": {"state": "CLOSED"},
        }

        unsupported_session_configuration = api.dispatch(
            "POST",
            "/api/2.0/sql/sessions",
            {
                "execution_mode": "FAST",
                "warehouse_id": warehouse_id,
                "session_confs": {"spark.sql.ansi.enabled": "false"},
            },
        )
        assert unsupported_session_configuration is not None
        assert unsupported_session_configuration.status == 501
        assert isinstance(unsupported_session_configuration.payload, dict)
        assert unsupported_session_configuration.payload["error_code"] == "UNSUPPORTED_OPERATION"

        missing = api.dispatch(
            "POST",
            "/api/2.0/sql/sessions",
            {"execution_mode": "FAST", "warehouse_id": "missing-warehouse"},
        )
        assert missing is not None
        assert missing.status == 404

        mismatched_warehouse = api.dispatch(
            "DELETE",
            f"/api/2.0/sql/sessions/{default_session_id}",
            {"session_id": default_session_id, "warehouse_id": "other-warehouse"},
        )
        assert mismatched_warehouse is not None
        assert mismatched_warehouse.status == 400
        api._sessions.get(default_session_id, warehouse_id=warehouse_id)

        closed = api.dispatch(
            "DELETE",
            f"/api/2.0/sql/sessions/{default_session_id}",
            {"session_id": default_session_id, "warehouse_id": warehouse_id},
        )
        assert closed is not None and closed.status == 200
        unknown_after_close = api.dispatch(
            "DELETE",
            f"/api/2.0/sql/sessions/{default_session_id}",
            {"session_id": default_session_id, "warehouse_id": warehouse_id},
        )
        assert unknown_after_close is not None and unknown_after_close.status == 404

        client.warehouses.stop(warehouse_id).result()
        stopped = api.dispatch(
            "POST",
            "/api/2.0/sql/sessions",
            {"execution_mode": "FAST", "warehouse_id": warehouse_id},
        )
        assert stopped is not None
        assert stopped.status == 409
        api.close()


def test_sdk_statement_update_commits_and_a_followup_statement_reads_it(tmp_path: Path) -> None:
    with local_statement_sdk_client(tmp_path / "workspace") as (client, _path, context):
        warehouse_id = _warehouse_id(client, name="mutation-statement-tests")
        _seed_orders(context)

        updated = client.statement_execution.execute_statement(
            "UPDATE orders SET active = false WHERE order_id = 1",
            warehouse_id,
            catalog="main",
            schema="default",
        )
        assert updated.statement_id is not None
        assert updated.status is not None
        assert updated.status.state is StatementState.SUCCEEDED

        visible = client.statement_execution.execute_statement(
            "SELECT order_id, active FROM orders WHERE order_id = 1",
            warehouse_id,
            catalog="main",
            schema="default",
        )
        assert visible.status is not None and visible.status.state is StatementState.SUCCEEDED
        assert visible.result is not None
        assert visible.result.data_array == [["1", "false"]]


@pytest.mark.parametrize("outcome", ["SUCCEEDED", "FAILED", "CANCELED"])
def test_terminal_statement_is_not_published_before_admission_release(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, outcome: str
) -> None:
    with local_statement_sdk_client(tmp_path / "workspace") as (client, _path, context):
        warehouse_id = _warehouse_id(client)
        query_entered, allow_query = Event(), Event()
        release_entered, allow_release = Event(), Event()
        warehouses = context.compute.warehouses
        execute = SqlStatementService.execute
        release = warehouses.release_statement

        def pause_query(self, *args, **kwargs):
            query_entered.set()
            assert allow_query.wait(8), "test did not release the real SQL execution"
            return execute(self, *args, **kwargs)

        def pause_release(statement_id):
            release_entered.set()
            assert allow_release.wait(8), "test did not release the admission barrier"
            return release(statement_id)

        monkeypatch.setattr(SqlStatementService, "execute", pause_query)
        monkeypatch.setattr(warehouses, "release_statement", pause_release)
        pending = client.statement_execution.execute_statement(
            "SELECT * FROM missing_table" if outcome == "FAILED" else "SELECT 1 AS n",
            warehouse_id,
            wait_timeout="0s",
        )
        assert pending.statement_id is not None
        try:
            assert query_entered.wait(5)
            if outcome == "CANCELED":
                client.warehouses.stop(warehouse_id).result()
            allow_query.set()
            assert release_entered.wait(5)
            visible = client.statement_execution.get_statement(pending.statement_id)
            assert visible.status is not None
            assert visible.status.state is StatementState.RUNNING
            assert warehouses.get_admission(pending.statement_id).state == "ACTIVE"
        finally:
            allow_query.set()
            allow_release.set()
        terminal = _get_until_terminal(client, pending.statement_id)
        assert terminal.status is not None
        assert terminal.status.state.value == outcome
        assert warehouses.get_admission(pending.statement_id).state == "RELEASED"
