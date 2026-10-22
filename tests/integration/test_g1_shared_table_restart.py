from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from threading import Thread

import pyarrow as pa
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    ColumnInfoTypeName,
    CreateWarehouseRequestWarehouseType,
    StatementState,
)

from brickhouse.diagnostic_server import create_diagnostic_server
from brickhouse.engines.sparkless import SparklessSessionFactory
from brickhouse.tables import DeltaRsTableService, TableHandle
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace, open_workspace


@contextmanager
def _local_sdk_service(
    workspace_path: Path,
) -> Iterator[tuple[WorkspaceClient, WorkspaceContext]]:
    if not workspace_path.exists():
        initialize_workspace(workspace_path)
    context = WorkspaceContext.local(open_workspace(workspace_path).path)
    context.__enter__()
    server = create_diagnostic_server(context, "127.0.0.1", 0)
    thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    client = WorkspaceClient(
        host=f"http://127.0.0.1:{server.server_port}",
        token="brickhouse-g1-local-test-token",
        auth_type="pat",
    )
    try:
        yield client, context
    finally:
        server.shutdown()
        thread.join(timeout=5)
        assert not thread.is_alive()
        server.server_close()
        context.close()


def _query_orders(client: WorkspaceClient, *, warehouse_id: str):
    result = client.statement_execution.execute_statement(
        "SELECT order_id, amount, active FROM orders ORDER BY order_id",
        warehouse_id,
        catalog="main",
        schema="default",
    )
    assert result.statement_id is not None
    assert result.status is not None
    assert result.status.state is StatementState.SUCCEEDED
    assert result.manifest is not None
    assert result.manifest.schema is not None
    assert result.manifest.schema.columns is not None
    assert [column.name for column in result.manifest.schema.columns] == [
        "order_id",
        "amount",
        "active",
    ]
    assert [column.type_name for column in result.manifest.schema.columns] == [
        ColumnInfoTypeName.LONG,
        ColumnInfoTypeName.DECIMAL,
        ColumnInfoTypeName.BOOLEAN,
    ]
    assert result.result is not None
    return result


def test_g1_sparkless_delta_sdk_and_fresh_session_survive_service_restart(
    tmp_path: Path,
) -> None:
    from sparkless.sql import types as T

    workspace_path = tmp_path / "workspace"
    input_data = pa.table(
        {
            "order_id": pa.array([1, 2, 3], type=pa.int64()),
            "amount": pa.array(
                [Decimal("10.50"), Decimal("5.25"), None],
                type=pa.decimal128(12, 2),
            ),
            "active": pa.array([True, True, False], type=pa.bool_()),
        }
    )
    expected_rows = [
        ["1", "10.50", "true"],
        ["2", "5.25", "true"],
        ["3", None, "false"],
    ]
    dataframe_schema = T.StructType(
        [
            T.StructField("order_id", T.LongType(), False),
            T.StructField("amount", T.DecimalType(12, 2), True),
            T.StructField("active", T.BooleanType(), False),
        ]
    )

    with _local_sdk_service(workspace_path) as (client, context):
        warehouse = client.warehouses.create(
            name="g1-shared-orders",
            warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
            enable_serverless_compute=False,
            cluster_size="2X-Small",
            auto_stop_mins=0,
        ).result()
        assert warehouse.id is not None

        with SparklessSessionFactory(context).open(app_name="g1-writer") as writer:
            commit = writer.create_managed_table(
                "main.default.orders",
                writer.spark.createDataFrame(input_data, dataframe_schema),
                operation_id="g1-orders-create",
            )
            assert commit.version == 0

        registration = context.tables.get("main", "default", "orders")
        table_id = registration.table_id
        table_location = registration.canonical_location
        before_restart = _query_orders(client, warehouse_id=warehouse.id)
        assert before_restart.result.data_array == expected_rows

        with SparklessSessionFactory(context).open(app_name="g1-fresh-reader") as reader:
            fresh_arrow = reader.table("main.default.orders").sort("order_id").toArrow()
            assert fresh_arrow.schema == input_data.schema
            assert fresh_arrow.to_pylist() == input_data.to_pylist()

        statement_id = before_restart.statement_id

    # Closing the loopback server and workspace context, then reacquiring both,
    # exercises the persisted catalog, Delta snapshot, and SQL result stores.
    with _local_sdk_service(workspace_path) as (restarted_client, restarted_context):
        assert restarted_context.tables.get("main", "default", "orders").table_id == table_id
        reopened_registration = restarted_context.tables.get("main", "default", "orders")
        assert reopened_registration.canonical_location == table_location

        old_statement = restarted_client.statement_execution.get_statement(statement_id)
        assert old_statement.status is not None
        assert old_statement.status.state is StatementState.SUCCEEDED
        assert old_statement.manifest is not None
        assert old_statement.manifest.schema is not None
        assert old_statement.result is not None
        assert old_statement.result.data_array == expected_rows

        restarted_warehouse = restarted_client.warehouses.create(
            name="g1-after-restart",
            warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
            enable_serverless_compute=False,
            cluster_size="2X-Small",
            auto_stop_mins=0,
        ).result()
        assert restarted_warehouse.id is not None
        after_restart = _query_orders(
            restarted_client,
            warehouse_id=restarted_warehouse.id,
        )
        assert after_restart.result.data_array == expected_rows
        assert after_restart.manifest is not None
        assert after_restart.manifest.schema == old_statement.manifest.schema

        with SparklessSessionFactory(restarted_context).open(
            app_name="g1-post-restart-reader"
        ) as reader:
            restarted_rows = reader.table("main.default.orders").sort("order_id").collect()
            assert restarted_rows == [
                (1, Decimal("10.50"), True),
                (2, Decimal("5.25"), True),
                (3, None, False),
            ]

        readback = DeltaRsTableService(restarted_context).read_table(
            TableHandle.from_registration(restarted_context.tables.get("main", "default", "orders"))
        )
        assert readback.snapshot.table.table_id == table_id
        assert readback.snapshot.version == 0
        assert readback.data.schema == input_data.schema
        assert readback.data.to_pylist() == input_data.to_pylist()
