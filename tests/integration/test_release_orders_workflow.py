from __future__ import annotations

import io
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from threading import Thread
from typing import Any

import pyarrow as pa
import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterSpec, DataSecurityMode, Environment
from databricks.sdk.service.jobs import (
    JobCluster,
    JobEnvironment,
    NotebookTask,
    RunResultState,
    SparkPythonTask,
    Task,
    TaskDependency,
)
from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType, StatementState
from databricks.sdk.service.workspace import ImportFormat, Language

from brickhouse.catalog import parse_identifier
from brickhouse.diagnostic_server import create_diagnostic_server
from brickhouse.engines.sparkless import SparklessSessionFactory
from brickhouse.tables import DeltaRsTableService, WriteMode
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace

ROOT = Path(__file__).resolve().parents[2]
VARIANTS = ("SL", "CSN", "CSE", "CSH", "CDN", "CDE", "CDH")
SCHEMA = pa.schema(
    [
        ("customer", pa.string()),
        ("order_count", pa.int64()),
        ("total_amount", pa.decimal128(22, 2)),
    ]
)
EXPECTED = [{"customer": "alice", "order_count": 2, "total_amount": Decimal("15.75")}]
SECRET = "synthetic-release-workflow-secret"


@contextmanager
def _service(path: Path) -> Iterator[tuple[WorkspaceClient, WorkspaceContext]]:
    if not path.exists():
        initialize_workspace(path)
    with WorkspaceContext.local(path) as context:
        server = create_diagnostic_server(context, "127.0.0.1", 0)
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()
        try:
            yield (
                WorkspaceClient(
                    host=f"http://127.0.0.1:{server.server_port}",
                    token="brickhouse-release-test-token",
                    auth_type="pat",
                ),
                context,
            )
        finally:
            server.shutdown()
            thread.join(timeout=5)
            server.server_close()
            assert not thread.is_alive()


def _assert_committed_data(
    client: WorkspaceClient, context: WorkspaceContext, warehouses: list[str]
) -> tuple[str, str]:
    service = DeltaRsTableService(context)
    table = service.resolve(
        parse_identifier("main.default.release_orders"), context.new_namespace_state()
    )
    committed = service.read_table(table)
    assert committed.snapshot.version == 1
    assert committed.data.schema == SCHEMA
    assert committed.data.to_pylist() == EXPECTED
    with SparklessSessionFactory(context).open() as fresh:
        data = fresh.table("main.default.release_orders").toArrow()
        assert data.schema == SCHEMA
        assert data.to_pylist() == EXPECTED
    for warehouse_id in warehouses:
        result = client.statement_execution.execute_statement(
            "SELECT customer, order_count, total_amount FROM main.default.release_orders",
            warehouse_id,
            wait_timeout="30s",
        )
        assert result.status is not None and result.status.state is StatementState.SUCCEEDED
        assert result.manifest is not None and result.manifest.schema is not None
        assert result.manifest.schema.columns is not None
        assert [(c.name, c.type_text) for c in result.manifest.schema.columns] == [
            ("customer", "STRING"),
            ("order_count", "BIGINT"),
            ("total_amount", "DECIMAL(22,2)"),
        ]
        assert result.result is not None
        assert result.result.data_array == [["alice", "2", "15.75"]]
    return table.table_id, str(table.location)


def _run_workflow(tmp_path: Path, variant: str, kind: str) -> None:
    workspace = tmp_path / "workspace"
    source = (ROOT / "tests/fixtures/release/orders_workload.py").read_text()
    if kind == "ipynb":
        nbformat = pytest.importorskip("nbformat")
        payload = nbformat.writes(
            nbformat.v4.new_notebook(cells=[nbformat.v4.new_code_cell(source)])
        )
        filename, file_format = "orders.ipynb", ImportFormat.JUPYTER
    else:
        payload = source
        filename, file_format = "orders.py", ImportFormat.SOURCE
        if kind == "source":
            payload = "# Databricks notebook source\n# COMMAND ----------\n" + payload

    with _service(workspace) as (client, context):
        context.tables.create_managed("main", "default", "release_orders")
        service = DeltaRsTableService(context)
        table = service.resolve(
            parse_identifier("main.default.release_orders"), context.new_namespace_state()
        )
        service.write_arrow(
            table,
            pa.Table.from_pylist([], schema=SCHEMA),
            WriteMode.CREATE,
            operation_id="release-orders-seed",
        )
        client.workspace.upload(
            f"/Workspace/Shared/{filename}",
            io.BytesIO(payload.encode()),
            format=file_format,
            language=Language.PYTHON,
            overwrite=True,
        )
        dedicated = variant.startswith("CD")
        security = (
            DataSecurityMode.DATA_SECURITY_MODE_DEDICATED
            if dedicated
            else DataSecurityMode.DATA_SECURITY_MODE_STANDARD
        )
        cluster = ClusterSpec(
            spark_version="spark35-local-v1",
            data_security_mode=security,
            single_user_name="brickhouse-release-owner" if dedicated else None,
            spark_conf={"spark.sql.ansi.enabled": "true", "spark.sql.session.timeZone": "UTC"},
            num_workers=0,
            autotermination_minutes=0,
        )
        options: dict[str, Any] = {}
        task_options: dict[str, Any] = {}
        existing_id: str | None = None
        if variant == "SL":
            options["environments"] = [
                JobEnvironment(
                    environment_key="local-python",
                    spec=Environment(environment_version="1", dependencies=[]),
                )
            ]
            task_options = {"environment_key": "local-python", "disable_auto_optimization": True}
        elif variant.endswith("E"):
            existing = client.clusters.create(
                "spark35-local-v1",
                cluster_name="release-existing",
                data_security_mode=security,
                single_user_name=cluster.single_user_name,
                spark_conf=cluster.spark_conf,
                num_workers=0,
                autotermination_minutes=0,
            ).result()
            existing_id = existing.cluster_id
            task_options["existing_cluster_id"] = existing_id
        elif variant.endswith("H"):
            options["job_clusters"] = [JobCluster(job_cluster_key="shared", new_cluster=cluster)]
            task_options["job_cluster_key"] = "shared"
        else:
            task_options["new_cluster"] = cluster
        tasks = []
        for role in ("produce", "consume"):
            params = {
                "table": "main.default.release_orders",
                "customer": "alice",
                "role": role,
                "task_values": "true" if kind == "python" else "false",
            }
            task = Task(
                task_key=role,
                depends_on=[TaskDependency(task_key="produce")] if role == "consume" else None,
                timeout_seconds=60 if kind == "python" else 120,
                **task_options,
            )
            if kind == "python":
                task.spark_python_task = SparkPythonTask(
                    python_file=f"/Workspace/Shared/{filename}",
                    parameters=[f"{key}={value}" for key, value in params.items()],
                )
            else:
                task.notebook_task = NotebookTask(
                    notebook_path=f"/Workspace/Shared/{filename}",
                    base_parameters=params,
                )
            tasks.append(task)
        job = client.jobs.create(name=f"release-{variant}-{kind}", tasks=tasks, **options)
        assert job.job_id is not None
        context.secret_provider.put("release-access", "api-key", SECRET)
        context.secret_provider.grant_job(job.job_id, "release-access")
        run = client.jobs.run_now(job.job_id).result(timeout=timedelta(seconds=90))
        outputs = [
            context.jobs.get_attempt_output(run.run_id, role, 1) for role in ("produce", "consume")
        ]
        assert run.state is not None and run.state.result_state is RunResultState.SUCCESS, outputs
        for output in outputs:
            assert output is not None
            assert 'RELEASE_ORDERS=[["alice", 2, "15.75"]]' in output.stdout
            assert SECRET not in output.stdout + output.stderr
        assert SECRET.encode() not in context.workspace.metadata_path.read_bytes()
        consumers = [
            context.compute.get_run_consumer(run.run_id, role) for role in ("produce", "consume")
        ]
        if variant == "SL":
            assert consumers == [None, None]
        else:
            assert all(c is not None and c.state == "released" for c in consumers)
            ids = {c.compute_id for c in consumers if c is not None}
            assert len(ids) == (2 if variant.endswith("N") else 1)
            if existing_id is not None:
                assert ids == {existing_id}
            for compute_id in ids:
                resource = context.compute.get_resource(compute_id)
                assert resource.is_simulated
                assert resource.state.value == ("RUNNING" if existing_id else "TERMINATED")
        warehouses = []
        for profile in ("serverless", "pro", "classic"):
            warehouse = client.warehouses.create(
                name=f"release-{profile}",
                warehouse_type=(
                    CreateWarehouseRequestWarehouseType.CLASSIC
                    if profile == "classic"
                    else CreateWarehouseRequestWarehouseType.PRO
                ),
                enable_serverless_compute=profile == "serverless",
                cluster_size="2X-Small",
                auto_stop_mins=0,
            ).result()
            assert warehouse.id is not None
            warehouses.append(warehouse.id)
        identity = _assert_committed_data(client, context, warehouses)
    with _service(workspace) as (client, context):
        assert _assert_committed_data(client, context, warehouses) == identity


@pytest.mark.parametrize("variant", VARIANTS)
def test_orders_python_workflow_all_compute_and_warehouse_profiles(
    tmp_path: Path,
    variant: str,
) -> None:
    _run_workflow(tmp_path, variant, "python")


@pytest.mark.parametrize("variant", VARIANTS)
def test_orders_ipynb_workflow_all_compute_and_warehouse_profiles(
    tmp_path: Path,
    variant: str,
) -> None:
    _run_workflow(tmp_path, variant, "ipynb")


@pytest.mark.parametrize("variant", ("SL", "CSN", "CDN"))
def test_orders_source_notebook_all_profile_families(tmp_path: Path, variant: str) -> None:
    _run_workflow(tmp_path, variant, "source")
