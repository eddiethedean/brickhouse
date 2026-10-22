from __future__ import annotations

import json
import os
import shutil
import subprocess
import urllib.error
import urllib.request
from datetime import timedelta
from pathlib import Path
from threading import Thread
from typing import Any

import pyarrow as pa
import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceDoesNotExist
from databricks.sdk.service.sql import (
    ColumnInfoTypeName,
    CreateWarehouseRequestWarehouseType,
    StatementState,
)

from brickhouse.catalog import parse_identifier
from brickhouse.compute.models import ComputeState
from brickhouse.diagnostic_server import create_diagnostic_server
from brickhouse.jobs.repository import JobNotFoundError
from brickhouse.tables import DeltaRsTableService, SnapshotSelector, WriteMode
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace

_CLI_VERSION = "Databricks CLI v1.21.0"
_LOCAL_TOKEN = "brickhouse-local-b0205-bundle-token"


@pytest.mark.skipif(
    not os.environ.get("DATABRICKS_CLI"),
    reason="set DATABRICKS_CLI to the pinned Databricks CLI v1.21.0 executable",
)
def test_cli_bundles_v1_21_0_run_g2_serverless_python_and_classic_notebook(
    tmp_path: Path,
) -> None:
    cli = Path(os.environ["DATABRICKS_CLI"]).expanduser().resolve()
    assert cli.is_file(), f"DATABRICKS_CLI does not name a file: {cli}"
    version = subprocess.run(
        [str(cli), "version"],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.strip()
    assert version == _CLI_VERSION

    projects = tmp_path / "projects"
    serverless_project = projects / "serverless"
    serverless_source = serverless_project / "src"
    serverless_source.mkdir(parents=True)
    (serverless_source / "g2_writer.py").write_text(
        "import json, os\n"
        "from pyspark.sql import SparkSession\n"
        "from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType\n"
        "spark = SparkSession.builder.getOrCreate()\n"
        "context = json.loads(os.environ['BRICKHOUSE_RUN_CONTEXT_JSON'])\n"
        "assert context['profile'] == 'serverless-local-v1', context\n"
        "dbutils.widgets.text('order_id', '0')\n"
        "dbutils.widgets.text('source', 'unknown')\n"
        "order_id = int(dbutils.widgets.get('order_id'))\n"
        "source = dbutils.widgets.get('source')\n"
        "schema = StructType([\n"
        "    StructField('order_id', LongType(), False),\n"
        "    StructField('authorized', BooleanType(), False),\n"
        "    StructField('source', StringType(), False),\n"
        "])\n"
        "frame = spark.createDataFrame([(order_id, True, source)], schema)\n"
        "frame.write.format('delta').mode('append').saveAsTable('main.default.job_orders')\n"
        "print('B0205_G2_COMMIT=' + json.dumps({'order_id': order_id, 'source': source}))\n"
        "print('B0205_PROFILE=' + context['profile'])\n",
        encoding="utf-8",
    )
    _write_serverless_bundle(serverless_project, order_id=42, source="bundle-serverless-v1")

    classic_project = projects / "classic"
    classic_source = classic_project / "src"
    classic_source.mkdir(parents=True)
    notebook = {
        "cells": [
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "import json, os\n",
                    "context = json.loads(os.environ['BRICKHOUSE_RUN_CONTEXT_JSON'])\n",
                    "assert context['profile'] == 'classic-standard-local-v1', context\n",
                    "rows = sorted((row.order_id, row.authorized, row.source) for row in "
                    "spark.table('main.default.job_orders').collect())\n",
                    "expected = [(7, False, 'baseline'), (42, True, 'bundle-serverless-v1'), "
                    "(43, True, 'bundle-serverless-v2')]\n",
                    "assert rows == expected, rows\n",
                    "print('B0205_G2_NOTEBOOK=' + json.dumps({'rows': rows, "
                    "'profile': context['profile']}, sort_keys=True))\n",
                ],
            }
        ],
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    (classic_source / "g2_reader.ipynb").write_text(json.dumps(notebook), encoding="utf-8")
    (classic_project / "databricks.yml").write_text(
        """bundle:
  name: brickhouse_b0205_classic
sync:
  paths:
    - src
targets:
  dev:
    default: true
    mode: development
resources:
  jobs:
    g2_notebook:
      name: brickhouse-b0205-classic-g2
      tasks:
        - task_key: reader
          notebook_task:
            notebook_path: ./src/g2_reader.ipynb
          new_cluster:
            spark_version: spark35-local-v1
            data_security_mode: DATA_SECURITY_MODE_STANDARD
            spark_conf:
              spark.sql.ansi.enabled: "true"
              spark.sql.session.timeZone: UTC
            num_workers: 0
""",
        encoding="utf-8",
    )

    descriptor = initialize_workspace(tmp_path / "workspace")
    config_file = tmp_path / "databrickscfg"
    config_file.write_text("", encoding="utf-8")
    home = tmp_path / "home"
    home.mkdir()

    with WorkspaceContext.local(descriptor.path) as context:
        registration = context.tables.create_managed("main", "default", "job_orders")
        table_service = DeltaRsTableService(context)
        table = table_service.resolve(
            parse_identifier("main.default.job_orders"), context.new_namespace_state()
        )
        table_service.write_arrow(
            table,
            pa.table(
                {
                    "order_id": pa.array([7], type=pa.int64()),
                    "authorized": pa.array([False], type=pa.bool_()),
                    "source": pa.array(["baseline"], type=pa.string()),
                }
            ),
            WriteMode.CREATE,
            operation_id="seed-b0205-g2-job-orders",
        )
        assert registration.table_id == table.table_id

        server = create_diagnostic_server(context, "127.0.0.1", 0)
        thread = Thread(
            target=server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )
        thread.start()
        host = f"http://127.0.0.1:{server.server_port}"
        environment = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": str(home),
            "TMPDIR": str(tmp_path),
            "DATABRICKS_HOST": host,
            "DATABRICKS_TOKEN": _LOCAL_TOKEN,
            "DATABRICKS_CONFIG_FILE": str(config_file),
        }
        client = WorkspaceClient(host=host, token=_LOCAL_TOKEN, auth_type="pat")

        def invoke(project: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
            result = subprocess.run(
                [str(cli), *arguments],
                cwd=project,
                env=environment,
                check=False,
                capture_output=True,
                text=True,
                timeout=90,
            )
            assert _LOCAL_TOKEN not in result.stdout + result.stderr
            assert result.returncode == 0, result.stderr
            return result

        try:
            _assert_validated(invoke(serverless_project, "bundle", "validate", "-o", "json"))
            first_deploy = invoke(
                serverless_project, "bundle", "deploy", "--auto-approve", "-o", "json"
            )
            assert "Created jobs.g2_writer" in first_deploy.stderr
            first_run = _bundle_run_output(
                invoke(serverless_project, "bundle", "run", "g2_writer", "-o", "json")
            )
            assert 'B0205_G2_COMMIT={"order_id": 42, "source": "bundle-serverless-v1"}' in first_run
            assert "B0205_PROFILE=serverless-local-v1" in first_run

            serverless_job = context.jobs.list_jobs()[0]
            assert serverless_job.settings["deployment"]["kind"] == "BUNDLE"
            serverless_job_id = serverless_job.job_id
            assert len(context.jobs.list_runs(serverless_job_id)) == 1

            _write_serverless_bundle(serverless_project, order_id=43, source="bundle-serverless-v2")
            _assert_validated(invoke(serverless_project, "bundle", "validate", "-o", "json"))
            update = invoke(serverless_project, "bundle", "deploy", "--auto-approve", "-o", "json")
            assert "Updated jobs.g2_writer" in update.stderr
            second_run = _bundle_run_output(
                invoke(serverless_project, "bundle", "run", "g2_writer", "-o", "json")
            )
            assert (
                'B0205_G2_COMMIT={"order_id": 43, "source": "bundle-serverless-v2"}' in second_run
            )
            assert "B0205_PROFILE=serverless-local-v1" in second_run
            current_job = _get_json(
                host,
                f"/api/2.2/jobs/get?job_id={serverless_job_id}",
            )
            assert current_job[0] == 200
            assert current_job[1]["job_id"] == serverless_job_id
            assert current_job[1]["settings"]["tasks"][0]["spark_python_task"]["parameters"] == [
                "order_id=43",
                "source=bundle-serverless-v2",
            ]
            serverless_definitions = [
                job for job in context.jobs.list_jobs() if job.name == serverless_job.name
            ]
            serverless_definition_ids = [job.job_id for job in serverless_definitions]
            assert len(serverless_definition_ids) == 2
            serverless_runs = [
                run
                for definition_id in serverless_definition_ids
                for run in context.jobs.list_runs(definition_id)
            ]
            assert len(serverless_runs) == 2
            assert all(run.state.value == "succeeded" for run in serverless_runs)
            for run in serverless_runs:
                assert context.compute.get_run_consumer(run.run_id, "writer") is None
            latest_run_payload = _get_json(
                host,
                f"/api/2.2/jobs/runs/get?run_id={max(run.run_id for run in serverless_runs)}",
            )
            assert latest_run_payload[0] == 200
            assert latest_run_payload[1]["job_id"] == serverless_job_id

            delta_snapshot = table_service.read_table(table, SnapshotSelector(version=2))
            assert delta_snapshot.data.schema.names == ["order_id", "authorized", "source"]
            assert [field.type for field in delta_snapshot.data.schema] == [
                pa.int64(),
                pa.bool_(),
                pa.string(),
            ]
            expected_rows = [
                (7, False, "baseline"),
                (42, True, "bundle-serverless-v1"),
                (43, True, "bundle-serverless-v2"),
            ]
            assert (
                sorted(
                    (row["order_id"], row["authorized"], row["source"])
                    for row in delta_snapshot.data.to_pylist()
                )
                == expected_rows
            )

            _assert_validated(invoke(classic_project, "bundle", "validate", "-o", "json"))
            classic_deploy = invoke(
                classic_project, "bundle", "deploy", "--auto-approve", "-o", "json"
            )
            assert "Created jobs.g2_notebook" in classic_deploy.stderr
            notebook_output = _bundle_run_output(
                invoke(classic_project, "bundle", "run", "g2_notebook", "-o", "json")
            )
            notebook_line = next(
                line.removeprefix("B0205_G2_NOTEBOOK=")
                for line in notebook_output.splitlines()
                if line.startswith("B0205_G2_NOTEBOOK=")
            )
            assert json.loads(notebook_line) == {
                "profile": "classic-standard-local-v1",
                "rows": [list(row) for row in expected_rows],
            }

            classic_job = next(
                job for job in context.jobs.list_jobs() if "brickhouse-b0205-classic-g2" in job.name
            )
            classic_runs = context.jobs.list_runs(classic_job.job_id)
            assert len(classic_runs) == 1 and classic_runs[0].state.value == "succeeded"
            consumer = context.compute.get_run_consumer(classic_runs[0].run_id, "reader")
            assert consumer is not None and consumer.state == "released"
            compute = context.compute.get_resource(consumer.compute_id)
            assert compute.state is ComputeState.TERMINATED
            assert compute.is_simulated is True
            assert compute.profile == "classic-standard-local-v1"

            warehouse = client.warehouses.create(
                name="b0205-g2-sql-check",
                warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
                enable_serverless_compute=True,
                cluster_size="2X-Small",
                auto_stop_mins=0,
            ).result(timeout=timedelta(seconds=30))
            assert warehouse.id is not None
            statement = client.statement_execution.execute_statement(
                "SELECT order_id, authorized, source FROM job_orders ORDER BY order_id",
                warehouse.id,
                catalog="main",
                schema="default",
            )
            assert (
                statement.status is not None and statement.status.state is StatementState.SUCCEEDED
            )
            assert statement.manifest is not None and statement.manifest.total_row_count == 3
            assert statement.manifest.schema is not None
            assert statement.manifest.schema.columns is not None
            assert [
                (column.name, column.type_name) for column in statement.manifest.schema.columns
            ] == [
                ("order_id", ColumnInfoTypeName.LONG),
                ("authorized", ColumnInfoTypeName.BOOLEAN),
                ("source", ColumnInfoTypeName.STRING),
            ]
            assert statement.result is not None
            assert statement.result.data_array == [
                ["7", "false", "baseline"],
                ["42", "true", "bundle-serverless-v1"],
                ["43", "true", "bundle-serverless-v2"],
            ]

            invalid_task = _post_json(
                host,
                "/api/2.2/jobs/create",
                {
                    "name": "unsupported-task",
                    "tasks": [
                        {
                            "task_key": "unsupported",
                            "spark_jar_task": {"main_class_name": "example.Main"},
                            "new_cluster": {
                                "spark_version": "spark35-local-v1",
                                "data_security_mode": "DATA_SECURITY_MODE_STANDARD",
                                "num_workers": 0,
                            },
                        }
                    ],
                },
            )
            assert invalid_task[0] == 400
            assert invalid_task[1]["message"] == "task type spark_jar_task is unsupported"

            invalid_compute = _post_json(
                host,
                "/api/2.2/jobs/create",
                {
                    "name": "invalid-serverless-and-classic-selectors",
                    "tasks": [
                        {
                            "task_key": "invalid",
                            "spark_python_task": {
                                "python_file": "/Workspace/Shared/unused.py",
                                "parameters": [],
                            },
                            "environment_key": "local-python",
                            "new_cluster": {
                                "spark_version": "spark35-local-v1",
                                "data_security_mode": "DATA_SECURITY_MODE_STANDARD",
                                "num_workers": 0,
                            },
                        }
                    ],
                    "environments": [
                        {
                            "environment_key": "local-python",
                            "spec": {"environment_version": "1", "dependencies": []},
                        }
                    ],
                },
            )
            assert invalid_compute[0] == 400
            assert "environment_key conflicts with classic compute" in invalid_compute[1]["message"]
            assert len(context.jobs.list_jobs()) == 3

            unsupported_resource = _post_json(host, "/api/2.0/pipelines", {})
            assert unsupported_resource[0] == 400
            assert "pipelines resources are unsupported" in unsupported_resource[1]["message"]
            assert len(context.jobs.list_jobs()) == 3

            client.warehouses.delete(warehouse.id)
            assert _get_json(host, f"/api/2.0/sql/warehouses/{warehouse.id}")[0] == 404
            for project in (serverless_project, classic_project):
                destroy = invoke(project, "bundle", "destroy", "--auto-approve", "-o", "json")
                assert "Destroy: 1 deleted" in destroy.stderr

            with pytest.raises(JobNotFoundError):
                context.jobs.get_active_job(serverless_job_id)
            with pytest.raises(JobNotFoundError):
                context.jobs.get_active_job(classic_job.job_id)
            assert (
                sum(
                    len(context.jobs.list_runs(definition_id))
                    for definition_id in serverless_definition_ids
                )
                == 2
            )
            assert len(context.jobs.list_runs(classic_job.job_id)) == 1
            for project_name in ("brickhouse_b0205_serverless", "brickhouse_b0205_classic"):
                bundle_path = (
                    descriptor.path
                    / "files"
                    / "Users"
                    / "brickhouse-local@invalid"
                    / ".bundle"
                    / project_name
                    / "dev"
                )
                assert not bundle_path.exists()
        finally:
            try:
                if "warehouse" in locals() and warehouse.id is not None:
                    client.warehouses.delete(warehouse.id)
            except ResourceDoesNotExist:
                pass
            server.shutdown()
            thread.join(timeout=5)
            server.server_close()

    shutil.rmtree(descriptor.path)
    assert not descriptor.path.exists()


def _write_serverless_bundle(project: Path, *, order_id: int, source: str) -> None:
    (project / "databricks.yml").write_text(
        f"""bundle:
  name: brickhouse_b0205_serverless
sync:
  paths:
    - src
targets:
  dev:
    default: true
    mode: development
resources:
  jobs:
    g2_writer:
      name: brickhouse-b0205-serverless-g2
      tasks:
        - task_key: writer
          spark_python_task:
            python_file: ./src/g2_writer.py
            parameters:
              - order_id={order_id}
              - source={source}
          environment_key: local-python
          disable_auto_optimization: true
      environments:
        - environment_key: local-python
          spec:
            environment_version: "1"
            dependencies: []
""",
        encoding="utf-8",
    )


def _assert_validated(result: subprocess.CompletedProcess[str]) -> None:
    payload = json.loads(result.stdout)
    assert payload["workspace"]["current_user"]["id"] == "brickhouse-local-user"


def _bundle_run_output(result: subprocess.CompletedProcess[str]) -> str:
    outputs = json.loads(result.stdout)["task_outputs"]
    assert len(outputs) == 1
    return outputs[0]["Output"]["logs"]


def _post_json(host: str, path: str, payload: dict[str, object]) -> tuple[int, dict[str, Any]]:
    request = urllib.request.Request(
        f"{host}{path}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read())


def _get_json(host: str, path: str) -> tuple[int, dict[str, Any]]:
    request = urllib.request.Request(f"{host}{path}", method="GET")
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read())
