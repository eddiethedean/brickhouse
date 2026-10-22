"""Run the documented G2 local job and read its Delta output through SQL."""

from __future__ import annotations

import argparse
import io
import json
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterSpec, DataSecurityMode
from databricks.sdk.service.jobs import SparkPythonTask, Task
from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType
from databricks.sdk.service.workspace import ImportFormat, Language


def run_orders_workflow(client: WorkspaceClient) -> dict[str, Any]:
    """Create a table, append real worker rows, and query the committed result."""
    table_name = f"orders_{uuid.uuid4().hex[:12]}"
    warehouse = client.warehouses.create(
        name=f"brickhouse-example-{table_name}",
        warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
        enable_serverless_compute=True,
        cluster_size="2X-Small",
        auto_stop_mins=0,
    ).result()
    if not warehouse.id:
        raise RuntimeError("local warehouse creation returned no ID")

    created = client.statement_execution.execute_statement(
        f"CREATE TABLE main.default.{table_name} "
        "(order_id BIGINT, customer STRING, amount DECIMAL(12, 2), "
        "active BOOLEAN) USING DELTA",
        warehouse.id,
        catalog="main",
        schema="default",
        wait_timeout="30s",
    )
    if (
        created.status is None
        or created.status.state is None
        or created.status.state.value != "SUCCEEDED"
    ):
        raise RuntimeError(f"table creation failed: {created.status}")

    worker_path = Path(__file__).with_name("orders_worker.py")
    client.workspace.upload(
        f"/Workspace/Shared/{table_name}.py",
        io.BytesIO(worker_path.read_bytes()),
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True,
    )
    cluster = ClusterSpec(
        spark_version="spark35-local-v1",
        data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
        spark_conf={
            "spark.sql.ansi.enabled": "true",
            "spark.sql.session.timeZone": "UTC",
        },
        num_workers=0,
    )
    job = client.jobs.create(
        name=f"brickhouse-example-{table_name}",
        tasks=[
            Task(
                task_key="write-orders",
                spark_python_task=SparkPythonTask(
                    python_file=f"/Workspace/Shared/{table_name}.py",
                    parameters=[table_name],
                ),
                new_cluster=cluster,
            )
        ],
    )
    if job.job_id is None:
        raise RuntimeError("local job creation returned no ID")

    run = client.jobs.run_now(job.job_id).result(timeout=timedelta(seconds=30))
    if run.run_id is None or run.state is None or run.state.result_state is None:
        raise RuntimeError("local job returned no terminal run state")
    if run.state.result_state.value != "SUCCESS":
        raise RuntimeError(f"local job did not succeed: {run.state.result_state.value}")

    statement = client.statement_execution.execute_statement(
        f"SELECT customer, COUNT(*) AS order_count, SUM(amount) AS total_amount "
        f"FROM main.default.{table_name} "
        "WHERE active = true AND amount IS NOT NULL "
        "GROUP BY customer ORDER BY customer",
        warehouse.id,
        catalog="main",
        schema="default",
        wait_timeout="30s",
    )
    if (
        statement.status is None
        or statement.status.state is None
        or statement.status.state.value != "SUCCEEDED"
    ):
        raise RuntimeError(f"aggregate query failed: {statement.status}")
    rows = statement.result.data_array if statement.result is not None else None
    expected = [["alice", "2", "15.75"]]
    if rows != expected:
        raise RuntimeError(f"unexpected aggregate result: {rows!r}; expected {expected!r}")

    return {
        "table": f"main.default.{table_name}",
        "warehouse_id": warehouse.id,
        "job_id": job.job_id,
        "run_id": run.run_id,
        "result": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="http://127.0.0.1:8000")
    args = parser.parse_args()
    client = WorkspaceClient(
        host=args.host,
        token="brickhouse-local-example",
        auth_type="pat",
    )
    print(json.dumps(run_orders_workflow(client), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
