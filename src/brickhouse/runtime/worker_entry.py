"""Execute a job script with its worker-scoped dbutils globals."""

from __future__ import annotations

import json
import os
import runpy
import sys
from pathlib import Path
from typing import cast

from brickhouse.runtime.dbutils_local import LocalDbutils
from brickhouse.runtime.secrets import (
    MAX_SECRET_SNAPSHOT_BYTES,
    decode_worker_secret_payload,
)
from brickhouse.runtime.worker import CONTEXT_ENV
from brickhouse.runtime.worker_auth import WORKER_TOKEN_ENV
from brickhouse.runtime.worker_imports import install_worker_imports
from brickhouse.runtime.worker_services import WORKER_SOCKET_ENV, WorkerServiceClient
from brickhouse.runtime.worker_session import (
    WorkerSparkSession,
    WorkerSparkSessionBuilder,
    close_worker_spark_session,
    configure_worker_spark_session,
)


def main(arguments: list[str] | None = None) -> None:
    """Run a script path and its argv inside an isolated local dbutils context."""
    values = list(sys.argv[1:] if arguments is None else arguments)
    if not values:
        raise SystemExit("worker entry requires a script path")
    script = str(Path(values[0]).resolve(strict=True))
    script_arguments = values[1:]
    try:
        raw_context = os.environ[CONTEXT_ENV]
        context = json.loads(raw_context)
    except (KeyError, json.JSONDecodeError) as error:
        raise RuntimeError("worker run context is missing or invalid") from error
    if not isinstance(context, dict):
        raise RuntimeError("worker run context must be a JSON object")
    dbutils_context = context.get("dbutils")
    if (
        not isinstance(dbutils_context, dict)
        or not {"dbfs_root", "widget_parameters"}.issubset(dbutils_context)
        or set(dbutils_context) - {"dbfs_root", "widget_parameters", "task_values_enabled"}
    ):
        raise RuntimeError("worker dbutils context is missing or invalid")
    dbfs_root = dbutils_context.get("dbfs_root")
    widget_parameters = dbutils_context.get("widget_parameters")
    if not isinstance(dbfs_root, str) or not isinstance(widget_parameters, dict):
        raise RuntimeError("worker dbutils configuration is malformed")
    if any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in widget_parameters.items()
    ):
        raise RuntimeError("worker widget parameters must be string values")
    task_values_enabled = dbutils_context.get("task_values_enabled", False)
    if type(task_values_enabled) is not bool:
        raise RuntimeError("worker task-values configuration is malformed")

    secret_payload = sys.stdin.buffer.read(MAX_SECRET_SNAPSHOT_BYTES + 1)
    if not secret_payload:
        secret_payload = b'{"scopes":{}}'
    secret_scopes = decode_worker_secret_payload(secret_payload)
    if sys.stdin.buffer.read(1):
        raise RuntimeError("worker secret payload exceeds its size limit")

    sys.argv[:] = [script, *script_arguments]
    script_parent = str(Path(script).parent)
    if script_parent not in sys.path:
        sys.path.insert(0, script_parent)
    worker_services: WorkerServiceClient | None = None
    try:
        if task_values_enabled:
            run_id = context.get("run_id")
            attempt_id = context.get("attempt_id")
            if not isinstance(run_id, str) or not isinstance(attempt_id, str):
                raise RuntimeError("worker task-values attempt identity is malformed")
            worker_services = WorkerServiceClient.from_environment(run_id, attempt_id)
        else:
            os.environ.pop(WORKER_TOKEN_ENV, None)
            os.environ.pop(WORKER_SOCKET_ENV, None)
        configure_worker_spark_session(context, worker_services)
        install_worker_imports(
            spark_session_class=WorkerSparkSession,
            spark_session_builder_class=WorkerSparkSessionBuilder,
        )
        runpy.run_path(
            script,
            init_globals={
                "dbutils": LocalDbutils(
                    dbfs_root,
                    cast(dict[str, str], widget_parameters),
                    secret_scopes,
                    worker_services,
                )
            },
            run_name="__main__",
        )
    finally:
        try:
            close_worker_spark_session()
        finally:
            if worker_services is not None:
                worker_services.close()


if __name__ == "__main__":
    main()
