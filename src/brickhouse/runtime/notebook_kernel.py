"""Initialize worker-scoped BrickHouse globals inside a notebook kernel."""

from __future__ import annotations

import atexit
import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from brickhouse.notebooks.source_format import (
    parse_databricks_python_source,
    resolve_run_target,
)
from brickhouse.runtime.dbutils_local import LocalDbutils, LocalDbutilsNotebook
from brickhouse.runtime.secrets import MAX_SECRET_SNAPSHOT_BYTES, decode_worker_secret_payload
from brickhouse.runtime.worker import CONTEXT_ENV, MAX_CONTEXT_BYTES
from brickhouse.runtime.worker_auth import WORKER_TOKEN_ENV
from brickhouse.runtime.worker_imports import install_worker_imports
from brickhouse.runtime.worker_services import WORKER_SOCKET_ENV, WorkerServiceClient
from brickhouse.runtime.worker_session import (
    WorkerSparkSession,
    WorkerSparkSessionBuilder,
    close_worker_spark_session,
    configure_worker_spark_session,
)

_WORKER_SERVICES: WorkerServiceClient | None = None
_INITIALIZED = False


def initialize_notebook_globals() -> dict[str, object]:
    """Install the selected worker facades in the active IPython user namespace."""
    global _INITIALIZED, _WORKER_SERVICES
    if _INITIALIZED:
        raise RuntimeError("notebook worker globals have already been initialized")
    try:
        serialized = os.environ[CONTEXT_ENV]
    except KeyError as error:
        raise RuntimeError("notebook worker context is missing") from error
    if len(serialized.encode("utf-8")) > MAX_CONTEXT_BYTES:
        raise RuntimeError("notebook worker context exceeds its size limit")
    try:
        context_value = json.loads(serialized)
    except json.JSONDecodeError as error:
        raise RuntimeError("notebook worker context is invalid JSON") from error
    if not isinstance(context_value, dict):
        raise RuntimeError("notebook worker context must be a JSON object")
    context = cast(dict[str, object], context_value)
    dbutils_context = context.get("dbutils")
    if not isinstance(dbutils_context, Mapping):
        raise RuntimeError("notebook worker dbutils context is missing")
    dbfs_root = dbutils_context.get("dbfs_root")
    widget_parameters = dbutils_context.get("widget_parameters")
    task_values_enabled = dbutils_context.get("task_values_enabled", False)
    if (
        not isinstance(dbfs_root, str)
        or not isinstance(widget_parameters, Mapping)
        or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in widget_parameters.items()
        )
        or type(task_values_enabled) is not bool
    ):
        raise RuntimeError("notebook worker dbutils context is malformed")
    notebook_root = os.environ.get("BRICKHOUSE_NOTEBOOK_ROOT")
    notebook_exit_token = os.environ.get("BRICKHOUSE_NOTEBOOK_EXIT_TOKEN")
    if not notebook_root or not notebook_exit_token:
        raise RuntimeError("notebook execution context is missing its local run boundary")
    root_path = Path(notebook_root).resolve(strict=True)
    if not root_path.is_dir():
        raise RuntimeError("notebook execution root must be an existing directory")
    secret_scopes = _read_notebook_secret_snapshot()

    def run_notebook(
        path: str,
        timeout_seconds: int,
        arguments: Mapping[str, str],
    ) -> str:
        from brickhouse.notebooks.runner import run_local_child_notebook

        child_service_environment: dict[str, str] | None = None
        child_service_descriptor: int | None = None
        if worker_services is not None:
            child_service_environment, child_service_descriptor = (
                worker_services.duplicate_environment_for_child()
            )
        try:
            return run_local_child_notebook(
                path,
                timeout_seconds,
                arguments,
                relative_to=Path.cwd(),
                allowed_root=root_path,
                worker_context=context,
                worker_service_environment=child_service_environment,
            )
        finally:
            if child_service_descriptor is not None:
                os.close(child_service_descriptor)

    worker_services: WorkerServiceClient | None = None
    if task_values_enabled:
        run_id = context.get("run_id")
        attempt_id = context.get("attempt_id")
        if not isinstance(run_id, str) or not isinstance(attempt_id, str):
            raise RuntimeError("notebook task-value attempt identity is malformed")
        worker_services = WorkerServiceClient.from_environment(run_id, attempt_id)
    else:
        os.environ.pop(WORKER_TOKEN_ENV, None)
        os.environ.pop(WORKER_SOCKET_ENV, None)

    try:
        configure_worker_spark_session(context, worker_services)
        install_worker_imports(
            spark_session_class=WorkerSparkSession,
            spark_session_builder_class=WorkerSparkSessionBuilder,
        )
        spark = WorkerSparkSession.builder.getOrCreate()
        dbutils = LocalDbutils(
            dbfs_root,
            cast(dict[str, str], widget_parameters),
            secret_scopes=secret_scopes,
            worker_services=worker_services,
            notebook=LocalDbutilsNotebook(run_notebook, notebook_exit_token),
        )
    except BaseException:
        close_worker_spark_session()
        if worker_services is not None:
            worker_services.close()
        raise

    _WORKER_SERVICES = worker_services
    _INITIALIZED = True
    atexit.register(_close_notebook_globals)
    return {
        "spark": spark,
        "dbutils": dbutils,
        "__brickhouse_execute_sql": _execute_sql_cell,
        "__brickhouse_run_source_notebook": _run_source_notebook,
    }


def _read_notebook_secret_snapshot() -> dict[str, dict[str, bytes]]:
    raw_descriptor = os.environ.pop("BRICKHOUSE_NOTEBOOK_SECRET_FD", None)
    if raw_descriptor is None:
        return {}
    try:
        descriptor = int(raw_descriptor)
    except ValueError as error:
        raise RuntimeError("notebook secret channel descriptor is malformed") from error
    if descriptor < 0:
        raise RuntimeError("notebook secret channel descriptor is invalid")
    payload = bytearray()
    try:
        while len(payload) <= MAX_SECRET_SNAPSHOT_BYTES:
            chunk = os.read(descriptor, min(8192, MAX_SECRET_SNAPSHOT_BYTES + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
    finally:
        os.close(descriptor)
    if len(payload) > MAX_SECRET_SNAPSHOT_BYTES:
        raise RuntimeError("notebook secret snapshot exceeds its size limit")
    return decode_worker_secret_payload(bytes(payload))


def _execute_sql_cell(query: str) -> Any:
    if not isinstance(query, str) or not query.strip():
        raise ValueError("%sql requires a non-empty query")
    from IPython import get_ipython
    from IPython.display import display

    shell = get_ipython()
    if shell is None:
        raise RuntimeError("notebook SQL handler requires an active IPython kernel")
    namespace = shell.user_ns
    spark = namespace.get("spark")
    if spark is None:
        raise RuntimeError("notebook SQL handler has no injected Spark session")
    result = spark.sql(query)
    namespace["_sqldf"] = result
    display_result = cast(Any, display)
    display_result(result.collect())
    return result


def _run_source_notebook(
    target: str,
    *,
    relative_to: str,
    allowed_root: str,
) -> None:
    from IPython import get_ipython

    shell = get_ipython()
    if shell is None:
        raise RuntimeError("%run requires an active IPython kernel")
    root = os.path.realpath(allowed_root)
    source_path = resolve_run_target(
        target,
        relative_to=relative_to,
        allowed_root=root,
    )
    _execute_source_export(source_path, shell.user_ns, root)


def _execute_source_export(source_path: Path, namespace: dict[str, Any], allowed_root: str) -> None:
    export = parse_databricks_python_source(source_path)
    for cell in export.cells:
        if cell.kind == "python":
            code = compile(cell.source, str(export.path), "exec")
            exec(code, namespace, namespace)
        elif cell.kind == "sql":
            _execute_sql_cell(cell.source)
        elif cell.kind == "run":
            if cell.target is None:
                raise RuntimeError("%run source cell is missing its target path")
            _run_source_notebook(
                cell.target,
                relative_to=str(export.path.parent),
                allowed_root=allowed_root,
            )
        elif cell.kind != "markdown":
            raise RuntimeError(f"unsupported %run source cell type: {cell.kind}")


def _close_notebook_globals() -> None:
    global _WORKER_SERVICES
    try:
        close_worker_spark_session()
    finally:
        if _WORKER_SERVICES is not None:
            _WORKER_SERVICES.close()
            _WORKER_SERVICES = None
