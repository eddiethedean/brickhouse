"""Supervise one SDK notebook task by executing it in a fresh local kernel."""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import cast

from brickhouse.notebooks.runner import (
    NotebookExecutionError,
    NotebookExecutionTimeoutError,
    execute_databricks_python_source,
    execute_ipynb,
)
from brickhouse.runtime.secrets import MAX_SECRET_SNAPSHOT_BYTES
from brickhouse.runtime.worker import CONTEXT_ENV, WorkerContext
from brickhouse.runtime.worker_auth import WORKER_TOKEN_ENV
from brickhouse.runtime.worker_services import WORKER_SOCKET_ENV


def main(arguments: list[str] | None = None) -> None:
    """Execute the workspace notebook selected by an immutable job attempt context."""
    values = list(sys.argv[1:] if arguments is None else arguments)
    if len(values) != 1:
        raise SystemExit("notebook worker requires one output artifact path")
    artifact_path = Path(values[0]).absolute()
    try:
        context = WorkerContext.from_mapping(_read_context()).to_mapping()
        task_arguments = context.get("task_arguments")
        if not isinstance(task_arguments, Mapping):
            raise ValueError("notebook task arguments are malformed")
        notebook_path = task_arguments.get("notebook_path")
        if not isinstance(notebook_path, str):
            raise ValueError("notebook task path is missing")
        local_path = _resolve_workspace_notebook(notebook_path)
        raw_parameters = context.pop("notebook_parameters", {})
        if not isinstance(raw_parameters, Mapping) or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in raw_parameters.items()
        ):
            raise ValueError("notebook parameters are malformed")
        notebook_context = dict(context)
        notebook_context["dbutils"] = {
            "dbfs_root": str(Path.cwd() / "dbfs"),
            "widget_parameters": dict(raw_parameters),
            "task_values_enabled": True,
        }
        worker_service_environment = {
            WORKER_TOKEN_ENV: os.environ[WORKER_TOKEN_ENV],
            WORKER_SOCKET_ENV: os.environ[WORKER_SOCKET_ENV],
        }
        secret_payload = sys.stdin.buffer.read(MAX_SECRET_SNAPSHOT_BYTES + 1)
        if not secret_payload or len(secret_payload) > MAX_SECRET_SNAPSHOT_BYTES:
            raise ValueError("notebook secret snapshot is missing or oversized")
        if local_path.suffix.casefold() == ".py":
            result = execute_databricks_python_source(
                local_path,
                artifact_path=artifact_path,
                worker_context=notebook_context,
                parameters=dict(raw_parameters),
                interpreter=sys.executable,
                cwd=local_path.parent,
                cell_timeout_seconds=None,
                notebook_root=Path.cwd(),
                worker_service_environment=worker_service_environment,
                worker_secret_payload=secret_payload,
            )
        else:
            result = execute_ipynb(
                local_path,
                artifact_path=artifact_path,
                worker_context=notebook_context,
                parameters=dict(raw_parameters),
                interpreter=sys.executable,
                cwd=local_path.parent,
                cell_timeout_seconds=None,
                notebook_root=Path.cwd(),
                worker_service_environment=worker_service_environment,
                worker_secret_payload=secret_payload,
            )
    except NotebookExecutionTimeoutError:
        print("notebook execution exceeded its run timeout", file=sys.stderr)
        raise SystemExit(124) from None
    except NotebookExecutionError as error:
        print(f"notebook execution failed ({error.error_type})", file=sys.stderr)
        raise SystemExit(1) from None
    _write_notebook_output(artifact_path)
    if result.exit_value is not None:
        print(f"NOTEBOOK_EXIT={result.exit_value}")


def _read_context() -> dict[str, object]:
    try:
        serialized = os.environ[CONTEXT_ENV]
        value = json.loads(serialized)
    except (KeyError, json.JSONDecodeError) as error:
        raise ValueError("notebook worker context is missing or invalid") from error
    if not isinstance(value, dict):
        raise ValueError("notebook worker context must be an object")
    return cast(dict[str, object], value)


def _resolve_workspace_notebook(path: str) -> Path:
    if not path.startswith("/Workspace/") or "\\" in path or "\x00" in path:
        raise ValueError("notebook task path must be a local /Workspace path")
    root = Path.cwd().resolve(strict=True)
    relative = Path(path.removeprefix("/Workspace/"))
    if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        raise ValueError("notebook task path is not normalized")
    unresolved = root / relative
    if not unresolved.exists() and not relative.suffix:
        unresolved = root / relative.with_name(f"{relative.name}.ipynb")
    candidate = unresolved.resolve(strict=True)
    if root not in candidate.parents or not candidate.is_file():
        raise ValueError("notebook task path is outside the workspace files root")
    return candidate


def _write_notebook_output(artifact_path: Path) -> None:
    try:
        document = json.loads(artifact_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise RuntimeError("notebook output artifact could not be read") from error
    cells = document.get("cells") if isinstance(document, dict) else None
    if not isinstance(cells, list):
        raise RuntimeError("notebook output artifact is malformed")
    for cell in cells:
        outputs = cell.get("outputs", []) if isinstance(cell, dict) else []
        if not isinstance(outputs, list):
            continue
        for output in outputs:
            if not isinstance(output, dict):
                continue
            if output.get("output_type") == "stream":
                _write_text(output.get("text"))
            elif output.get("output_type") in {"display_data", "execute_result"}:
                data = output.get("data")
                if isinstance(data, dict):
                    _write_text(data.get("text/plain"))


def _write_text(value: object) -> None:
    if isinstance(value, str):
        sys.stdout.write(value)
        if value and not value.endswith("\n"):
            sys.stdout.write("\n")
    elif isinstance(value, list) and all(isinstance(part, str) for part in value):
        _write_text("".join(value))


if __name__ == "__main__":
    main()
