"""Execute a parameterized ipynb in a fresh, explicitly selected Python kernel."""

from __future__ import annotations

import base64
import binascii
import json
import keyword
import math
import os
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from brickhouse.notebooks.source_format import (
    DatabricksPythonExport,
    NotebookSourceCell,
    NotebookSourceFormatError,
    parse_databricks_python_source,
    validate_source_export_tree,
)
from brickhouse.runtime.dbutils_local import NotebookRunError, NotebookRunTimeoutError
from brickhouse.runtime.secrets import MAX_SECRET_SNAPSHOT_BYTES
from brickhouse.runtime.worker import (
    MAX_WORKER_TIMEOUT_SECONDS,
    WorkerContext,
    _worker_environment,
)
from brickhouse.runtime.worker_auth import WORKER_TOKEN_ENV
from brickhouse.runtime.worker_services import WORKER_SOCKET_ENV
from brickhouse.runtime.worker_session import validate_worker_spark_session_context

_KERNEL_NAME = "brickhouse-python"
_BOOTSTRAP_TAG = "brickhouse-bootstrap"
_PARAMETERS_TAG = "injected-parameters"
_MAX_NOTEBOOK_BYTES = 16 * 1024 * 1024
_MAX_PARAMETER_BYTES = 16 * 1024
_KERNEL_ENVIRONMENT_TIMEOUT_SECONDS = 10
_NOTEBOOK_ROOT_ENV = "BRICKHOUSE_NOTEBOOK_ROOT"
_NOTEBOOK_EXIT_TOKEN_ENV = "BRICKHOUSE_NOTEBOOK_EXIT_TOKEN"
_NOTEBOOK_SECRET_FD_ENV = "BRICKHOUSE_NOTEBOOK_SECRET_FD"
_REQUIREMENT_PATTERN = re.compile(r"([A-Za-z0-9_.-]+)==([A-Za-z0-9_.+-]+)\Z")
_REQUIRED_KERNEL_DISTRIBUTIONS = {
    "ipykernel": "7.4.0",
    "sparkless": "4.15.2",
}
_RESERVED_PARAMETERS = frozenset(
    {
        "__builtins__",
        "dbutils",
        "display",
        "get_ipython",
        "spark",
    }
)
_BOOTSTRAP_SOURCE = (
    "from brickhouse.runtime.notebook_kernel import initialize_notebook_globals\n"
    "globals().update(initialize_notebook_globals())\n"
    "del initialize_notebook_globals\n"
)


class NotebookDependencyError(RuntimeError):
    """The optional notebook execution dependencies are not installed."""


class NotebookEnvironmentError(ValueError):
    """The selected notebook interpreter or compute environment is not mapped."""


class NotebookExecutionError(RuntimeError):
    """Notebook execution failed after writing the executed notebook artifact."""

    def __init__(self, artifact_path: Path, error_type: str) -> None:
        super().__init__(
            f"notebook execution failed with {error_type}; output artifact: {artifact_path}"
        )
        self.artifact_path = artifact_path
        self.error_type = error_type


class NotebookExecutionTimeoutError(TimeoutError):
    """A notebook exceeded its configured total execution timeout."""

    def __init__(self, artifact_path: Path) -> None:
        super().__init__(f"notebook execution timed out; output artifact: {artifact_path}")
        self.artifact_path = artifact_path


@dataclass(frozen=True, slots=True)
class NotebookExecutionResult:
    """Successful notebook execution and its saved cell outputs."""

    artifact_path: Path
    kernel_id: str
    executed_code_cells: int
    exit_value: str | None = None


def execute_databricks_python_source(
    notebook_path: str | Path,
    *,
    artifact_path: str | Path,
    worker_context: Mapping[str, object],
    parameters: Mapping[str, object] | None = None,
    interpreter: str | Path | None = None,
    cwd: str | Path | None = None,
    cell_timeout_seconds: int | None = 60,
    execution_timeout_seconds: int | None = None,
    notebook_root: str | Path | None = None,
    worker_service_environment: Mapping[str, str] | None = None,
    worker_secret_payload: bytes | None = None,
) -> NotebookExecutionResult:
    """Execute the declared local Python Databricks source-export subset.

    The top-level export and its relative `%run` source tree are parsed and
    validated before a kernel starts. `%run` handlers execute inside the current
    kernel namespace, while SQL and Markdown cells receive their own handlers.
    """
    source_export = parse_databricks_python_source(notebook_path)
    validate_source_export_tree(source_export)
    _validate_source_export_environment(source_export, worker_context)
    try:
        import nbformat
    except ImportError as error:
        raise NotebookDependencyError(
            "Databricks source notebook execution requires the optional notebook dependencies; "
            "install brickhouse[notebooks]"
        ) from error

    notebook_cells = [
        _convert_source_cell(cell, source_export, nbformat) for cell in source_export.cells
    ]
    new_notebook = cast(Any, nbformat.v4.new_notebook)
    notebook_metadata: dict[str, object] = {"language_info": {"name": "python"}}
    if source_export.environment_version is not None or source_export.dependencies:
        environment_metadata: dict[str, object] = {"dependencies": list(source_export.dependencies)}
        if source_export.environment_version is not None:
            environment_metadata["environment_version"] = source_export.environment_version
        notebook_metadata["application/vnd.databricks.v1+notebook"] = {
            "environmentMetadata": environment_metadata
        }
    notebook = new_notebook(
        cells=notebook_cells,
        metadata=notebook_metadata,
    )
    working_directory = source_export.path.parent if cwd is None else cwd
    with tempfile.TemporaryDirectory(prefix="brickhouse-source-export-") as temporary_root:
        generated_path = Path(temporary_root) / "converted.ipynb"
        write_notebook = cast(Any, nbformat.write)
        write_notebook(notebook, generated_path)
        return execute_ipynb(
            generated_path,
            artifact_path=artifact_path,
            worker_context=worker_context,
            parameters=parameters,
            interpreter=interpreter,
            cwd=working_directory,
            cell_timeout_seconds=cell_timeout_seconds,
            execution_timeout_seconds=execution_timeout_seconds,
            notebook_root=notebook_root,
            worker_service_environment=worker_service_environment,
            worker_secret_payload=worker_secret_payload,
        )


def _convert_source_cell(
    cell: NotebookSourceCell, export: DatabricksPythonExport, nbformat: Any
) -> Any:
    if cell.kind == "python":
        return nbformat.v4.new_code_cell(cell.source)
    if cell.kind == "sql":
        literal = json.dumps(cell.source, ensure_ascii=False)
        return nbformat.v4.new_code_cell(f"__brickhouse_execute_sql({literal})")
    if cell.kind == "markdown":
        return nbformat.v4.new_markdown_cell(cell.source)
    if cell.kind == "run":
        if cell.target is None:
            raise NotebookSourceFormatError("%run source cell is missing its target path")
        target = json.dumps(cell.target, ensure_ascii=False)
        relative_to = json.dumps(str(export.path.parent), ensure_ascii=False)
        allowed_root = json.dumps(str(export.path.parent), ensure_ascii=False)
        code = (
            "__brickhouse_run_source_notebook("
            f"{target}, relative_to={relative_to}, allowed_root={allowed_root})"
        )
        return nbformat.v4.new_code_cell(code)
    raise NotebookSourceFormatError(f"unknown source notebook cell type: {cell.kind}")


def execute_ipynb(
    notebook_path: str | Path,
    *,
    artifact_path: str | Path,
    worker_context: Mapping[str, object],
    parameters: Mapping[str, object] | None = None,
    interpreter: str | Path | None = None,
    cwd: str | Path | None = None,
    cell_timeout_seconds: int | None = 60,
    execution_timeout_seconds: int | None = None,
    notebook_root: str | Path | None = None,
    worker_service_environment: Mapping[str, str] | None = None,
    worker_secret_payload: bytes | None = None,
) -> NotebookExecutionResult:
    """Execute an ipynb in a fresh kernel and atomically save its output artifact.

    The worker context is copied into a bounded finite-JSON snapshot. The kernel gets
    only that snapshot and the minimal worker environment; it does not inherit caller
    credentials, Python paths, or notebook kernels configured on the host.
    """
    source_path = _resolve_notebook(notebook_path)
    output_path = _resolve_output_path(artifact_path, source_path)
    working_directory = _resolve_working_directory(cwd, source_path.parent)
    allowed_root = _resolve_notebook_root(notebook_root, working_directory)
    selected_interpreter = _resolve_interpreter(interpreter)
    timeout = _validate_cell_timeout(cell_timeout_seconds)
    execution_timeout = _validate_execution_timeout(execution_timeout_seconds)
    context = WorkerContext.from_mapping(worker_context)
    context_values = context.to_mapping()
    service_environment, kernel_service_fds = _validate_worker_service_environment(
        worker_service_environment
    )
    if worker_secret_payload is not None and (
        not isinstance(worker_secret_payload, bytes)
        or not worker_secret_payload
        or len(worker_secret_payload) > MAX_SECRET_SNAPSHOT_BYTES
    ):
        raise ValueError("notebook secret snapshot is empty or exceeds its size limit")
    _validate_notebook_worker_context(
        context_values,
        worker_service_enabled=bool(service_environment),
    )
    _validate_notebook_environment(context, selected_interpreter)
    selected_parameters = _validate_parameters({} if parameters is None else parameters)

    try:
        import nbformat
        from jupyter_client.kernelspec import KernelSpecManager
        from jupyter_client.manager import AsyncKernelManager
        from nbclient import NotebookClient
    except ImportError as error:
        raise NotebookDependencyError(
            "ipynb execution requires the optional notebook dependencies; "
            "install brickhouse[notebooks]"
        ) from error

    try:
        read_notebook = cast(Any, nbformat.read)
        notebook = read_notebook(source_path, as_version=4)
    except Exception as error:
        raise ValueError("notebook file is not valid nbformat JSON") from error
    _validate_ipynb_environment(notebook, context_values)
    if source_path.stat().st_size > _MAX_NOTEBOOK_BYTES:
        raise ValueError(f"notebook exceeds the {_MAX_NOTEBOOK_BYTES}-byte size limit")
    _validate_python_notebook(notebook, nbformat)
    kernel_id = uuid.uuid4().hex
    exit_token = uuid.uuid4().hex
    kernel_name = f"{_KERNEL_NAME}-{kernel_id[:12]}"
    new_code_cell = cast(Any, nbformat.v4.new_code_cell)
    bootstrap_cell = new_code_cell(
        _BOOTSTRAP_SOURCE,
        metadata={"tags": [_BOOTSTRAP_TAG, "hide_input"]},
    )
    notebook.cells.insert(0, bootstrap_cell)
    if selected_parameters:
        parameter_cell = _make_parameter_cell(selected_parameters, nbformat)
        insertion_index = _parameter_insertion_index(notebook, bootstrap_cell)
        notebook.cells.insert(insertion_index, parameter_cell)

    notebook.metadata["kernelspec"] = {
        "display_name": "BrickHouse Python",
        "language": "python",
        "name": kernel_name,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with tempfile.TemporaryDirectory(prefix="brickhouse-notebook-") as runtime_root_text:
            runtime_root = Path(runtime_root_text)
            kernels_root = runtime_root / "kernels"
            spec_root = kernels_root / kernel_name
            spec_root.mkdir(parents=True)
            ipython_dir = runtime_root / "ipython"
            jupyter_config_dir = runtime_root / "jupyter-config"
            jupyter_data_dir = runtime_root / "jupyter-data"
            jupyter_runtime_dir = runtime_root / "jupyter-runtime"
            for directory in (
                ipython_dir,
                jupyter_config_dir,
                jupyter_data_dir,
                jupyter_runtime_dir,
            ):
                directory.mkdir()
            _write_kernel_spec(
                spec_root / "kernel.json",
                interpreter=selected_interpreter,
                ipython_dir=ipython_dir,
                jupyter_config_dir=jupyter_config_dir,
                jupyter_data_dir=jupyter_data_dir,
                jupyter_runtime_dir=jupyter_runtime_dir,
            )
            execution_deadline = (
                time.monotonic() + execution_timeout if execution_timeout is not None else None
            )
            spec_manager = KernelSpecManager(
                kernel_dirs=[str(kernels_root)],
                ensure_native_kernel=False,
            )
            kernel_manager = AsyncKernelManager(
                kernel_name=kernel_name,
                kernel_spec_manager=spec_manager,
            )
            client = NotebookClient(
                notebook,
                km=kernel_manager,
                kernel_name=kernel_name,
                timeout=timeout,
                timeout_func=(
                    _deadline_timeout_func(execution_deadline, timeout)
                    if execution_deadline is not None
                    else None
                ),
                startup_timeout=(
                    min(60, execution_timeout) if execution_timeout is not None else 60
                ),
                allow_errors=False,
                resources={"metadata": {"path": str(working_directory)}},
            )
            # NotebookClient does not own a manager supplied by the caller. This
            # runner created it and must always shut down its fresh kernel.
            client.owns_km = True
            environment = _worker_environment(context)
            environment.update(service_environment)
            environment[_NOTEBOOK_ROOT_ENV] = str(allowed_root)
            environment[_NOTEBOOK_EXIT_TOKEN_ENV] = exit_token
            secret_read_descriptor: int | None = None
            secret_write_descriptor: int | None = None
            secret_writer: threading.Thread | None = None
            kernel_pass_fds = kernel_service_fds
            if worker_secret_payload is not None:
                secret_read_descriptor, secret_write_descriptor = os.pipe()
                environment[_NOTEBOOK_SECRET_FD_ENV] = str(secret_read_descriptor)
                kernel_pass_fds += (secret_read_descriptor,)
                secret_writer = threading.Thread(
                    target=_write_secret_pipe,
                    args=(secret_write_descriptor, worker_secret_payload),
                    name="brickhouse-notebook-secret-pipe",
                    daemon=True,
                )
                secret_writer.start()
            try:
                previous_sigterm_handler = None
                if threading.current_thread() is threading.main_thread():
                    previous_sigterm_handler = signal.getsignal(signal.SIGTERM)
                    signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)
                try:
                    client.execute(
                        cwd=str(working_directory),
                        env=environment,
                        pass_fds=kernel_pass_fds,
                        cleanup_kc=True,
                    )
                finally:
                    if previous_sigterm_handler is not None:
                        signal.signal(signal.SIGTERM, previous_sigterm_handler)
                    if secret_read_descriptor is not None:
                        os.close(secret_read_descriptor)
                    if secret_writer is not None:
                        secret_writer.join(timeout=2)
            except KeyboardInterrupt:
                _abort_kernel_manager(kernel_manager)
                raise
            except Exception as error:
                exit_value = _extract_notebook_exit(notebook, exit_token)
                if exit_value is not None:
                    _remove_notebook_exit_output(notebook, exit_token)
                    _remove_bootstrap_cell(notebook, bootstrap_cell)
                    _write_notebook_atomically(notebook, output_path, nbformat)
                    return NotebookExecutionResult(
                        artifact_path=output_path,
                        kernel_id=kernel_id,
                        executed_code_cells=_executed_code_cell_count(notebook),
                        exit_value=exit_value,
                    )
                _remove_bootstrap_cell(notebook, bootstrap_cell)
                _write_notebook_atomically(notebook, output_path, nbformat)
                if execution_deadline is not None and (
                    _execution_deadline_passed(execution_deadline)
                    or (timeout is None and type(error).__name__ == "CellTimeoutError")
                ):
                    raise NotebookExecutionTimeoutError(output_path) from error
                raise NotebookExecutionError(output_path, type(error).__name__) from error
            except BaseException:
                _abort_kernel_manager(kernel_manager)
                raise
            _remove_bootstrap_cell(notebook, bootstrap_cell)
            _write_notebook_atomically(notebook, output_path, nbformat)
            if execution_deadline is not None and _execution_deadline_passed(execution_deadline):
                raise NotebookExecutionTimeoutError(output_path)
            return NotebookExecutionResult(
                artifact_path=output_path,
                kernel_id=kernel_id,
                executed_code_cells=_executed_code_cell_count(notebook),
            )
    except NotebookExecutionTimeoutError:
        raise
    except NotebookExecutionError:
        raise
    except Exception as error:
        if "notebook" in locals():
            _remove_bootstrap_cell(notebook, bootstrap_cell)
            _write_notebook_atomically(notebook, output_path, nbformat)
        raise NotebookExecutionError(output_path, type(error).__name__) from error


def _resolve_notebook(path: str | Path) -> Path:
    if not isinstance(path, (str, Path)):
        raise ValueError("notebook path must be a filesystem path")
    candidate = Path(path).expanduser()
    try:
        resolved = candidate.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise ValueError("notebook file does not exist") from error
    if not resolved.is_file() or resolved.suffix.casefold() != ".ipynb":
        raise ValueError("notebook path must name an existing .ipynb file")
    if resolved.stat().st_size > _MAX_NOTEBOOK_BYTES:
        raise ValueError(f"notebook exceeds the {_MAX_NOTEBOOK_BYTES}-byte size limit")
    return resolved


def _resolve_output_path(path: str | Path, source_path: Path) -> Path:
    if not isinstance(path, (str, Path)):
        raise ValueError("notebook artifact path must be a filesystem path")
    candidate = Path(path).expanduser()
    if candidate.suffix.casefold() != ".ipynb":
        raise ValueError("notebook artifact path must end in .ipynb")
    output_path = (candidate.parent.resolve(strict=False) / candidate.name).absolute()
    if output_path == source_path:
        raise ValueError("notebook artifact path must not overwrite its input")
    return output_path


def _resolve_working_directory(path: str | Path | None, default: Path) -> Path:
    candidate = default if path is None else Path(path).expanduser()
    try:
        resolved = candidate.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise ValueError("notebook working directory does not exist") from error
    if not resolved.is_dir():
        raise ValueError("notebook working directory must be a directory")
    return resolved


def _resolve_child_notebook(
    path: str,
    *,
    relative_to: str | Path,
    allowed_root: str | Path,
) -> Path:
    if not isinstance(path, str) or not path or "\x00" in path:
        raise ValueError("notebook path must be a non-empty relative path")
    requested = Path(path).expanduser()
    if requested.is_absolute():
        raise ValueError("local child notebooks must use relative paths")
    if requested.suffix and requested.suffix.casefold() not in {".ipynb", ".py"}:
        raise ValueError("local child notebooks must be .ipynb or marked Python .py exports")
    try:
        base = Path(relative_to).resolve(strict=True)
        root = Path(allowed_root).resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise ValueError("local notebook source root is unavailable") from error
    if not base.is_dir() or not root.is_dir():
        raise ValueError("local notebook source roots must be directories")
    if base != root and root not in base.parents:
        raise ValueError("local notebook directory is outside the allowed source root")

    candidates = (
        [requested]
        if requested.suffix
        else [requested.with_suffix(".ipynb"), requested.with_suffix(".py")]
    )
    found: list[Path] = []
    for candidate in candidates:
        try:
            resolved = (base / candidate).resolve(strict=True)
        except (OSError, RuntimeError):
            continue
        if resolved != root and root not in resolved.parents:
            raise ValueError("local child notebook path escapes the allowed source root")
        if resolved.is_file() and resolved.suffix.casefold() in {".ipynb", ".py"}:
            found.append(resolved)
    if len(found) > 1:
        raise ValueError("local child notebook path is ambiguous; include its file extension")
    if not found:
        raise FileNotFoundError("local child notebook does not exist")
    return found[0]


def run_local_child_notebook(
    path: str,
    timeout_seconds: int,
    arguments: Mapping[str, str],
    *,
    relative_to: str | Path,
    allowed_root: str | Path,
    worker_context: Mapping[str, object],
    worker_service_environment: Mapping[str, str] | None = None,
) -> str:
    """Execute a child notebook in a new kernel and return its `dbutils.notebook.exit` value."""
    source_path = _resolve_child_notebook(
        path,
        relative_to=relative_to,
        allowed_root=allowed_root,
    )
    context = dict(worker_context)
    dbutils_context = context.get("dbutils")
    if not isinstance(dbutils_context, Mapping):
        raise ValueError("notebook child context is missing dbutils configuration")
    child_dbutils_context = dict(dbutils_context)
    inherited_parameters = child_dbutils_context.get("widget_parameters")
    if not isinstance(inherited_parameters, Mapping):
        raise ValueError("notebook child context has malformed widget parameters")
    selected_parameters = {
        key: value for key, value in inherited_parameters.items() if isinstance(key, str)
    }
    selected_parameters.update(arguments)
    child_dbutils_context["widget_parameters"] = selected_parameters
    context["dbutils"] = child_dbutils_context

    with tempfile.TemporaryDirectory(prefix="brickhouse-notebook-child-") as temporary_root:
        artifact_path = Path(temporary_root) / "executed-child.ipynb"
        execution_options: dict[str, object] = {
            "artifact_path": artifact_path,
            "worker_context": context,
            "cwd": source_path.parent,
            "cell_timeout_seconds": None,
            "execution_timeout_seconds": timeout_seconds,
            "notebook_root": allowed_root,
            "worker_service_environment": worker_service_environment,
        }
        if source_path.suffix.casefold() == ".py":
            try:
                result = execute_databricks_python_source(
                    source_path,
                    **cast(Any, execution_options),
                )
            except NotebookExecutionTimeoutError:
                raise NotebookRunTimeoutError("child notebook exceeded timeout_seconds") from None
            except NotebookExecutionError as error:
                raise NotebookRunError(error.error_type) from None
        else:
            try:
                result = execute_ipynb(source_path, **cast(Any, execution_options))
            except NotebookExecutionTimeoutError:
                raise NotebookRunTimeoutError("child notebook exceeded timeout_seconds") from None
            except NotebookExecutionError as error:
                raise NotebookRunError(error.error_type) from None
        return result.exit_value or ""


def _resolve_interpreter(path: str | Path | None) -> Path:
    candidate = Path(sys.executable if path is None else path).expanduser().absolute()
    if not candidate.is_file() or (os.name != "nt" and not os.access(candidate, os.X_OK)):
        raise ValueError("notebook kernel interpreter must be an executable file")
    return candidate


def _validate_cell_timeout(value: int | None) -> int | None:
    if value is None:
        return None
    if type(value) is not int or not 1 <= value <= MAX_WORKER_TIMEOUT_SECONDS:
        raise ValueError(
            f"cell_timeout_seconds must be an integer from 1 to {MAX_WORKER_TIMEOUT_SECONDS}"
        )
    return value


def _validate_execution_timeout(value: int | None) -> int | None:
    if value is None or value == 0:
        return None
    if type(value) is not int or not 1 <= value <= MAX_WORKER_TIMEOUT_SECONDS:
        raise ValueError(
            "execution_timeout_seconds must be 0 or an integer from 1 to "
            f"{MAX_WORKER_TIMEOUT_SECONDS}"
        )
    return value


def _resolve_notebook_root(path: str | Path | None, cwd: Path) -> Path:
    candidate = cwd if path is None else Path(path).expanduser()
    try:
        resolved = candidate.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise ValueError("notebook root does not exist") from error
    if not resolved.is_dir():
        raise ValueError("notebook root must be a directory")
    if cwd != resolved and resolved not in cwd.parents:
        raise ValueError("notebook working directory must be inside the notebook root")
    return resolved


def _deadline_timeout_func(
    deadline: float,
    cell_timeout_seconds: int | None,
) -> Any:
    def timeout_for_cell(_cell: Any) -> int:
        remaining = max(1, math.ceil(deadline - time.monotonic()))
        if cell_timeout_seconds is None:
            return remaining
        return min(remaining, cell_timeout_seconds)

    return timeout_for_cell


def _execution_deadline_passed(deadline: float) -> bool:
    # The notebook client uses a per-cell timeout based on the same monotonic clock.
    # This final check covers time spent starting a kernel or handling empty notebooks.
    return time.monotonic() >= deadline


def _executed_code_cell_count(notebook: Any) -> int:
    return sum(
        cell.cell_type == "code"
        and cell.execution_count is not None
        and _PARAMETERS_TAG not in cell.get("metadata", {}).get("tags", [])
        for cell in notebook.cells
    )


def _validate_worker_service_environment(
    value: Mapping[str, str] | None,
) -> tuple[dict[str, str], tuple[int, ...]]:
    if value is None:
        return {}, ()
    if not isinstance(value, Mapping) or set(value) != {WORKER_TOKEN_ENV, WORKER_SOCKET_ENV}:
        raise ValueError("notebook worker service environment is malformed")
    token = value.get(WORKER_TOKEN_ENV)
    descriptor_value = value.get(WORKER_SOCKET_ENV)
    if not isinstance(token, str) or not token or not isinstance(descriptor_value, str):
        raise ValueError("notebook worker service environment is malformed")
    if os.name != "posix":
        raise ValueError("notebook worker services require POSIX file descriptor passing")
    try:
        descriptor = int(descriptor_value)
        os.fstat(descriptor)
    except (OSError, ValueError) as error:
        raise ValueError("notebook worker service descriptor is unavailable") from error
    if descriptor < 0:
        raise ValueError("notebook worker service descriptor is invalid")
    return {WORKER_TOKEN_ENV: token, WORKER_SOCKET_ENV: descriptor_value}, (descriptor,)


def _validate_notebook_worker_context(
    context: Mapping[str, object],
    *,
    worker_service_enabled: bool,
) -> None:
    dbutils_context = context.get("dbutils")
    if not isinstance(dbutils_context, dict):
        raise ValueError("notebook worker context must include dbutils configuration")
    if set(dbutils_context) - {"dbfs_root", "widget_parameters", "task_values_enabled"}:
        raise ValueError("notebook dbutils context contains unsupported fields")
    root = dbutils_context.get("dbfs_root")
    widget_parameters = dbutils_context.get("widget_parameters")
    task_values_enabled = dbutils_context.get("task_values_enabled", False)
    if not isinstance(root, str) or not Path(root).is_absolute():
        raise ValueError("notebook dbutils context must name an absolute DBFS root")
    if not isinstance(widget_parameters, dict) or any(
        not isinstance(name, str) or not isinstance(value, str)
        for name, value in widget_parameters.items()
    ):
        raise ValueError("notebook widget parameters must be string values")
    if type(task_values_enabled) is not bool:
        raise ValueError("notebook task-values configuration must be a boolean")
    if task_values_enabled and not worker_service_enabled:
        raise ValueError("notebook task values require the authenticated notebook service")
    execution_context = context.get("execution_context")
    if execution_context is not None and not isinstance(execution_context, Mapping):
        raise ValueError("notebook worker execution context is malformed")
    validate_worker_spark_session_context(context)


def _validate_source_export_environment(
    source_export: DatabricksPythonExport,
    context: Mapping[str, object],
) -> None:
    nested_context = context.get("execution_context")
    execution_context = nested_context if isinstance(nested_context, Mapping) else context
    if execution_context.get("compute_mode") != "serverless":
        return
    environment_source = execution_context.get("environment_source")
    if environment_source == "job":
        return
    if environment_source == "notebook" and source_export.environment_version is None:
        raise NotebookEnvironmentError(
            "serverless notebook source is missing its declared environment metadata"
        )
    if source_export.environment_version is None:
        return
    if source_export.environment_version != execution_context.get("environment_version") or list(
        source_export.dependencies
    ) != execution_context.get("dependencies"):
        raise NotebookEnvironmentError(
            "serverless source-export environment does not match the selected notebook environment"
        )


def _validate_ipynb_environment(notebook: Any, context: Mapping[str, object]) -> None:
    nested_context = context.get("execution_context")
    execution_context = nested_context if isinstance(nested_context, Mapping) else context
    if execution_context.get("compute_mode") != "serverless":
        return
    environment_source = execution_context.get("environment_source")
    if environment_source == "job":
        return
    metadata = notebook.get("metadata", {})
    databricks = metadata.get("application/vnd.databricks.v1+notebook", {})
    specification = (
        databricks.get("environmentMetadata") if isinstance(databricks, Mapping) else None
    )
    if specification is None:
        if environment_source == "notebook":
            raise NotebookEnvironmentError(
                "serverless notebook is missing its declared environment metadata"
            )
        return
    if not isinstance(specification, Mapping):
        raise NotebookEnvironmentError("notebook environment metadata must be an object")
    version = specification.get("environment_version")
    dependencies = specification.get("dependencies", [])
    if version != execution_context.get(
        "environment_version"
    ) or dependencies != execution_context.get("dependencies"):
        raise NotebookEnvironmentError(
            "serverless notebook environment does not match the selected local environment"
        )


def _raise_keyboard_interrupt(signum: int, frame: Any) -> None:
    del signum, frame
    raise KeyboardInterrupt


def _write_secret_pipe(descriptor: int, payload: bytes) -> None:
    try:
        remaining = memoryview(payload)
        while remaining:
            written = os.write(descriptor, remaining)
            remaining = remaining[written:]
    except OSError:
        pass
    finally:
        os.close(descriptor)


def _validate_notebook_environment(context: WorkerContext, interpreter: Path) -> None:
    values = context.to_mapping()
    nested_context = values.get("execution_context")
    if nested_context is not None and not isinstance(nested_context, Mapping):
        raise NotebookEnvironmentError("notebook worker execution context is malformed")
    execution_context = nested_context if isinstance(nested_context, Mapping) else values
    mode = execution_context.get("compute_mode")
    environment_profile = execution_context.get("environment_profile")
    environment_version = execution_context.get("environment_version")
    raw_dependencies = execution_context.get("dependencies", [])

    if not isinstance(raw_dependencies, Sequence) or isinstance(
        raw_dependencies, (str, bytes, bytearray)
    ):
        raise NotebookEnvironmentError("notebook environment dependencies must be a string list")
    dependencies: dict[str, str] = {}
    for requirement in raw_dependencies:
        if not isinstance(requirement, str):
            raise NotebookEnvironmentError("notebook environment dependencies must be exact pins")
        matched = _REQUIREMENT_PATTERN.fullmatch(requirement)
        if matched is None:
            raise NotebookEnvironmentError("notebook environment dependencies must be exact pins")
        package_name = matched.group(1).casefold().replace("_", "-").replace(".", "-")
        if package_name in dependencies:
            raise NotebookEnvironmentError("notebook environment dependencies cannot be duplicated")
        dependencies[package_name] = matched.group(2)

    if mode == "serverless":
        if not isinstance(environment_profile, str) or environment_version != "1":
            raise NotebookEnvironmentError(
                "serverless notebook tasks require a mapped local environment version 1"
            )
    elif mode == "classic":
        if environment_profile is not None or environment_version is not None or dependencies:
            raise NotebookEnvironmentError(
                "classic notebook tasks do not accept a serverless environment profile"
            )
    else:
        raise NotebookEnvironmentError("notebook compute mode is not mapped")

    versions = _inspect_kernel_environment(interpreter, context)
    python_major = versions.get("python_major")
    python_minor = versions.get("python_minor")
    if type(python_major) is not int or type(python_minor) is not int:
        raise NotebookEnvironmentError(
            "selected notebook interpreter did not report Python version"
        )
    if mode == "serverless":
        expected_profile = f"serverless-python{python_major}{python_minor}-local-v1"
        if environment_profile != expected_profile:
            raise NotebookEnvironmentError(
                "serverless notebook environment profile does not match the selected interpreter"
            )

    packages = versions.get("packages")
    if not isinstance(packages, Mapping):
        raise NotebookEnvironmentError("selected notebook interpreter did not report packages")
    for package_name, expected_version in _REQUIRED_KERNEL_DISTRIBUTIONS.items():
        installed = packages.get(package_name)
        if installed != expected_version:
            raise NotebookDependencyError(
                f"selected notebook interpreter requires {package_name}=={expected_version}"
            )
    for normalized_name, expected_version in dependencies.items():
        installed = packages.get(normalized_name)
        if installed != expected_version:
            raise NotebookEnvironmentError(
                "a requested notebook environment dependency is not installed at its pinned version"
            )


def _inspect_kernel_environment(
    interpreter: Path,
    context: WorkerContext,
) -> Mapping[str, object]:
    source = (
        "import importlib.metadata as metadata, json, sys\n"
        "names = ('ipykernel', 'sparkless', 'deltalake', 'pyarrow')\n"
        "packages = {}\n"
        "for name in names:\n"
        "    try:\n"
        "        packages[name] = metadata.version(name)\n"
        "    except metadata.PackageNotFoundError:\n"
        "        packages[name] = None\n"
        "print('__BRICKHOUSE_ENV__' + json.dumps({"
        "'python_major': sys.version_info.major, 'python_minor': sys.version_info.minor, "
        "'packages': packages}, separators=(',', ':')))\n"
    )
    try:
        completed = subprocess.run(
            [str(interpreter), "-I", "-c", source],
            env=_worker_environment(context),
            check=False,
            capture_output=True,
            text=True,
            timeout=_KERNEL_ENVIRONMENT_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise NotebookEnvironmentError(
            "selected notebook interpreter could not be checked before kernel startup"
        ) from error
    marker = "__BRICKHOUSE_ENV__"
    line = next((line for line in completed.stdout.splitlines() if line.startswith(marker)), None)
    if completed.returncode != 0 or line is None:
        raise NotebookEnvironmentError(
            "selected notebook interpreter could not report its mapped local environment"
        )
    try:
        result = json.loads(line[len(marker) :])
    except json.JSONDecodeError as error:
        raise NotebookEnvironmentError(
            "selected notebook interpreter returned invalid environment metadata"
        ) from error
    if not isinstance(result, dict):
        raise NotebookEnvironmentError("selected notebook environment metadata is malformed")
    return cast(Mapping[str, object], result)


def _validate_python_notebook(notebook: Any, nbformat: Any) -> None:
    try:
        nbformat.validate(notebook)
    except Exception as error:
        raise ValueError("notebook contents do not satisfy nbformat validation") from error
    language = notebook.metadata.get("language_info", {}).get("name")
    kernelspec = notebook.metadata.get("kernelspec", {})
    kernel_language = kernelspec.get("language") if isinstance(kernelspec, Mapping) else None
    if language not in (None, "python") or kernel_language not in (None, "python"):
        raise ValueError("only Python ipynb notebooks are supported")
    parameter_cells = [
        cell
        for cell in notebook.cells
        if cell.cell_type == "code" and "parameters" in cell.get("metadata", {}).get("tags", [])
    ]
    if len(parameter_cells) > 1:
        raise ValueError("notebook may contain only one cell tagged 'parameters'")


def _validate_parameters(parameters: Mapping[str, object]) -> dict[str, object]:
    if not isinstance(parameters, Mapping):
        raise ValueError("notebook parameters must be a mapping")
    selected: dict[str, object] = {}
    for name, value in parameters.items():
        if (
            not isinstance(name, str)
            or not name.isidentifier()
            or keyword.iskeyword(name)
            or name.startswith("_")
            or name in _RESERVED_PARAMETERS
        ):
            raise ValueError(f"notebook parameter name is invalid or reserved: {name!r}")
        selected[name] = value
    try:
        serialized = json.dumps(
            selected,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as error:
        raise ValueError("notebook parameters must contain finite JSON values") from error
    if len(serialized.encode("utf-8")) > _MAX_PARAMETER_BYTES:
        raise ValueError(f"notebook parameters exceed the {_MAX_PARAMETER_BYTES}-byte limit")
    return selected


def _make_parameter_cell(parameters: Mapping[str, object], nbformat: Any) -> Any:
    serialized = json.dumps(
        parameters,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    source = (
        "import json as __brickhouse_json\n"
        f"__brickhouse_parameters = __brickhouse_json.loads({serialized!r})\n"
        "globals().update(__brickhouse_parameters)\n"
        "del __brickhouse_parameters, __brickhouse_json\n"
    )
    return nbformat.v4.new_code_cell(
        source,
        metadata={"tags": [_PARAMETERS_TAG]},
    )


def _parameter_insertion_index(notebook: Any, bootstrap_cell: Any) -> int:
    parameter_indices = [
        index
        for index, cell in enumerate(notebook.cells)
        if cell is not bootstrap_cell
        and cell.cell_type == "code"
        and "parameters" in cell.get("metadata", {}).get("tags", [])
    ]
    return parameter_indices[0] + 1 if parameter_indices else 1


def _write_kernel_spec(
    path: Path,
    *,
    interpreter: Path,
    ipython_dir: Path,
    jupyter_config_dir: Path,
    jupyter_data_dir: Path,
    jupyter_runtime_dir: Path,
) -> None:
    spec = {
        "argv": [
            str(interpreter),
            "-m",
            "ipykernel_launcher",
            "--ipython-dir",
            str(ipython_dir),
            "-f",
            "{connection_file}",
        ],
        "display_name": "BrickHouse Python",
        "language": "python",
        "env": {
            "IPYTHONDIR": str(ipython_dir),
            "JUPYTER_CONFIG_DIR": str(jupyter_config_dir),
            "JUPYTER_DATA_DIR": str(jupyter_data_dir),
            "JUPYTER_RUNTIME_DIR": str(jupyter_runtime_dir),
        },
    }
    path.write_text(json.dumps(spec, separators=(",", ":")), encoding="utf-8")


def _remove_bootstrap_cell(notebook: Any, cell: Any) -> None:
    notebook.cells = [existing for existing in notebook.cells if existing is not cell]


def _extract_notebook_exit(notebook: Any, token: str) -> str | None:
    prefix = f"{token}:"
    for cell in notebook.cells:
        for output in cell.get("outputs", []):
            if output.get("output_type") != "error" or output.get("ename") != "NotebookExitSignal":
                continue
            encoded = output.get("evalue")
            if not isinstance(encoded, str) or not encoded.startswith(prefix):
                continue
            try:
                decoded = base64.b64decode(encoded[len(prefix) :], altchars=b"-_", validate=True)
                return decoded.decode("utf-8")
            except (binascii.Error, UnicodeDecodeError):
                return None
    return None


def _remove_notebook_exit_output(notebook: Any, token: str) -> None:
    prefix = f"{token}:"
    for cell in notebook.cells:
        cell.outputs = [
            output
            for output in cell.get("outputs", [])
            if not (
                output.get("output_type") == "error"
                and output.get("ename") == "NotebookExitSignal"
                and isinstance(output.get("evalue"), str)
                and output.get("evalue").startswith(prefix)
            )
        ]


def _abort_kernel_manager(kernel_manager: Any) -> None:
    """Stop an owned kernel process group when its caller is interrupted."""
    try:
        kernel_manager.shutdown_kernel(now=True)
        return
    except BaseException:
        provisioner = getattr(kernel_manager, "provisioner", None)
        process = getattr(provisioner, "process", None)
        if process is None:
            return
        try:
            if process.poll() is None:
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
        except (OSError, ProcessLookupError):
            pass
        try:
            process.wait(timeout=5)
        except (OSError, subprocess.TimeoutExpired):
            pass


def _write_notebook_atomically(notebook: Any, path: Path, nbformat: Any) -> None:
    temporary_name: str | None = None
    try:
        serialized = nbformat.writes(notebook, version=4)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=path.parent,
        )
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(serialized)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, path)
        temporary_name = None
    except OSError as error:
        raise RuntimeError("could not persist the executed notebook artifact") from error
    finally:
        if temporary_name is not None:
            Path(temporary_name).unlink(missing_ok=True)
