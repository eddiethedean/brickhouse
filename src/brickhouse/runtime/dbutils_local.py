"""Worker-local dbutils.fs and dbutils.widgets implementations.

These helpers operate on BrickHouse-owned local state. They are deliberately
constructed inside each job worker and do not alter the parent interpreter.
"""

from __future__ import annotations

import base64
import json
import math
import os
import shutil
import stat
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from brickhouse.runtime.secrets import SecretAccessDeniedError, SecretNotFoundError
from brickhouse.runtime.worker_services import WorkerServiceClient, WorkerServiceError

MAX_DBUTILS_PATH_BYTES = 4096
MAX_DBUTILS_FILE_BYTES = 64 * 1024 * 1024
MAX_DBUTILS_HEAD_BYTES = 1024 * 1024
MAX_DBUTILS_TREE_ENTRIES = 10_000
MAX_NOTEBOOK_RUN_ARGUMENTS = 16 * 1024
MAX_NOTEBOOK_EXIT_VALUE_BYTES = 1024 * 1024
MAX_NOTEBOOK_RUN_TIMEOUT_SECONDS = 24 * 60 * 60
MAX_NOTEBOOK_PATH_BYTES = 4096


class DbutilsOperationNotSupportedError(NotImplementedError):
    """A dbutils operation is outside the implemented local worker profile."""


class DbutilsPathError(ValueError):
    """A path is outside the supported DBFS namespace or crosses its root."""


class NotebookExitSignal(Exception):
    """Private control signal used to stop a child notebook successfully."""

    def __init__(self, token: str, value: str) -> None:
        encoded = base64.urlsafe_b64encode(value.encode("utf-8")).decode("ascii")
        super().__init__(f"{token}:{encoded}")
        self.value = value


class NotebookRunError(RuntimeError):
    """A child notebook failed without exposing its temporary output path."""

    def __init__(self, error_type: str) -> None:
        super().__init__(f"child notebook execution failed ({error_type})")
        self.error_type = error_type


class NotebookRunTimeoutError(TimeoutError):
    """A child notebook exceeded the caller's timeout."""


class LocalDbutilsNotebook:
    """The isolated local subset of ``dbutils.notebook``."""

    def __init__(
        self,
        run_callback: Callable[[str, int, Mapping[str, str]], str],
        exit_token: str,
    ) -> None:
        self._run_callback = run_callback
        self._exit_token = exit_token

    def run(
        self,
        path: str,
        timeout_seconds: int = 0,
        arguments: Mapping[str, str] | None = None,
    ) -> str:
        """Run one isolated local child notebook and return its string exit value."""
        if not isinstance(path, str) or not path or "\x00" in path:
            raise ValueError("notebook path must be a non-empty string")
        if len(path.encode("utf-8")) > MAX_NOTEBOOK_PATH_BYTES:
            raise ValueError(f"notebook path exceeds the {MAX_NOTEBOOK_PATH_BYTES}-byte limit")
        if (
            type(timeout_seconds) is not int
            or timeout_seconds < 0
            or timeout_seconds > MAX_NOTEBOOK_RUN_TIMEOUT_SECONDS
        ):
            raise ValueError(
                f"timeout_seconds must be from 0 to {MAX_NOTEBOOK_RUN_TIMEOUT_SECONDS}"
            )
        if arguments is None:
            selected_arguments: dict[str, str] = {}
        elif isinstance(arguments, Mapping):
            selected_arguments = dict(arguments)
        else:
            raise TypeError("notebook arguments must be a string-to-string mapping")
        if any(
            not isinstance(key, str)
            or not isinstance(value, str)
            or not key.isascii()
            or not value.isascii()
            or "\x00" in key
            or "\x00" in value
            for key, value in selected_arguments.items()
        ):
            raise ValueError("notebook argument names and values must be ASCII strings")
        try:
            encoded_size = len(
                json.dumps(selected_arguments, separators=(",", ":"), sort_keys=True).encode(
                    "ascii"
                )
            )
        except (TypeError, UnicodeEncodeError) as error:
            raise ValueError("notebook arguments must be an ASCII string mapping") from error
        if encoded_size > MAX_NOTEBOOK_RUN_ARGUMENTS:
            raise ValueError(
                f"notebook arguments exceed the {MAX_NOTEBOOK_RUN_ARGUMENTS}-byte limit"
            )
        return self._run_callback(path, timeout_seconds, selected_arguments)

    def exit(self, value: str = "") -> None:
        """Stop the current notebook and return a string to its caller."""
        if not isinstance(value, str):
            raise TypeError("notebook exit value must be a string")
        try:
            encoded_size = len(value.encode("utf-8"))
        except UnicodeEncodeError as error:
            raise ValueError("notebook exit value must be valid UTF-8") from error
        if encoded_size > MAX_NOTEBOOK_EXIT_VALUE_BYTES:
            raise ValueError(
                f"notebook exit value exceeds the {MAX_NOTEBOOK_EXIT_VALUE_BYTES}-byte limit"
            )
        raise NotebookExitSignal(self._exit_token, value)

    def __getattr__(self, name: str) -> Any:
        raise DbutilsOperationNotSupportedError(
            f"dbutils.notebook.{name} is not implemented by the local worker profile"
        )


@dataclass(frozen=True, slots=True)
class FileInfo:
    """The supported immutable subset of Databricks ``FileInfo``."""

    path: str
    name: str
    size: int
    isDir: bool
    isFile: bool
    modificationTime: int


class LocalDbutilsFs:
    """Map ``dbfs:/`` and ``/dbfs/`` to one symlink-checked local root."""

    def __init__(self, root: str | os.PathLike[str]) -> None:
        candidate = Path(root).expanduser()
        try:
            if candidate.is_symlink():
                raise DbutilsPathError("the local DBFS root cannot be a symbolic link")
            candidate.mkdir(parents=True, exist_ok=True)
            self._root = candidate.resolve(strict=True)
        except (OSError, RuntimeError) as error:
            raise DbutilsPathError("the local DBFS root is unavailable") from error
        if not self._root.is_dir():
            raise DbutilsPathError("the local DBFS root must be a directory")

    def ls(self, path: str) -> list[FileInfo]:
        """List one directory and return stable, local file metadata."""
        directory = self._resolve(path)
        if not directory.is_dir():
            raise NotADirectoryError(path)
        result: list[FileInfo] = []
        for entry in os.scandir(directory):
            if entry.is_symlink():
                raise DbutilsPathError("DBFS paths cannot traverse symbolic links")
            info = entry.stat(follow_symlinks=False)
            is_dir = stat.S_ISDIR(info.st_mode)
            is_file = stat.S_ISREG(info.st_mode)
            if not (is_dir or is_file):
                raise DbutilsPathError("DBFS supports regular files and directories only")
            relative = (
                Path(entry.name)
                if directory == self._root
                else directory.relative_to(self._root) / entry.name
            )
            logical_path = self._format_path(relative)
            if is_dir:
                logical_path += "/"
            result.append(
                FileInfo(
                    path=logical_path,
                    name=entry.name,
                    size=info.st_size,
                    isDir=is_dir,
                    isFile=is_file,
                    modificationTime=info.st_mtime_ns // 1_000_000,
                )
            )
        return sorted(result, key=lambda item: item.name)

    def head(self, path: str, max_bytes: int = 65_536) -> str:
        """Read a bounded UTF-8 preview of a regular file."""
        if type(max_bytes) is not int or not 0 <= max_bytes <= MAX_DBUTILS_HEAD_BYTES:
            raise ValueError(f"max_bytes must be from 0 to {MAX_DBUTILS_HEAD_BYTES}")
        source = self._resolve(path)
        if not source.is_file():
            raise IsADirectoryError(path)
        with source.open("rb") as stream:
            return stream.read(max_bytes).decode("utf-8", errors="replace")

    def put(self, path: str, contents: str, overwrite: bool = False) -> bool:
        """Write UTF-8 text atomically to the mapped local DBFS."""
        if not isinstance(contents, str):
            raise TypeError("contents must be a string")
        if type(overwrite) is not bool:
            raise TypeError("overwrite must be a boolean")
        encoded = contents.encode("utf-8")
        if len(encoded) > MAX_DBUTILS_FILE_BYTES:
            raise ValueError(f"contents exceed the {MAX_DBUTILS_FILE_BYTES}-byte limit")
        target = self._resolve(path)
        self._require_parent(target)
        if target.exists() and target.is_dir():
            raise IsADirectoryError(path)
        if target.exists() and not overwrite:
            raise FileExistsError(path)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".brickhouse-dbutils-", dir=target.parent
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            self._resolve(path)
            if overwrite:
                os.replace(temporary, target)
            else:
                os.link(temporary, target, follow_symlinks=False)
                temporary.unlink()
        except FileExistsError:
            temporary.unlink(missing_ok=True)
            raise
        except OSError:
            temporary.unlink(missing_ok=True)
            raise
        return True

    def cp(self, source: str, destination: str, recurse: bool = False) -> bool:
        """Copy a file, or a bounded directory tree when ``recurse`` is true."""
        return self._copy_or_move(source, destination, recurse=recurse, move=False)

    def mv(self, source: str, destination: str, recurse: bool = False) -> bool:
        """Move a file, or a directory tree when ``recurse`` is true."""
        return self._copy_or_move(source, destination, recurse=recurse, move=True)

    def rm(self, path: str, recurse: bool = False) -> bool:
        """Remove a mapped file or directory; missing paths return ``False``."""
        if type(recurse) is not bool:
            raise TypeError("recurse must be a boolean")
        target = self._resolve(path)
        if target == self._root:
            raise DbutilsPathError("the local DBFS root cannot be removed")
        if not target.exists():
            return False
        if target.is_dir():
            if recurse:
                self._validate_tree(target)
                shutil.rmtree(target)
            else:
                target.rmdir()
        else:
            target.unlink()
        return True

    def mkdirs(self, path: str) -> bool:
        """Create a directory and its parents under the local DBFS root."""
        target = self._resolve(path)
        target.mkdir(parents=True, exist_ok=True)
        self._resolve(path)
        if not target.is_dir():
            raise FileExistsError(path)
        return True

    def _copy_or_move(self, source: str, destination: str, *, recurse: bool, move: bool) -> bool:
        if type(recurse) is not bool:
            raise TypeError("recurse must be a boolean")
        origin = self._resolve(source)
        target = self._resolve(destination)
        if not origin.exists():
            raise FileNotFoundError(source)
        if origin == self._root:
            raise DbutilsPathError("the local DBFS root cannot be copied or moved")
        is_directory = origin.is_dir()
        if is_directory and not recurse:
            raise IsADirectoryError("directory operations require recurse=True")
        if not is_directory and not origin.is_file():
            raise DbutilsPathError("DBFS supports regular files and directories only")
        if target.is_dir():
            target = target / origin.name
            target = self._resolve(self._format_path(target.relative_to(self._root)))
        self._require_parent(target)
        if is_directory:
            if target == origin or origin in target.parents:
                raise DbutilsPathError("a directory cannot be copied or moved inside itself")
            if target.exists():
                raise FileExistsError(str(target))
            entries, total_bytes = self._validate_tree(origin)
            if entries > MAX_DBUTILS_TREE_ENTRIES or total_bytes > MAX_DBUTILS_FILE_BYTES:
                raise ValueError("DBFS directory operation exceeds its local size limits")
            if move:
                os.rename(origin, target)
            else:
                shutil.copytree(origin, target, symlinks=False)
                self._resolve(self._format_path(target.relative_to(self._root)))
            return True
        if origin.stat().st_size > MAX_DBUTILS_FILE_BYTES:
            raise ValueError(f"source exceeds the {MAX_DBUTILS_FILE_BYTES}-byte limit")
        if move:
            os.replace(origin, target)
            return True
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".brickhouse-dbutils-", dir=target.parent
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as output, origin.open("rb") as input_stream:
                shutil.copyfileobj(input_stream, output, length=1024 * 1024)
                output.flush()
                os.fsync(output.fileno())
            self._resolve(self._format_path(target.relative_to(self._root)))
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
        return True

    def _validate_tree(self, directory: Path) -> tuple[int, int]:
        count = 0
        total_bytes = 0
        for current, directories, filenames in os.walk(directory, followlinks=False):
            current_path = Path(current)
            for name in (*directories, *filenames):
                candidate = current_path / name
                if candidate.is_symlink():
                    raise DbutilsPathError("DBFS directory operations cannot cross symbolic links")
                info = candidate.stat(follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    continue
                if not stat.S_ISREG(info.st_mode):
                    raise DbutilsPathError("DBFS supports regular files and directories only")
                count += 1
                total_bytes += info.st_size
                if count > MAX_DBUTILS_TREE_ENTRIES or total_bytes > MAX_DBUTILS_FILE_BYTES:
                    raise ValueError("DBFS directory operation exceeds its local size limits")
        return count, total_bytes

    def _require_parent(self, target: Path) -> None:
        if target.parent != self._root and not target.parent.is_dir():
            raise FileNotFoundError(str(target.parent))
        self._resolve(self._format_path(target.parent.relative_to(self._root)))

    def _resolve(self, path: str) -> Path:
        relative = self._relative_path(path)
        candidate = self._root.joinpath(*relative)
        current = self._root
        for index, part in enumerate(relative):
            current = current / part
            try:
                mode = current.lstat().st_mode
            except FileNotFoundError:
                continue
            if stat.S_ISLNK(mode):
                raise DbutilsPathError("DBFS paths cannot traverse symbolic links")
            if index < len(relative) - 1 and not stat.S_ISDIR(mode):
                raise NotADirectoryError(str(current))
        resolved = candidate.resolve(strict=False)
        if not resolved.is_relative_to(self._root):
            raise DbutilsPathError("DBFS path escapes its local workspace root")
        return candidate

    @staticmethod
    def _relative_path(path: str) -> tuple[str, ...]:
        if (
            not isinstance(path, str)
            or not path
            or len(path.encode("utf-8")) > MAX_DBUTILS_PATH_BYTES
        ):
            raise DbutilsPathError("DBFS path must be a non-empty bounded string")
        if "\x00" in path or "\\" in path:
            raise DbutilsPathError("DBFS path contains an unsupported character")
        if path == "dbfs:/" or path == "/dbfs":
            return ()
        if path.startswith("dbfs:/"):
            raw = path[len("dbfs:/") :]
        elif path.startswith("/dbfs/"):
            raw = path[len("/dbfs/") :]
        else:
            raise DbutilsPathError("only dbfs:/ and /dbfs/ paths are supported")
        if raw.endswith("/"):
            raw = raw[:-1]
        parts = raw.split("/")
        if not raw or any(part in {"", ".", ".."} for part in parts):
            raise DbutilsPathError("DBFS paths must be normalized and cannot traverse parents")
        return tuple(parts)

    @staticmethod
    def _format_path(relative: Path | tuple[str, ...]) -> str:
        parts = relative.parts if isinstance(relative, Path) else relative
        return "dbfs:/" + "/".join(parts)


class LocalDbutilsWidgets:
    """Widget declarations and parameter bindings for one worker attempt."""

    def __init__(self, parameters: Mapping[str, str] | None = None) -> None:
        self._parameters = dict(parameters or {})
        self._values: dict[str, str] = {}
        self._labels: dict[str, str | None] = {}

    def text(self, name: str, defaultValue: str, label: str | None = None) -> None:
        self._declare(name, defaultValue, label)

    def dropdown(
        self,
        name: str,
        defaultValue: str,
        choices: Sequence[str],
        label: str | None = None,
    ) -> None:
        options = _validate_choices(choices)
        if defaultValue not in options:
            raise ValueError("dropdown defaultValue must be one of choices")
        value = self._parameters.get(name, defaultValue)
        if value not in options:
            raise ValueError(f"widget parameter {name!r} is not one of choices")
        self._declare(name, defaultValue, label, value=value)

    def combobox(
        self,
        name: str,
        defaultValue: str,
        choices: Sequence[str],
        label: str | None = None,
    ) -> None:
        _validate_choices(choices)
        self._declare(name, defaultValue, label)

    def multiselect(
        self,
        name: str,
        defaultValue: str,
        choices: Sequence[str],
        label: str | None = None,
    ) -> None:
        options = _validate_choices(choices)
        defaults = _csv_values(defaultValue)
        if any(value not in options for value in defaults):
            raise ValueError("multiselect defaultValue values must be in choices")
        value = self._parameters.get(name, defaultValue)
        if any(selected not in options for selected in _csv_values(value)):
            raise ValueError(f"widget parameter {name!r} contains a value outside choices")
        self._declare(name, defaultValue, label, value=value)

    def get(self, name: str) -> str:
        """Return a declared widget's resolved string value."""
        self._validate_name(name)
        try:
            return self._values[name]
        except KeyError as error:
            raise KeyError(f"widget {name!r} has not been declared") from error

    def getAll(self) -> dict[str, str]:
        """Return a detached mapping of declared widget values."""
        return dict(self._values)

    def remove(self, name: str) -> None:
        self._validate_name(name)
        if name not in self._values:
            raise KeyError(f"widget {name!r} has not been declared")
        del self._values[name]
        self._labels.pop(name, None)

    def removeAll(self) -> None:
        self._values.clear()
        self._labels.clear()

    def _declare(
        self,
        name: str,
        default_value: str,
        label: str | None,
        *,
        value: str | None = None,
    ) -> None:
        self._validate_name(name)
        self._validate_value(default_value, "defaultValue")
        if label is not None:
            self._validate_value(label, "label")
        selected = value if value is not None else self._parameters.get(name, default_value)
        self._validate_value(selected, "widget parameter")
        self._values[name] = selected
        self._labels[name] = label

    @staticmethod
    def _validate_name(name: str) -> None:
        if not isinstance(name, str) or not name.strip() or len(name) > 256 or "\x00" in name:
            raise ValueError("widget name must contain 1 to 256 characters")

    @staticmethod
    def _validate_value(value: str, label: str) -> None:
        if not isinstance(value, str) or len(value.encode("utf-8")) > 4096 or "\x00" in value:
            raise ValueError(f"{label} must be a string of at most 4096 UTF-8 bytes")


@dataclass(frozen=True, slots=True)
class SecretScope:
    """Scope metadata returned by the local ``dbutils.secrets.listScopes`` subset."""

    name: str


@dataclass(frozen=True, slots=True)
class SecretMetadata:
    """Key metadata returned without revealing a secret value."""

    key: str


class LocalDbutilsSecrets:
    """Resolve only values included in the current job's one-attempt snapshot."""

    def __init__(self, scopes: Mapping[str, Mapping[str, bytes]]) -> None:
        self._scopes = {scope: dict(values) for scope, values in scopes.items()}

    def get(self, scope: str, key: str) -> str:
        """Return an authorized UTF-8 secret value or raise an explicit error."""
        value = self._get_bytes(scope, key)
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ValueError("secret value is not valid UTF-8; use getBytes") from error

    def getBytes(self, scope: str, key: str) -> bytes:
        """Return an authorized secret as bytes."""
        return self._get_bytes(scope, key)

    def listScopes(self) -> list[SecretScope]:
        """List only scopes granted to the current job."""
        return [SecretScope(name) for name in sorted(self._scopes)]

    def list(self, scope: str) -> list[SecretMetadata]:
        """List keys in a granted scope without revealing any values."""
        values = self._authorized_scope(scope)
        return [SecretMetadata(key) for key in sorted(values)]

    def _get_bytes(self, scope: str, key: str) -> bytes:
        self._validate_name(scope, "scope")
        self._validate_name(key, "key")
        values = self._authorized_scope(scope)
        try:
            return values[key]
        except KeyError as error:
            raise SecretNotFoundError(
                f"secret key {key!r} does not exist in the authorized scope"
            ) from error

    def _authorized_scope(self, scope: str) -> Mapping[str, bytes]:
        self._validate_name(scope, "scope")
        try:
            return self._scopes[scope]
        except KeyError as error:
            raise SecretAccessDeniedError(
                "job is not authorized for the requested secret scope"
            ) from error

    @staticmethod
    def _validate_name(value: str, label: str) -> None:
        if not isinstance(value, str) or not value or len(value) > 128 or "\x00" in value:
            raise ValueError(f"secret {label} is invalid")

    def __getattr__(self, name: str) -> Any:
        raise DbutilsOperationNotSupportedError(
            f"dbutils.secrets.{name} is not implemented by the local worker profile"
        )


_TASK_VALUE_UNSET = object()


class LocalDbutilsTaskValues:
    """Durable, run-scoped task value calls routed through the parent service."""

    def __init__(self, client: WorkerServiceClient) -> None:
        self._client = client

    def set(self, key: str, value: object) -> bool:
        """Persist one finite JSON value under the current task and attempt."""
        _validate_task_value_key(key)
        result = self._client.call("task_values", "set", {"key": key, "value": value})
        if type(result) is not bool:
            raise WorkerServiceError("task value set returned a malformed result")
        return result

    def get(
        self,
        taskKey: str,
        key: str,
        default: object = _TASK_VALUE_UNSET,
        debugValue: object = _TASK_VALUE_UNSET,
    ) -> object:
        """Read an upstream task value, using only an explicit non-None default."""
        _validate_task_value_key(key)
        if not isinstance(taskKey, str) or not taskKey:
            raise ValueError("taskKey must be a non-empty string")
        if default is not _TASK_VALUE_UNSET:
            if default is None:
                raise ValueError("default cannot be None")
            _validate_local_task_value(default, "default")
        if debugValue is not _TASK_VALUE_UNSET:
            if debugValue is None:
                raise ValueError("debugValue cannot be None")
            _validate_local_task_value(debugValue, "debugValue")
        try:
            return self._client.call(
                "task_values",
                "get",
                {"task_key": taskKey, "key": key},
            )
        except KeyError as error:
            if default is not _TASK_VALUE_UNSET:
                return default
            raise ValueError(f"task value {key!r} was not set by task {taskKey!r}") from error

    def __getattr__(self, name: str) -> Any:
        raise DbutilsOperationNotSupportedError(
            f"dbutils.jobs.taskValues.{name} is not implemented by the local worker profile"
        )


class LocalDbutilsJobs:
    """The supported task-values adapter under ``dbutils.jobs``."""

    def __init__(self, client: WorkerServiceClient) -> None:
        self.taskValues = LocalDbutilsTaskValues(client)

    def __getattr__(self, name: str) -> Any:
        raise DbutilsOperationNotSupportedError(
            f"dbutils.jobs.{name} is not implemented by the local worker profile"
        )


class LocalDbutils:
    """The worker-injected subset of the ``dbutils`` namespace."""

    def __init__(
        self,
        dbfs_root: str,
        widget_parameters: Mapping[str, str],
        secret_scopes: Mapping[str, Mapping[str, bytes]] | None = None,
        worker_services: WorkerServiceClient | None = None,
        notebook: LocalDbutilsNotebook | None = None,
    ) -> None:
        self.fs = LocalDbutilsFs(dbfs_root)
        self.widgets = LocalDbutilsWidgets(widget_parameters)
        self.secrets = LocalDbutilsSecrets(secret_scopes or {})
        if notebook is not None:
            self.notebook = notebook
        if worker_services is not None:
            self.jobs = LocalDbutilsJobs(worker_services)

    def __getattr__(self, name: str) -> Any:
        raise DbutilsOperationNotSupportedError(
            f"dbutils.{name} is not implemented by the local worker profile"
        )


def _validate_choices(choices: Sequence[str]) -> tuple[str, ...]:
    if isinstance(choices, (str, bytes, bytearray)) or not isinstance(choices, Sequence):
        raise TypeError("choices must be a sequence of strings")
    if len(choices) > 1000:
        raise ValueError("choices cannot contain more than 1000 values")
    result = tuple(choices)
    for value in result:
        LocalDbutilsWidgets._validate_value(value, "choice")
    if len(set(result)) != len(result):
        raise ValueError("choices cannot contain duplicates")
    return result


def _csv_values(value: str) -> tuple[str, ...]:
    LocalDbutilsWidgets._validate_value(value, "multiselect value")
    if value == "":
        return ()
    return tuple(value.split(","))


def _validate_task_value_key(value: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or "\x00" in value
        or len(value.encode("utf-8")) > 256
    ):
        raise ValueError("task value key must contain 1 to 256 UTF-8 bytes")


def _validate_local_task_value(value: object, label: str) -> None:
    def is_json(item: object) -> bool:
        if item is None or type(item) in {str, bool, int}:
            return True
        if type(item) is float:
            return math.isfinite(item)
        if type(item) is list:
            return all(is_json(child) for child in item)
        if type(item) is dict:
            return all(type(key) is str and is_json(child) for key, child in item.items())
        return False

    if not is_json(value):
        raise ValueError(f"{label} must contain JSON-compatible finite data")
    try:
        size = len(
            json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        )
    except (TypeError, ValueError, UnicodeEncodeError) as error:
        raise ValueError(f"{label} must contain JSON-compatible finite data") from error
    if size > 48 * 1024:
        raise ValueError(f"{label} exceeds the 49152-byte task value limit")
