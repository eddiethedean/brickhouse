"""Workspace-owned Python source files used by local job tasks."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from brickhouse.workspace.context import WorkspaceContext

MAX_WORKSPACE_FILE_BYTES = 1024 * 1024


class WorkspaceFileError(RuntimeError):
    """Base error for local workspace file operations."""


class InvalidWorkspaceFileError(WorkspaceFileError, ValueError):
    """A workspace file path or upload does not match the supported profile."""


class WorkspaceFileNotFoundError(WorkspaceFileError):
    """A requested local workspace file does not exist."""


class WorkspaceFileAlreadyExistsError(WorkspaceFileError):
    """An upload attempted to replace a file without overwrite enabled."""


class UnsupportedWorkspaceFileError(WorkspaceFileError):
    """A valid workspace object format is not supported by this local profile."""


@dataclass(frozen=True, slots=True)
class WorkspaceFileRecord:
    """Metadata for one workspace-owned Python source file."""

    workspace_path: str
    local_path: Path
    size_bytes: int


@dataclass(frozen=True, slots=True)
class WorkspaceObjectRecord:
    """Metadata for a safe workspace file or directory path."""

    workspace_path: str
    object_type: str
    local_path: Path
    size_bytes: int | None


class WorkspaceFileService:
    """Read and atomically import bounded Python sources under one workspace root."""

    def __init__(self, context: WorkspaceContext) -> None:
        self._context = context

    @property
    def root(self) -> Path:
        """Return the workspace-owned source root after checking its identity."""
        self._context._require_active()
        root = self._context.workspace.path / "files"
        if root.is_symlink() or not root.is_dir():
            raise WorkspaceFileError("workspace files root is missing or is not a directory")
        return root.resolve(strict=True)

    def import_python(
        self,
        workspace_path: str,
        content: bytes,
        *,
        overwrite: bool = False,
    ) -> WorkspaceFileRecord:
        """Atomically import one UTF-8 Python file without following workspace symlinks."""
        if not isinstance(content, bytes):
            raise InvalidWorkspaceFileError("Python source content must be bytes")
        try:
            content.decode("utf-8-sig")
        except UnicodeDecodeError as error:
            raise InvalidWorkspaceFileError("Python source must be UTF-8 encoded") from error
        return self._import_managed_file(workspace_path, content, overwrite, ".py")

    def import_bundle_json(
        self,
        workspace_path: str,
        content: bytes,
        *,
        overwrite: bool = False,
    ) -> WorkspaceFileRecord:
        """Import one bounded JSON workspace artifact used by the pinned bundle client."""
        if not isinstance(content, bytes):
            raise InvalidWorkspaceFileError("bundle JSON content must be bytes")
        try:
            json.loads(content)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise InvalidWorkspaceFileError("bundle JSON content must be valid JSON") from error
        return self._import_managed_file(workspace_path, content, overwrite, ".json")

    def _import_managed_file(
        self,
        workspace_path: str,
        content: bytes,
        overwrite: bool,
        suffix: str,
    ) -> WorkspaceFileRecord:
        if len(content) > MAX_WORKSPACE_FILE_BYTES:
            raise InvalidWorkspaceFileError(
                f"workspace file exceeds the {MAX_WORKSPACE_FILE_BYTES}-byte local limit"
            )
        if type(overwrite) is not bool:
            raise InvalidWorkspaceFileError("overwrite must be a boolean")

        relative = _relative_workspace_path(workspace_path)
        if not relative.name.endswith(suffix):
            raise UnsupportedWorkspaceFileError(
                f"only {suffix} files are supported by this workspace import profile"
            )
        root = self.root
        parent = root
        for part in relative.parts[:-1]:
            parent = parent / part
            if parent.is_symlink():
                raise InvalidWorkspaceFileError("workspace path cannot traverse a symbolic link")
            if not parent.exists():
                try:
                    parent.mkdir()
                except FileExistsError:
                    pass
            if not parent.is_dir():
                raise InvalidWorkspaceFileError("workspace path parent is not a directory")
            if not parent.resolve(strict=True).is_relative_to(root):
                raise InvalidWorkspaceFileError("workspace path escapes its owned files root")

        target = parent / relative.name
        if target.is_symlink():
            raise InvalidWorkspaceFileError("workspace file cannot be a symbolic link")
        if target.exists() and not target.is_file():
            raise InvalidWorkspaceFileError("workspace file target is not a regular file")
        if target.exists() and not overwrite:
            raise WorkspaceFileAlreadyExistsError("workspace file already exists")
        if not target.resolve(strict=False).is_relative_to(root):
            raise InvalidWorkspaceFileError("workspace path escapes its owned files root")

        descriptor, temporary_name = tempfile.mkstemp(prefix=".brickhouse-upload-", dir=parent)
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            if overwrite:
                os.replace(temporary, target)
            else:
                try:
                    os.link(temporary, target, follow_symlinks=False)
                except FileExistsError as error:
                    raise WorkspaceFileAlreadyExistsError(
                        "workspace file already exists"
                    ) from error
                temporary.unlink()
        except OSError as error:
            temporary.unlink(missing_ok=True)
            raise WorkspaceFileError("could not persist workspace Python source") from error
        return WorkspaceFileRecord(workspace_path, target, len(content))

    def get_python(self, workspace_path: str) -> WorkspaceFileRecord:
        """Resolve an existing Python file while refusing symlinks and root escapes."""
        relative = _relative_python_path(workspace_path)
        root = self.root
        target = root.joinpath(*relative.parts)
        current = root
        for part in relative.parts:
            current = current / part
            if current.is_symlink():
                raise InvalidWorkspaceFileError("workspace path cannot traverse a symbolic link")
        if not target.exists():
            raise WorkspaceFileNotFoundError("workspace Python source does not exist")
        try:
            resolved = target.resolve(strict=True)
        except OSError as error:
            raise WorkspaceFileNotFoundError("workspace Python source does not exist") from error
        if not resolved.is_relative_to(root):
            raise InvalidWorkspaceFileError("workspace path escapes its owned files root")
        if not resolved.is_file():
            raise InvalidWorkspaceFileError("workspace Python source is not a regular file")
        size = resolved.stat().st_size
        if size > MAX_WORKSPACE_FILE_BYTES:
            raise InvalidWorkspaceFileError("workspace Python source exceeds the local size limit")
        return WorkspaceFileRecord(workspace_path, resolved, size)

    def get_workspace_object(self, workspace_path: str) -> WorkspaceObjectRecord:
        """Inspect a managed workspace file or directory without following symlinks."""
        relative = _relative_workspace_path(workspace_path)
        root = self.root
        target = root.joinpath(*relative.parts)
        current = root
        for part in relative.parts:
            current = current / part
            if current.is_symlink():
                raise InvalidWorkspaceFileError("workspace path cannot traverse a symbolic link")
        if not target.exists():
            raise WorkspaceFileNotFoundError("workspace object does not exist")
        try:
            resolved = target.resolve(strict=True)
        except OSError as error:
            raise WorkspaceFileNotFoundError("workspace object does not exist") from error
        if not resolved.is_relative_to(root):
            raise InvalidWorkspaceFileError("workspace path escapes its owned files root")
        if resolved.is_dir():
            return WorkspaceObjectRecord(workspace_path, "DIRECTORY", resolved, None)
        if not resolved.is_file():
            raise InvalidWorkspaceFileError("workspace object is not a regular file or directory")
        size = resolved.stat().st_size
        if size > MAX_WORKSPACE_FILE_BYTES:
            raise InvalidWorkspaceFileError("workspace file exceeds the local size limit")
        return WorkspaceObjectRecord(workspace_path, "FILE", resolved, size)

    def create_directory(self, workspace_path: str) -> WorkspaceObjectRecord:
        """Create one managed workspace directory and its missing parents safely."""
        relative = _relative_workspace_path(workspace_path)
        root = self.root
        target = root
        for part in relative.parts:
            target = target / part
            if target.is_symlink():
                raise InvalidWorkspaceFileError("workspace path cannot traverse a symbolic link")
            if target.exists():
                if not target.is_dir():
                    raise InvalidWorkspaceFileError("workspace directory path is a regular file")
            else:
                try:
                    target.mkdir()
                except FileExistsError as error:
                    if target.is_symlink() or not target.is_dir():
                        raise InvalidWorkspaceFileError(
                            "workspace directory path is not a regular directory"
                        ) from error
            if not target.resolve(strict=True).is_relative_to(root):
                raise InvalidWorkspaceFileError("workspace path escapes its owned files root")
        return WorkspaceObjectRecord(workspace_path, "DIRECTORY", target, None)

    def delete_workspace_object(self, workspace_path: str, *, recursive: bool = False) -> None:
        """Delete one owned file or directory without following a workspace symlink."""
        if type(recursive) is not bool:
            raise InvalidWorkspaceFileError("recursive must be a boolean")
        record = self.get_workspace_object(workspace_path)
        try:
            if record.object_type == "FILE":
                record.local_path.unlink()
            elif recursive:
                shutil.rmtree(record.local_path)
            else:
                record.local_path.rmdir()
        except OSError as error:
            raise WorkspaceFileError("could not delete the workspace object") from error

    def delete_python(self, workspace_path: str) -> None:
        """Remove one owned Python source file without following workspace symlinks."""
        record = self.get_python(workspace_path)
        try:
            record.local_path.unlink()
        except OSError as error:
            raise WorkspaceFileError("could not remove workspace Python source") from error


def _relative_python_path(workspace_path: str) -> PurePosixPath:
    relative = _relative_workspace_path(workspace_path)
    if not relative.name.endswith(".py"):
        raise UnsupportedWorkspaceFileError(
            "only normalized Python source paths below /Workspace are supported"
        )
    return relative


def _relative_workspace_path(workspace_path: str) -> PurePosixPath:
    if not isinstance(workspace_path, str) or len(workspace_path) > 4096:
        raise InvalidWorkspaceFileError("workspace path must be a bounded string")
    if (
        not workspace_path.startswith("/Workspace/")
        or "\\" in workspace_path
        or "\x00" in workspace_path
    ):
        raise InvalidWorkspaceFileError("workspace path must be an absolute /Workspace path")
    normalized = PurePosixPath(workspace_path)
    if (
        normalized.as_posix() != workspace_path
        or len(normalized.parts) < 4
        or normalized.parts[2] not in {"Shared", "Users"}
        or any(part in {"", ".", ".."} for part in normalized.parts[2:])
    ):
        raise UnsupportedWorkspaceFileError(
            "only normalized paths below /Workspace/Shared or /Workspace/Users are supported"
        )
    return PurePosixPath(*normalized.parts[2:])
