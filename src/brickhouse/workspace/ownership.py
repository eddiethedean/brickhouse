"""Cross-process exclusive ownership for a local workspace."""

from __future__ import annotations

import json
import os
import stat
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import BinaryIO, Self, cast

from brickhouse.workspace.lifecycle import (
    Workspace,
    _migrate_workspace_metadata,
    open_workspace,
)

try:
    import fcntl
except ImportError:  # pragma: no cover - exercised on non-POSIX platforms
    fcntl = None  # type: ignore[assignment]


class WorkspaceOwnershipError(RuntimeError):
    """A workspace could not be acquired or safely released."""


class WorkspaceInUseError(WorkspaceOwnershipError):
    """Another process currently owns this workspace."""


class WorkspaceOwner:
    """An acquired workspace lock that releases deterministically on close."""

    def __init__(self, workspace: Workspace) -> None:
        self.workspace = workspace
        self.lock_path = workspace.path / ".brickhouse-owner.lock"
        self._handle: BinaryIO | None = None
        self.previous_owner: dict[str, object] | None = None

    @property
    def is_acquired(self) -> bool:
        return self._handle is not None

    @property
    def recovered_stale_owner(self) -> bool:
        """Whether the last process left diagnostic owner data behind on exit."""
        return self.previous_owner is not None

    def acquire(self) -> Self:
        if self.is_acquired:
            raise WorkspaceOwnershipError("this owner already holds its workspace lock")
        if fcntl is None:
            raise WorkspaceOwnershipError(
                "workspace ownership locks require POSIX advisory file locking"
            )

        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(self.lock_path, flags, 0o600)
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                os.close(descriptor)
                raise WorkspaceOwnershipError("workspace owner lock must be a regular file")
            handle = os.fdopen(descriptor, "r+b", buffering=0)
        except OSError as error:
            raise WorkspaceOwnershipError("could not open workspace owner lock") from error

        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            handle.close()
            raise WorkspaceInUseError("workspace is already owned by another process") from error
        except OSError as error:
            handle.close()
            raise WorkspaceOwnershipError("could not acquire workspace owner lock") from error

        try:
            handle.seek(0)
            previous_data = handle.read()
            if previous_data:
                try:
                    value = json.loads(previous_data.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    value = None
                if isinstance(value, dict):
                    self.previous_owner = cast(dict[str, object], value)
            record = {
                "pid": os.getpid(),
                "workspace_id": self.workspace.workspace_id,
                "acquired_at": datetime.now(UTC).isoformat(),
            }
            handle.seek(0)
            handle.truncate(0)
            handle.write(json.dumps(record, sort_keys=True).encode("utf-8"))
            os.fsync(handle.fileno())
            self._handle = handle
            try:
                self.workspace = _migrate_workspace_metadata(self.workspace)
            except BaseException:
                self.release()
                raise
            return self
        except OSError as error:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()
            raise WorkspaceOwnershipError("could not record workspace ownership") from error

    def release(self) -> None:
        handle = self._handle
        if handle is None:
            return
        self._handle = None
        error: OSError | None = None
        try:
            handle.seek(0)
            handle.truncate(0)
            os.fsync(handle.fileno())
        except OSError as caught:
            error = caught
        finally:
            try:
                if fcntl is not None:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError as caught:
                error = error or caught
            finally:
                handle.close()
        if error is not None:
            raise WorkspaceOwnershipError("could not release workspace owner lock") from error

    def __enter__(self) -> Self:
        if not self.is_acquired:
            self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.release()


def acquire_workspace_owner(path: str | Path) -> WorkspaceOwner:
    """Validate and exclusively own one workspace until the returned handle closes."""
    return WorkspaceOwner(open_workspace(path)).acquire()
