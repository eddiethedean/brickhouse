"""Ephemeral SQL connector sessions scoped to one active workspace service."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from threading import RLock
from types import MappingProxyType

from brickhouse.catalog.repository import CatalogRepositoryError
from brickhouse.compute.warehouse import WarehouseConflictError, WarehouseState
from brickhouse.workspace.context import WorkspaceContext


class SqlKernelSessionError(ValueError):
    """A SQL connector session request violates the selected local profile."""


class SqlKernelSessionNotFoundError(LookupError):
    """No live connector session exists for the requested identifier."""


class UnsupportedSqlKernelSessionOperationError(SqlKernelSessionError):
    """A session setting or operation is outside the implemented connector profile."""


@dataclass(frozen=True, slots=True)
class SqlKernelSession:
    """One live client session bound to a persisted local warehouse and namespace."""

    session_id: str
    warehouse_id: str
    execution_mode: str
    catalog: str
    schema: str
    session_confs: Mapping[str, str]


class SqlKernelSessionService:
    """Own transient REST/kernel sessions for a single diagnostic server."""

    def __init__(self, context: WorkspaceContext) -> None:
        self._context = context
        self._lock = RLock()
        self._sessions: dict[str, SqlKernelSession] = {}
        self._closed = False

    def open(
        self,
        *,
        warehouse_id: str,
        execution_mode: str,
        catalog: str | None = None,
        schema: str | None = None,
        session_confs: Mapping[str, str] | None = None,
    ) -> SqlKernelSession:
        """Open a session only for a running warehouse and supported empty conf set."""
        if execution_mode != "FAST":
            raise SqlKernelSessionError(
                "the local SQL connector profile supports execution_mode=FAST"
            )
        if session_confs is None:
            confs: dict[str, str] = {}
        elif not isinstance(session_confs, Mapping):
            raise SqlKernelSessionError("session_confs must be an object of string settings")
        else:
            confs = {}
            for key, value in session_confs.items():
                if not isinstance(key, str) or not isinstance(value, str):
                    raise SqlKernelSessionError("session_confs must contain string keys and values")
                confs[key] = value
        if confs:
            raise UnsupportedSqlKernelSessionOperationError(
                "the local SQL connector profile does not apply per-session configuration"
            )

        warehouse = self._context.compute.warehouses.get(warehouse_id)
        if warehouse.state is not WarehouseState.RUNNING:
            raise WarehouseConflictError(f"SQL warehouse is not running: {warehouse_id}")

        namespace = self._context.new_namespace_state()
        try:
            if catalog is not None:
                namespace.use_catalog(catalog)
            if schema is not None:
                namespace.use_schema(schema)
        except CatalogRepositoryError as error:
            raise SqlKernelSessionError(str(error)) from error

        record = SqlKernelSession(
            session_id=uuid.uuid4().hex,
            warehouse_id=warehouse_id,
            execution_mode=execution_mode,
            catalog=namespace.current_catalog,
            schema=namespace.current_schema,
            session_confs=MappingProxyType(confs),
        )
        with self._lock:
            if self._closed:
                raise SqlKernelSessionError("the local SQL session service is shutting down")
            self._sessions[record.session_id] = record
        return record

    def get(self, session_id: str, *, warehouse_id: str | None = None) -> SqlKernelSession:
        """Return a live session and optionally require its warehouse identity."""
        if not isinstance(session_id, str) or not session_id:
            raise SqlKernelSessionNotFoundError("SQL connector session does not exist")
        with self._lock:
            if self._closed:
                raise SqlKernelSessionNotFoundError("SQL connector session does not exist")
            record = self._sessions.get(session_id)
        if record is None:
            raise SqlKernelSessionNotFoundError("SQL connector session does not exist")
        if warehouse_id is not None and record.warehouse_id != warehouse_id:
            raise SqlKernelSessionError("SQL connector session belongs to another warehouse")
        return record

    def close(self, session_id: str, *, warehouse_id: str | None = None) -> None:
        """Close a live session after verifying the path and body identities match."""
        record = self.get(session_id, warehouse_id=warehouse_id)
        with self._lock:
            if self._sessions.get(record.session_id) is not record:
                raise SqlKernelSessionNotFoundError("SQL connector session does not exist")
            del self._sessions[record.session_id]

    def close_all(self) -> None:
        """Revoke all transient sessions as the owning server shuts down."""
        with self._lock:
            self._closed = True
            self._sessions.clear()
