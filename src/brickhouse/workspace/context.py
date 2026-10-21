"""Instance-scoped embedded workspace lifecycle."""

from __future__ import annotations

from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Self

from brickhouse.workspace.lifecycle import Workspace as WorkspaceDescriptor
from brickhouse.workspace.ownership import WorkspaceOwner, acquire_workspace_owner

if TYPE_CHECKING:
    from brickhouse.catalog import (
        CatalogRepository,
        NamespaceState,
        SchemaRepository,
        TableRepository,
    )
    from brickhouse.compute.service import ComputeService
    from brickhouse.jobs.repository import JobRepository
    from brickhouse.runtime.outcomes import WorkerOutcomeJournal
    from brickhouse.runtime.secrets import SecretProvider
    from brickhouse.tables.journal import TableOperationJournal


class WorkspaceContextError(RuntimeError):
    """An embedded workspace context is in an invalid lifecycle state."""


class WorkspaceContext:
    """Own one initialized workspace for the duration of a context manager.

    ``WorkspaceContext`` is the composition root shared by embedded callers and
    the CLI. It provides workspace metadata, ownership, instance-scoped SQLite
    repositories, and fresh namespace state values. It does not imply that API,
    SQL, execution sessions, or job services exist.
    """

    _catalogs: CatalogRepository | None
    _schemas: SchemaRepository | None
    _tables: TableRepository | None
    _table_operations: TableOperationJournal | None
    _worker_outcomes: WorkerOutcomeJournal | None
    _secret_provider: SecretProvider | None
    _jobs: JobRepository | None
    _compute: ComputeService | None

    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory).expanduser().resolve()
        self._owner: WorkspaceOwner | None = None
        self._closed = False
        self._catalogs = None
        self._schemas = None
        self._tables = None
        self._table_operations = None
        self._worker_outcomes = None
        self._secret_provider = None
        self._jobs = None
        self._compute = None

    @classmethod
    def local(cls, directory: str | Path) -> WorkspaceContext:
        """Create a context for an existing local workspace."""
        return cls(directory)

    @property
    def workspace(self) -> WorkspaceDescriptor:
        """Return the validated descriptor after ownership has been acquired."""
        if self._owner is None:
            raise WorkspaceContextError("workspace context is not active")
        return self._owner.workspace

    @property
    def is_active(self) -> bool:
        return self._owner is not None and self._owner.is_acquired

    @property
    def catalogs(self) -> CatalogRepository:
        """Return this context's catalog metadata repository."""
        self._require_active()
        if self._catalogs is None:
            from brickhouse.catalog import CatalogRepository

            self._catalogs = CatalogRepository(self)
        return self._catalogs

    @property
    def schemas(self) -> SchemaRepository:
        """Return this context's schema metadata repository."""
        self._require_active()
        if self._schemas is None:
            from brickhouse.catalog import SchemaRepository

            self._schemas = SchemaRepository(self)
        return self._schemas

    @property
    def tables(self) -> TableRepository:
        """Return this context's table registration repository."""
        self._require_active()
        if self._tables is None:
            from brickhouse.catalog import TableRepository

            self._tables = TableRepository(self)
        return self._tables

    @property
    def table_operations(self) -> TableOperationJournal:
        """Return this context's durable Delta operation journal repository."""
        self._require_active()
        if self._table_operations is None:
            from brickhouse.tables.journal import TableOperationJournal

            self._table_operations = TableOperationJournal(self)
        return self._table_operations

    @property
    def worker_outcomes(self) -> WorkerOutcomeJournal:
        """Return the durable worker-attempt outcome journal for this workspace."""
        self._require_active()
        if self._worker_outcomes is None:
            from brickhouse.runtime.outcomes import WorkerOutcomeJournal

            self._worker_outcomes = WorkerOutcomeJournal(self)
        return self._worker_outcomes

    @property
    def secret_provider(self) -> SecretProvider:
        """Return the process-memory local secret provider for explicit job grants."""
        self._require_active()
        if self._secret_provider is None:
            from brickhouse.runtime.secrets import SecretProvider

            self._secret_provider = SecretProvider()
        return self._secret_provider

    @property
    def jobs(self) -> JobRepository:
        """Return this context's durable job/run/task repository."""
        self._require_active()
        if self._jobs is None:
            from brickhouse.jobs.repository import JobRepository

            self._jobs = JobRepository(self)
        return self._jobs

    @property
    def compute(self) -> ComputeService:
        """Return this workspace's classic compute lifecycle service."""
        self._require_active()
        if self._compute is None:
            from brickhouse.compute.service import ComputeService

            self._compute = ComputeService(self)
        return self._compute

    def new_namespace_state(self) -> NamespaceState:
        """Create fresh current-catalog/schema values for one future session."""
        self._require_active()
        from brickhouse.catalog import IdentifierResolver

        return IdentifierResolver(self).new_state()

    def _require_active(self) -> None:
        if not self.is_active:
            raise WorkspaceContextError("workspace context is not active")

    def __enter__(self) -> Self:
        if self._closed:
            raise WorkspaceContextError("workspace context has already been closed")
        if self._owner is not None:
            raise WorkspaceContextError("workspace context is already active")
        self._owner = acquire_workspace_owner(self.directory)
        return self

    def close(self) -> None:
        """Release workspace ownership. Repeated calls are safe."""
        owner = self._owner
        if owner is None:
            if self._secret_provider is not None:
                self._secret_provider.clear()
            self._closed = True
            return
        self._owner = None
        self._closed = True
        if self._secret_provider is not None:
            self._secret_provider.clear()
        owner.release()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
