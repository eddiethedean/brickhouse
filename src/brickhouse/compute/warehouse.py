"""Local SQL warehouse identity, lifecycle, and statement admission contracts."""

from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from threading import Lock
from typing import TYPE_CHECKING

from brickhouse.compute.models import (
    ComputeConflictError,
    ComputeNotFoundError,
    InvalidComputeRequestError,
    UnsupportedComputeEffectError,
)
from brickhouse.compute.repository import ComputeRepositoryError

if TYPE_CHECKING:
    from brickhouse.workspace.context import WorkspaceContext

_CLUSTER_SIZES = {
    "2X-Small",
    "X-Small",
    "Small",
    "Medium",
    "Large",
    "X-Large",
    "2X-Large",
    "3X-Large",
    "4X-Large",
    "5X-Large",
}
_SPOT_INSTANCE_POLICY = "COST_OPTIMIZED"
MAX_ACTIVE_STATEMENTS_PER_WAREHOUSE = 1
MAX_ACTIVE_STATEMENTS_PER_WORKSPACE = 2
MAX_QUEUED_STATEMENTS_PER_WAREHOUSE = 8
_MAX_INT32 = 2_147_483_647


class WarehouseState(StrEnum):
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    DELETED = "DELETED"


class WarehouseNotFoundError(ComputeNotFoundError):
    """A local SQL warehouse identity does not exist or was deleted."""


class InvalidWarehouseRequestError(InvalidComputeRequestError):
    """A SQL warehouse request is invalid for the supported local profile."""


class UnsupportedWarehouseEffectError(UnsupportedComputeEffectError):
    """A valid SQL warehouse option requests an effect not implemented locally."""


class WarehouseConflictError(ComputeConflictError):
    """A warehouse identity or admission request conflicts with persisted state."""


class WarehouseAdmissionLimitError(WarehouseConflictError):
    """The selected local warehouse admission limit has been reached."""


@dataclass(frozen=True, slots=True)
class WarehouseConfiguration:
    name: str
    warehouse_type: str
    enable_serverless_compute: bool
    cluster_size: str
    auto_stop_mins: int
    enable_photon: bool
    min_num_clusters: int
    max_num_clusters: int
    statement_timeout: int | None

    @property
    def profile(self) -> str:
        if self.warehouse_type == "CLASSIC":
            return "sql-classic-local-v1"
        if self.enable_serverless_compute:
            return "sql-serverless-local-v1"
        return "sql-pro-local-v1"


@dataclass(frozen=True, slots=True)
class WarehouseRecord:
    warehouse_id: str
    name: str
    warehouse_type: str
    enable_serverless_compute: bool
    cluster_size: str
    auto_stop_mins: int
    enable_photon: bool
    min_num_clusters: int
    max_num_clusters: int
    statement_timeout: int | None
    state: WarehouseState
    created_at: str
    updated_at: str
    started_at: str | None
    stopped_at: str | None
    last_activity_at: str
    generation: int
    deleted_at: str | None

    @property
    def profile(self) -> str:
        if self.warehouse_type == "CLASSIC":
            return "sql-classic-local-v1"
        if self.enable_serverless_compute:
            return "sql-serverless-local-v1"
        return "sql-pro-local-v1"

    @property
    def is_simulated(self) -> bool:
        """Warehouse allocation and its infrastructure lifecycle are simulated."""
        return True

    @property
    def spot_instance_policy(self) -> str:
        """The fixed policy exposed by this simulated local warehouse profile."""
        return _SPOT_INSTANCE_POLICY


@dataclass(frozen=True, slots=True)
class WarehouseAdmissionLease:
    statement_id: str
    warehouse_id: str
    generation: int
    state: str
    admitted_at: str
    released_at: str | None
    invalidated_at: str | None

    @property
    def is_active(self) -> bool:
        return self.state == "ACTIVE"

    @property
    def is_queued(self) -> bool:
        return self.state == "QUEUED"


@dataclass(frozen=True, slots=True)
class WarehouseLifecycleEvent:
    event_id: int
    warehouse_id: str
    event_type: str
    state: WarehouseState
    generation: int
    invalidates_statements: bool
    occurred_at: str
    details_json: str


def normalize_warehouse_configuration(
    body: Mapping[str, object],
    *,
    previous: WarehouseRecord | None = None,
) -> WarehouseConfiguration:
    """Validate the metadata subset supported by the local warehouse profiles."""
    if not isinstance(body, Mapping):
        raise InvalidWarehouseRequestError("warehouse settings must be a JSON object")
    data = dict(body)
    if any(not isinstance(key, str) for key in data):
        raise InvalidWarehouseRequestError("warehouse setting names must be strings")
    allowed = {
        "name",
        "warehouse_type",
        "enable_serverless_compute",
        "cluster_size",
        "auto_stop_mins",
        "enable_photon",
        "min_num_clusters",
        "max_num_clusters",
        "statement_timeout",
        "spot_instance_policy",
    }
    unsupported = set(data) - allowed
    if unsupported:
        fields = ", ".join(sorted(unsupported))
        raise UnsupportedWarehouseEffectError(f"unsupported warehouse request field(s): {fields}")
    spot_instance_policy = data.get("spot_instance_policy", _SPOT_INSTANCE_POLICY)
    if spot_instance_policy != _SPOT_INSTANCE_POLICY:
        raise UnsupportedWarehouseEffectError(
            "only the fixed COST_OPTIMIZED spot_instance_policy is represented locally"
        )

    name = data.get("name", previous.name if previous is not None else None)
    if not isinstance(name, str) or not name.strip() or len(name) >= 100:
        raise InvalidWarehouseRequestError("name must contain 1 to 99 non-whitespace characters")
    name = name.strip()

    warehouse_type = data.get(
        "warehouse_type", previous.warehouse_type if previous is not None else None
    )
    if not isinstance(warehouse_type, str) or warehouse_type not in {"PRO", "CLASSIC"}:
        raise InvalidWarehouseRequestError("warehouse_type must be PRO or CLASSIC")

    explicit_type_change = (
        previous is not None
        and "warehouse_type" in data
        and warehouse_type != previous.warehouse_type
    )
    default_serverless = (
        False if previous is None or explicit_type_change else previous.enable_serverless_compute
    )
    serverless = data.get("enable_serverless_compute", default_serverless)
    if type(serverless) is not bool:
        raise InvalidWarehouseRequestError("enable_serverless_compute must be a boolean")
    if warehouse_type == "CLASSIC" and serverless:
        raise InvalidWarehouseRequestError(
            "enable_serverless_compute=true requires warehouse_type=PRO"
        )

    cluster_size = data.get("cluster_size", previous.cluster_size if previous else None)
    if not isinstance(cluster_size, str) or cluster_size not in _CLUSTER_SIZES:
        raise InvalidWarehouseRequestError("cluster_size is not in the supported SQL size set")

    auto_stop = data.get("auto_stop_mins", previous.auto_stop_mins if previous else 120)
    if (
        type(auto_stop) is not int
        or auto_stop < 0
        or auto_stop > _MAX_INT32
        or (0 < auto_stop < 10)
    ):
        raise InvalidWarehouseRequestError("auto_stop_mins must be 0 or at least 10")

    photon = data.get("enable_photon", previous.enable_photon if previous else True)
    if type(photon) is not bool:
        raise InvalidWarehouseRequestError("enable_photon must be a boolean")

    minimum = data.get("min_num_clusters", previous.min_num_clusters if previous else 1)
    maximum = data.get(
        "max_num_clusters",
        previous.max_num_clusters if previous else (minimum if type(minimum) is int else 1),
    )
    if type(minimum) is not int or minimum < 1 or minimum > 30:
        raise InvalidWarehouseRequestError("min_num_clusters must be an integer from 1 to 30")
    if type(maximum) is not int or maximum < 1 or maximum > 40:
        raise InvalidWarehouseRequestError("max_num_clusters must be an integer from 1 to 40")
    if maximum < minimum:
        raise InvalidWarehouseRequestError("max_num_clusters must be at least min_num_clusters")

    timeout = data.get(
        "statement_timeout", previous.statement_timeout if previous is not None else None
    )
    if timeout is not None and (type(timeout) is not int or timeout < 0 or timeout > _MAX_INT32):
        raise InvalidWarehouseRequestError(
            "statement_timeout must be a non-negative 32-bit integer"
        )

    return WarehouseConfiguration(
        name,
        warehouse_type,
        serverless,
        cluster_size,
        auto_stop,
        photon,
        minimum,
        maximum,
        timeout,
    )


class WarehouseRepository:
    """Transactional access to warehouse records and statement admission history."""

    def __init__(self, context: WorkspaceContext) -> None:
        self._context = context

    @contextmanager
    def transaction(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self._context.workspace.metadata_path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            if write:
                connection.execute("BEGIN IMMEDIATE")
            yield connection
            if write:
                connection.commit()
        except BaseException:
            if write and connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def get(self, connection: sqlite3.Connection, warehouse_id: str) -> WarehouseRecord | None:
        row = connection.execute(
            "SELECT * FROM sql_warehouses WHERE warehouse_id = ? AND deleted_at IS NULL",
            (warehouse_id,),
        ).fetchone()
        return _warehouse_record(row) if row is not None else None

    def list_warehouses(
        self,
        connection: sqlite3.Connection,
        *,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[WarehouseRecord]:
        query = (
            "SELECT * FROM sql_warehouses WHERE deleted_at IS NULL "
            "ORDER BY created_at, warehouse_id"
        )
        parameters: tuple[object, ...] = ()
        if limit is not None:
            query += " LIMIT ? OFFSET ?"
            parameters = (limit, offset)
        rows = connection.execute(query, parameters).fetchall()
        return [_warehouse_record(row) for row in rows]

    def count(self, connection: sqlite3.Connection) -> int:
        row = connection.execute(
            "SELECT count(*) FROM sql_warehouses WHERE deleted_at IS NULL"
        ).fetchone()
        assert row is not None
        return int(row[0])

    def insert(
        self,
        connection: sqlite3.Connection,
        *,
        warehouse_id: str,
        configuration: WarehouseConfiguration,
        now: str,
    ) -> None:
        connection.execute(
            "INSERT INTO sql_warehouses (warehouse_id, name, warehouse_type, "
            "enable_serverless_compute, cluster_size, auto_stop_mins, enable_photon, "
            "min_num_clusters, max_num_clusters, statement_timeout, state, created_at, "
            "updated_at, started_at, last_activity_at, generation) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'RUNNING', ?, ?, ?, ?, 1)",
            (
                warehouse_id,
                configuration.name,
                configuration.warehouse_type,
                int(configuration.enable_serverless_compute),
                configuration.cluster_size,
                configuration.auto_stop_mins,
                int(configuration.enable_photon),
                configuration.min_num_clusters,
                configuration.max_num_clusters,
                configuration.statement_timeout,
                now,
                now,
                now,
                now,
            ),
        )

    def update(
        self,
        connection: sqlite3.Connection,
        warehouse_id: str,
        changes: Mapping[str, object],
    ) -> None:
        allowed = {
            "name",
            "warehouse_type",
            "enable_serverless_compute",
            "cluster_size",
            "auto_stop_mins",
            "enable_photon",
            "min_num_clusters",
            "max_num_clusters",
            "statement_timeout",
            "state",
            "updated_at",
            "started_at",
            "stopped_at",
            "last_activity_at",
            "generation",
            "deleted_at",
        }
        if not changes or set(changes) - allowed:
            raise ComputeRepositoryError("invalid SQL warehouse update")
        assignments = ", ".join(f"{field} = ?" for field in changes)
        cursor = connection.execute(
            f"UPDATE sql_warehouses SET {assignments} "
            "WHERE warehouse_id = ? AND deleted_at IS NULL",
            (*changes.values(), warehouse_id),
        )
        if cursor.rowcount != 1:
            raise ComputeRepositoryError("SQL warehouse disappeared during update")

    def append_event(
        self,
        connection: sqlite3.Connection,
        *,
        warehouse_id: str,
        event_type: str,
        state: WarehouseState,
        generation: int,
        invalidates_statements: bool,
        now: str,
        details_json: str = "{}",
    ) -> None:
        connection.execute(
            "INSERT INTO sql_warehouse_lifecycle_events "
            "(warehouse_id, event_type, state, generation, invalidates_statements, "
            "occurred_at, details_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                warehouse_id,
                event_type,
                state.value,
                generation,
                int(invalidates_statements),
                now,
                details_json,
            ),
        )

    def invalidate_admissions(
        self,
        connection: sqlite3.Connection,
        warehouse_id: str,
        now: str,
    ) -> None:
        # Queued work has not started and can be terminally invalidated at once.
        # Active work keeps its lease until the worker reaches a safe terminal
        # outcome; its old generation already fences any later commit.
        connection.execute(
            "UPDATE sql_statement_admission_leases SET state = 'INVALIDATED', "
            "invalidated_at = ? WHERE warehouse_id = ? AND state = 'QUEUED'",
            (now, warehouse_id),
        )

    def get_lease(
        self,
        connection: sqlite3.Connection,
        statement_id: str,
    ) -> WarehouseAdmissionLease | None:
        row = connection.execute(
            "SELECT * FROM sql_statement_admission_leases WHERE statement_id = ?",
            (statement_id,),
        ).fetchone()
        return _lease_record(row) if row is not None else None

    def list_events(
        self,
        connection: sqlite3.Connection,
        warehouse_id: str,
    ) -> list[WarehouseLifecycleEvent]:
        rows = connection.execute(
            "SELECT * FROM sql_warehouse_lifecycle_events WHERE warehouse_id = ? ORDER BY event_id",
            (warehouse_id,),
        ).fetchall()
        return [_event_record(row) for row in rows]


class SqlWarehouseLifecycle:
    """Apply local SQL warehouse state transitions and admission fencing.

    Warehouse sizing and cloud allocation are simulated. Admission capacity is
    an explicit local limit (one running statement per warehouse), not a
    claim about Databricks queueing or concurrency behavior.
    """

    def __init__(
        self,
        context: WorkspaceContext,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._repository = WarehouseRepository(context)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._listener_lock = Lock()
        self._invalidation_listeners: dict[str, Callable[[str], None]] = {}

    def add_admission_invalidation_listener(
        self,
        listener: Callable[[str], None],
    ) -> Callable[[], None]:
        """Register a process-local callback for warehouse admission invalidation."""
        token = uuid.uuid4().hex
        with self._listener_lock:
            self._invalidation_listeners[token] = listener

        def remove() -> None:
            with self._listener_lock:
                self._invalidation_listeners.pop(token, None)

        return remove

    def _notify_admission_invalidated(self, warehouse_id: str) -> None:
        with self._listener_lock:
            listeners = tuple(self._invalidation_listeners.values())
        for listener in listeners:
            try:
                listener(warehouse_id)
            except Exception:
                # The durable admission invalidation is authoritative even if an
                # in-process cancellation observer is unavailable during shutdown.
                continue

    def create(self, body: Mapping[str, object]) -> WarehouseRecord:
        configuration = normalize_warehouse_configuration(body)
        warehouse_id = f"bhw-{uuid.uuid4().hex}"
        now = self._now()
        try:
            with self._repository.transaction(write=True) as connection:
                self._repository.insert(
                    connection,
                    warehouse_id=warehouse_id,
                    configuration=configuration,
                    now=now,
                )
                self._repository.append_event(
                    connection,
                    warehouse_id=warehouse_id,
                    event_type="created_and_started",
                    state=WarehouseState.RUNNING,
                    generation=1,
                    invalidates_statements=False,
                    now=now,
                )
                return self._require(connection, warehouse_id)
        except sqlite3.IntegrityError as error:
            raise WarehouseConflictError("SQL warehouse name already exists") from error
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not persist SQL warehouse creation") from error

    def get(self, warehouse_id: str) -> WarehouseRecord:
        self._validate_warehouse_id(warehouse_id)
        try:
            with self._repository.transaction() as connection:
                return self._require(connection, warehouse_id)
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not read SQL warehouse metadata") from error

    def list_warehouses(
        self,
        *,
        page_size: int | None = None,
        offset: int = 0,
    ) -> list[WarehouseRecord]:
        if page_size is not None and (type(page_size) is not int or page_size < 1):
            raise InvalidWarehouseRequestError("page_size must be a positive integer")
        if type(offset) is not int or offset < 0:
            raise InvalidWarehouseRequestError("page offset must be a non-negative integer")
        try:
            with self._repository.transaction() as connection:
                return self._repository.list_warehouses(
                    connection,
                    limit=page_size,
                    offset=offset,
                )
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not list SQL warehouses") from error

    def count(self) -> int:
        try:
            with self._repository.transaction() as connection:
                return self._repository.count(connection)
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not count SQL warehouses") from error

    def count_active_statements(self, warehouse_id: str) -> int:
        self._validate_warehouse_id(warehouse_id)
        try:
            with self._repository.transaction() as connection:
                self._require(connection, warehouse_id)
                row = connection.execute(
                    "SELECT count(*) FROM sql_statement_admission_leases "
                    "WHERE warehouse_id = ? AND state = 'ACTIVE'",
                    (warehouse_id,),
                ).fetchone()
                assert row is not None
                return int(row[0])
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not count active SQL statements") from error

    def edit(self, warehouse_id: str, body: Mapping[str, object]) -> WarehouseRecord:
        self._validate_warehouse_id(warehouse_id)
        now = self._now()
        try:
            with self._repository.transaction(write=True) as connection:
                previous = self._require(connection, warehouse_id)
                configuration = normalize_warehouse_configuration(body, previous=previous)
                changed = _configuration_changed(previous, configuration)
                needs_start = previous.state is WarehouseState.STOPPED
                generation = previous.generation + int(changed or needs_start)
                if changed or needs_start:
                    self._repository.invalidate_admissions(connection, warehouse_id, now)
                self._repository.update(
                    connection,
                    warehouse_id,
                    {
                        "name": configuration.name,
                        "warehouse_type": configuration.warehouse_type,
                        "enable_serverless_compute": int(configuration.enable_serverless_compute),
                        "cluster_size": configuration.cluster_size,
                        "auto_stop_mins": configuration.auto_stop_mins,
                        "enable_photon": int(configuration.enable_photon),
                        "min_num_clusters": configuration.min_num_clusters,
                        "max_num_clusters": configuration.max_num_clusters,
                        "statement_timeout": configuration.statement_timeout,
                        "state": WarehouseState.RUNNING.value,
                        "updated_at": now,
                        "started_at": now if needs_start else previous.started_at,
                        "stopped_at": None,
                        "last_activity_at": now if needs_start else previous.last_activity_at,
                        "generation": generation,
                    },
                )
                self._repository.append_event(
                    connection,
                    warehouse_id=warehouse_id,
                    event_type="edited_and_started" if needs_start else "edited",
                    state=WarehouseState.RUNNING,
                    generation=generation,
                    invalidates_statements=changed or needs_start,
                    now=now,
                )
                record = self._require(connection, warehouse_id)
            if changed or needs_start:
                self._notify_admission_invalidated(warehouse_id)
            return record
        except sqlite3.IntegrityError as error:
            raise WarehouseConflictError("SQL warehouse name already exists") from error
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not edit SQL warehouse metadata") from error

    def start(self, warehouse_id: str) -> WarehouseRecord:
        self._validate_warehouse_id(warehouse_id)
        now = self._now()
        try:
            with self._repository.transaction(write=True) as connection:
                record = self._require(connection, warehouse_id)
                if record.state is WarehouseState.RUNNING:
                    return record
                generation = record.generation + 1
                self._repository.update(
                    connection,
                    warehouse_id,
                    {
                        "state": WarehouseState.RUNNING.value,
                        "updated_at": now,
                        "started_at": now,
                        "stopped_at": None,
                        "last_activity_at": now,
                        "generation": generation,
                    },
                )
                self._repository.append_event(
                    connection,
                    warehouse_id=warehouse_id,
                    event_type="started",
                    state=WarehouseState.RUNNING,
                    generation=generation,
                    invalidates_statements=False,
                    now=now,
                )
                return self._require(connection, warehouse_id)
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not start SQL warehouse") from error

    def stop(self, warehouse_id: str) -> WarehouseRecord:
        self._validate_warehouse_id(warehouse_id)
        now = self._now()
        try:
            with self._repository.transaction(write=True) as connection:
                record = self._require(connection, warehouse_id)
                if record.state is WarehouseState.STOPPED:
                    return record
                generation = record.generation + 1
                self._repository.invalidate_admissions(connection, warehouse_id, now)
                self._repository.update(
                    connection,
                    warehouse_id,
                    {
                        "state": WarehouseState.STOPPED.value,
                        "updated_at": now,
                        "stopped_at": now,
                        "generation": generation,
                    },
                )
                self._repository.append_event(
                    connection,
                    warehouse_id=warehouse_id,
                    event_type="stopped",
                    state=WarehouseState.STOPPED,
                    generation=generation,
                    invalidates_statements=True,
                    now=now,
                )
                self._promote_queued(connection, now)
                record = self._require(connection, warehouse_id)
            self._notify_admission_invalidated(warehouse_id)
            return record
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not stop SQL warehouse") from error

    def delete(self, warehouse_id: str) -> None:
        self._validate_warehouse_id(warehouse_id)
        now = self._now()
        try:
            with self._repository.transaction(write=True) as connection:
                record = self._require(connection, warehouse_id)
                generation = record.generation + 1
                self._repository.invalidate_admissions(connection, warehouse_id, now)
                self._repository.update(
                    connection,
                    warehouse_id,
                    {
                        "state": WarehouseState.DELETED.value,
                        "updated_at": now,
                        "stopped_at": now
                        if record.state is WarehouseState.RUNNING
                        else record.stopped_at,
                        "generation": generation,
                        "deleted_at": now,
                    },
                )
                self._repository.append_event(
                    connection,
                    warehouse_id=warehouse_id,
                    event_type="deleted",
                    state=WarehouseState.DELETED,
                    generation=generation,
                    invalidates_statements=True,
                    now=now,
                )
                self._promote_queued(connection, now)
            self._notify_admission_invalidated(warehouse_id)
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not delete SQL warehouse") from error

    def expire_idle(self, *, at: datetime | None = None) -> list[WarehouseRecord]:
        """Stop warehouses whose configured idle interval has elapsed."""
        now = _format_time(at if at is not None else self._clock())
        now_value = _parse_time(now)
        stopped: list[WarehouseRecord] = []
        try:
            with self._repository.transaction(write=True) as connection:
                records = self._repository.list_warehouses(connection)
                for record in records:
                    if record.state is not WarehouseState.RUNNING or record.auto_stop_mins == 0:
                        continue
                    active = connection.execute(
                        "SELECT 1 FROM sql_statement_admission_leases "
                        "WHERE warehouse_id = ? AND generation = ? "
                        "AND state IN ('ACTIVE', 'QUEUED') LIMIT 1",
                        (record.warehouse_id, record.generation),
                    ).fetchone()
                    if active is not None:
                        continue
                    idle_minutes = (
                        now_value - _parse_time(record.last_activity_at)
                    ).total_seconds() / 60
                    if idle_minutes < record.auto_stop_mins:
                        continue
                    stopped.append(
                        self._stop_in_transaction(connection, record, now, "auto_stopped")
                    )
            for record in stopped:
                self._notify_admission_invalidated(record.warehouse_id)
            return stopped
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not expire idle SQL warehouses") from error

    def admit_statement(
        self,
        warehouse_id: str,
        statement_id: str,
    ) -> WarehouseAdmissionLease:
        self._validate_warehouse_id(warehouse_id)
        self._validate_statement_id(statement_id)
        now = self._now()
        try:
            with self._repository.transaction(write=True) as connection:
                warehouse = self._require(connection, warehouse_id)
                if warehouse.state is not WarehouseState.RUNNING:
                    raise WarehouseConflictError(f"warehouse is not running: {warehouse_id}")
                existing = self._repository.get_lease(connection, statement_id)
                if existing is not None:
                    if (
                        existing.warehouse_id == warehouse_id
                        and existing.generation == warehouse.generation
                        and existing.state in {"ACTIVE", "QUEUED"}
                    ):
                        return existing
                    raise WarehouseConflictError(
                        f"statement admission already exists: {statement_id}"
                    )
                warehouse_active = connection.execute(
                    "SELECT count(*) FROM sql_statement_admission_leases "
                    "WHERE warehouse_id = ? AND state = 'ACTIVE'",
                    (warehouse_id,),
                ).fetchone()
                workspace_active = connection.execute(
                    "SELECT count(*) FROM sql_statement_admission_leases WHERE state = 'ACTIVE'"
                ).fetchone()
                queued = connection.execute(
                    "SELECT count(*) FROM sql_statement_admission_leases "
                    "WHERE warehouse_id = ? AND generation = ? AND state = 'QUEUED'",
                    (warehouse_id, warehouse.generation),
                ).fetchone()
                assert warehouse_active is not None
                assert workspace_active is not None
                assert queued is not None
                state = "ACTIVE"
                if (
                    int(warehouse_active[0]) >= MAX_ACTIVE_STATEMENTS_PER_WAREHOUSE
                    or int(workspace_active[0]) >= MAX_ACTIVE_STATEMENTS_PER_WORKSPACE
                ):
                    if int(queued[0]) >= MAX_QUEUED_STATEMENTS_PER_WAREHOUSE:
                        raise WarehouseAdmissionLimitError(
                            "local SQL admission is full: one active per warehouse, "
                            "two active workspace-wide, and eight queued per warehouse"
                        )
                    state = "QUEUED"
                connection.execute(
                    "INSERT INTO sql_statement_admission_leases "
                    "(statement_id, warehouse_id, generation, state, admitted_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (statement_id, warehouse_id, warehouse.generation, state, now),
                )
                if state == "ACTIVE":
                    self._repository.update(
                        connection,
                        warehouse_id,
                        {"last_activity_at": now, "updated_at": now},
                    )
                lease = self._repository.get_lease(connection, statement_id)
                assert lease is not None
                return lease
        except (WarehouseNotFoundError, WarehouseConflictError):
            raise
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not admit SQL statement") from error

    def release_statement(self, statement_id: str) -> WarehouseAdmissionLease:
        self._validate_statement_id(statement_id)
        now = self._now()
        try:
            with self._repository.transaction(write=True) as connection:
                lease = self._repository.get_lease(connection, statement_id)
                if lease is None:
                    raise WarehouseNotFoundError(
                        f"statement admission does not exist: {statement_id}"
                    )
                if lease.state not in {"ACTIVE", "QUEUED"}:
                    return lease
                connection.execute(
                    "UPDATE sql_statement_admission_leases SET state = 'RELEASED', released_at = ? "
                    "WHERE statement_id = ? AND state IN ('ACTIVE', 'QUEUED')",
                    (now, statement_id),
                )
                warehouse = self._repository.get(connection, lease.warehouse_id)
                if warehouse is not None and warehouse.generation == lease.generation:
                    if lease.is_active:
                        self._repository.update(
                            connection,
                            lease.warehouse_id,
                            {"last_activity_at": now, "updated_at": now},
                        )
                if lease.is_active:
                    self._promote_queued(connection, now)
                released = self._repository.get_lease(connection, statement_id)
                assert released is not None
                return released
        except WarehouseNotFoundError:
            raise
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not release SQL statement admission") from error

    def get_admission(self, statement_id: str) -> WarehouseAdmissionLease:
        self._validate_statement_id(statement_id)
        try:
            with self._repository.transaction() as connection:
                lease = self._repository.get_lease(connection, statement_id)
                if lease is None:
                    raise WarehouseNotFoundError(
                        f"statement admission does not exist: {statement_id}"
                    )
                return lease
        except WarehouseNotFoundError:
            raise
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not read SQL statement admission") from error

    def validate_admission(self, statement_id: str) -> bool:
        """Check whether a lease still belongs to a running warehouse generation."""
        lease = self.get_admission(statement_id)
        if not lease.is_active:
            return False
        try:
            with self._repository.transaction() as connection:
                warehouse = self._repository.get(connection, lease.warehouse_id)
                return (
                    warehouse is not None
                    and warehouse.state is WarehouseState.RUNNING
                    and warehouse.generation == lease.generation
                )
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not validate SQL statement admission") from error

    def list_events(self, warehouse_id: str) -> list[WarehouseLifecycleEvent]:
        self._validate_warehouse_id(warehouse_id)
        try:
            with self._repository.transaction() as connection:
                row = connection.execute(
                    "SELECT 1 FROM sql_warehouses WHERE warehouse_id = ?",
                    (warehouse_id,),
                ).fetchone()
                if row is None:
                    raise WarehouseNotFoundError(f"SQL warehouse does not exist: {warehouse_id}")
                return self._repository.list_events(connection, warehouse_id)
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not read SQL warehouse events") from error

    def _stop_in_transaction(
        self,
        connection: sqlite3.Connection,
        record: WarehouseRecord,
        now: str,
        event_type: str,
    ) -> WarehouseRecord:
        generation = record.generation + 1
        self._repository.invalidate_admissions(connection, record.warehouse_id, now)
        self._repository.update(
            connection,
            record.warehouse_id,
            {
                "state": WarehouseState.STOPPED.value,
                "updated_at": now,
                "stopped_at": now,
                "generation": generation,
            },
        )
        self._repository.append_event(
            connection,
            warehouse_id=record.warehouse_id,
            event_type=event_type,
            state=WarehouseState.STOPPED,
            generation=generation,
            invalidates_statements=True,
            now=now,
        )
        self._promote_queued(connection, now)
        return self._require(connection, record.warehouse_id)

    def _promote_queued(self, connection: sqlite3.Connection, now: str) -> None:
        """Activate queued requests in arrival order while global slots are free."""
        while True:
            active = connection.execute(
                "SELECT count(*) FROM sql_statement_admission_leases WHERE state = 'ACTIVE'"
            ).fetchone()
            assert active is not None
            if int(active[0]) >= MAX_ACTIVE_STATEMENTS_PER_WORKSPACE:
                return
            candidates = connection.execute(
                "SELECT lease.statement_id, lease.warehouse_id, lease.generation "
                "FROM sql_statement_admission_leases AS lease "
                "JOIN sql_warehouses AS warehouse USING(warehouse_id) "
                "WHERE lease.state = 'QUEUED' AND warehouse.state = 'RUNNING' "
                "AND warehouse.deleted_at IS NULL "
                "AND warehouse.generation = lease.generation "
                "AND NOT EXISTS (SELECT 1 FROM sql_statement_admission_leases AS active_lease "
                "WHERE active_lease.warehouse_id = lease.warehouse_id "
                "AND active_lease.state = 'ACTIVE') "
                "ORDER BY lease.admitted_at, lease.statement_id"
            ).fetchall()
            if not candidates:
                return
            statement_id, warehouse_id, generation = candidates[0]
            connection.execute(
                "UPDATE sql_statement_admission_leases SET state = 'ACTIVE' "
                "WHERE statement_id = ? AND state = 'QUEUED'",
                (statement_id,),
            )
            warehouse = self._repository.get(connection, warehouse_id)
            if warehouse is not None and warehouse.generation == generation:
                self._repository.update(
                    connection,
                    warehouse_id,
                    {"last_activity_at": now, "updated_at": now},
                )
            else:
                connection.execute(
                    "UPDATE sql_statement_admission_leases SET state = 'INVALIDATED', "
                    "invalidated_at = ? WHERE statement_id = ? AND state = 'ACTIVE'",
                    (now, statement_id),
                )

    def _require(self, connection: sqlite3.Connection, warehouse_id: str) -> WarehouseRecord:
        record = self._repository.get(connection, warehouse_id)
        if record is None:
            raise WarehouseNotFoundError(f"SQL warehouse does not exist: {warehouse_id}")
        return record

    def _now(self) -> str:
        return _format_time(self._clock())

    @staticmethod
    def _validate_warehouse_id(warehouse_id: str) -> None:
        if not isinstance(warehouse_id, str) or not warehouse_id or len(warehouse_id) > 128:
            raise InvalidWarehouseRequestError("warehouse_id must be a bounded non-empty string")

    @staticmethod
    def _validate_statement_id(statement_id: str) -> None:
        if not isinstance(statement_id, str) or not statement_id or len(statement_id) > 128:
            raise InvalidWarehouseRequestError("statement_id must be a bounded non-empty string")


def _warehouse_record(row: sqlite3.Row) -> WarehouseRecord:
    return WarehouseRecord(
        row["warehouse_id"],
        row["name"],
        row["warehouse_type"],
        bool(row["enable_serverless_compute"]),
        row["cluster_size"],
        row["auto_stop_mins"],
        bool(row["enable_photon"]),
        row["min_num_clusters"],
        row["max_num_clusters"],
        row["statement_timeout"],
        WarehouseState(row["state"]),
        row["created_at"],
        row["updated_at"],
        row["started_at"],
        row["stopped_at"],
        row["last_activity_at"],
        row["generation"],
        row["deleted_at"],
    )


def _lease_record(row: sqlite3.Row) -> WarehouseAdmissionLease:
    return WarehouseAdmissionLease(
        row["statement_id"],
        row["warehouse_id"],
        row["generation"],
        row["state"],
        row["admitted_at"],
        row["released_at"],
        row["invalidated_at"],
    )


def _event_record(row: sqlite3.Row) -> WarehouseLifecycleEvent:
    return WarehouseLifecycleEvent(
        row["event_id"],
        row["warehouse_id"],
        row["event_type"],
        WarehouseState(row["state"]),
        row["generation"],
        bool(row["invalidates_statements"]),
        row["occurred_at"],
        row["details_json"],
    )


def _configuration_changed(
    record: WarehouseRecord,
    configuration: WarehouseConfiguration,
) -> bool:
    return (
        record.name != configuration.name
        or record.warehouse_type != configuration.warehouse_type
        or record.enable_serverless_compute != configuration.enable_serverless_compute
        or record.cluster_size != configuration.cluster_size
        or record.auto_stop_mins != configuration.auto_stop_mins
        or record.enable_photon != configuration.enable_photon
        or record.min_num_clusters != configuration.min_num_clusters
        or record.max_num_clusters != configuration.max_num_clusters
        or record.statement_timeout != configuration.statement_timeout
    )


def _format_time(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise InvalidWarehouseRequestError("warehouse clock must return a timezone-aware datetime")
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
