"""Workspace-scoped classic compute lifecycle and consumer coordination."""

from __future__ import annotations

import json
import re
import sqlite3
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime

from brickhouse.compute.models import (
    ClusterConfiguration,
    ComputeConflictError,
    ComputeConsumerRecord,
    ComputeError,
    ComputeLifecycleEvent,
    ComputeNotFoundError,
    ComputeRecord,
    ComputeResourceKind,
    ComputeState,
    ComputeStateTransitionError,
    InvalidComputeRequestError,
    normalize_cluster_configuration,
)
from brickhouse.compute.repository import ComputeRepository, ComputeRepositoryError
from brickhouse.workspace.context import WorkspaceContext

_COMPUTE_KEY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
_TASK_KEY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")


class ComputeService:
    """Apply classic compute lifecycle rules using durable workspace metadata.

    All represented cluster allocation is simulated. This service never starts
    cloud VMs, Spark, a JVM, or remote compute.
    """

    def __init__(
        self,
        context: WorkspaceContext,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._context = context
        self._repository = ComputeRepository(context)
        self._clock = clock or (lambda: datetime.now(UTC))

    def create_cluster(self, body: Mapping[str, object]) -> ComputeRecord:
        """Create local all-purpose cluster metadata in an immediately running state."""
        config = normalize_cluster_configuration(body)
        compute_id = f"bhc-{uuid.uuid4().hex}"
        now = self._now()
        try:
            with self._repository.transaction(write=True) as connection:
                self._repository.insert(
                    connection,
                    compute_id=compute_id,
                    resource_kind=ComputeResourceKind.ALL_PURPOSE,
                    profile=config.profile,
                    state=ComputeState.PENDING,
                    run_id=None,
                    compute_key=None,
                    cluster_name=config.cluster_name,
                    spark_version=config.spark_version,
                    access_mode=config.access_mode,
                    single_user_name=config.single_user_name,
                    settings_json=config.settings_json,
                    autotermination_minutes=config.autotermination_minutes,
                    created_at=now,
                    updated_at=now,
                    last_activity_at=now,
                    generation=1,
                )
                self._append_event(
                    connection, compute_id, "created", ComputeState.PENDING, 1, False, now
                )
                self._repository.update(
                    connection,
                    compute_id,
                    {"state": ComputeState.RUNNING.value, "started_at": now},
                )
                self._append_event(
                    connection, compute_id, "started", ComputeState.RUNNING, 1, False, now
                )
                record = self._require(connection, compute_id)
            return record
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not persist cluster creation") from error

    def get_cluster(self, compute_id: str) -> ComputeRecord:
        self._validate_compute_id(compute_id)
        try:
            with self._repository.transaction() as connection:
                record = self._require(connection, compute_id)
                self._require_all_purpose(record)
                return record
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not read cluster metadata") from error

    def get_resource(self, compute_id: str) -> ComputeRecord:
        """Read any persisted compute resource for internal job orchestration."""
        self._validate_compute_id(compute_id)
        try:
            with self._repository.transaction() as connection:
                return self._require(connection, compute_id)
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not read compute resource metadata") from error

    def list_clusters(
        self,
        *,
        page_size: int | None = None,
        offset: int = 0,
    ) -> list[ComputeRecord]:
        if page_size is not None and (type(page_size) is not int or page_size < 1):
            raise InvalidComputeRequestError("page_size must be a positive integer")
        if type(offset) is not int or offset < 0:
            raise InvalidComputeRequestError("page offset must be a non-negative integer")
        try:
            with self._repository.transaction() as connection:
                return self._repository.list(
                    connection,
                    resource_kind=ComputeResourceKind.ALL_PURPOSE,
                    limit=page_size,
                    offset=offset,
                )
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not list cluster metadata") from error

    def count_clusters(self) -> int:
        try:
            with self._repository.transaction() as connection:
                return self._repository.count(
                    connection,
                    resource_kind=ComputeResourceKind.ALL_PURPOSE,
                )
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not count cluster metadata") from error

    def edit_cluster(self, compute_id: str, body: Mapping[str, object]) -> ComputeRecord:
        self._validate_compute_id(compute_id)
        try:
            with self._repository.transaction(write=True) as connection:
                previous = self._require(connection, compute_id)
                self._require_all_purpose(previous)
                if previous.state is not ComputeState.RUNNING:
                    raise ComputeStateTransitionError("only running clusters can be edited")
                if self._has_unreleased_consumers(connection, compute_id):
                    raise ComputeConflictError("cluster has active or registered job consumers")
                config = normalize_cluster_configuration(body, previous=previous)
                changed = not _same_configuration(previous, config)
                now = self._now()
                generation = previous.generation + int(changed)
                self._repository.update(
                    connection,
                    compute_id,
                    {
                        "cluster_name": config.cluster_name,
                        "spark_version": config.spark_version,
                        "access_mode": config.access_mode,
                        "single_user_name": config.single_user_name,
                        "settings_json": config.settings_json,
                        "autotermination_minutes": config.autotermination_minutes,
                        "updated_at": now,
                        "generation": generation,
                    },
                )
                self._append_event(
                    connection,
                    compute_id,
                    "edited",
                    ComputeState.RUNNING,
                    generation,
                    changed,
                    now,
                )
                return self._require(connection, compute_id)
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not edit cluster metadata") from error

    def restart_cluster(self, compute_id: str) -> ComputeRecord:
        self._validate_compute_id(compute_id)
        try:
            with self._repository.transaction(write=True) as connection:
                record = self._require(connection, compute_id)
                self._require_all_purpose(record)
                if record.state is not ComputeState.RUNNING:
                    raise ComputeStateTransitionError("only running clusters can be restarted")
                if self._has_unreleased_consumers(connection, compute_id) and not (
                    self._has_active_consumers(connection, compute_id)
                ):
                    raise ComputeConflictError(
                        "cluster has registered job consumers but no active worker to interrupt"
                    )
                now = self._now()
                generation = record.generation + 1
                self._repository.update(
                    connection,
                    compute_id,
                    {
                        "state": ComputeState.RESTARTING.value,
                        "updated_at": now,
                        "generation": generation,
                    },
                )
                self._append_event(
                    connection,
                    compute_id,
                    "restarted",
                    ComputeState.RESTARTING,
                    generation,
                    True,
                    now,
                )
                self._repository.update(
                    connection,
                    compute_id,
                    {
                        "state": ComputeState.RUNNING.value,
                        "updated_at": now,
                        "started_at": now,
                        "last_activity_at": now,
                    },
                )
                self._append_event(
                    connection,
                    compute_id,
                    "restarted",
                    ComputeState.RUNNING,
                    generation,
                    False,
                    now,
                )
                return self._require(connection, compute_id)
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not restart cluster metadata") from error

    def start_cluster(self, compute_id: str) -> ComputeRecord:
        self._validate_compute_id(compute_id)
        try:
            with self._repository.transaction(write=True) as connection:
                record = self._require(connection, compute_id)
                self._require_all_purpose(record)
                if record.state is ComputeState.RUNNING:
                    return record
                if record.state is not ComputeState.TERMINATED:
                    raise ComputeStateTransitionError("only terminated clusters can be started")
                if self._has_unreleased_consumers(connection, compute_id):
                    raise ComputeConflictError("cluster has active or registered job consumers")
                now = self._now()
                generation = record.generation + 1
                self._repository.update(
                    connection,
                    compute_id,
                    {
                        "state": ComputeState.PENDING.value,
                        "updated_at": now,
                        "generation": generation,
                        "terminated_at": None,
                    },
                )
                self._append_event(
                    connection,
                    compute_id,
                    "started",
                    ComputeState.PENDING,
                    generation,
                    True,
                    now,
                )
                self._repository.update(
                    connection,
                    compute_id,
                    {
                        "state": ComputeState.RUNNING.value,
                        "updated_at": now,
                        "started_at": now,
                        "last_activity_at": now,
                    },
                )
                self._append_event(
                    connection,
                    compute_id,
                    "started",
                    ComputeState.RUNNING,
                    generation,
                    False,
                    now,
                )
                return self._require(connection, compute_id)
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not start cluster metadata") from error

    def terminate_cluster(self, compute_id: str, *, automatic: bool = False) -> ComputeRecord:
        self._validate_compute_id(compute_id)
        try:
            with self._repository.transaction(write=True) as connection:
                record = self._require(connection, compute_id)
                self._require_all_purpose(record)
                if record.state is ComputeState.TERMINATED:
                    return record
                if record.state not in {
                    ComputeState.RUNNING,
                    ComputeState.RESTARTING,
                    ComputeState.PENDING,
                }:
                    raise ComputeStateTransitionError(
                        f"cluster cannot terminate from state {record.state.value}"
                    )
                if self._has_unreleased_consumers(connection, compute_id) and not (
                    self._has_active_consumers(connection, compute_id)
                ):
                    raise ComputeConflictError(
                        "cluster has registered job consumers but no active worker to interrupt"
                    )
                now = self._now()
                generation = record.generation + 1
                transition_type = "autoterminating" if automatic else "terminating"
                final_type = "autoterminated" if automatic else "terminated"
                self._repository.update(
                    connection,
                    compute_id,
                    {
                        "state": ComputeState.TERMINATING.value,
                        "updated_at": now,
                        "generation": generation,
                    },
                )
                self._append_event(
                    connection,
                    compute_id,
                    transition_type,
                    ComputeState.TERMINATING,
                    generation,
                    True,
                    now,
                )
                self._repository.update(
                    connection,
                    compute_id,
                    {
                        "state": ComputeState.TERMINATED.value,
                        "updated_at": now,
                        "terminated_at": now,
                    },
                )
                self._append_event(
                    connection,
                    compute_id,
                    final_type,
                    ComputeState.TERMINATED,
                    generation,
                    False,
                    now,
                )
                return self._require(connection, compute_id)
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not terminate cluster metadata") from error

    def permanently_delete_cluster(self, compute_id: str) -> None:
        self._validate_compute_id(compute_id)
        try:
            with self._repository.transaction(write=True) as connection:
                record = self._require(connection, compute_id)
                self._require_all_purpose(record)
                if record.state is not ComputeState.TERMINATED:
                    raise ComputeStateTransitionError(
                        "clusters must be terminated before permanent deletion"
                    )
                now = self._now()
                self._repository.update(
                    connection,
                    compute_id,
                    {"deleted_at": now, "updated_at": now},
                )
                self._append_event(
                    connection,
                    compute_id,
                    "deleted",
                    ComputeState.TERMINATED,
                    record.generation,
                    False,
                    now,
                )
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not delete cluster metadata") from error

    def provision_job_compute(
        self,
        run_id: int,
        compute_key: str,
        kind: ComputeResourceKind,
        body: Mapping[str, object],
        task_keys: Sequence[str],
    ) -> ComputeRecord:
        """Reserve a new or shared jobs resource and its full planned consumer set."""
        if kind not in (ComputeResourceKind.JOB_NEW, ComputeResourceKind.JOB_SHARED):
            raise InvalidComputeRequestError("job compute kind must be job_new or job_shared")
        if type(run_id) is not int or run_id < 1:
            raise InvalidComputeRequestError("run_id must be a positive integer")
        if not isinstance(compute_key, str) or not _COMPUTE_KEY_RE.fullmatch(compute_key):
            raise InvalidComputeRequestError("compute_key must be a bounded identifier")
        keys = tuple(task_keys)
        if (
            not keys
            or len(set(keys)) != len(keys)
            or any(not isinstance(key, str) or not _TASK_KEY_RE.fullmatch(key) for key in keys)
        ):
            raise InvalidComputeRequestError("task_keys must be unique bounded identifiers")
        if kind is ComputeResourceKind.JOB_NEW and len(keys) != 1:
            raise InvalidComputeRequestError("job_new compute must have exactly one consumer")
        config = normalize_cluster_configuration(body)
        now = self._now()
        compute_id = f"bhc-{uuid.uuid4().hex}"
        try:
            with self._repository.transaction(write=True) as connection:
                existing = self._repository.get_by_run_key(connection, run_id, compute_key)
                if existing is not None:
                    existing_consumers = tuple(
                        item.task_key
                        for item in self._repository.list_consumers(connection, existing.compute_id)
                    )
                    if (
                        existing.resource_kind is kind
                        and existing_consumers == tuple(sorted(keys))
                        and _same_configuration(existing, config)
                    ):
                        return existing
                    raise ComputeConflictError("compute key already has a different run resource")
                run = connection.execute(
                    "SELECT state FROM job_runs WHERE run_id = ?", (run_id,)
                ).fetchone()
                if run is None:
                    raise InvalidComputeRequestError(f"job run does not exist: {run_id}")
                if run["state"] not in {"queued", "running"}:
                    raise ComputeStateTransitionError("job compute requires an active job run")
                found_keys = {
                    row[0]
                    for row in connection.execute(
                        "SELECT task_key FROM job_run_tasks WHERE run_id = ?", (run_id,)
                    ).fetchall()
                }
                if not set(keys) <= found_keys:
                    raise InvalidComputeRequestError("all compute consumers must belong to the run")
                self._repository.insert(
                    connection,
                    compute_id=compute_id,
                    resource_kind=kind,
                    profile=config.profile,
                    state=ComputeState.PENDING,
                    run_id=run_id,
                    compute_key=compute_key,
                    cluster_name=config.cluster_name,
                    spark_version=config.spark_version,
                    access_mode=config.access_mode,
                    single_user_name=config.single_user_name,
                    settings_json=config.settings_json,
                    autotermination_minutes=config.autotermination_minutes,
                    created_at=now,
                    updated_at=now,
                    last_activity_at=now,
                    generation=1,
                )
                self._append_event(
                    connection, compute_id, "created", ComputeState.PENDING, 1, False, now
                )
                for key in sorted(keys):
                    self._repository.insert_consumer(
                        connection,
                        compute_id=compute_id,
                        run_id=run_id,
                        task_key=key,
                        registered_at=now,
                    )
                    self._append_event(
                        connection,
                        compute_id,
                        "consumer_registered",
                        ComputeState.PENDING,
                        1,
                        False,
                        now,
                        {"task_key": key},
                    )
                return self._require(connection, compute_id)
        except ComputeError:
            raise
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not create job compute metadata") from error

    def register_all_purpose_consumer(
        self,
        compute_id: str,
        run_id: int,
        task_key: str,
    ) -> ComputeConsumerRecord:
        self._validate_compute_id(compute_id)
        self._validate_consumer_key(run_id, task_key)
        try:
            with self._repository.transaction(write=True) as connection:
                record = self._require(connection, compute_id)
                self._require_all_purpose(record)
                existing = self._repository.get_consumer(connection, compute_id, run_id, task_key)
                if existing is not None:
                    if existing.state == "registered":
                        return existing
                    raise ComputeConflictError("this job task already consumed the cluster")
                run = connection.execute(
                    "SELECT state FROM job_runs WHERE run_id = ?", (run_id,)
                ).fetchone()
                task = connection.execute(
                    "SELECT 1 FROM job_run_tasks WHERE run_id = ? AND task_key = ?",
                    (run_id, task_key),
                ).fetchone()
                if run is None or task is None or run["state"] not in {"queued", "running"}:
                    raise InvalidComputeRequestError(
                        "consumer must be an active submitted job task"
                    )
                now = self._now()
                self._repository.insert_consumer(
                    connection,
                    compute_id=compute_id,
                    run_id=run_id,
                    task_key=task_key,
                    registered_at=now,
                )
                self._append_event(
                    connection,
                    compute_id,
                    "consumer_registered",
                    record.state,
                    record.generation,
                    False,
                    now,
                    {"run_id": run_id, "task_key": task_key},
                )
                consumer = self._repository.get_consumer(connection, compute_id, run_id, task_key)
                assert consumer is not None
                return consumer
        except ComputeError:
            raise
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not register cluster consumer") from error

    def start_for_consumer(
        self,
        compute_id: str,
        run_id: int,
        task_key: str,
        *,
        autostart: bool = False,
    ) -> ComputeRecord:
        self._validate_compute_id(compute_id)
        self._validate_consumer_key(run_id, task_key)
        if type(autostart) is not bool:
            raise InvalidComputeRequestError("autostart must be a boolean")
        try:
            with self._repository.transaction(write=True) as connection:
                record = self._require(connection, compute_id)
                consumer = self._repository.get_consumer(connection, compute_id, run_id, task_key)
                if consumer is None:
                    raise InvalidComputeRequestError("job task is not registered as a consumer")
                if consumer.state != "registered":
                    raise ComputeConflictError("job task consumer has already started or released")
                now = self._now()
                if record.state is ComputeState.TERMINATED:
                    if record.resource_kind is not ComputeResourceKind.ALL_PURPOSE or not autostart:
                        raise ComputeStateTransitionError(
                            "terminated compute requires explicit all-purpose autostart"
                        )
                    generation = record.generation + 1
                    self._repository.update(
                        connection,
                        compute_id,
                        {
                            "state": ComputeState.PENDING.value,
                            "updated_at": now,
                            "generation": generation,
                            "terminated_at": None,
                        },
                    )
                    self._append_event(
                        connection,
                        compute_id,
                        "started",
                        ComputeState.PENDING,
                        generation,
                        True,
                        now,
                    )
                    record = self._require(connection, compute_id)
                if record.state is ComputeState.PENDING:
                    invalidates = record.generation > 1
                    self._repository.update(
                        connection,
                        compute_id,
                        {
                            "state": ComputeState.RUNNING.value,
                            "updated_at": now,
                            "started_at": now,
                            "last_activity_at": now,
                        },
                    )
                    self._append_event(
                        connection,
                        compute_id,
                        "started",
                        ComputeState.RUNNING,
                        record.generation,
                        invalidates,
                        now,
                    )
                elif record.state is ComputeState.RUNNING:
                    self._repository.update(
                        connection,
                        compute_id,
                        {"updated_at": now, "last_activity_at": now},
                    )
                else:
                    raise ComputeStateTransitionError(
                        f"compute cannot accept a consumer while {record.state.value}"
                    )
                self._repository.update_consumer(
                    connection,
                    compute_id=compute_id,
                    run_id=run_id,
                    task_key=task_key,
                    state="active",
                    started_at=now,
                    released_at=None,
                )
                current = self._require(connection, compute_id)
                self._repository.append_consumer_event(
                    connection,
                    compute=current,
                    event_type="consumer_started",
                    occurred_at=now,
                    details_json=_json({"run_id": run_id, "task_key": task_key}),
                )
                return current
        except ComputeError:
            raise
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not start job compute consumer") from error

    def release_consumer(
        self,
        compute_id: str,
        run_id: int,
        task_key: str,
    ) -> ComputeRecord:
        self._validate_compute_id(compute_id)
        self._validate_consumer_key(run_id, task_key)
        try:
            with self._repository.transaction(write=True) as connection:
                record = self._require(connection, compute_id)
                consumer = self._repository.get_consumer(connection, compute_id, run_id, task_key)
                if consumer is None:
                    raise InvalidComputeRequestError("job task is not registered as a consumer")
                if consumer.state == "released":
                    return record
                now = self._now()
                self._repository.update_consumer(
                    connection,
                    compute_id=compute_id,
                    run_id=run_id,
                    task_key=task_key,
                    state="released",
                    started_at=consumer.started_at,
                    released_at=now,
                )
                self._repository.append_consumer_event(
                    connection,
                    compute=record,
                    event_type="consumer_released",
                    occurred_at=now,
                    details_json=_json({"run_id": run_id, "task_key": task_key}),
                )
                if record.resource_kind is ComputeResourceKind.ALL_PURPOSE:
                    self._repository.update(
                        connection,
                        compute_id,
                        {"updated_at": now, "last_activity_at": now},
                    )
                    return self._require(connection, compute_id)
                consumers = self._repository.list_consumers(connection, compute_id)
                if consumers and all(item.state == "released" for item in consumers):
                    return self._terminate_job_resource(connection, record, now)
                return self._require(connection, compute_id)
        except ComputeError:
            raise
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not release job compute consumer") from error

    def expire_idle(self, *, at: datetime | None = None) -> list[ComputeRecord]:
        """Terminate idle all-purpose resources using an injectable UTC clock."""
        now = _format_time(at if at is not None else self._clock())
        now_value = _parse_time(now)
        expired: list[ComputeRecord] = []
        try:
            with self._repository.transaction(write=True) as connection:
                records = [
                    item
                    for item in self._repository.list(connection)
                    if item.resource_kind is ComputeResourceKind.ALL_PURPOSE
                    and item.state is ComputeState.RUNNING
                    and item.autotermination_minutes not in (None, 0)
                ]
                for record in records:
                    if self._has_unreleased_consumers(connection, record.compute_id):
                        continue
                    idle_minutes = (
                        now_value - _parse_time(record.last_activity_at)
                    ).total_seconds() / 60
                    if (
                        record.autotermination_minutes is None
                        or record.autotermination_minutes == 0
                        or idle_minutes < record.autotermination_minutes
                    ):
                        continue
                    expired.append(self._terminate_record(connection, record, now, automatic=True))
            return expired
        except ComputeError:
            raise
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not expire idle compute resources") from error

    def get_consumers(self, compute_id: str) -> list[ComputeConsumerRecord]:
        self._validate_compute_id(compute_id)
        try:
            with self._repository.transaction() as connection:
                self._require(connection, compute_id)
                return list(self._repository.list_consumers(connection, compute_id))
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not read compute consumers") from error

    def get_run_consumer(self, run_id: int, task_key: str) -> ComputeConsumerRecord | None:
        """Return the one resource binding reserved for a submitted job task."""
        self._validate_consumer_key(run_id, task_key)
        try:
            with self._repository.transaction() as connection:
                return self._repository.get_run_consumer(connection, run_id, task_key)
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not read a job compute consumer") from error

    def release_run_consumers(self, run_id: int) -> None:
        """Release every registered or active consumer after its job run settles."""
        if type(run_id) is not int or run_id < 1:
            raise InvalidComputeRequestError("run_id must be a positive integer")
        try:
            with self._repository.transaction() as connection:
                consumers = tuple(self._repository.list_run_consumers(connection, run_id))
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not list job compute consumers") from error
        for consumer in consumers:
            if consumer.state != "released":
                self.release_consumer(consumer.compute_id, run_id, consumer.task_key)

    def get_lifecycle_events(self, compute_id: str) -> list[ComputeLifecycleEvent]:
        self._validate_compute_id(compute_id)
        try:
            with self._repository.transaction() as connection:
                self._require(connection, compute_id)
                return list(self._repository.list_events(connection, compute_id))
        except sqlite3.Error as error:
            raise ComputeRepositoryError("could not read compute lifecycle events") from error

    def _terminate_job_resource(
        self,
        connection: sqlite3.Connection,
        record: ComputeRecord,
        now: str,
    ) -> ComputeRecord:
        return self._terminate_record(connection, record, now, automatic=False)

    def _terminate_record(
        self,
        connection: sqlite3.Connection,
        record: ComputeRecord,
        now: str,
        *,
        automatic: bool,
    ) -> ComputeRecord:
        if record.state is ComputeState.TERMINATED:
            return record
        if record.state not in {ComputeState.RUNNING, ComputeState.PENDING}:
            raise ComputeStateTransitionError(
                f"compute cannot terminate from state {record.state.value}"
            )
        generation = record.generation + 1
        start_event = "autoterminating" if automatic else "terminating"
        finish_event = "autoterminated" if automatic else "terminated"
        self._repository.update(
            connection,
            record.compute_id,
            {
                "state": ComputeState.TERMINATING.value,
                "updated_at": now,
                "generation": generation,
            },
        )
        self._append_event(
            connection,
            record.compute_id,
            start_event,
            ComputeState.TERMINATING,
            generation,
            True,
            now,
        )
        self._repository.update(
            connection,
            record.compute_id,
            {
                "state": ComputeState.TERMINATED.value,
                "updated_at": now,
                "terminated_at": now,
            },
        )
        self._append_event(
            connection,
            record.compute_id,
            finish_event,
            ComputeState.TERMINATED,
            generation,
            False,
            now,
        )
        return self._require(connection, record.compute_id)

    def _append_event(
        self,
        connection: sqlite3.Connection,
        compute_id: str,
        event_type: str,
        state: ComputeState,
        generation: int,
        invalidates_sessions: bool,
        occurred_at: str,
        details: Mapping[str, object] | None = None,
    ) -> None:
        self._repository.append_event(
            connection,
            compute_id=compute_id,
            event_type=event_type,
            state=state,
            generation=generation,
            invalidates_sessions=invalidates_sessions,
            occurred_at=occurred_at,
            details_json=_json(details or {}),
        )

    def _require(self, connection: sqlite3.Connection, compute_id: str) -> ComputeRecord:
        record = self._repository.get(connection, compute_id)
        if record is None:
            raise ComputeNotFoundError(f"cluster does not exist: {compute_id}")
        return record

    @staticmethod
    def _require_all_purpose(record: ComputeRecord) -> None:
        if record.resource_kind is not ComputeResourceKind.ALL_PURPOSE:
            raise ComputeNotFoundError(f"cluster does not exist: {record.compute_id}")

    def _has_unreleased_consumers(self, connection: sqlite3.Connection, compute_id: str) -> bool:
        row = connection.execute(
            "SELECT 1 FROM compute_consumers WHERE compute_id = ? AND state != 'released' LIMIT 1",
            (compute_id,),
        ).fetchone()
        return row is not None

    def _has_active_consumers(self, connection: sqlite3.Connection, compute_id: str) -> bool:
        row = connection.execute(
            "SELECT 1 FROM compute_consumers WHERE compute_id = ? AND state = 'active' LIMIT 1",
            (compute_id,),
        ).fetchone()
        return row is not None

    def _now(self) -> str:
        return _format_time(self._clock())

    @staticmethod
    def _validate_compute_id(compute_id: str) -> None:
        if not isinstance(compute_id, str) or not compute_id or len(compute_id) > 128:
            raise InvalidComputeRequestError("cluster_id must be a non-empty bounded string")

    @staticmethod
    def _validate_consumer_key(run_id: int, task_key: str) -> None:
        if type(run_id) is not int or run_id < 1:
            raise InvalidComputeRequestError("run_id must be a positive integer")
        if not isinstance(task_key, str) or not _TASK_KEY_RE.fullmatch(task_key):
            raise InvalidComputeRequestError("task_key must be a bounded identifier")


def _same_configuration(record: ComputeRecord, config: ClusterConfiguration) -> bool:
    return (
        record.cluster_name == config.cluster_name
        and record.spark_version == config.spark_version
        and record.access_mode == config.access_mode
        and record.profile == config.profile
        and record.single_user_name == config.single_user_name
        and record.settings_json == config.settings_json
        and record.autotermination_minutes == config.autotermination_minutes
    )


def _json(value: Mapping[str, object]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _format_time(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise InvalidComputeRequestError("compute clock must return a timezone-aware datetime")
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
