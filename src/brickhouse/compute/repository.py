"""SQLite persistence for the workspace-scoped compute lifecycle."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING

from brickhouse.compute.models import (
    ComputeConsumerRecord,
    ComputeLifecycleEvent,
    ComputeRecord,
    ComputeResourceKind,
    ComputeState,
)

if TYPE_CHECKING:
    from brickhouse.workspace.context import WorkspaceContext


class ComputeRepositoryError(RuntimeError):
    """A compute metadata operation could not be persisted or read."""


class ComputeRepository:
    """Read and write compute records in the authoritative workspace database."""

    _RESOURCE_FIELDS = {
        "profile",
        "state",
        "cluster_name",
        "spark_version",
        "access_mode",
        "single_user_name",
        "settings_json",
        "autotermination_minutes",
        "updated_at",
        "started_at",
        "terminated_at",
        "last_activity_at",
        "generation",
        "deleted_at",
    }

    def __init__(self, context: WorkspaceContext) -> None:
        self._context = context

    @contextmanager
    def transaction(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        workspace = self._context.workspace
        connection = sqlite3.connect(workspace.metadata_path, timeout=5.0)
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

    def get(self, connection: sqlite3.Connection, compute_id: str) -> ComputeRecord | None:
        row = connection.execute(
            "SELECT * FROM compute_resources WHERE compute_id = ? AND deleted_at IS NULL",
            (compute_id,),
        ).fetchone()
        return _resource_record(row) if row is not None else None

    def get_by_run_key(
        self,
        connection: sqlite3.Connection,
        run_id: int,
        compute_key: str,
    ) -> ComputeRecord | None:
        row = connection.execute(
            "SELECT * FROM compute_resources WHERE run_id = ? AND compute_key = ? "
            "AND deleted_at IS NULL",
            (run_id, compute_key),
        ).fetchone()
        return _resource_record(row) if row is not None else None

    def list(
        self,
        connection: sqlite3.Connection,
        *,
        resource_kind: ComputeResourceKind | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[ComputeRecord]:
        sql = "SELECT * FROM compute_resources WHERE deleted_at IS NULL "
        parameters: list[object] = []
        if resource_kind is not None:
            sql += "AND resource_kind = ? "
            parameters.append(resource_kind.value)
        sql += "ORDER BY created_at, compute_id"
        if limit is not None:
            sql += " LIMIT ? OFFSET ?"
            parameters.extend((limit, offset))
            rows = connection.execute(sql, parameters).fetchall()
        else:
            rows = connection.execute(sql, parameters).fetchall()
        return [_resource_record(row) for row in rows]

    def count(
        self,
        connection: sqlite3.Connection,
        *,
        resource_kind: ComputeResourceKind | None = None,
    ) -> int:
        if resource_kind is None:
            row = connection.execute(
                "SELECT count(*) FROM compute_resources WHERE deleted_at IS NULL"
            ).fetchone()
        else:
            row = connection.execute(
                "SELECT count(*) FROM compute_resources "
                "WHERE deleted_at IS NULL AND resource_kind = ?",
                (resource_kind.value,),
            ).fetchone()
        assert row is not None
        return int(row[0])

    def insert(
        self,
        connection: sqlite3.Connection,
        *,
        compute_id: str,
        resource_kind: ComputeResourceKind,
        profile: str,
        state: ComputeState,
        run_id: int | None,
        compute_key: str | None,
        cluster_name: str,
        spark_version: str,
        access_mode: str,
        single_user_name: str | None,
        settings_json: str,
        autotermination_minutes: int | None,
        created_at: str,
        updated_at: str,
        last_activity_at: str,
        generation: int,
    ) -> None:
        connection.execute(
            "INSERT INTO compute_resources "
            "(compute_id, resource_kind, profile, state, run_id, compute_key, cluster_name, "
            "spark_version, access_mode, single_user_name, settings_json, "
            "autotermination_minutes, created_at, updated_at, last_activity_at, generation) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                compute_id,
                resource_kind.value,
                profile,
                state.value,
                run_id,
                compute_key,
                cluster_name,
                spark_version,
                access_mode,
                single_user_name,
                settings_json,
                autotermination_minutes,
                created_at,
                updated_at,
                last_activity_at,
                generation,
            ),
        )

    def update(
        self,
        connection: sqlite3.Connection,
        compute_id: str,
        changes: Mapping[str, object],
    ) -> None:
        if not changes or set(changes) - self._RESOURCE_FIELDS:
            raise ComputeRepositoryError("invalid compute resource update")
        assignments = ", ".join(f"{field} = ?" for field in changes)
        cursor = connection.execute(
            f"UPDATE compute_resources SET {assignments} WHERE compute_id = ? "
            "AND deleted_at IS NULL",
            (*changes.values(), compute_id),
        )
        if cursor.rowcount != 1:
            raise ComputeRepositoryError("compute resource disappeared during update")

    def append_event(
        self,
        connection: sqlite3.Connection,
        *,
        compute_id: str,
        event_type: str,
        state: ComputeState,
        generation: int,
        invalidates_sessions: bool,
        occurred_at: str,
        details_json: str,
    ) -> None:
        connection.execute(
            "INSERT INTO compute_lifecycle_events "
            "(compute_id, event_type, state, generation, invalidates_sessions, occurred_at, "
            "details_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                compute_id,
                event_type,
                state.value,
                generation,
                int(invalidates_sessions),
                occurred_at,
                details_json,
            ),
        )

    def get_consumer(
        self,
        connection: sqlite3.Connection,
        compute_id: str,
        run_id: int,
        task_key: str,
    ) -> ComputeConsumerRecord | None:
        row = connection.execute(
            "SELECT * FROM compute_consumers WHERE compute_id = ? AND run_id = ? AND task_key = ?",
            (compute_id, run_id, task_key),
        ).fetchone()
        return _consumer_record(row) if row is not None else None

    def list_consumers(
        self,
        connection: sqlite3.Connection,
        compute_id: str,
    ) -> Sequence[ComputeConsumerRecord]:
        rows = connection.execute(
            "SELECT * FROM compute_consumers WHERE compute_id = ? ORDER BY run_id, task_key",
            (compute_id,),
        ).fetchall()
        return [_consumer_record(row) for row in rows]

    def list_run_consumers(
        self,
        connection: sqlite3.Connection,
        run_id: int,
    ) -> Sequence[ComputeConsumerRecord]:
        rows = connection.execute(
            "SELECT * FROM compute_consumers WHERE run_id = ? ORDER BY task_key, compute_id",
            (run_id,),
        ).fetchall()
        return [_consumer_record(row) for row in rows]

    def get_run_consumer(
        self,
        connection: sqlite3.Connection,
        run_id: int,
        task_key: str,
    ) -> ComputeConsumerRecord | None:
        row = connection.execute(
            "SELECT * FROM compute_consumers WHERE run_id = ? AND task_key = ?",
            (run_id, task_key),
        ).fetchone()
        return _consumer_record(row) if row is not None else None

    def insert_consumer(
        self,
        connection: sqlite3.Connection,
        *,
        compute_id: str,
        run_id: int,
        task_key: str,
        registered_at: str,
    ) -> None:
        connection.execute(
            "INSERT INTO compute_consumers "
            "(compute_id, run_id, task_key, state, registered_at) "
            "VALUES (?, ?, ?, 'registered', ?)",
            (compute_id, run_id, task_key, registered_at),
        )

    def update_consumer(
        self,
        connection: sqlite3.Connection,
        *,
        compute_id: str,
        run_id: int,
        task_key: str,
        state: str,
        started_at: str | None,
        released_at: str | None,
    ) -> None:
        cursor = connection.execute(
            "UPDATE compute_consumers SET state = ?, started_at = ?, released_at = ? "
            "WHERE compute_id = ? AND run_id = ? AND task_key = ?",
            (state, started_at, released_at, compute_id, run_id, task_key),
        )
        if cursor.rowcount != 1:
            raise ComputeRepositoryError("compute consumer disappeared during update")

    def append_consumer_event(
        self,
        connection: sqlite3.Connection,
        *,
        compute: ComputeRecord,
        event_type: str,
        occurred_at: str,
        details_json: str,
    ) -> None:
        self.append_event(
            connection,
            compute_id=compute.compute_id,
            event_type=event_type,
            state=compute.state,
            generation=compute.generation,
            invalidates_sessions=False,
            occurred_at=occurred_at,
            details_json=details_json,
        )

    def list_events(
        self,
        connection: sqlite3.Connection,
        compute_id: str,
    ) -> Sequence[ComputeLifecycleEvent]:
        rows = connection.execute(
            "SELECT * FROM compute_lifecycle_events WHERE compute_id = ? ORDER BY event_id",
            (compute_id,),
        ).fetchall()
        return [_event_record(row) for row in rows]


def _resource_record(row: sqlite3.Row) -> ComputeRecord:
    return ComputeRecord(
        row["compute_id"],
        ComputeResourceKind(row["resource_kind"]),
        row["profile"],
        ComputeState(row["state"]),
        row["run_id"],
        row["compute_key"],
        row["cluster_name"],
        row["spark_version"],
        row["access_mode"],
        row["single_user_name"],
        row["settings_json"],
        row["autotermination_minutes"],
        row["created_at"],
        row["updated_at"],
        row["started_at"],
        row["terminated_at"],
        row["last_activity_at"],
        row["generation"],
        row["deleted_at"],
    )


def _consumer_record(row: sqlite3.Row) -> ComputeConsumerRecord:
    return ComputeConsumerRecord(
        row["compute_id"],
        row["run_id"],
        row["task_key"],
        row["state"],
        row["registered_at"],
        row["started_at"],
        row["released_at"],
    )


def _event_record(row: sqlite3.Row) -> ComputeLifecycleEvent:
    return ComputeLifecycleEvent(
        row["event_id"],
        row["compute_id"],
        row["event_type"],
        ComputeState(row["state"]),
        row["generation"],
        bool(row["invalidates_sessions"]),
        row["occurred_at"],
        row["details_json"],
    )
