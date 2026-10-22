"""Persistent lifecycle for the initial local Statement Execution API profile."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import Event, Lock, Thread
from typing import TYPE_CHECKING, cast

from brickhouse.catalog.identifiers import NamespaceState
from brickhouse.catalog.repository import CatalogRepositoryError
from brickhouse.compute.warehouse import (
    WarehouseAdmissionLimitError,
    WarehouseConflictError,
    WarehouseNotFoundError,
    WarehouseRecord,
    WarehouseState,
)
from brickhouse.sql.cancellation import SqlStatementCancelledError, SqlStatementControl
from brickhouse.sql.errors import SqlStatementError
from brickhouse.sql.parameters import (
    SqlStatementParameter,
    deserialize_statement_parameters,
    serialize_statement_parameters,
    validate_statement_parameters,
)
from brickhouse.sql.results import (
    MAX_INLINE_RESULT_BYTES,
    MAX_RETAINED_SQL_RESULT_BYTES,
    SQL_RESULT_RETENTION_SECONDS,
    SqlResultLimitError,
    encode_json_array,
    split_json_array_result,
)

if TYPE_CHECKING:
    from brickhouse.workspace.context import WorkspaceContext


class SqlStatementRepositoryError(RuntimeError):
    """The persisted SQL statement lifecycle could not be read or updated."""


class SqlStatementNotFoundError(LookupError):
    """No local SQL statement exists for the requested statement ID."""


@dataclass(frozen=True, slots=True)
class SqlStatementRecord:
    statement_id: str
    warehouse_id: str
    sql_text: str
    parameters_json: str | None
    row_limit: int | None
    byte_limit: int | None
    catalog_name: str
    schema_name: str
    state: str
    created_at: str
    started_at: str | None
    finished_at: str | None
    error_code: str | None
    error_message: str | None
    sql_state: str | None
    manifest_json: str | None
    result_json: str | None
    result_row_count: int | None
    result_byte_count: int | None
    result_closed: bool = False


class _SqlStatementRepository:
    def __init__(self, context: WorkspaceContext) -> None:
        self._context = context

    @contextmanager
    def _connection(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
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
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    def create(
        self,
        *,
        statement_id: str,
        warehouse_id: str,
        sql_text: str,
        parameters_json: str | None,
        row_limit: int | None,
        byte_limit: int | None,
        namespace: NamespaceState,
    ) -> SqlStatementRecord:
        now = _utc_now()
        try:
            with self._connection(write=True) as connection:
                connection.execute(
                    "INSERT INTO sql_statements "
                    "(statement_id, warehouse_id, sql_text, parameters_json, row_limit, "
                    "byte_limit, catalog_name, schema_name, state, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?)",
                    (
                        statement_id,
                        warehouse_id,
                        sql_text,
                        parameters_json,
                        row_limit,
                        byte_limit,
                        namespace.current_catalog,
                        namespace.current_schema,
                        now,
                    ),
                )
                row = connection.execute(
                    "SELECT * FROM sql_statements WHERE statement_id = ?", (statement_id,)
                ).fetchone()
                assert row is not None
                return _statement_record(row)
        except sqlite3.Error as error:
            raise SqlStatementRepositoryError("could not persist SQL statement request") from error

    def get(self, statement_id: str) -> SqlStatementRecord:
        try:
            with self._connection(write=True) as connection:
                now = _utc_now()
                self._expire_results(connection, now)
                self._enforce_retention(connection, required_bytes=0, now=now)
                row = connection.execute(
                    "SELECT * FROM sql_statements WHERE statement_id = ?", (statement_id,)
                ).fetchone()
                payload = connection.execute(
                    "SELECT * FROM sql_statement_results WHERE statement_id = ?",
                    (statement_id,),
                ).fetchone()
                first_chunk = connection.execute(
                    "SELECT result_json FROM sql_statement_result_chunks "
                    "WHERE statement_id = ? AND chunk_index = 0",
                    (statement_id,),
                ).fetchone()
            if row is None:
                raise SqlStatementNotFoundError(f"SQL statement does not exist: {statement_id}")
            return _statement_record(row, payload, first_chunk)
        except SqlStatementNotFoundError:
            raise
        except sqlite3.Error as error:
            raise SqlStatementRepositoryError("could not read SQL statement state") from error

    def get_result_chunk(self, statement_id: str, chunk_index: int) -> str | None:
        try:
            with self._connection() as connection:
                row = connection.execute(
                    "SELECT result_json FROM sql_statement_result_chunks "
                    "WHERE statement_id = ? AND chunk_index = ?",
                    (statement_id, chunk_index),
                ).fetchone()
            return str(row[0]) if row is not None else None
        except sqlite3.Error as error:
            raise SqlStatementRepositoryError("could not read SQL result chunk") from error

    def close_result(self, statement_id: str) -> None:
        """Release a terminal statement's retained chunks while preserving its record."""
        try:
            with self._connection(write=True) as connection:
                now = _utc_now()
                self._expire_results(connection, now)
                row = connection.execute(
                    "SELECT state FROM sql_statements WHERE statement_id = ?",
                    (statement_id,),
                ).fetchone()
                if row is None:
                    raise SqlStatementNotFoundError(f"SQL statement does not exist: {statement_id}")
                if row["state"] in {"PENDING", "RUNNING"}:
                    raise SqlStatementStateError(
                        "cannot close SQL result while the statement is active"
                    )
                self._close_result(connection, statement_id, now)
        except (SqlStatementNotFoundError, SqlStatementStateError):
            raise
        except sqlite3.Error as error:
            raise SqlStatementRepositoryError("could not close SQL statement result") from error

    @staticmethod
    def _close_result(connection: sqlite3.Connection, statement_id: str, now: str) -> int:
        row = connection.execute(
            "SELECT stored_bytes FROM sql_statement_results "
            "WHERE statement_id = ? AND has_result = 1 AND closed_at IS NULL",
            (statement_id,),
        ).fetchone()
        if row is None:
            return 0
        stored_bytes = int(row[0])
        connection.execute(
            "UPDATE sql_statement_results SET manifest_json = NULL, closed_at = ?, "
            "stored_bytes = 0 WHERE statement_id = ? AND closed_at IS NULL",
            (now, statement_id),
        )
        connection.execute(
            "DELETE FROM sql_statement_result_chunks WHERE statement_id = ?",
            (statement_id,),
        )
        return stored_bytes

    @classmethod
    def _expire_results(cls, connection: sqlite3.Connection, now: str) -> None:
        expired_ids = [
            str(row[0])
            for row in connection.execute(
                "SELECT statement_id FROM sql_statement_results "
                "WHERE has_result = 1 AND closed_at IS NULL AND expires_at <= ?",
                (now,),
            ).fetchall()
        ]
        for statement_id in expired_ids:
            cls._close_result(connection, statement_id, now)

    @classmethod
    def _enforce_retention(
        cls,
        connection: sqlite3.Connection,
        *,
        required_bytes: int,
        now: str,
    ) -> None:
        total = connection.execute(
            "SELECT COALESCE(sum(stored_bytes), 0) FROM sql_statement_results "
            "WHERE has_result = 1 AND closed_at IS NULL"
        ).fetchone()
        assert total is not None
        retained_bytes = int(total[0])
        while retained_bytes + required_bytes > MAX_RETAINED_SQL_RESULT_BYTES:
            oldest = connection.execute(
                "SELECT statement_id FROM sql_statement_results "
                "WHERE has_result = 1 AND closed_at IS NULL "
                "ORDER BY expires_at, statement_id LIMIT 1"
            ).fetchone()
            if oldest is None:
                return
            retained_bytes -= cls._close_result(connection, str(oldest[0]), now)

    def claim(self, statement_id: str) -> tuple[SqlStatementRecord, bool]:
        try:
            with self._connection(write=True) as connection:
                row = connection.execute(
                    "SELECT * FROM sql_statements WHERE statement_id = ?", (statement_id,)
                ).fetchone()
                if row is None:
                    raise SqlStatementNotFoundError(f"SQL statement does not exist: {statement_id}")
                if row["state"] != "PENDING":
                    return _statement_record(row), False
                admission = connection.execute(
                    "SELECT lease.state AS lease_state, lease.generation AS lease_generation, "
                    "warehouse.state AS warehouse_state, warehouse.generation AS generation, "
                    "warehouse.deleted_at AS deleted_at "
                    "FROM sql_statement_admission_leases AS lease "
                    "LEFT JOIN sql_warehouses AS warehouse USING(warehouse_id) "
                    "WHERE lease.statement_id = ?",
                    (statement_id,),
                ).fetchone()
                # Submission creates the durable request immediately before acquiring its
                # lease. A concurrent poll may observe that short interval; leave it pending.
                if admission is None or admission["lease_state"] == "QUEUED":
                    return _statement_record(row), False
                is_current = (
                    admission["lease_state"] == "ACTIVE"
                    and admission["warehouse_state"] == WarehouseState.RUNNING.value
                    and admission["deleted_at"] is None
                    and admission["lease_generation"] == admission["generation"]
                )
                if not is_current:
                    connection.execute(
                        "UPDATE sql_statements SET state = 'CANCELED', finished_at = ?, "
                        "error_code = 'STATEMENT_CANCELED', "
                        "error_message = 'warehouse admission was invalidated before execution', "
                        "sql_state = '57014' WHERE statement_id = ? AND state = 'PENDING'",
                        (_utc_now(), statement_id),
                    )
                    row = connection.execute(
                        "SELECT * FROM sql_statements WHERE statement_id = ?", (statement_id,)
                    ).fetchone()
                    assert row is not None
                    return _statement_record(row), False
                cursor = connection.execute(
                    "UPDATE sql_statements SET state = 'RUNNING', started_at = ? "
                    "WHERE statement_id = ? AND state = 'PENDING'",
                    (_utc_now(), statement_id),
                )
                if cursor.rowcount != 1:
                    raise SqlStatementRepositoryError(
                        "SQL statement cannot run from its current state"
                    )
                row = connection.execute(
                    "SELECT * FROM sql_statements WHERE statement_id = ?", (statement_id,)
                ).fetchone()
                assert row is not None
                return _statement_record(row), True
        except SqlStatementNotFoundError:
            raise
        except sqlite3.Error as error:
            raise SqlStatementRepositoryError("could not claim SQL statement execution") from error

    def succeed(
        self,
        statement_id: str,
        *,
        manifest: dict[str, object] | None,
        chunks: list[dict[str, object]] | None,
        row_count: int,
        byte_count: int,
    ) -> None:
        if (manifest is None) != (chunks is None):
            raise SqlResultLimitError("SQL result manifest and chunks must be present together")
        manifest_json = _json_dump(manifest) if manifest is not None else None
        serialized_chunks = [_json_dump(chunk) for chunk in chunks] if chunks is not None else []
        has_result = manifest_json is not None
        stored_bytes = len(manifest_json.encode("utf-8")) if manifest_json is not None else 0
        stored_bytes += sum(len(chunk.encode("utf-8")) for chunk in serialized_chunks)
        if stored_bytes > MAX_INLINE_RESULT_BYTES:
            raise SqlResultLimitError(
                "inline SQL result manifest and chunks exceed "
                f"{MAX_INLINE_RESULT_BYTES} encoded bytes"
            )
        now = _utc_now()
        expires_at = (
            (datetime.fromisoformat(now) + timedelta(seconds=SQL_RESULT_RETENTION_SECONDS))
            .astimezone(UTC)
            .isoformat(timespec="microseconds")
            if has_result
            else None
        )
        try:
            with self._connection(write=True) as connection:
                self._expire_results(connection, now)
                self._enforce_retention(
                    connection,
                    required_bytes=stored_bytes,
                    now=now,
                )
                cursor = connection.execute(
                    "UPDATE sql_statements SET state = 'SUCCEEDED', finished_at = ?, "
                    "manifest_json = '{}', result_json = '{}', result_row_count = ?, "
                    "result_byte_count = ? WHERE statement_id = ? AND state = 'RUNNING'",
                    (
                        now,
                        row_count,
                        byte_count,
                        statement_id,
                    ),
                )
                if cursor.rowcount != 1:
                    raise SqlStatementRepositoryError(
                        "SQL statement cannot succeed from its current state"
                    )
                connection.execute(
                    "INSERT INTO sql_statement_results "
                    "(statement_id, has_result, manifest_json, expires_at, stored_bytes) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        statement_id,
                        int(has_result),
                        manifest_json,
                        expires_at,
                        stored_bytes,
                    ),
                )
                connection.executemany(
                    "INSERT INTO sql_statement_result_chunks "
                    "(statement_id, chunk_index, result_json, stored_bytes) "
                    "VALUES (?, ?, ?, ?)",
                    [
                        (
                            statement_id,
                            index,
                            serialized_chunk,
                            len(serialized_chunk.encode("utf-8")),
                        )
                        for index, serialized_chunk in enumerate(serialized_chunks)
                    ],
                )
        except SqlStatementRepositoryError:
            raise
        except sqlite3.Error as error:
            raise SqlStatementRepositoryError("could not persist SQL statement result") from error

    def fail(
        self,
        statement_id: str,
        *,
        error_code: str,
        message: str,
        sql_state: str,
    ) -> None:
        try:
            with self._connection(write=True) as connection:
                cursor = connection.execute(
                    "UPDATE sql_statements SET state = 'FAILED', "
                    "started_at = COALESCE(started_at, ?), "
                    "finished_at = ?, error_code = ?, error_message = ?, sql_state = ? "
                    "WHERE statement_id = ? AND state IN ('PENDING', 'RUNNING')",
                    (
                        _utc_now(),
                        _utc_now(),
                        error_code,
                        message[:4096],
                        sql_state,
                        statement_id,
                    ),
                )
                if cursor.rowcount != 1:
                    raise SqlStatementRepositoryError(
                        "SQL statement cannot fail from its current state"
                    )
        except SqlStatementRepositoryError:
            raise
        except sqlite3.Error as error:
            raise SqlStatementRepositoryError("could not persist SQL statement failure") from error

    def cancel(self, statement_id: str, *, message: str) -> None:
        try:
            with self._connection(write=True) as connection:
                cursor = connection.execute(
                    "UPDATE sql_statements SET state = 'CANCELED', finished_at = ?, "
                    "error_code = 'CANCELLED', error_message = ?, sql_state = '57014' "
                    "WHERE statement_id = ? AND state IN ('PENDING', 'RUNNING')",
                    (_utc_now(), message[:4096], statement_id),
                )
                if cursor.rowcount != 1:
                    raise SqlStatementRepositoryError(
                        "SQL statement cannot be canceled from its current state"
                    )
        except SqlStatementRepositoryError:
            raise
        except sqlite3.Error as error:
            raise SqlStatementRepositoryError(
                "could not persist SQL statement cancellation"
            ) from error

    def cancel_pending(self, statement_id: str, *, message: str) -> bool:
        """Cancel only work that has not been claimed by an execution worker."""
        try:
            with self._connection(write=True) as connection:
                cursor = connection.execute(
                    "UPDATE sql_statements SET state = 'CANCELED', finished_at = ?, "
                    "error_code = 'CANCELLED', error_message = ?, sql_state = '57014' "
                    "WHERE statement_id = ? AND state = 'PENDING'",
                    (_utc_now(), message[:4096], statement_id),
                )
                return cursor.rowcount == 1
        except sqlite3.Error as error:
            raise SqlStatementRepositoryError(
                "could not persist queued SQL statement cancellation"
            ) from error

    def fail_interrupted(self) -> list[str]:
        """Mark execution that was active at process restart without replaying it."""
        try:
            with self._connection(write=True) as connection:
                statement_ids = [
                    str(row[0])
                    for row in connection.execute(
                        "SELECT statement_id FROM sql_statements WHERE state = 'RUNNING'"
                    ).fetchall()
                ]
                if not statement_ids:
                    return []
                cursor = connection.execute(
                    "UPDATE sql_statements SET state = 'FAILED', finished_at = ?, "
                    "error_code = 'INTERNAL_ERROR', "
                    "error_message = 'local SQL execution was interrupted by process restart', "
                    "sql_state = 'HY000' WHERE state = 'RUNNING'",
                    (_utc_now(),),
                )
                assert cursor.rowcount == len(statement_ids)
                return statement_ids
        except sqlite3.Error as error:
            raise SqlStatementRepositoryError(
                "could not reconcile interrupted SQL statements"
            ) from error


class SqlStatementExecutionService:
    """Submit, run, poll, and fetch local SQL statements with durable outcomes."""

    def __init__(self, context: WorkspaceContext) -> None:
        self._context = context
        self._repository = _SqlStatementRepository(context)
        self._worker_lock = Lock()
        self._workers: dict[str, tuple[Thread, Event]] = {}
        self._controls: dict[str, tuple[str, SqlStatementControl]] = {}
        self._closed = False
        self._remove_invalidation_listener = (
            self._context.compute.warehouses.add_admission_invalidation_listener(
                self._warehouse_admissions_invalidated
            )
        )
        interrupted = self._repository.fail_interrupted()
        for statement_id in interrupted:
            try:
                self._context.compute.warehouses.release_statement(statement_id)
            except WarehouseNotFoundError:
                # The terminal statement record is authoritative if legacy/corrupt metadata
                # has no corresponding admission lease.
                continue

    def submit(
        self,
        *,
        sql_text: str,
        warehouse_id: str,
        catalog: str | None,
        schema: str | None,
        asynchronous: bool,
        parameters: tuple[SqlStatementParameter, ...] = (),
        row_limit: int | None = None,
        byte_limit: int | None = None,
        wait_timeout_seconds: int | None = None,
        cancel_on_wait_timeout: bool = False,
    ) -> dict[str, object]:
        validate_statement_parameters(sql_text, parameters)
        with self._worker_lock:
            if self._closed:
                raise SqlStatementStateError("the local SQL statement service is shutting down")
            warehouse = self._context.compute.warehouses.get(warehouse_id)
            if warehouse.state is not WarehouseState.RUNNING:
                raise WarehouseConflictError(f"SQL warehouse is not running: {warehouse_id}")
            namespace = self._context.new_namespace_state()
            if catalog is not None:
                namespace.use_catalog(catalog)
            if schema is not None:
                namespace.use_schema(schema)
            statement_id = str(uuid.uuid4())
            self._repository.create(
                statement_id=statement_id,
                warehouse_id=warehouse_id,
                sql_text=sql_text,
                parameters_json=serialize_statement_parameters(parameters),
                row_limit=row_limit,
                byte_limit=byte_limit,
                namespace=namespace,
            )
            try:
                lease = self._context.compute.warehouses.admit_statement(warehouse_id, statement_id)
            except WarehouseAdmissionLimitError as error:
                self._repository.fail(
                    statement_id,
                    error_code="RESOURCE_EXHAUSTED",
                    message=str(error),
                    sql_state="54000",
                )
                return self._response(self._repository.get(statement_id))
            except WarehouseConflictError as error:
                self._repository.cancel(statement_id, message=str(error))
                return self._response(self._repository.get(statement_id))

            control = SqlStatementControl(
                commit_guard=lambda: self._context.compute.warehouses.validate_admission(
                    statement_id
                )
            )
            self._controls[statement_id] = (warehouse_id, control)

            if wait_timeout_seconds is not None:
                initial_record = self._repository.get(statement_id)
                try:
                    completed = self._start_continuation_locked(statement_id)
                except RuntimeError as error:
                    self._repository.fail(
                        statement_id,
                        error_code="INTERNAL_ERROR",
                        message=f"could not start local SQL continuation: {type(error).__name__}",
                        sql_state="HY000",
                    )
                    self._release_statement(statement_id)
                    self._forget_control(statement_id)
                    return self._response(self._repository.get(statement_id))
            else:
                initial_record = None
                completed = None
        if wait_timeout_seconds is None and not asynchronous and lease.is_active:
            self._execute_claimed(statement_id)
        if wait_timeout_seconds is not None:
            assert completed is not None
            if wait_timeout_seconds > 0:
                completed.wait(timeout=wait_timeout_seconds)
            if not completed.is_set() and cancel_on_wait_timeout:
                self.cancel(
                    statement_id,
                    message="SQL statement exceeded wait_timeout and cancellation was requested",
                )
                completed.wait(timeout=5.0)
            if wait_timeout_seconds == 0 and not cancel_on_wait_timeout:
                assert initial_record is not None
                return self._response(initial_record)
            if wait_timeout_seconds > 0 and not completed.is_set():
                return self._response(self._repository.get(statement_id))
            if wait_timeout_seconds > 0 or cancel_on_wait_timeout:
                return self._response(self._repository.get(statement_id))
            assert initial_record is not None
            return self._response(initial_record)
        return self._response(self._repository.get(statement_id))

    def cancel(
        self,
        statement_id: str,
        *,
        message: str = "SQL statement cancellation requested",
    ) -> dict[str, object]:
        """Request cancellation and return the statement's persisted current state."""
        record = self._repository.get(statement_id)
        if record.state not in {"PENDING", "RUNNING"}:
            return self._response(record)
        if record.state == "PENDING" and self._repository.cancel_pending(
            statement_id,
            message=message,
        ):
            control = self._control(statement_id)
            if control is not None:
                control.mark_terminal()
            self._release_statement(statement_id)
            self._forget_control(statement_id)
            return self._response(self._repository.get(statement_id))
        control = self._control(statement_id)
        if control is not None:
            control.request_cancel(message)
        return self._response(self._repository.get(statement_id))

    def _warehouse_admissions_invalidated(self, warehouse_id: str) -> None:
        with self._worker_lock:
            statement_ids = [
                statement_id
                for statement_id, (selected_warehouse, _control) in self._controls.items()
                if selected_warehouse == warehouse_id
            ]
        for statement_id in statement_ids:
            try:
                record = self._repository.get(statement_id)
            except SqlStatementNotFoundError:
                continue
            if record.state == "PENDING":
                self.cancel(
                    statement_id,
                    message="SQL warehouse stopped before statement execution",
                )
            elif record.state == "RUNNING":
                control = self._control(statement_id)
                if control is not None:
                    control.request_cancel(
                        "SQL warehouse stopped while the statement was executing"
                    )

    def _control(self, statement_id: str) -> SqlStatementControl | None:
        with self._worker_lock:
            entry = self._controls.get(statement_id)
            return entry[1] if entry is not None else None

    def _forget_control(self, statement_id: str) -> None:
        with self._worker_lock:
            self._controls.pop(statement_id, None)

    def _continue_until_terminal(self, statement_id: str, completed: Event) -> None:
        """Run admitted work in the background and retain its lease through completion."""
        try:
            while True:
                record = self._repository.get(statement_id)
                if record.state not in {"PENDING", "RUNNING"}:
                    return
                if record.state == "PENDING":
                    try:
                        lease = self._context.compute.warehouses.get_admission(statement_id)
                    except WarehouseNotFoundError:
                        self.cancel(
                            statement_id,
                            message="SQL statement admission disappeared before execution",
                        )
                        continue
                    if lease.is_queued:
                        if self._closed:
                            self.cancel(
                                statement_id,
                                message=(
                                    "local SQL service shut down before queued execution started"
                                ),
                            )
                            continue
                    else:
                        self._execute_claimed(statement_id)
                        continue
                time.sleep(0.02)
        finally:
            completed.set()
            with self._worker_lock:
                self._workers.pop(statement_id, None)

    def _start_continuation_locked(self, statement_id: str) -> Event:
        existing = self._workers.get(statement_id)
        if existing is not None:
            return existing[1]
        if self._closed:
            raise SqlStatementStateError("the local SQL statement service is shutting down")
        completed = Event()
        runner = Thread(
            target=self._continue_until_terminal,
            args=(statement_id, completed),
            name=f"brickhouse-sql-{statement_id[:8]}",
            daemon=False,
        )
        self._workers[statement_id] = (runner, completed)
        try:
            runner.start()
        except RuntimeError:
            self._workers.pop(statement_id, None)
            raise
        return completed

    def close(self) -> None:
        """Stop queued continuations and wait for active native SQL calls to finish."""
        with self._worker_lock:
            self._closed = True
            workers = tuple(thread for thread, _completed in self._workers.values())
        for worker in workers:
            worker.join()
        self._remove_invalidation_listener()

    def _release_statement(self, statement_id: str) -> None:
        try:
            self._context.compute.warehouses.release_statement(statement_id)
        except WarehouseNotFoundError:
            pass

    def warehouse(self, warehouse_id: str) -> WarehouseRecord:
        """Resolve an SDK/kernel warehouse ID to the persistent local resource identity."""
        return self._context.compute.warehouses.get(warehouse_id)

    def get(self, statement_id: str) -> dict[str, object]:
        record = self._repository.get(statement_id)
        if record.state == "PENDING":
            with self._worker_lock:
                self._start_continuation_locked(statement_id)
            record = self._repository.get(statement_id)
        return self._response(record)

    def get_chunk(self, statement_id: str, chunk_index: int) -> dict[str, object]:
        record = self._repository.get(statement_id)
        if record.state == "PENDING":
            with self._worker_lock:
                self._start_continuation_locked(statement_id)
            record = self._repository.get(statement_id)
        if record.result_closed:
            raise SqlStatementStateError("SQL statement result retention has expired")
        if record.state != "SUCCEEDED":
            raise SqlStatementStateError(
                f"SQL statement result is unavailable while state is {record.state}"
            )
        if record.result_json is None:
            raise SqlStatementChunkNotFoundError(
                f"SQL result chunk does not exist: {statement_id}/{chunk_index}"
            )
        serialized = (
            record.result_json
            if chunk_index == 0
            else self._repository.get_result_chunk(statement_id, chunk_index)
        )
        if serialized is None:
            raise SqlStatementChunkNotFoundError(
                f"SQL result chunk does not exist: {statement_id}/{chunk_index}"
            )
        result = json.loads(serialized)
        if not isinstance(result, dict):
            raise SqlStatementChunkNotFoundError(
                f"SQL statement has no row result chunk: {statement_id}/{chunk_index}"
            )
        return result

    def close_result(self, statement_id: str) -> dict[str, object]:
        """Close retained result chunks without deleting statement history."""
        self._repository.close_result(statement_id)
        return self._response(self._repository.get(statement_id))

    def _execute_claimed(self, statement_id: str) -> None:
        record, claimed = self._repository.claim(statement_id)
        if not claimed:
            return
        control = self._control(statement_id)
        try:
            warehouses = self._context.compute.warehouses
            if not warehouses.validate_admission(statement_id):
                self._repository.cancel(
                    statement_id,
                    message="SQL warehouse stopped before statement execution",
                )
                return
            namespace = self._context.new_namespace_state()
            namespace.use_catalog(record.catalog_name)
            namespace.use_schema(record.schema_name)
            from brickhouse.sql.service import SqlStatementService

            execution = SqlStatementService(self._context, namespace=namespace).execute(
                record.sql_text,
                operation_id=f"statement-{record.statement_id}",
                parameters=deserialize_statement_parameters(record.parameters_json),
                cancellation=control,
                row_limit=record.row_limit,
                byte_limit=record.byte_limit,
            )
            # Reads may finish after a stop/reset request. Discard their result and expose
            # cancellation; completed DDL effects remain successful and durable.
            if execution.operation.value == "select" and not warehouses.validate_admission(
                statement_id
            ):
                self._repository.cancel(
                    statement_id,
                    message="SQL warehouse stopped while the statement was executing",
                )
                return
            manifest, result, row_count, byte_count = encode_json_array(
                execution.data,
                row_limit=record.row_limit,
                byte_limit=record.byte_limit,
                total_row_count=execution.total_row_count,
                result_truncated=execution.result_truncated,
            )
            manifest, chunks = split_json_array_result(manifest, result)
            self._repository.succeed(
                statement_id,
                manifest=manifest,
                chunks=chunks,
                row_count=row_count,
                byte_count=byte_count,
            )
        except SqlStatementCancelledError as error:
            self._repository.cancel(statement_id, message=str(error))
        except (SqlStatementError, CatalogRepositoryError) as error:
            self._repository.fail(
                statement_id,
                error_code="BAD_REQUEST",
                message=str(error),
                sql_state="42000",
            )
        except WarehouseNotFoundError as error:
            self._repository.cancel(
                statement_id,
                message=str(error),
            )
        except SqlResultLimitError as error:
            self._repository.fail(
                statement_id,
                error_code="RESOURCE_EXHAUSTED",
                message=str(error),
                sql_state="54000",
            )
        except Exception as error:
            self._repository.fail(
                statement_id,
                error_code="INTERNAL_ERROR",
                message=f"local SQL execution failed: {type(error).__name__}: {error}",
                sql_state="HY000",
            )
        finally:
            if control is not None:
                control.mark_terminal()
            self._release_statement(statement_id)
            self._forget_control(statement_id)

    @staticmethod
    def _response(record: SqlStatementRecord) -> dict[str, object]:
        response: dict[str, object] = {
            "statement_id": record.statement_id,
            "status": {"state": "CLOSED" if record.result_closed else record.state},
        }
        if record.state in {"FAILED", "CANCELED"}:
            status = response["status"]
            assert isinstance(status, dict)
            if record.error_code is not None and record.error_message is not None:
                status["error"] = {
                    "error_code": record.error_code,
                    "message": record.error_message,
                }
            if record.sql_state is not None:
                status["sql_state"] = record.sql_state
        if record.state == "SUCCEEDED" and not record.result_closed:
            manifest = _json_load(record.manifest_json)
            result = _json_load(record.result_json)
            if manifest is not None:
                response["manifest"] = manifest
            if result is not None:
                response["result"] = result
        return response


class SqlStatementStateError(RuntimeError):
    """A result is not available for the current statement state."""


class SqlStatementChunkNotFoundError(LookupError):
    """A requested inline result chunk does not exist."""


def _statement_record(
    row: sqlite3.Row,
    result_header: sqlite3.Row | None = None,
    first_chunk: sqlite3.Row | None = None,
) -> SqlStatementRecord:
    return SqlStatementRecord(
        statement_id=row["statement_id"],
        warehouse_id=row["warehouse_id"],
        sql_text=row["sql_text"],
        parameters_json=row["parameters_json"],
        row_limit=row["row_limit"],
        byte_limit=row["byte_limit"],
        catalog_name=row["catalog_name"],
        schema_name=row["schema_name"],
        state=row["state"],
        created_at=row["created_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        error_code=row["error_code"],
        error_message=row["error_message"],
        sql_state=row["sql_state"],
        manifest_json=(
            result_header["manifest_json"]
            if result_header is not None and result_header["has_result"]
            else None
        ),
        result_json=(first_chunk["result_json"] if first_chunk is not None else None),
        result_row_count=row["result_row_count"],
        result_byte_count=row["result_byte_count"],
        result_closed=(result_header is not None and result_header["closed_at"] is not None),
    )


def _json_dump(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _json_load(value: str | None) -> object | None:
    if value is None:
        return None
    return cast(object, json.loads(value))


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds")
