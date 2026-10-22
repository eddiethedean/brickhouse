"""Durable per-table operation identity and commit journal."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from brickhouse.workspace.context import WorkspaceContext


class OperationJournalError(RuntimeError):
    """The local operation journal could not safely read or update an operation."""


class OperationKeyReuseError(OperationJournalError):
    """An operation key was reused for a different table payload or mode."""


class OperationStateConflictError(OperationJournalError):
    """An operation attempted an invalid durable state transition."""


class TableOperationState(StrEnum):
    PREPARED = "prepared"
    COMMITTING = "committing"
    COMMITTED = "committed"
    UNCERTAIN = "uncertain"
    NO_OP = "no_op"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class TableOperationRecord:
    table_id: str
    operation_id: str
    fingerprint: str
    mode: str
    schema_fingerprint: str
    input_sha256: str
    transaction_version: int | None
    base_version: int
    row_count: int
    state: TableOperationState
    commit_version: int | None
    error_code: str | None
    created_at: str
    updated_at: str


class TableOperationJournal:
    """Serialize one table's operation IDs, payload fingerprints, and commit versions."""

    def __init__(self, context: WorkspaceContext) -> None:
        self._context = context

    def get(self, table_id: str, operation_id: str) -> TableOperationRecord | None:
        try:
            with self._connection() as connection:
                row = connection.execute(
                    "SELECT * FROM table_operation_journal WHERE table_id = ? AND operation_id = ?",
                    (table_id, operation_id),
                ).fetchone()
            return None if row is None else self._record(row)
        except sqlite3.Error as error:
            raise OperationJournalError("could not read the table operation journal") from error

    def latest_transaction_version(self, table_id: str) -> int | None:
        try:
            with self._connection() as connection:
                row = connection.execute(
                    "SELECT max(transaction_version) FROM table_operation_journal "
                    "WHERE table_id = ?",
                    (table_id,),
                ).fetchone()
            return None if row is None else row[0]
        except sqlite3.Error as error:
            raise OperationJournalError("could not inspect table transaction sequence") from error

    def latest_unfinished(self, table_id: str) -> TableOperationRecord | None:
        try:
            with self._connection() as connection:
                row = connection.execute(
                    "SELECT * FROM table_operation_journal WHERE table_id = ? "
                    "AND state NOT IN ('committed', 'no_op', 'failed') "
                    "ORDER BY transaction_version DESC LIMIT 1",
                    (table_id,),
                ).fetchone()
            return None if row is None else self._record(row)
        except sqlite3.Error as error:
            raise OperationJournalError("could not inspect unfinished table operations") from error

    def has_unfinished_operation_id(self, operation_id: str) -> bool:
        """Check whether any table still needs this operation's staged input."""
        if not isinstance(operation_id, str) or not operation_id:
            raise ValueError("operation_id must be a non-empty string")
        try:
            with self._connection() as connection:
                row = connection.execute(
                    "SELECT 1 FROM table_operation_journal WHERE operation_id = ? "
                    "AND state IN ('prepared', 'committing', 'uncertain') LIMIT 1",
                    (operation_id,),
                ).fetchone()
            return row is not None
        except sqlite3.Error as error:
            raise OperationJournalError("could not inspect staged operation references") from error

    def prepare(
        self,
        *,
        table_id: str,
        operation_id: str,
        fingerprint: str,
        mode: str,
        schema_fingerprint: str,
        input_sha256: str,
        base_version: int,
        row_count: int,
    ) -> tuple[TableOperationRecord, bool]:
        """Persist or return an operation record and allocate its monotonic Delta version."""
        try:
            with self._connection(write=True) as connection:
                row = connection.execute(
                    "SELECT * FROM table_operation_journal WHERE table_id = ? AND operation_id = ?",
                    (table_id, operation_id),
                ).fetchone()
                if row is not None:
                    record = self._record(row)
                    if record.fingerprint != fingerprint:
                        raise OperationKeyReuseError(
                            f"operation ID {operation_id!r} was reused with a different payload"
                        )
                    return record, False

                latest = connection.execute(
                    "SELECT max(transaction_version) FROM table_operation_journal "
                    "WHERE table_id = ?",
                    (table_id,),
                ).fetchone()
                transaction_version = 0 if latest is None or latest[0] is None else latest[0] + 1
                now = datetime.now(UTC).isoformat()
                connection.execute(
                    "INSERT INTO table_operation_journal "
                    "(table_id, operation_id, fingerprint, mode, schema_fingerprint, input_sha256, "
                    "transaction_version, base_version, row_count, state, commit_version, "
                    "error_code, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'prepared', NULL, NULL, ?, ?)",
                    (
                        table_id,
                        operation_id,
                        fingerprint,
                        mode,
                        schema_fingerprint,
                        input_sha256,
                        transaction_version,
                        base_version,
                        row_count,
                        now,
                        now,
                    ),
                )
                row = connection.execute(
                    "SELECT * FROM table_operation_journal WHERE table_id = ? AND operation_id = ?",
                    (table_id, operation_id),
                ).fetchone()
                assert row is not None
                return self._record(row), True
        except OperationJournalError:
            raise
        except sqlite3.Error as error:
            raise OperationJournalError("could not prepare a table operation") from error

    def mark_committing(self, table_id: str, operation_id: str) -> TableOperationRecord:
        return self._transition(
            table_id,
            operation_id,
            allowed=(TableOperationState.PREPARED,),
            state=TableOperationState.COMMITTING,
        )

    def mark_uncertain(
        self,
        table_id: str,
        operation_id: str,
        *,
        error_code: str = "provider_outcome_unconfirmed",
    ) -> TableOperationRecord:
        try:
            with self._connection(write=True) as connection:
                cursor = connection.execute(
                    "UPDATE table_operation_journal SET state = 'uncertain', error_code = ?, "
                    "updated_at = ? WHERE table_id = ? AND operation_id = ? "
                    "AND state IN ('committing', 'uncertain')",
                    (error_code, datetime.now(UTC).isoformat(), table_id, operation_id),
                )
                row = connection.execute(
                    "SELECT * FROM table_operation_journal WHERE table_id = ? AND operation_id = ?",
                    (table_id, operation_id),
                ).fetchone()
                if row is None:
                    raise OperationStateConflictError("cannot mark an unknown operation uncertain")
                record = self._record(row)
                if cursor.rowcount == 0 and record.state is not TableOperationState.UNCERTAIN:
                    raise OperationStateConflictError(
                        f"operation {operation_id!r} cannot become uncertain from "
                        f"{record.state.value}"
                    )
                return record
        except OperationJournalError:
            raise
        except sqlite3.Error as error:
            raise OperationJournalError("could not mark a table operation uncertain") from error

    def finalize_committed(
        self,
        table_id: str,
        operation_id: str,
        commit_version: int,
        *,
        row_count: int | None = None,
    ) -> TableOperationRecord:
        if commit_version < 0:
            raise ValueError("commit_version must be zero or greater")
        if row_count is not None and row_count < 0:
            raise ValueError("row_count must be zero or greater")
        try:
            with self._connection(write=True) as connection:
                row_count_sql = "row_count = ?, " if row_count is not None else ""
                parameters: tuple[object, ...] = ((row_count,) if row_count is not None else ()) + (
                    commit_version,
                    datetime.now(UTC).isoformat(),
                    table_id,
                    operation_id,
                )
                cursor = connection.execute(
                    "UPDATE table_operation_journal SET state = 'committed', "
                    + row_count_sql
                    + "commit_version = ?, "
                    "error_code = NULL, updated_at = ? WHERE table_id = ? AND operation_id = ? "
                    "AND state IN ('committing', 'uncertain')",
                    parameters,
                )
                row = connection.execute(
                    "SELECT * FROM table_operation_journal WHERE table_id = ? AND operation_id = ?",
                    (table_id, operation_id),
                ).fetchone()
                if row is None:
                    raise OperationStateConflictError("cannot finalize an unknown operation")
                record = self._record(row)
                if record.state is TableOperationState.COMMITTED:
                    if record.commit_version != commit_version:
                        raise OperationStateConflictError(
                            f"operation {operation_id!r} has conflicting committed versions"
                        )
                    if row_count is not None and record.row_count != row_count:
                        raise OperationStateConflictError(
                            f"operation {operation_id!r} has conflicting row counts"
                        )
                    return record
                if cursor.rowcount == 0:
                    raise OperationStateConflictError(
                        f"operation {operation_id!r} cannot commit from {record.state.value}"
                    )
                return record
        except OperationJournalError:
            raise
        except sqlite3.Error as error:
            raise OperationJournalError("could not finalize a table operation") from error

    def finalize_no_op(self, table_id: str, operation_id: str) -> TableOperationRecord:
        """Persist a successful zero-row mutation without reserving a Delta txn version."""
        try:
            with self._connection(write=True) as connection:
                cursor = connection.execute(
                    "UPDATE table_operation_journal SET state = 'no_op', "
                    "transaction_version = NULL, row_count = 0, error_code = NULL, updated_at = ? "
                    "WHERE table_id = ? AND operation_id = ? AND state = 'committing'",
                    (datetime.now(UTC).isoformat(), table_id, operation_id),
                )
                row = connection.execute(
                    "SELECT * FROM table_operation_journal WHERE table_id = ? AND operation_id = ?",
                    (table_id, operation_id),
                ).fetchone()
                if row is None:
                    raise OperationStateConflictError("cannot finalize an unknown operation")
                record = self._record(row)
                if record.state is TableOperationState.NO_OP:
                    return record
                if cursor.rowcount == 0:
                    raise OperationStateConflictError(
                        f"operation {operation_id!r} cannot become a no-op from "
                        f"{record.state.value}"
                    )
                return record
        except OperationJournalError:
            raise
        except sqlite3.Error as error:
            raise OperationJournalError("could not finalize a no-op table operation") from error

    def finalize_failed(
        self,
        table_id: str,
        operation_id: str,
        *,
        error_code: str,
    ) -> TableOperationRecord:
        """Record a provider rejection proven to have left Delta unchanged."""
        if not error_code or len(error_code) > 64:
            raise ValueError("error_code must contain 1 to 64 characters")
        try:
            with self._connection(write=True) as connection:
                cursor = connection.execute(
                    "UPDATE table_operation_journal SET state = 'failed', "
                    "transaction_version = NULL, row_count = 0, error_code = ?, updated_at = ? "
                    "WHERE table_id = ? AND operation_id = ? AND state = 'committing'",
                    (error_code, datetime.now(UTC).isoformat(), table_id, operation_id),
                )
                row = connection.execute(
                    "SELECT * FROM table_operation_journal WHERE table_id = ? AND operation_id = ?",
                    (table_id, operation_id),
                ).fetchone()
                if row is None:
                    raise OperationStateConflictError("cannot finalize an unknown operation")
                record = self._record(row)
                if record.state is TableOperationState.FAILED:
                    if record.error_code != error_code:
                        raise OperationStateConflictError(
                            f"operation {operation_id!r} has a different recorded failure"
                        )
                    return record
                if cursor.rowcount == 0:
                    raise OperationStateConflictError(
                        f"operation {operation_id!r} cannot fail from {record.state.value}"
                    )
                return record
        except OperationJournalError:
            raise
        except sqlite3.Error as error:
            raise OperationJournalError("could not finalize a failed table operation") from error

    def _transition(
        self,
        table_id: str,
        operation_id: str,
        *,
        allowed: tuple[TableOperationState, ...],
        state: TableOperationState,
    ) -> TableOperationRecord:
        try:
            with self._connection(write=True) as connection:
                row = connection.execute(
                    "SELECT * FROM table_operation_journal WHERE table_id = ? AND operation_id = ?",
                    (table_id, operation_id),
                ).fetchone()
                if row is None:
                    raise OperationStateConflictError("cannot transition an unknown operation")
                record = self._record(row)
                if record.state is state:
                    return record
                if record.state not in allowed:
                    raise OperationStateConflictError(
                        f"operation {operation_id!r} cannot transition from {record.state.value}"
                    )
                connection.execute(
                    "UPDATE table_operation_journal SET state = ?, updated_at = ? "
                    "WHERE table_id = ? AND operation_id = ?",
                    (state.value, datetime.now(UTC).isoformat(), table_id, operation_id),
                )
                updated = connection.execute(
                    "SELECT * FROM table_operation_journal WHERE table_id = ? AND operation_id = ?",
                    (table_id, operation_id),
                ).fetchone()
                assert updated is not None
                return self._record(updated)
        except OperationJournalError:
            raise
        except sqlite3.Error as error:
            raise OperationJournalError("could not update a table operation state") from error

    @contextmanager
    def _connection(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
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
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _record(row: sqlite3.Row) -> TableOperationRecord:
        return TableOperationRecord(
            table_id=row["table_id"],
            operation_id=row["operation_id"],
            fingerprint=row["fingerprint"],
            mode=row["mode"],
            schema_fingerprint=row["schema_fingerprint"],
            input_sha256=row["input_sha256"],
            transaction_version=row["transaction_version"],
            base_version=row["base_version"],
            row_count=row["row_count"],
            state=TableOperationState(row["state"]),
            commit_version=row["commit_version"],
            error_code=row["error_code"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
