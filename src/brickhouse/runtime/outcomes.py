"""Durable SQLite records for real local worker-attempt outcomes."""

from __future__ import annotations

import os
import re
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from brickhouse.runtime.worker import WorkerResult
    from brickhouse.workspace.context import WorkspaceContext

_IDENTIFIER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")


class WorkerOutcomeError(RuntimeError):
    """A durable worker-attempt outcome could not be created or updated."""


class WorkerOutcomeConflictError(WorkerOutcomeError):
    """An attempt is missing, duplicated, or already terminal."""


@dataclass(frozen=True, slots=True)
class WorkerAttemptOutcome:
    """Persisted worker state, bounded output counts, and real process status."""

    run_id: str
    attempt_id: str
    state: str
    exit_code: int | None
    termination_reason: str
    started_at: str
    finished_at: str | None
    interpreter: Path
    script: Path
    cwd: Path
    stdout_bytes: int
    stderr_bytes: int
    stdout_truncated: bool
    stderr_truncated: bool
    cleanup_error_type: str | None
    worker_pid: int | None


class WorkerOutcomeJournal:
    """Persist one terminal result per immutable run/attempt identity."""

    def __init__(self, context: WorkspaceContext) -> None:
        self._context = context

    def start_attempt(
        self,
        run_id: str,
        attempt_id: str,
        *,
        interpreter: Path,
        script: Path,
        cwd: Path,
        started_at: str,
    ) -> None:
        """Durably reserve the attempt before starting a child process."""
        _validate_scope(run_id, attempt_id)
        self._context._require_active()
        try:
            with self._connect() as connection:
                connection.execute(
                    "INSERT INTO worker_attempt_outcomes "
                    "(run_id, attempt_id, state, exit_code, termination_reason, started_at, "
                    "finished_at, interpreter, script, cwd) "
                    "VALUES (?, ?, 'running', NULL, 'running', ?, NULL, ?, ?, ?)",
                    (
                        run_id,
                        attempt_id,
                        started_at,
                        str(interpreter),
                        str(script),
                        str(cwd),
                    ),
                )
        except sqlite3.IntegrityError as error:
            raise WorkerOutcomeConflictError("worker attempt already exists") from error
        except sqlite3.Error as error:
            raise WorkerOutcomeError("could not persist worker attempt start") from error

    def finish_attempt(self, run_id: str, attempt_id: str, result: WorkerResult) -> None:
        """Persist the observed exit status after child reaping and staging cleanup."""
        _validate_scope(run_id, attempt_id)
        self._context._require_active()
        reason = result.termination_reason
        if reason not in {"completed", "timeout", "cancelled"}:
            raise WorkerOutcomeError("worker result has an invalid termination reason")
        if reason == "timeout":
            state = "timed_out"
        elif reason == "cancelled":
            state = "cancelled"
        else:
            state = "succeeded" if result.exit_code == 0 else "failed"
        finished_at = datetime.now(UTC).isoformat()
        try:
            with self._connect() as connection:
                cursor = connection.execute(
                    "UPDATE worker_attempt_outcomes SET state = ?, exit_code = ?, "
                    "termination_reason = ?, finished_at = ?, stdout_bytes = ?, stderr_bytes = ?, "
                    "stdout_truncated = ?, stderr_truncated = ?, cleanup_error_type = ? "
                    "WHERE run_id = ? AND attempt_id = ? AND state = 'running'",
                    (
                        state,
                        result.exit_code,
                        reason,
                        finished_at,
                        len(result.stdout.encode("utf-8")),
                        len(result.stderr.encode("utf-8")),
                        int(result.stdout_truncated),
                        int(result.stderr_truncated),
                        result.cleanup_error_type,
                        run_id,
                        attempt_id,
                    ),
                )
                if cursor.rowcount != 1:
                    raise WorkerOutcomeConflictError("worker attempt is missing or terminal")
        except sqlite3.Error as error:
            raise WorkerOutcomeError("could not persist worker attempt result") from error

    def attach_process(self, run_id: str, attempt_id: str, worker_pid: int) -> None:
        """Record the owned child PID immediately after successful process creation."""
        _validate_scope(run_id, attempt_id)
        if type(worker_pid) is not int or worker_pid < 1:
            raise WorkerOutcomeError("worker process id must be a positive integer")
        self._context._require_active()
        try:
            with self._connect() as connection:
                cursor = connection.execute(
                    "UPDATE worker_attempt_outcomes SET worker_pid = ? "
                    "WHERE run_id = ? AND attempt_id = ? AND state = 'running' "
                    "AND worker_pid IS NULL",
                    (worker_pid, run_id, attempt_id),
                )
                if cursor.rowcount != 1:
                    raise WorkerOutcomeConflictError(
                        "worker attempt is missing or already attached"
                    )
        except sqlite3.Error as error:
            raise WorkerOutcomeError("could not persist worker process identity") from error

    def interrupt_attempt(self, run_id: str, attempt_id: str) -> None:
        """Mark an unobserved worker as interrupted without inventing an exit code."""
        _validate_scope(run_id, attempt_id)
        self._context._require_active()
        finished_at = datetime.now(UTC).isoformat()
        try:
            with self._connect() as connection:
                cursor = connection.execute(
                    "UPDATE worker_attempt_outcomes SET state = 'interrupted', "
                    "termination_reason = 'interrupted', finished_at = ? "
                    "WHERE run_id = ? AND attempt_id = ? AND state = 'running'",
                    (finished_at, run_id, attempt_id),
                )
                if cursor.rowcount != 1:
                    raise WorkerOutcomeConflictError("worker attempt is missing or terminal")
        except sqlite3.Error as error:
            raise WorkerOutcomeError("could not persist interrupted worker state") from error

    def fail_to_launch(
        self,
        run_id: str,
        attempt_id: str,
        *,
        cleanup_error_type: str | None = None,
    ) -> None:
        """Record a failed process start without inventing a child exit code."""
        _validate_scope(run_id, attempt_id)
        self._context._require_active()
        finished_at = datetime.now(UTC).isoformat()
        try:
            with self._connect() as connection:
                cursor = connection.execute(
                    "UPDATE worker_attempt_outcomes SET state = 'failed', "
                    "termination_reason = 'launch_failed', finished_at = ?, "
                    "cleanup_error_type = ? "
                    "WHERE run_id = ? AND attempt_id = ? AND state = 'running'",
                    (finished_at, cleanup_error_type, run_id, attempt_id),
                )
                if cursor.rowcount != 1:
                    raise WorkerOutcomeConflictError("worker attempt is missing or terminal")
        except sqlite3.Error as error:
            raise WorkerOutcomeError("could not persist worker launch failure") from error

    def get(self, run_id: str, attempt_id: str) -> WorkerAttemptOutcome | None:
        """Read one attempt outcome after workspace ownership has been acquired."""
        _validate_scope(run_id, attempt_id)
        self._context._require_active()
        try:
            with self._connect() as connection:
                row = connection.execute(
                    "SELECT run_id, attempt_id, state, exit_code, termination_reason, "
                    "started_at, finished_at, interpreter, script, cwd, stdout_bytes, "
                    "stderr_bytes, stdout_truncated, stderr_truncated, cleanup_error_type, "
                    "worker_pid "
                    "FROM worker_attempt_outcomes WHERE run_id = ? AND attempt_id = ?",
                    (run_id, attempt_id),
                ).fetchone()
        except sqlite3.Error as error:
            raise WorkerOutcomeError("could not read worker attempt outcome") from error
        if row is None:
            return None
        return WorkerAttemptOutcome(
            run_id=row[0],
            attempt_id=row[1],
            state=row[2],
            exit_code=row[3],
            termination_reason=row[4],
            started_at=row[5],
            finished_at=row[6],
            interpreter=Path(row[7]),
            script=Path(row[8]),
            cwd=Path(row[9]),
            stdout_bytes=row[10],
            stderr_bytes=row[11],
            stdout_truncated=bool(row[12]),
            stderr_truncated=bool(row[13]),
            cleanup_error_type=row[14],
            worker_pid=row[15],
        )

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self._context.workspace.metadata_path, timeout=5.0)


def _validate_scope(run_id: str, attempt_id: str) -> None:
    if _IDENTIFIER_RE.fullmatch(run_id) is None:
        raise WorkerOutcomeError("worker run_id is invalid")
    if _IDENTIFIER_RE.fullmatch(attempt_id) is None:
        raise WorkerOutcomeError("worker attempt_id is invalid")


def worker_process_exists(pid: int | None) -> bool:
    """Check whether a recorded worker PID still denotes an executing process."""
    if type(pid) is not int or pid < 1:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    if sys.platform.startswith("linux") and os.path.isdir("/proc"):
        try:
            stat = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
            state = stat[stat.rfind(")") + 2 :].split(maxsplit=1)[0]
        except (OSError, IndexError):
            return False
        return state != "Z"
    if sys.platform == "darwin":
        try:
            result = subprocess.run(
                ["ps", "-p", str(pid), "-o", "stat="],
                capture_output=True,
                check=False,
                text=True,
                timeout=2,
            )
        except (OSError, subprocess.TimeoutExpired):
            return True
        state = result.stdout.strip()
        return result.returncode == 0 and bool(state) and not state.startswith("Z")
    return True
