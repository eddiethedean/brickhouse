"""Durable job definitions, submissions, task states, and attempts."""

from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from brickhouse.jobs.models import (
    MAX_JOB_OUTPUT_BYTES_PER_STREAM,
    InvalidJobDefinitionError,
    JobAttemptState,
    JobRecord,
    JobRunRecord,
    JobRunState,
    JobSubmission,
    JobTaskAttemptOutput,
    JobTaskAttemptRecord,
    JobTaskDefinition,
    JobTaskRunRecord,
    JobTaskState,
    canonical_settings,
    validate_job_dag,
)

if TYPE_CHECKING:
    from brickhouse.workspace.context import WorkspaceContext

_TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}\Z")
_ERROR_CODE_RE = re.compile(r"[A-Z][A-Z0-9_.-]{0,63}\Z")
MAX_TASK_VALUE_JSON_BYTES = 48 * 1024
MAX_TASK_VALUES_PER_RUN = 250


class JobRepositoryError(RuntimeError):
    """Base error for durable job metadata operations."""


class JobNotFoundError(JobRepositoryError):
    """A requested immutable job definition does not exist."""


class JobRunNotFoundError(JobRepositoryError):
    """A requested run does not exist."""


class JobTaskNotFoundError(JobRepositoryError):
    """A requested task is not part of the submitted run."""


class SubmissionTokenConflictError(JobRepositoryError):
    """An idempotency token was reused for different submitted content."""


class JobStateTransitionError(JobRepositoryError):
    """A state change is not legal for the persisted job, run, or attempt."""


class JobDependencyNotReadyError(JobRepositoryError):
    """A task cannot start until its dependencies succeed."""


class JobDependencyFailedError(JobRepositoryError):
    """A task depends on a task that failed, was skipped, or was cancelled."""


class JobRepository:
    """Persist immutable definitions and validated per-run task transitions.

    The execution service performs sequential DAG scheduling. Concurrency admission,
    automatic retries, worker launch, and restart-time reaping are composed by the
    B014 service slices. This repository supplies their durable state contract and
    an explicit interrupted transition that never fabricates an exit status or
    success.
    """

    def __init__(self, context: WorkspaceContext) -> None:
        self._context = context

    def create_job(
        self,
        name: str,
        tasks: Sequence[JobTaskDefinition],
        *,
        settings: Mapping[str, object] | None = None,
    ) -> JobRecord:
        """Create an immutable job definition with a bounded acyclic task graph."""
        if not isinstance(name, str) or not name.strip() or len(name) > 256:
            raise InvalidJobDefinitionError("job name must contain 1 to 256 characters")
        if isinstance(tasks, (str, bytes, bytearray)) or not isinstance(tasks, Sequence):
            raise InvalidJobDefinitionError("tasks must be a sequence of JobTaskDefinition values")
        if any(not isinstance(task, JobTaskDefinition) for task in tasks):
            raise InvalidJobDefinitionError("tasks must contain JobTaskDefinition values")
        task_tuple = validate_job_dag(tasks)
        settings_json = canonical_settings(
            settings if settings is not None else {}, label="job settings"
        )
        definition_json = _job_definition_json(name, settings_json, task_tuple)
        definition_sha256 = hashlib.sha256(definition_json.encode("utf-8")).hexdigest()
        created_at = _now()
        try:
            with self._connection(write=True) as connection:
                cursor = connection.execute(
                    "INSERT INTO jobs "
                    "(name, settings_json, task_count, definition_sha256, created_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (name, settings_json, len(task_tuple), definition_sha256, created_at),
                )
                job_id = cursor.lastrowid
                if job_id is None:
                    raise JobRepositoryError("job definition did not receive an identity")
                connection.executemany(
                    "INSERT INTO job_tasks "
                    "(job_id, task_key, task_type, task_order, depends_on_json, settings_json) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    [
                        (
                            job_id,
                            task.task_key,
                            task.task_type,
                            order,
                            _json(list(task.depends_on)),
                            task.settings_json,
                        )
                        for order, task in enumerate(task_tuple)
                    ],
                )
        except sqlite3.Error as error:
            raise JobRepositoryError("could not persist the job definition") from error
        return self.get_job(job_id)

    def get_job(self, job_id: int) -> JobRecord:
        _validate_id(job_id, "job_id")
        try:
            with self._connection() as connection:
                row = connection.execute(
                    "SELECT job_id, name, settings_json, task_count, definition_sha256, created_at "
                    "FROM jobs WHERE job_id = ?",
                    (job_id,),
                ).fetchone()
                if row is None:
                    raise JobNotFoundError(f"job does not exist: {job_id}")
                task_rows = connection.execute(
                    "SELECT task_key, task_type, depends_on_json, settings_json "
                    "FROM job_tasks WHERE job_id = ? ORDER BY task_order",
                    (job_id,),
                ).fetchall()
            tasks = tuple(
                JobTaskDefinition(
                    task_row["task_key"],
                    task_type=task_row["task_type"],
                    depends_on=tuple(json.loads(task_row["depends_on_json"])),
                    settings=json.loads(task_row["settings_json"]),
                )
                for task_row in task_rows
            )
            if len(tasks) != row["task_count"]:
                raise JobRepositoryError("persisted job definition has an incomplete task list")
            actual_fingerprint = hashlib.sha256(
                _job_definition_json(row["name"], row["settings_json"], tasks).encode("utf-8")
            ).hexdigest()
            if actual_fingerprint != row["definition_sha256"]:
                raise JobRepositoryError("persisted job definition fingerprint does not match")
            return JobRecord(
                row["job_id"],
                row["name"],
                row["settings_json"],
                row["definition_sha256"],
                row["created_at"],
                tasks,
            )
        except sqlite3.Error as error:
            raise JobRepositoryError("could not read the job definition") from error

    def get_active_job(self, job_id: int) -> JobRecord:
        """Read a job only while its SDK resource identity remains undeleted."""
        job = self.get_job(job_id)
        try:
            with self._connection() as connection:
                deleted = connection.execute(
                    "SELECT 1 FROM job_deletions WHERE job_id = ?", (job_id,)
                ).fetchone()
            if deleted is not None:
                raise JobNotFoundError(f"job does not exist: {job_id}")
            return job
        except sqlite3.Error as error:
            raise JobRepositoryError("could not read active job state") from error

    def delete_job(self, job_id: int) -> None:
        """Hide a job from new API work while retaining its immutable history."""
        _validate_id(job_id, "job_id")
        try:
            with self._connection(write=True) as connection:
                exists = connection.execute(
                    "SELECT 1 FROM jobs WHERE job_id = ?", (job_id,)
                ).fetchone()
                if exists is None:
                    raise JobNotFoundError(f"job does not exist: {job_id}")
                deleted = connection.execute(
                    "SELECT 1 FROM job_deletions WHERE job_id = ?", (job_id,)
                ).fetchone()
                if deleted is not None:
                    raise JobNotFoundError(f"job does not exist: {job_id}")
                connection.execute(
                    "INSERT INTO job_deletions(job_id, deleted_at) VALUES (?, ?)",
                    (job_id, _now()),
                )
        except JobNotFoundError:
            raise
        except sqlite3.Error as error:
            raise JobRepositoryError("could not persist job deletion") from error

    def list_jobs(self) -> list[JobRecord]:
        try:
            with self._connection() as connection:
                job_ids = [
                    row[0] for row in connection.execute("SELECT job_id FROM jobs ORDER BY job_id")
                ]
            return [self.get_job(job_id) for job_id in job_ids]
        except sqlite3.Error as error:
            raise JobRepositoryError("could not list job definitions") from error

    def submit_run(
        self,
        job_id: int,
        *,
        submitted_settings: Mapping[str, object] | None = None,
        submission_token: str | None = None,
    ) -> JobSubmission:
        """Persist an immutable submission; replay a token only for identical input."""
        _validate_id(job_id, "job_id")
        settings_json = canonical_settings(
            submitted_settings if submitted_settings is not None else {},
            label="submitted settings",
        )
        if submission_token is not None and (
            not isinstance(submission_token, str) or _TOKEN_RE.fullmatch(submission_token) is None
        ):
            raise InvalidJobDefinitionError(
                "submission_token must be a simple token of at most 64 characters"
            )
        fingerprint_document = {"job_id": job_id, "settings": json.loads(settings_json)}
        fingerprint = hashlib.sha256(_json(fingerprint_document).encode("utf-8")).hexdigest()
        created_at = _now()
        replayed = False
        try:
            with self._connection(write=True) as connection:
                if (
                    connection.execute("SELECT 1 FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
                    is None
                ):
                    raise JobNotFoundError(f"job does not exist: {job_id}")
                run_id: int | None = None
                if submission_token is not None:
                    prior = connection.execute(
                        "SELECT run_id, request_fingerprint FROM job_runs "
                        "WHERE submission_token = ?",
                        (submission_token,),
                    ).fetchone()
                    if prior is not None:
                        if prior["request_fingerprint"] != fingerprint:
                            raise SubmissionTokenConflictError(
                                "submission token was already used for different job settings"
                            )
                        run_id = prior["run_id"]
                        replayed = True
                if run_id is None:
                    cursor = connection.execute(
                        "INSERT INTO job_runs "
                        "(job_id, state, submission_token, request_fingerprint, "
                        "submitted_settings_json, created_at) "
                        "VALUES (?, 'queued', ?, ?, ?, ?)",
                        (job_id, submission_token, fingerprint, settings_json, created_at),
                    )
                    run_id = cursor.lastrowid
                    if run_id is None:
                        raise JobRepositoryError("job run did not receive an identity")
                    tasks = connection.execute(
                        "SELECT task_key, task_type, task_order, depends_on_json, settings_json "
                        "FROM job_tasks WHERE job_id = ? ORDER BY task_order",
                        (job_id,),
                    ).fetchall()
                    connection.executemany(
                        "INSERT INTO job_run_tasks "
                        "(run_id, job_id, task_key, task_type, task_order, depends_on_json, "
                        "settings_json, state) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')",
                        [
                            (
                                run_id,
                                job_id,
                                task["task_key"],
                                task["task_type"],
                                task["task_order"],
                                task["depends_on_json"],
                                task["settings_json"],
                            )
                            for task in tasks
                        ],
                    )
        except sqlite3.IntegrityError as error:
            raise JobRepositoryError("could not persist the job submission") from error
        except sqlite3.Error as error:
            raise JobRepositoryError("could not persist the job submission") from error
        assert run_id is not None
        return JobSubmission(self.get_run(run_id), replayed)

    def find_submission_by_token(
        self,
        job_id: int,
        *,
        submitted_settings: Mapping[str, object] | None,
        submission_token: str,
    ) -> JobSubmission | None:
        """Look up an idempotent replay without admitting or creating new work."""
        _validate_id(job_id, "job_id")
        if not isinstance(submission_token, str) or _TOKEN_RE.fullmatch(submission_token) is None:
            raise InvalidJobDefinitionError(
                "submission_token must be a simple token of at most 64 characters"
            )
        settings_json = canonical_settings(
            submitted_settings if submitted_settings is not None else {},
            label="submitted settings",
        )
        fingerprint = hashlib.sha256(
            _json({"job_id": job_id, "settings": json.loads(settings_json)}).encode("utf-8")
        ).hexdigest()
        try:
            with self._connection() as connection:
                row = connection.execute(
                    "SELECT run_id, request_fingerprint FROM job_runs WHERE submission_token = ?",
                    (submission_token,),
                ).fetchone()
            if row is None:
                return None
            if row["request_fingerprint"] != fingerprint:
                raise SubmissionTokenConflictError(
                    "submission token was already used for different job settings"
                )
            return JobSubmission(self.get_run(row["run_id"]), True)
        except sqlite3.Error as error:
            raise JobRepositoryError("could not read the submitted job token") from error

    def get_run(self, run_id: int) -> JobRunRecord:
        _validate_id(run_id, "run_id")
        try:
            with self._connection() as connection:
                row = connection.execute(
                    "SELECT * FROM job_runs WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
            if row is None:
                raise JobRunNotFoundError(f"job run does not exist: {run_id}")
            return _run_record(row)
        except sqlite3.Error as error:
            raise JobRepositoryError("could not read the job run") from error

    def list_runs(self, job_id: int) -> list[JobRunRecord]:
        _validate_id(job_id, "job_id")
        try:
            with self._connection() as connection:
                rows = connection.execute(
                    "SELECT * FROM job_runs WHERE job_id = ? ORDER BY run_id",
                    (job_id,),
                ).fetchall()
            return [_run_record(row) for row in rows]
        except sqlite3.Error as error:
            raise JobRepositoryError("could not list job runs") from error

    def list_task_runs(self, run_id: int) -> list[JobTaskRunRecord]:
        _validate_id(run_id, "run_id")
        try:
            with self._connection() as connection:
                rows = connection.execute(
                    "SELECT * FROM job_run_tasks WHERE run_id = ? ORDER BY task_order",
                    (run_id,),
                ).fetchall()
            if not rows and self.get_run(run_id) is None:  # pragma: no cover - defensive
                raise JobRunNotFoundError(f"job run does not exist: {run_id}")
            return [_task_run_record(row) for row in rows]
        except sqlite3.Error as error:
            raise JobRepositoryError("could not list submitted job tasks") from error

    def queue_attempt(self, run_id: int, task_key: str) -> JobTaskAttemptRecord:
        """Allocate the next distinct attempt after dependencies have succeeded."""
        _validate_id(run_id, "run_id")
        _validate_task_key(task_key)
        try:
            with self._connection(write=True) as connection:
                run = connection.execute(
                    "SELECT state FROM job_runs WHERE run_id = ?", (run_id,)
                ).fetchone()
                if run is None:
                    raise JobRunNotFoundError(f"job run does not exist: {run_id}")
                if JobRunState(run["state"]) not in {JobRunState.QUEUED, JobRunState.RUNNING}:
                    raise JobStateTransitionError("attempts can only be queued for an active run")
                task = connection.execute(
                    "SELECT state, depends_on_json FROM job_run_tasks "
                    "WHERE run_id = ? AND task_key = ?",
                    (run_id, task_key),
                ).fetchone()
                if task is None:
                    raise JobTaskNotFoundError(f"task is not part of run {run_id}: {task_key}")
                task_state = JobTaskState(task["state"])
                if task_state not in {JobTaskState.PENDING, JobTaskState.FAILED}:
                    raise JobStateTransitionError(
                        f"task {task_key!r} cannot start from state {task_state.value!r}"
                    )
                dependencies = tuple(json.loads(task["depends_on_json"]))
                for dependency in dependencies:
                    dependency_row = connection.execute(
                        "SELECT state FROM job_run_tasks WHERE run_id = ? AND task_key = ?",
                        (run_id, dependency),
                    ).fetchone()
                    if dependency_row is None:
                        raise JobRepositoryError("submitted job dependency is missing")
                    dependency_state = JobTaskState(dependency_row["state"])
                    if dependency_state in {
                        JobTaskState.FAILED,
                        JobTaskState.CANCELLED,
                        JobTaskState.SKIPPED,
                        JobTaskState.INTERRUPTED,
                    }:
                        raise JobDependencyFailedError(
                            f"task {task_key!r} has unsuccessful dependency {dependency!r}"
                        )
                    if dependency_state is not JobTaskState.SUCCEEDED:
                        raise JobDependencyNotReadyError(
                            f"task {task_key!r} is waiting for dependency {dependency!r}"
                        )
                previous = connection.execute(
                    "SELECT coalesce(max(attempt_number), 0) FROM job_task_attempts "
                    "WHERE run_id = ? AND task_key = ?",
                    (run_id, task_key),
                ).fetchone()
                attempt_number = previous[0] + 1
                attempt_id = str(uuid.uuid4())
                connection.execute(
                    "INSERT INTO job_task_attempts "
                    "(run_id, task_key, attempt_number, attempt_id, state, termination_reason) "
                    "VALUES (?, ?, ?, ?, 'queued', 'queued')",
                    (run_id, task_key, attempt_number, attempt_id),
                )
                cursor = connection.execute(
                    "UPDATE job_run_tasks SET state = 'queued', attempt_count = ?, "
                    "finished_at = NULL, error_code = NULL WHERE run_id = ? "
                    "AND task_key = ? AND state IN ('pending', 'failed')",
                    (attempt_number, run_id, task_key),
                )
                if cursor.rowcount != 1:
                    raise JobStateTransitionError("task changed while an attempt was being queued")
            return self.get_attempt(run_id, task_key, attempt_number)
        except sqlite3.IntegrityError as error:
            raise JobStateTransitionError("could not allocate a unique task attempt") from error
        except sqlite3.Error as error:
            raise JobRepositoryError("could not queue a task attempt") from error

    def start_attempt(
        self, run_id: int, task_key: str, attempt_number: int
    ) -> JobTaskAttemptRecord:
        """Mark a queued attempt as running before the worker process is launched."""
        _validate_id(run_id, "run_id")
        _validate_task_key(task_key)
        _validate_attempt_number(attempt_number)
        started_at = _now()
        try:
            with self._connection(write=True) as connection:
                run = connection.execute(
                    "SELECT state FROM job_runs WHERE run_id = ?", (run_id,)
                ).fetchone()
                if run is None:
                    raise JobRunNotFoundError(f"job run does not exist: {run_id}")
                if JobRunState(run["state"]) not in {JobRunState.QUEUED, JobRunState.RUNNING}:
                    raise JobStateTransitionError("attempts can only start for an active run")
                cursor = connection.execute(
                    "UPDATE job_task_attempts SET state = 'running', "
                    "termination_reason = 'running', "
                    "started_at = ? WHERE run_id = ? AND task_key = ? AND attempt_number = ? "
                    "AND state = 'queued'",
                    (started_at, run_id, task_key, attempt_number),
                )
                if cursor.rowcount != 1:
                    raise JobStateTransitionError("task attempt is missing or is not queued")
                cursor = connection.execute(
                    "UPDATE job_run_tasks SET state = 'running', "
                    "started_at = coalesce(started_at, ?) WHERE run_id = ? AND task_key = ? "
                    "AND state = 'queued'",
                    (started_at, run_id, task_key),
                )
                if cursor.rowcount != 1:
                    raise JobStateTransitionError("submitted task is missing or is not queued")
                connection.execute(
                    "UPDATE job_runs SET state = 'running', started_at = coalesce(started_at, ?) "
                    "WHERE run_id = ? AND state = 'queued'",
                    (started_at, run_id),
                )
            return self.get_attempt(run_id, task_key, attempt_number)
        except sqlite3.Error as error:
            raise JobRepositoryError("could not start the task attempt") from error

    def finish_attempt(
        self,
        run_id: int,
        task_key: str,
        attempt_number: int,
        *,
        exit_code: int | None,
        termination_reason: str,
        error_code: str | None = None,
        output: JobTaskAttemptOutput | None = None,
    ) -> JobTaskAttemptRecord:
        """Persist an observed worker result without replacing it with a fake status."""
        _validate_id(run_id, "run_id")
        _validate_task_key(task_key)
        _validate_attempt_number(attempt_number)
        _validate_error_code(error_code)
        if output is not None:
            if (
                not isinstance(output, JobTaskAttemptOutput)
                or output.run_id != run_id
                or output.task_key != task_key
                or output.attempt_number != attempt_number
                or not isinstance(output.stdout, str)
                or not isinstance(output.stderr, str)
                or type(output.stdout_truncated) is not bool
                or type(output.stderr_truncated) is not bool
                or len(output.stdout.encode("utf-8")) > MAX_JOB_OUTPUT_BYTES_PER_STREAM
                or len(output.stderr.encode("utf-8")) > MAX_JOB_OUTPUT_BYTES_PER_STREAM
            ):
                raise JobRepositoryError("attempt output is invalid or exceeds its byte limit")
        if exit_code is not None and (
            type(exit_code) is not int or exit_code < -255 or exit_code > 255
        ):
            raise JobRepositoryError("exit_code must be an observed integer from -255 to 255")
        mapping: dict[str, tuple[JobAttemptState, JobTaskState]] = {
            "completed": (
                JobAttemptState.SUCCEEDED,
                JobTaskState.SUCCEEDED,
            ),
            "timeout": (JobAttemptState.TIMED_OUT, JobTaskState.FAILED),
            "cancelled": (JobAttemptState.CANCELLED, JobTaskState.CANCELLED),
            "interrupted": (JobAttemptState.INTERRUPTED, JobTaskState.INTERRUPTED),
            "launch_failed": (JobAttemptState.FAILED, JobTaskState.FAILED),
        }
        if termination_reason not in mapping:
            raise JobRepositoryError("termination_reason is not supported")
        if termination_reason == "completed" and exit_code is None:
            raise JobRepositoryError("completed attempts require the observed exit code")
        if termination_reason in {"timeout", "cancelled"} and exit_code is None:
            raise JobRepositoryError("terminated workers require the observed exit code")
        if termination_reason in {"launch_failed", "interrupted"} and exit_code is not None:
            raise JobRepositoryError("no exit code may be fabricated when no exit was observed")
        attempt_state, task_state = mapping[termination_reason]
        if termination_reason == "completed" and exit_code != 0:
            attempt_state, task_state = JobAttemptState.FAILED, JobTaskState.FAILED
            error_code = error_code or "WORKER_EXIT_NONZERO"
        if termination_reason == "completed" and exit_code == 0 and error_code is not None:
            raise JobRepositoryError("successful attempts cannot have an error code")
        if termination_reason == "timeout":
            error_code = error_code or "WORKER_TIMEOUT"
        if termination_reason == "launch_failed":
            error_code = error_code or "WORKER_LAUNCH_FAILED"

        finished_at = _now()
        try:
            with self._connection(write=True) as connection:
                if output is not None:
                    connection.execute(
                        "INSERT INTO job_task_attempt_outputs "
                        "(run_id, task_key, attempt_number, stdout, stderr, "
                        "stdout_truncated, stderr_truncated) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            run_id,
                            task_key,
                            attempt_number,
                            output.stdout,
                            output.stderr,
                            int(output.stdout_truncated),
                            int(output.stderr_truncated),
                        ),
                    )
                cursor = connection.execute(
                    "UPDATE job_task_attempts SET state = ?, termination_reason = ?, "
                    "finished_at = ?, exit_code = ?, error_code = ? WHERE run_id = ? "
                    "AND task_key = ? AND attempt_number = ? AND state = 'running'",
                    (
                        attempt_state.value,
                        termination_reason,
                        finished_at,
                        exit_code,
                        error_code,
                        run_id,
                        task_key,
                        attempt_number,
                    ),
                )
                if cursor.rowcount != 1:
                    raise JobStateTransitionError("task attempt is missing or is not running")
                cursor = connection.execute(
                    "UPDATE job_run_tasks SET state = ?, finished_at = ?, error_code = ? "
                    "WHERE run_id = ? AND task_key = ? AND state = 'running'",
                    (task_state.value, finished_at, error_code, run_id, task_key),
                )
                if cursor.rowcount != 1:
                    raise JobStateTransitionError("submitted task is missing or is not running")
            return self.get_attempt(run_id, task_key, attempt_number)
        except sqlite3.Error as error:
            raise JobRepositoryError("could not persist the task attempt outcome") from error

    def set_task_value(
        self,
        run_id: int,
        task_key: str,
        attempt_number: int,
        key: str,
        value: object,
    ) -> bool:
        """Persist or update one JSON value from the current running task attempt."""
        _validate_id(run_id, "run_id")
        _validate_task_key(task_key)
        _validate_attempt_number(attempt_number)
        _validate_task_value_key(key)
        value_json = _serialize_task_value(value)
        if len(value_json.encode("utf-8")) > MAX_TASK_VALUE_JSON_BYTES:
            raise ValueError(f"task value JSON exceeds the {MAX_TASK_VALUE_JSON_BYTES}-byte limit")
        try:
            with self._connection(write=True) as connection:
                active = connection.execute(
                    "SELECT 1 FROM job_runs AS run "
                    "JOIN job_run_tasks AS task USING(run_id) "
                    "JOIN job_task_attempts AS attempt USING(run_id, task_key) "
                    "WHERE run.run_id = ? AND task.task_key = ? "
                    "AND attempt.attempt_number = ? AND run.state = 'running' "
                    "AND task.state = 'running' AND task.attempt_count = ? "
                    "AND attempt.state = 'running'",
                    (run_id, task_key, attempt_number, attempt_number),
                ).fetchone()
                if active is None:
                    raise JobStateTransitionError(
                        "task values can only be set by the active running attempt"
                    )
                exists = connection.execute(
                    "SELECT 1 FROM job_task_values WHERE run_id = ? AND task_key = ? "
                    "AND value_key = ? LIMIT 1",
                    (run_id, task_key, key),
                ).fetchone()
                if exists is None:
                    count = connection.execute(
                        "SELECT count(*) FROM (SELECT task_key, value_key "
                        "FROM job_task_values WHERE run_id = ? "
                        "GROUP BY task_key, value_key)",
                        (run_id,),
                    ).fetchone()[0]
                    if count >= MAX_TASK_VALUES_PER_RUN:
                        raise ValueError(
                            f"a job run cannot define more than {MAX_TASK_VALUES_PER_RUN} "
                            "distinct task values"
                        )
                connection.execute(
                    "INSERT INTO job_task_values "
                    "(run_id, task_key, attempt_number, value_key, value_json, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(run_id, task_key, attempt_number, value_key) "
                    "DO UPDATE SET value_json = excluded.value_json, "
                    "updated_at = excluded.updated_at",
                    (run_id, task_key, attempt_number, key, value_json, _now()),
                )
            return True
        except sqlite3.Error as error:
            raise JobRepositoryError("could not persist the task value") from error

    def get_task_value(
        self,
        run_id: int,
        reader_task_key: str,
        reader_attempt_number: int,
        upstream_task_key: str,
        key: str,
    ) -> tuple[bool, object | None]:
        """Read the latest successful upstream value within one active run attempt."""
        _validate_id(run_id, "run_id")
        _validate_task_key(reader_task_key)
        _validate_attempt_number(reader_attempt_number)
        _validate_task_key(upstream_task_key)
        _validate_task_value_key(key)
        try:
            with self._connection() as connection:
                reader = connection.execute(
                    "SELECT task.state, task.attempt_count, attempt.state AS attempt_state, "
                    "run.state AS run_state FROM job_run_tasks AS task "
                    "JOIN job_runs AS run USING(run_id) "
                    "JOIN job_task_attempts AS attempt USING(run_id, task_key) "
                    "WHERE task.run_id = ? AND task.task_key = ? "
                    "AND attempt.attempt_number = ?",
                    (run_id, reader_task_key, reader_attempt_number),
                ).fetchone()
                if reader is None:
                    raise ValueError("the current task attempt does not exist in this run")
                if (
                    reader["run_state"] != JobRunState.RUNNING.value
                    or reader["state"] != JobTaskState.RUNNING.value
                    or reader["attempt_state"] != JobAttemptState.RUNNING.value
                    or reader["attempt_count"] != reader_attempt_number
                ):
                    raise PermissionError("task value read is outside the active reader attempt")
                rows = connection.execute(
                    "SELECT task_key, depends_on_json, state FROM job_run_tasks WHERE run_id = ?",
                    (run_id,),
                ).fetchall()
                tasks = {row["task_key"]: row for row in rows}
                upstream = tasks.get(upstream_task_key)
                if upstream is None:
                    raise ValueError(f"task {upstream_task_key!r} does not exist in this run")
                if reader_task_key not in tasks:
                    raise ValueError(f"task {reader_task_key!r} does not exist in this run")
                ancestors: set[str] = set()
                pending = list(json.loads(tasks[reader_task_key]["depends_on_json"]))
                while pending:
                    dependency = pending.pop()
                    if dependency in ancestors:
                        continue
                    ancestors.add(dependency)
                    dependency_row = tasks.get(dependency)
                    if dependency_row is None:
                        raise JobRepositoryError("submitted job dependency is missing")
                    pending.extend(json.loads(dependency_row["depends_on_json"]))
                if upstream_task_key not in ancestors:
                    raise PermissionError(
                        "task values are readable only from declared upstream tasks"
                    )
                if upstream["state"] != JobTaskState.SUCCEEDED.value:
                    raise ValueError(f"upstream task {upstream_task_key!r} has not succeeded")
                row = connection.execute(
                    "SELECT tv.value_json FROM job_task_values AS tv "
                    "JOIN job_task_attempts AS attempt USING(run_id, task_key, attempt_number) "
                    "WHERE tv.run_id = ? AND tv.task_key = ? "
                    "AND tv.value_key = ? AND attempt.state = 'succeeded' "
                    "ORDER BY tv.attempt_number DESC LIMIT 1",
                    (run_id, upstream_task_key, key),
                ).fetchone()
            if row is None:
                return False, None
            return True, json.loads(row["value_json"])
        except sqlite3.Error as error:
            raise JobRepositoryError("could not read the task value") from error

    def mark_task_skipped(
        self, run_id: int, task_key: str, *, error_code: str = "DEPENDENCY_FAILED"
    ) -> JobTaskRunRecord:
        """Persist a scheduler's explicit failed-dependency decision."""
        _validate_id(run_id, "run_id")
        _validate_task_key(task_key)
        _validate_error_code(error_code)
        finished_at = _now()
        try:
            with self._connection(write=True) as connection:
                cursor = connection.execute(
                    "UPDATE job_run_tasks SET state = 'skipped', finished_at = ?, error_code = ? "
                    "WHERE run_id = ? AND task_key = ? AND state = 'pending'",
                    (finished_at, error_code, run_id, task_key),
                )
                if cursor.rowcount != 1:
                    if (
                        connection.execute(
                            "SELECT 1 FROM job_runs WHERE run_id = ?", (run_id,)
                        ).fetchone()
                        is None
                    ):
                        raise JobRunNotFoundError(f"job run does not exist: {run_id}")
                    raise JobStateTransitionError("only a pending task can be skipped")
            return self.get_task_run(run_id, task_key)
        except sqlite3.Error as error:
            raise JobRepositoryError("could not record the skipped task") from error

    def finish_run(
        self,
        run_id: int,
        state: JobRunState,
        *,
        error_code: str | None = None,
    ) -> JobRunRecord:
        """Commit one terminal run state after every task has a truthful outcome."""
        _validate_id(run_id, "run_id")
        _validate_error_code(error_code)
        if not isinstance(state, JobRunState) or state not in {
            JobRunState.SUCCEEDED,
            JobRunState.FAILED,
            JobRunState.CANCELLED,
        }:
            raise JobStateTransitionError("finish_run requires succeeded, failed, or cancelled")
        finished_at = _now()
        try:
            with self._connection(write=True) as connection:
                run = connection.execute(
                    "SELECT state FROM job_runs WHERE run_id = ?", (run_id,)
                ).fetchone()
                if run is None:
                    raise JobRunNotFoundError(f"job run does not exist: {run_id}")
                current = JobRunState(run["state"])
                if current not in {JobRunState.QUEUED, JobRunState.RUNNING}:
                    raise JobStateTransitionError(
                        f"job run cannot finish from state {current.value!r}"
                    )
                rows = connection.execute(
                    "SELECT state FROM job_run_tasks WHERE run_id = ?", (run_id,)
                ).fetchall()
                states = [JobTaskState(row["state"]) for row in rows]
                if not states:
                    raise JobStateTransitionError("a run without submitted tasks cannot finish")
                active = {JobTaskState.PENDING, JobTaskState.QUEUED, JobTaskState.RUNNING}
                if state is JobRunState.SUCCEEDED and any(
                    task_state is not JobTaskState.SUCCEEDED for task_state in states
                ):
                    raise JobStateTransitionError("a run succeeds only after every task succeeds")
                if state is JobRunState.FAILED and (
                    any(task_state in active for task_state in states)
                    or JobTaskState.FAILED not in states
                ):
                    raise JobStateTransitionError(
                        "a failed run requires a failed task and no unfinished tasks"
                    )
                if state is JobRunState.CANCELLED and (
                    any(task_state in active for task_state in states)
                    or JobTaskState.CANCELLED not in states
                ):
                    raise JobStateTransitionError(
                        "a cancelled run requires cancelled tasks and no unfinished tasks"
                    )
                connection.execute(
                    "UPDATE job_runs SET state = ?, finished_at = ?, error_code = ? "
                    "WHERE run_id = ?",
                    (state.value, finished_at, error_code, run_id),
                )
            return self.get_run(run_id)
        except sqlite3.Error as error:
            raise JobRepositoryError("could not finish the job run") from error

    def interrupt_run(
        self, run_id: int, *, error_code: str = "WORKSPACE_RESTARTED"
    ) -> JobRunRecord:
        """Record unknown in-flight outcomes as interrupted without inventing exits.

        Callers must first stop/reap owned workers and reconcile any B013 outcomes
        they observed. Pending tasks remain pending for later recovery policy.
        """
        _validate_id(run_id, "run_id")
        _validate_error_code(error_code)
        finished_at = _now()
        try:
            with self._connection(write=True) as connection:
                run = connection.execute(
                    "SELECT state FROM job_runs WHERE run_id = ?", (run_id,)
                ).fetchone()
                if run is None:
                    raise JobRunNotFoundError(f"job run does not exist: {run_id}")
                current = JobRunState(run["state"])
                if current not in {JobRunState.QUEUED, JobRunState.RUNNING}:
                    raise JobStateTransitionError(
                        f"job run cannot be interrupted from state {current.value!r}"
                    )
                connection.execute(
                    "UPDATE job_task_attempts SET state = 'interrupted', "
                    "termination_reason = 'interrupted', finished_at = ?, error_code = ? "
                    "WHERE run_id = ? AND state IN ('queued', 'running')",
                    (finished_at, error_code, run_id),
                )
                connection.execute(
                    "UPDATE job_run_tasks SET state = 'interrupted', finished_at = ?, "
                    "error_code = ? WHERE run_id = ? AND state IN ('queued', 'running')",
                    (finished_at, error_code, run_id),
                )
                connection.execute(
                    "UPDATE job_runs SET state = 'interrupted', finished_at = ?, error_code = ? "
                    "WHERE run_id = ?",
                    (finished_at, error_code, run_id),
                )
            return self.get_run(run_id)
        except sqlite3.Error as error:
            raise JobRepositoryError("could not record the interrupted job run") from error

    def get_task_run(self, run_id: int, task_key: str) -> JobTaskRunRecord:
        _validate_id(run_id, "run_id")
        _validate_task_key(task_key)
        try:
            with self._connection() as connection:
                row = connection.execute(
                    "SELECT * FROM job_run_tasks WHERE run_id = ? AND task_key = ?",
                    (run_id, task_key),
                ).fetchone()
            if row is None:
                if self.get_run(run_id) is None:  # pragma: no cover - defensive
                    raise JobRunNotFoundError(f"job run does not exist: {run_id}")
                raise JobTaskNotFoundError(f"task is not part of run {run_id}: {task_key}")
            return _task_run_record(row)
        except sqlite3.Error as error:
            raise JobRepositoryError("could not read the submitted job task") from error

    def get_attempt(self, run_id: int, task_key: str, attempt_number: int) -> JobTaskAttemptRecord:
        _validate_id(run_id, "run_id")
        _validate_task_key(task_key)
        _validate_attempt_number(attempt_number)
        try:
            with self._connection() as connection:
                row = connection.execute(
                    "SELECT * FROM job_task_attempts WHERE run_id = ? AND task_key = ? "
                    "AND attempt_number = ?",
                    (run_id, task_key, attempt_number),
                ).fetchone()
            if row is None:
                raise JobTaskNotFoundError(
                    f"task attempt does not exist: run={run_id}, task={task_key!r}, "
                    f"attempt={attempt_number}"
                )
            return _attempt_record(row)
        except sqlite3.Error as error:
            raise JobRepositoryError("could not read the task attempt") from error

    def get_attempt_output(
        self, run_id: int, task_key: str, attempt_number: int
    ) -> JobTaskAttemptOutput | None:
        """Return bounded output for one terminal attempt, if it has been captured."""
        _validate_id(run_id, "run_id")
        _validate_task_key(task_key)
        _validate_attempt_number(attempt_number)
        try:
            with self._connection() as connection:
                row = connection.execute(
                    "SELECT run_id, task_key, attempt_number, stdout, stderr, "
                    "stdout_truncated, stderr_truncated FROM job_task_attempt_outputs "
                    "WHERE run_id = ? AND task_key = ? AND attempt_number = ?",
                    (run_id, task_key, attempt_number),
                ).fetchone()
            if row is None:
                self.get_attempt(run_id, task_key, attempt_number)
                return None
            return JobTaskAttemptOutput(
                row["run_id"],
                row["task_key"],
                row["attempt_number"],
                row["stdout"],
                row["stderr"],
                bool(row["stdout_truncated"]),
                bool(row["stderr_truncated"]),
            )
        except sqlite3.Error as error:
            raise JobRepositoryError("could not read task attempt output") from error

    def list_attempts(self, run_id: int, task_key: str | None = None) -> list[JobTaskAttemptRecord]:
        _validate_id(run_id, "run_id")
        if task_key is not None:
            _validate_task_key(task_key)
        try:
            with self._connection() as connection:
                if task_key is None:
                    rows = connection.execute(
                        "SELECT * FROM job_task_attempts WHERE run_id = ? "
                        "ORDER BY task_key, attempt_number",
                        (run_id,),
                    ).fetchall()
                else:
                    rows = connection.execute(
                        "SELECT * FROM job_task_attempts WHERE run_id = ? AND task_key = ? "
                        "ORDER BY attempt_number",
                        (run_id, task_key),
                    ).fetchall()
            if not rows:
                self.get_run(run_id)
            return [_attempt_record(row) for row in rows]
        except sqlite3.Error as error:
            raise JobRepositoryError("could not list task attempts") from error

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
            if write and connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()


def _run_record(row: sqlite3.Row) -> JobRunRecord:
    return JobRunRecord(
        row["run_id"],
        row["job_id"],
        JobRunState(row["state"]),
        row["submission_token"],
        row["request_fingerprint"],
        row["submitted_settings_json"],
        row["created_at"],
        row["started_at"],
        row["finished_at"],
        row["error_code"],
    )


def _task_run_record(row: sqlite3.Row) -> JobTaskRunRecord:
    return JobTaskRunRecord(
        row["run_id"],
        row["job_id"],
        row["task_key"],
        row["task_type"],
        row["task_order"],
        tuple(json.loads(row["depends_on_json"])),
        row["settings_json"],
        JobTaskState(row["state"]),
        row["attempt_count"],
        row["started_at"],
        row["finished_at"],
        row["error_code"],
    )


def _attempt_record(row: sqlite3.Row) -> JobTaskAttemptRecord:
    return JobTaskAttemptRecord(
        row["run_id"],
        row["task_key"],
        row["attempt_number"],
        row["attempt_id"],
        JobAttemptState(row["state"]),
        row["termination_reason"],
        row["started_at"],
        row["finished_at"],
        row["exit_code"],
        row["error_code"],
    )


def _job_definition_json(
    name: str,
    settings_json: str,
    tasks: Sequence[JobTaskDefinition],
) -> str:
    return _json(
        {
            "name": name,
            "settings": json.loads(settings_json),
            "tasks": [
                {
                    "task_key": task.task_key,
                    "task_type": task.task_type,
                    "depends_on": list(task.depends_on),
                    "settings": json.loads(task.settings_json),
                }
                for task in tasks
            ],
        }
    )


def _validate_id(value: int, label: str) -> None:
    if type(value) is not int or value < 1:
        raise JobRepositoryError(f"{label} must be a positive integer")


def _validate_attempt_number(value: int) -> None:
    if type(value) is not int or value < 1:
        raise JobRepositoryError("attempt_number must be a positive integer")


def _validate_task_key(value: str) -> None:
    if not isinstance(value, str) or re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,127}", value) is None:
        raise JobRepositoryError("task_key must be a simple identifier")


def _validate_task_value_key(value: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or "\x00" in value
        or len(value.encode("utf-8")) > 256
    ):
        raise ValueError("task value key must contain 1 to 256 UTF-8 bytes")


def _serialize_task_value(value: object) -> str:
    def is_json_value(item: object) -> bool:
        if item is None or type(item) in {str, bool, int}:
            return True
        if type(item) is float:
            return math.isfinite(item)
        if type(item) is list:
            return all(is_json_value(child) for child in item)
        if type(item) is dict:
            return all(type(key) is str and is_json_value(child) for key, child in item.items())
        return False

    if not is_json_value(value):
        raise ValueError("task value must contain JSON-compatible finite data")
    return _json(value)


def _validate_error_code(value: str | None) -> None:
    if value is not None and _ERROR_CODE_RE.fullmatch(value) is None:
        raise JobRepositoryError("error_code must be a bounded symbolic code")


def _json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _now() -> str:
    return datetime.now(UTC).isoformat()
