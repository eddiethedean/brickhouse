"""Immutable job definitions and durable run/attempt value types."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import cast

MAX_TASKS_PER_JOB = 10
MAX_SETTINGS_BYTES = 64 * 1024
MAX_JOB_OUTPUT_BYTES_PER_STREAM = 1024 * 1024
_KEY_RE = re.compile(r"[A-Za-z][A-Za-z0-9_-]{0,127}\Z")
_SECRET_SETTING_KEYS = frozenset(
    {
        "access_token",
        "authorization",
        "client_secret",
        "credential",
        "credentials",
        "password",
        "private_key",
        "secret",
        "token",
    }
)


class InvalidJobDefinitionError(ValueError):
    """A job definition or persisted JSON setting is invalid."""


class JobRunState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    INTERRUPTED = "interrupted"

    @property
    def is_terminal(self) -> bool:
        return self in {
            JobRunState.SUCCEEDED,
            JobRunState.FAILED,
            JobRunState.CANCELLED,
            JobRunState.INTERRUPTED,
        }


class JobTaskState(StrEnum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"
    INTERRUPTED = "interrupted"

    @property
    def is_terminal(self) -> bool:
        return self in {
            JobTaskState.SUCCEEDED,
            JobTaskState.FAILED,
            JobTaskState.CANCELLED,
            JobTaskState.SKIPPED,
            JobTaskState.INTERRUPTED,
        }


class JobAttemptState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"
    INTERRUPTED = "interrupted"

    @property
    def is_terminal(self) -> bool:
        return self not in {JobAttemptState.QUEUED, JobAttemptState.RUNNING}


@dataclass(frozen=True, slots=True, init=False)
class JobTaskDefinition:
    """A supported Python script or notebook task with immutable settings."""

    task_key: str
    task_type: str
    depends_on: tuple[str, ...]
    settings_json: str

    def __init__(
        self,
        task_key: str,
        *,
        task_type: str = "python_script",
        depends_on: Sequence[str] = (),
        settings: Mapping[str, object] | None = None,
    ) -> None:
        if not isinstance(task_key, str) or _KEY_RE.fullmatch(task_key) is None:
            raise InvalidJobDefinitionError("task_key must be a simple identifier")
        if task_type not in {"python_script", "notebook"}:
            raise InvalidJobDefinitionError(f"unsupported job task type: {task_type!r}")
        if isinstance(depends_on, (str, bytes, bytearray)):
            raise InvalidJobDefinitionError("depends_on must be a sequence of task identifiers")
        dependency_tuple = tuple(depends_on)
        if any(
            not isinstance(value, str) or _KEY_RE.fullmatch(value) is None
            for value in dependency_tuple
        ):
            raise InvalidJobDefinitionError("task dependencies must be simple task identifiers")
        if len(set(dependency_tuple)) != len(dependency_tuple):
            raise InvalidJobDefinitionError("task dependencies cannot contain duplicates")
        settings_json = canonical_settings(
            settings if settings is not None else {}, label="task settings"
        )
        object.__setattr__(self, "task_key", task_key)
        object.__setattr__(self, "task_type", task_type)
        object.__setattr__(self, "depends_on", dependency_tuple)
        object.__setattr__(self, "settings_json", settings_json)

    @property
    def settings(self) -> dict[str, object]:
        """Return a fresh copy of this task's immutable settings snapshot."""
        return cast(dict[str, object], json.loads(self.settings_json))


@dataclass(frozen=True, slots=True)
class JobRecord:
    job_id: int
    name: str
    settings_json: str
    definition_sha256: str
    created_at: str
    tasks: tuple[JobTaskDefinition, ...]

    @property
    def settings(self) -> dict[str, object]:
        return cast(dict[str, object], json.loads(self.settings_json))


@dataclass(frozen=True, slots=True)
class JobRunRecord:
    run_id: int
    job_id: int
    state: JobRunState
    submission_token: str | None
    request_fingerprint: str
    submitted_settings_json: str
    created_at: str
    started_at: str | None
    finished_at: str | None
    error_code: str | None
    admission_key: str
    dispatched_at: str | None

    @property
    def submitted_settings(self) -> dict[str, object]:
        return cast(dict[str, object], json.loads(self.submitted_settings_json))


@dataclass(frozen=True, slots=True)
class JobTaskRunRecord:
    run_id: int
    job_id: int
    task_key: str
    task_type: str
    task_order: int
    depends_on: tuple[str, ...]
    settings_json: str
    state: JobTaskState
    attempt_count: int
    started_at: str | None
    finished_at: str | None
    error_code: str | None

    @property
    def settings(self) -> dict[str, object]:
        """Return the immutable execution settings captured for this submitted task."""
        return cast(dict[str, object], json.loads(self.settings_json))


@dataclass(frozen=True, slots=True)
class JobTaskAttemptRecord:
    run_id: int
    task_key: str
    attempt_number: int
    attempt_id: str
    state: JobAttemptState
    termination_reason: str
    started_at: str | None
    finished_at: str | None
    exit_code: int | None
    error_code: str | None


@dataclass(frozen=True, slots=True)
class JobTaskAttemptOutput:
    """Bounded stdout/stderr snapshot persisted with one terminal attempt."""

    run_id: int
    task_key: str
    attempt_number: int
    stdout: str = field(repr=False)
    stderr: str = field(repr=False)
    stdout_truncated: bool
    stderr_truncated: bool
    expired_at: str | None = None


@dataclass(frozen=True, slots=True)
class JobSubmission:
    run: JobRunRecord
    replayed: bool


def canonical_settings(value: Mapping[str, object], *, label: str) -> str:
    """Validate a bounded, finite JSON object without serializing secret fields."""
    if not isinstance(value, Mapping):
        raise InvalidJobDefinitionError(f"{label} must be a JSON object")
    _reject_secret_fields(value, label=label)
    try:
        serialized = json.dumps(
            dict(value),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as error:
        raise InvalidJobDefinitionError(f"{label} must contain finite JSON data") from error
    if len(serialized.encode("utf-8")) > MAX_SETTINGS_BYTES:
        raise InvalidJobDefinitionError(f"{label} exceeds the {MAX_SETTINGS_BYTES}-byte limit")
    return serialized


def validate_job_dag(tasks: Sequence[JobTaskDefinition]) -> tuple[JobTaskDefinition, ...]:
    """Validate task identity, dependencies, limits, and acyclic execution order."""
    task_tuple = tuple(tasks)
    if not task_tuple:
        raise InvalidJobDefinitionError("a job must contain at least one task")
    if len(task_tuple) > MAX_TASKS_PER_JOB:
        raise InvalidJobDefinitionError(f"a job cannot contain more than {MAX_TASKS_PER_JOB} tasks")
    by_key = {task.task_key: task for task in task_tuple}
    if len(by_key) != len(task_tuple):
        raise InvalidJobDefinitionError("job task keys must be unique")
    for task in task_tuple:
        for dependency in task.depends_on:
            if dependency not in by_key:
                raise InvalidJobDefinitionError(
                    f"task {task.task_key!r} depends on missing task {dependency!r}"
                )
            if dependency == task.task_key:
                raise InvalidJobDefinitionError(f"task {task.task_key!r} cannot depend on itself")

    marks: dict[str, int] = {}

    def visit(task_key: str) -> None:
        mark = marks.get(task_key, 0)
        if mark == 1:
            raise InvalidJobDefinitionError("job task dependencies contain a cycle")
        if mark == 2:
            return
        marks[task_key] = 1
        for dependency in by_key[task_key].depends_on:
            visit(dependency)
        marks[task_key] = 2

    for task in task_tuple:
        visit(task.task_key)
    return task_tuple


def _reject_secret_fields(value: object, *, label: str) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str):
                raise InvalidJobDefinitionError(f"{label} object keys must be strings")
            normalized = key.casefold().replace("-", "_")
            compact = normalized.replace("_", "")
            if normalized in _SECRET_SETTING_KEYS or compact.endswith(
                ("token", "secret", "password", "credential", "credentials", "apikey", "privatekey")
            ):
                raise InvalidJobDefinitionError(
                    f"{label} cannot persist credential field {key!r}; "
                    "use a scoped secret reference"
                )
            _reject_secret_fields(child, label=label)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for child in value:
            _reject_secret_fields(child, label=label)
    elif isinstance(value, float) and not math.isfinite(value):
        raise InvalidJobDefinitionError(f"{label} must contain finite JSON data")
