"""Launch one Python worker with a frozen context and bounded output capture."""

from __future__ import annotations

import json
import math
import os
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any, cast

from brickhouse.runtime.worker_auth import (
    MAX_WORKER_TOKEN_TTL_SECONDS,
    WorkerAuthenticator,
    WorkerCredential,
)
from brickhouse.runtime.worker_services import WorkerCallHandler, WorkerServiceSession

if TYPE_CHECKING:
    from collections.abc import Callable

    from brickhouse.runtime.outcomes import WorkerOutcomeJournal

CONTEXT_ENV = "BRICKHOUSE_RUN_CONTEXT_JSON"
DEFAULT_STDOUT_LIMIT_BYTES = 1024 * 1024
DEFAULT_STDERR_LIMIT_BYTES = 1024 * 1024
MAX_CAPTURE_LIMIT_BYTES = 64 * 1024 * 1024
MAX_CONTEXT_BYTES = 16 * 1024
MAX_WORKER_TIMEOUT_SECONDS = 24 * 60 * 60
MAX_TERMINATION_GRACE_SECONDS = 60
_READ_SIZE = 8192


class WorkerConfigurationError(ValueError):
    """Worker inputs are incomplete, invalid, or exceed their declared bounds."""


class WorkerLaunchError(RuntimeError):
    """The configured Python interpreter could not start a worker process."""


@dataclass(frozen=True, slots=True)
class WorkerContext:
    """An immutable JSON snapshot supplied to one worker attempt.

    Store canonical JSON rather than caller-owned dictionaries so later mutations to
    parameters or namespace state cannot change the context after submission.
    Credentials belong to a separate scoped worker protocol and must not be placed in
    this context.
    """

    serialized: str

    def __post_init__(self) -> None:
        try:
            decoded = json.loads(self.serialized)
            canonical = _canonical_json(decoded)
        except (TypeError, ValueError) as error:
            raise WorkerConfigurationError("worker context must be valid finite JSON") from error
        if not isinstance(decoded, dict):
            raise WorkerConfigurationError("worker context must be a JSON object")
        if len(canonical.encode("utf-8")) > MAX_CONTEXT_BYTES:
            raise WorkerConfigurationError(
                f"worker context exceeds the {MAX_CONTEXT_BYTES}-byte limit"
            )
        object.__setattr__(self, "serialized", canonical)

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> WorkerContext:
        """Take an immutable, size-checked snapshot of a JSON-compatible mapping."""
        try:
            serialized = _canonical_json(dict(value))
        except (TypeError, ValueError) as error:
            raise WorkerConfigurationError(
                "worker context values must be finite JSON data"
            ) from error
        return cls(serialized)

    def to_mapping(self) -> dict[str, object]:
        """Return a new mapping decoded from the immutable snapshot."""
        decoded = json.loads(self.serialized)
        if not isinstance(decoded, dict):  # Kept defensive for static type narrowing.
            raise WorkerConfigurationError("worker context must be a JSON object")
        return cast(dict[str, object], decoded)


@dataclass(frozen=True, slots=True)
class WorkerResult:
    """Observed process result with stdout/stderr already bounded by byte limits."""

    exit_code: int
    stdout: str
    stderr: str
    stdout_truncated: bool
    stderr_truncated: bool
    stdout_limit_bytes: int
    stderr_limit_bytes: int
    interpreter: Path
    script: Path
    cwd: Path
    termination_reason: str = "completed"
    cleanup_error_type: str | None = None


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _resolve_file(path: str | Path, label: str, *, executable: bool = False) -> Path:
    candidate = Path(path).expanduser()
    try:
        # Keep a venv's interpreter symlink intact: resolving it jumps to the base
        # Python executable and causes the child to ignore that venv's pyvenv.cfg.
        resolved = candidate.absolute() if executable else candidate.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise WorkerConfigurationError(
            f"worker {label} does not resolve to an existing path"
        ) from error
    if not resolved.is_file():
        raise WorkerConfigurationError(f"worker {label} must be a file")
    if executable and os.name != "nt" and not os.access(resolved, os.X_OK):
        raise WorkerConfigurationError(f"worker {label} is not executable")
    return resolved


def _resolve_directory(path: str | Path) -> Path:
    try:
        resolved = Path(path).expanduser().resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise WorkerConfigurationError(
            "worker cwd does not resolve to an existing directory"
        ) from error
    if not resolved.is_dir():
        raise WorkerConfigurationError("worker cwd must be a directory")
    return resolved


def _validate_limit(value: int, label: str) -> int:
    if type(value) is not int or value < 0 or value > MAX_CAPTURE_LIMIT_BYTES:
        raise WorkerConfigurationError(
            f"{label} must be an integer from 0 to {MAX_CAPTURE_LIMIT_BYTES} bytes"
        )
    return value


def _validate_arguments(value: Sequence[str]) -> tuple[str, ...]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        raise WorkerConfigurationError("worker arguments must be a sequence of strings")
    if len(value) > 100 or any(
        not isinstance(argument, str) or len(argument) > 4096 or "\x00" in argument
        for argument in value
    ):
        raise WorkerConfigurationError("worker arguments exceed their count or size limits")
    return tuple(value)


def _worker_environment(
    context: WorkerContext, credential: WorkerCredential | None = None
) -> dict[str, str]:
    # Do not inherit user credentials, Python import paths, or other process-local
    # session state. The interpreter is launched by its resolved absolute path.
    environment = {
        CONTEXT_ENV: context.serialized,
        "PYTHONUNBUFFERED": "1",
    }
    values = context.to_mapping()
    if "dbutils" in values or "notebook_parameters" in values:
        # The entry module is part of the installed BrickHouse package. Use its
        # resolved package root rather than inheriting a caller-controlled path.
        environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[2])
    if credential is not None:
        if values.get("run_id") != credential.run_id:
            raise WorkerConfigurationError("worker credential does not match context run_id")
        if values.get("attempt_id") != credential.attempt_id:
            raise WorkerConfigurationError("worker credential does not match context attempt_id")
        environment.update(credential.worker_environment())
    if os.name == "nt":
        for key in ("SystemRoot", "WINDIR", "TEMP", "TMP"):
            value = os.environ.get(key)
            if value is not None:
                environment[key] = value
    return environment


def _capture_bounded(stream: IO[bytes], limit: int) -> tuple[bytes, bool]:
    retained = bytearray()
    truncated = False
    while True:
        chunk = stream.read(_READ_SIZE)
        if not chunk:
            break
        remaining = limit - len(retained)
        if remaining > 0:
            retained.extend(chunk[:remaining])
        if len(chunk) > remaining:
            truncated = True
    return bytes(retained), truncated


def run_python_worker(
    script: str | Path,
    *,
    cwd: str | Path,
    context: WorkerContext,
    arguments: Sequence[str] = (),
    interpreter: str | Path | None = None,
    stdout_limit_bytes: int = DEFAULT_STDOUT_LIMIT_BYTES,
    stderr_limit_bytes: int = DEFAULT_STDERR_LIMIT_BYTES,
    credential: WorkerCredential | None = None,
    authenticator: WorkerAuthenticator | None = None,
    worker_call_handler: WorkerCallHandler | None = None,
    timeout_seconds: float | None = None,
    cancel_event: threading.Event | None = None,
    termination_grace_seconds: float = 2,
    staging_cleanup: Callable[[], None] | None = None,
    outcome_journal: WorkerOutcomeJournal | None = None,
    secret_payload: bytes | None = None,
    secret_redactions: Sequence[bytes] = (),
) -> WorkerResult:
    """Run one script with an absolute interpreter, explicit cwd, and bounded pipes.

    The process receives only the immutable run context and a minimal platform
    environment. No shell or PATH lookup is involved. Timeout/cancellation terminate
    the owned POSIX process group, escalate after the grace period, then reap the child.
    Optional outcome and cleanup hooks run after process termination.
    """
    if not isinstance(context, WorkerContext):
        raise WorkerConfigurationError("worker context must be a WorkerContext")
    worker_arguments = _validate_arguments(arguments)
    redactions = _validate_secret_redactions(secret_redactions)
    if secret_payload is not None:
        scope = context.to_mapping()
        if "dbutils" not in scope and "notebook_parameters" not in scope:
            raise WorkerConfigurationError(
                "secret payload requires a dbutils or notebook worker entry"
            )
        from brickhouse.runtime.secrets import MAX_SECRET_SNAPSHOT_BYTES

        if (
            not isinstance(secret_payload, bytes)
            or not secret_payload
            or len(secret_payload) > MAX_SECRET_SNAPSHOT_BYTES
        ):
            raise WorkerConfigurationError("worker secret payload is empty or exceeds its limit")
    stdout_limit = _validate_limit(stdout_limit_bytes, "stdout_limit_bytes")
    stderr_limit = _validate_limit(stderr_limit_bytes, "stderr_limit_bytes")
    _validate_timeout(timeout_seconds)
    _validate_grace(termination_grace_seconds)
    if (credential is None) != (authenticator is None):
        raise WorkerConfigurationError(
            "worker credential and authenticator must be supplied together"
        )
    scope = context.to_mapping()
    run_id = scope.get("run_id")
    attempt_id = scope.get("attempt_id")
    resolved_interpreter = _resolve_file(
        sys.executable if interpreter is None else interpreter,
        "interpreter",
        executable=True,
    )
    resolved_script = _resolve_file(script, "script")
    resolved_cwd = _resolve_directory(cwd)
    service_session: WorkerServiceSession | None = None
    if worker_call_handler is not None:
        if os.name != "posix":
            raise WorkerConfigurationError(
                "private worker services require POSIX socket descriptor passing"
            )
        if not isinstance(run_id, str) or not isinstance(attempt_id, str):
            raise WorkerConfigurationError(
                "private worker services require string run_id and attempt_id context"
            )
        if credential is None:
            authenticator = WorkerAuthenticator()
            credential = authenticator.issue(
                run_id,
                attempt_id,
                ttl_seconds=MAX_WORKER_TOKEN_TTL_SECONDS,
            )
        assert authenticator is not None
        service_session = WorkerServiceSession(
            run_id,
            attempt_id,
            worker_call_handler,
            authenticator=authenticator,
            credential=credential,
        )
    try:
        environment = _worker_environment(context, credential)
        if service_session is not None:
            environment.update(service_session.worker_environment())
    except BaseException:
        if service_session is not None:
            service_session.close()
        else:
            _revoke_attempt(authenticator, run_id, attempt_id)
        raise
    command = (
        [
            str(resolved_interpreter),
            "-m",
            "brickhouse.runtime.worker_entry",
            str(resolved_script),
            *worker_arguments,
        ]
        if "dbutils" in scope
        else [str(resolved_interpreter), str(resolved_script), *worker_arguments]
    )
    started_at = datetime.now(UTC).isoformat()
    if outcome_journal is not None:
        if not isinstance(run_id, str) or not isinstance(attempt_id, str):
            raise WorkerConfigurationError(
                "persisted worker context requires string run_id and attempt_id"
            )
        try:
            outcome_journal.start_attempt(
                run_id,
                attempt_id,
                interpreter=resolved_interpreter,
                script=resolved_script,
                cwd=resolved_cwd,
                started_at=started_at,
            )
        except BaseException:
            if service_session is not None:
                service_session.close()
            else:
                _revoke_attempt(authenticator, run_id, attempt_id)
            raise

    try:
        launch_options: dict[str, Any] = {}
        if service_session is not None:
            launch_options["pass_fds"] = service_session.child_pass_fds()
        process = subprocess.Popen(
            command,
            cwd=resolved_cwd,
            env=environment,
            stdin=subprocess.PIPE if secret_payload is not None else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            close_fds=True,
            start_new_session=(os.name == "posix"),
            **launch_options,
        )
    except OSError as error:
        if service_session is not None:
            service_session.close()
        cleanup_error_type = _run_cleanup(staging_cleanup)
        if outcome_journal is not None:
            outcome_journal.fail_to_launch(
                cast(str, run_id),
                cast(str, attempt_id),
                cleanup_error_type=cleanup_error_type,
            )
        _revoke_attempt(authenticator, run_id, attempt_id)
        raise WorkerLaunchError("could not start the configured Python worker") from error

    if service_session is not None:
        try:
            service_session.start()
        except Exception as error:
            _terminate_worker(process, termination_grace_seconds)
            process.wait()
            service_session.close()
            cleanup_error_type = _run_cleanup(staging_cleanup)
            if outcome_journal is not None:
                outcome_journal.fail_to_launch(
                    cast(str, run_id),
                    cast(str, attempt_id),
                    cleanup_error_type=cleanup_error_type,
                )
            raise WorkerLaunchError("could not start private worker services") from error

    if secret_payload is not None:
        try:
            if process.stdin is None:  # pragma: no cover - Popen contract.
                raise WorkerLaunchError("worker secret pipe was not created")
            process.stdin.write(secret_payload)
            process.stdin.close()
        except (BrokenPipeError, OSError, WorkerLaunchError) as error:
            _terminate_worker(process, termination_grace_seconds)
            process.wait()
            if service_session is not None:
                service_session.close()
            for stream in (process.stdin, process.stdout, process.stderr):
                if stream is not None:
                    stream.close()
            cleanup_error_type = _run_cleanup(staging_cleanup)
            if outcome_journal is not None:
                outcome_journal.fail_to_launch(
                    cast(str, run_id),
                    cast(str, attempt_id),
                    cleanup_error_type=cleanup_error_type,
                )
            _revoke_attempt(authenticator, run_id, attempt_id)
            raise WorkerLaunchError("could not send the scoped secret payload") from error

    if outcome_journal is not None:
        try:
            outcome_journal.attach_process(cast(str, run_id), cast(str, attempt_id), process.pid)
        except BaseException:
            _terminate_worker(process, termination_grace_seconds)
            process.wait()
            for stream in (process.stdout, process.stderr):
                if stream is not None:
                    stream.close()
            outcome_journal.interrupt_attempt(cast(str, run_id), cast(str, attempt_id))
            if service_session is not None:
                service_session.close()
            else:
                _revoke_attempt(authenticator, run_id, attempt_id)
            _run_cleanup(staging_cleanup)
            raise

    stdout_pipe = process.stdout
    stderr_pipe = process.stderr
    if stdout_pipe is None or stderr_pipe is None:  # pragma: no cover - Popen contract.
        process.kill()
        process.wait()
        if service_session is not None:
            service_session.close()
        raise WorkerLaunchError("worker output pipes were not created")

    stdout_capture: list[tuple[bytes, bool]] = []
    stderr_capture: list[tuple[bytes, bool]] = []
    stdout_reader = threading.Thread(
        target=lambda: stdout_capture.append(_capture_bounded(stdout_pipe, stdout_limit)),
        name="brickhouse-worker-stdout",
        daemon=True,
    )
    stderr_reader = threading.Thread(
        target=lambda: stderr_capture.append(_capture_bounded(stderr_pipe, stderr_limit)),
        name="brickhouse-worker-stderr",
        daemon=True,
    )
    stdout_reader.start()
    stderr_reader.start()
    termination_reason = "completed"
    interrupted: BaseException | None = None
    timeout_deadline = None if timeout_seconds is None else time.monotonic() + timeout_seconds
    try:
        termination_reason = _wait_for_worker(
            process,
            timeout_seconds=timeout_seconds,
            cancel_event=cancel_event,
            termination_grace_seconds=termination_grace_seconds,
        )
        if termination_reason == "completed":
            termination_reason = _wait_for_output_capture(
                process,
                stdout_reader,
                stderr_reader,
                timeout_deadline=timeout_deadline,
                cancel_event=cancel_event,
                termination_grace_seconds=termination_grace_seconds,
            )
    except BaseException as error:
        interrupted = error
        termination_reason = "cancelled"
        _terminate_worker(process, termination_grace_seconds)
    finally:
        if process.poll() is None:
            _terminate_worker(process, termination_grace_seconds)
        elif _process_group_exists(process):
            # The worker leader may finish while a child still belongs to its
            # session. Reap that session before reporting completion.
            _terminate_worker(process, termination_grace_seconds)
        exit_code = process.wait()
        if service_session is not None:
            service_session.close()
        else:
            _revoke_attempt(authenticator, run_id, attempt_id)
        stdout_reader.join(timeout=max(1.0, termination_grace_seconds + 0.5))
        stderr_reader.join(timeout=max(1.0, termination_grace_seconds + 0.5))

    # A process which deliberately detached from the worker session can retain
    # a pipe after the deadline. The daemon reader is bounded by the caller's
    # timeout; keep its captured prefix and mark the stream incomplete.
    stdout_bytes, stdout_truncated = stdout_capture[0] if stdout_capture else (b"", True)
    stderr_bytes, stderr_truncated = stderr_capture[0] if stderr_capture else (b"", True)
    if credential is not None:
        stdout_bytes = _redact_worker_token(stdout_bytes, credential._token)
        stderr_bytes = _redact_worker_token(stderr_bytes, credential._token)
    for redaction in redactions:
        stdout_bytes = _redact_secret_value(stdout_bytes, redaction)
        stderr_bytes = _redact_secret_value(stderr_bytes, redaction)
    cleanup_error_type = _run_cleanup(staging_cleanup)
    result = WorkerResult(
        exit_code=exit_code,
        stdout=stdout_bytes.decode("utf-8", errors="replace"),
        stderr=stderr_bytes.decode("utf-8", errors="replace"),
        stdout_truncated=stdout_truncated,
        stderr_truncated=stderr_truncated,
        stdout_limit_bytes=stdout_limit,
        stderr_limit_bytes=stderr_limit,
        interpreter=resolved_interpreter,
        script=resolved_script,
        cwd=resolved_cwd,
        termination_reason=termination_reason,
        cleanup_error_type=cleanup_error_type,
    )
    if outcome_journal is not None:
        outcome_journal.finish_attempt(cast(str, run_id), cast(str, attempt_id), result)
    if interrupted is not None:
        raise interrupted
    return result


def _validate_timeout(value: float | None) -> None:
    if value is None:
        return
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or value > MAX_WORKER_TIMEOUT_SECONDS
        or value <= 0
        or not math.isfinite(value)
    ):
        raise WorkerConfigurationError(
            f"timeout_seconds must be greater than zero and at most {MAX_WORKER_TIMEOUT_SECONDS}"
        )


def _validate_grace(value: float) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or value > MAX_TERMINATION_GRACE_SECONDS
        or value < 0
        or not math.isfinite(value)
    ):
        raise WorkerConfigurationError(
            f"termination_grace_seconds must be from 0 to {MAX_TERMINATION_GRACE_SECONDS}"
        )


def _run_cleanup(cleanup: Callable[[], None] | None) -> str | None:
    if cleanup is None:
        return None
    try:
        cleanup()
    except Exception as error:
        return type(error).__name__
    return None


def _redact_worker_token(output: bytes, token: str) -> bytes:
    """Remove a logged bearer and any truncated suffix that reveals its prefix."""
    secret = token.encode("ascii")
    redacted = output.replace(secret, b"[worker token redacted]")
    for overlap in range(min(len(secret) - 1, len(redacted)), 0, -1):
        if redacted.endswith(secret[:overlap]):
            return redacted[:-overlap]
    return redacted


def _validate_secret_redactions(values: Sequence[bytes]) -> tuple[bytes, ...]:
    if isinstance(values, (str, bytes, bytearray)) or not isinstance(values, Sequence):
        raise WorkerConfigurationError("secret redactions must be a sequence of byte strings")
    if len(values) > 1000 or any(not isinstance(value, bytes) for value in values):
        raise WorkerConfigurationError("secret redactions exceed their type or count limit")
    return tuple(
        sorted({value for value in values if value}, key=lambda value: (-len(value), value))
    )


def _redact_secret_value(output: bytes, secret: bytes) -> bytes:
    redacted = output.replace(secret, b"*" * len(secret))
    overlap = min(len(secret) - 1, len(redacted))
    for partial_length in range(overlap, 0, -1):
        if redacted.endswith(secret[:partial_length]):
            return redacted[:-partial_length] + b"*" * partial_length
    return redacted


def _revoke_attempt(
    authenticator: WorkerAuthenticator | None, run_id: object, attempt_id: object
) -> None:
    if authenticator is not None and isinstance(run_id, str) and isinstance(attempt_id, str):
        authenticator.revoke_attempt(run_id, attempt_id)


def _wait_for_worker(
    process: subprocess.Popen[bytes],
    *,
    timeout_seconds: float | None,
    cancel_event: threading.Event | None,
    termination_grace_seconds: float,
) -> str:
    deadline = None if timeout_seconds is None else time.monotonic() + timeout_seconds
    while process.poll() is None:
        if cancel_event is not None and cancel_event.is_set():
            _terminate_worker(process, termination_grace_seconds)
            return "cancelled"
        if deadline is not None and time.monotonic() >= deadline:
            _terminate_worker(process, termination_grace_seconds)
            return "timeout"
        wait_for = 0.05
        if deadline is not None:
            wait_for = min(wait_for, max(0.001, deadline - time.monotonic()))
        try:
            process.wait(timeout=wait_for)
        except subprocess.TimeoutExpired:
            continue
    return "completed"


def _wait_for_output_capture(
    process: subprocess.Popen[bytes],
    stdout_reader: threading.Thread,
    stderr_reader: threading.Thread,
    *,
    timeout_deadline: float | None,
    cancel_event: threading.Event | None,
    termination_grace_seconds: float,
) -> str:
    """Keep the worker deadline active while descendants retain inherited pipes."""
    while stdout_reader.is_alive() or stderr_reader.is_alive():
        if cancel_event is not None and cancel_event.is_set():
            _terminate_worker(process, termination_grace_seconds)
            return "cancelled"
        if timeout_deadline is not None and time.monotonic() >= timeout_deadline:
            _terminate_worker(process, termination_grace_seconds)
            return "timeout"
        stdout_reader.join(timeout=0.01)
        stderr_reader.join(timeout=0.01)
    return "completed"


def _signal_worker_group(process: subprocess.Popen[bytes], selected_signal: int) -> None:
    if os.name == "posix":
        try:
            os.killpg(process.pid, selected_signal)
        except ProcessLookupError:
            pass
        except PermissionError:
            # A session can outlive its leader. If the platform refuses a
            # group signal after leader exit, still terminate the direct worker
            # when it remains live and keep cleanup from masking its outcome.
            if process.poll() is None:
                if selected_signal == signal.SIGTERM:
                    process.terminate()
                else:
                    process.kill()
    elif process.poll() is None:
        if selected_signal == signal.SIGTERM:
            process.terminate()
        else:
            process.kill()


def _process_group_exists(process: subprocess.Popen[bytes]) -> bool:
    if os.name != "posix":
        return process.poll() is None
    try:
        os.killpg(process.pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _terminate_worker(process: subprocess.Popen[bytes], termination_grace_seconds: float) -> None:
    _signal_worker_group(process, signal.SIGTERM)
    try:
        process.wait(timeout=termination_grace_seconds)
    except subprocess.TimeoutExpired:
        pass
    if _process_group_exists(process):
        _signal_worker_group(process, signal.SIGKILL)
    process.wait()
