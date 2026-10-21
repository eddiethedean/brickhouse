"""Process-memory secret provider for explicitly granted local job scopes."""

from __future__ import annotations

import base64
import binascii
import json
import re
import threading
from dataclasses import dataclass, field

MAX_SECRET_VALUE_BYTES = 4096
MAX_SECRET_SNAPSHOT_BYTES = 48 * 1024
MAX_SECRET_PROVIDER_BYTES = 1024 * 1024
MAX_SECRET_PROVIDER_SCOPES = 1000
MAX_SECRET_SCOPES_PER_JOB = 100
MAX_SECRET_KEYS_PER_SCOPE = 1000
_SCOPE_KEY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")


class SecretProviderError(RuntimeError):
    """The local secret provider cannot apply or resolve a requested secret."""


class SecretAccessDeniedError(PermissionError, SecretProviderError):
    """A job has not been granted the requested scope."""


class SecretNotFoundError(KeyError, SecretProviderError):
    """An authorized scope does not contain the requested key."""


class SecretConfigurationError(ValueError, SecretProviderError):
    """Secret configuration is malformed or exceeds its explicit limits."""


@dataclass(frozen=True, slots=True)
class WorkerSecretSnapshot:
    """One job's bounded secret payload and exact output-redaction values."""

    payload: bytes = field(repr=False)
    redaction_values: tuple[bytes, ...] = field(repr=False)
    scope_names: tuple[str, ...]


class SecretProvider:
    """Hold configured secret bytes only in process memory and grant scopes by job ID.

    The provider is intentionally not backed by workspace SQLite, run manifests,
    ordinary configuration, `.env`, or environment variables. An embedding
    application configures values and grants before submitting local jobs.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._values: dict[str, dict[str, bytes]] = {}
        self._job_scopes: dict[int, set[str]] = {}

    def put(self, scope: str, key: str, value: str | bytes) -> None:
        """Set one in-memory scope value without persisting it to disk."""
        normalized_scope = _validate_identifier(scope, "scope")
        normalized_key = _validate_identifier(key, "key")
        if isinstance(value, str):
            encoded = value.encode("utf-8")
        elif isinstance(value, bytes):
            encoded = value
        else:
            raise SecretConfigurationError("secret values must be strings or bytes")
        if len(encoded) > MAX_SECRET_VALUE_BYTES:
            raise SecretConfigurationError(
                f"secret values cannot exceed {MAX_SECRET_VALUE_BYTES} bytes"
            )
        with self._lock:
            total_size = sum(
                len(existing)
                for scope_values in self._values.values()
                for existing in scope_values.values()
            )
            previous = self._values.get(normalized_scope, {}).get(normalized_key, b"")
            if total_size - len(previous) + len(encoded) > MAX_SECRET_PROVIDER_BYTES:
                raise SecretConfigurationError(
                    "configured local secret values cannot exceed "
                    f"{MAX_SECRET_PROVIDER_BYTES} bytes"
                )
            if (
                normalized_scope not in self._values
                and len(self._values) >= MAX_SECRET_PROVIDER_SCOPES
            ):
                raise SecretConfigurationError(
                    "the local provider cannot contain more than "
                    f"{MAX_SECRET_PROVIDER_SCOPES} scopes"
                )
            values = self._values.setdefault(normalized_scope, {})
            values[normalized_key] = encoded
            if len(values) > MAX_SECRET_KEYS_PER_SCOPE:
                del values[normalized_key]
                raise SecretConfigurationError(
                    f"a scope cannot contain more than {MAX_SECRET_KEYS_PER_SCOPE} keys"
                )

    def grant_job(self, job_id: int, scope: str) -> None:
        """Allow one existing local job to access a named secret scope."""
        if type(job_id) is not int or job_id < 1:
            raise SecretConfigurationError("job_id must be a positive integer")
        normalized_scope = _validate_identifier(scope, "scope")
        with self._lock:
            scopes = self._job_scopes.setdefault(job_id, set())
            scopes.add(normalized_scope)
            if len(scopes) > MAX_SECRET_SCOPES_PER_JOB:
                scopes.remove(normalized_scope)
                raise SecretConfigurationError(
                    f"a job cannot have more than {MAX_SECRET_SCOPES_PER_JOB} scopes"
                )

    def revoke_job(self, job_id: int, scope: str | None = None) -> None:
        """Revoke one job's scope or every scope without changing secret values."""
        if type(job_id) is not int or job_id < 1:
            raise SecretConfigurationError("job_id must be a positive integer")
        with self._lock:
            if scope is None:
                self._job_scopes.pop(job_id, None)
                return
            normalized_scope = _validate_identifier(scope, "scope")
            scopes = self._job_scopes.get(job_id)
            if scopes is None:
                return
            scopes.discard(normalized_scope)
            if not scopes:
                self._job_scopes.pop(job_id, None)

    def snapshot_for_job(self, job_id: int) -> WorkerSecretSnapshot:
        """Create a bounded one-way worker payload with only this job's grants."""
        if type(job_id) is not int or job_id < 1:
            raise SecretConfigurationError("job_id must be a positive integer")
        with self._lock:
            scopes = tuple(sorted(self._job_scopes.get(job_id, set())))
            snapshot = {
                scope: dict(sorted(self._values.get(scope, {}).items())) for scope in scopes
            }
        encoded_scopes = {
            scope: {key: base64.b64encode(value).decode("ascii") for key, value in values.items()}
            for scope, values in snapshot.items()
        }
        payload = json.dumps(
            {"scopes": encoded_scopes},
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        if len(payload) > MAX_SECRET_SNAPSHOT_BYTES:
            raise SecretConfigurationError(
                f"authorized secret snapshot exceeds {MAX_SECRET_SNAPSHOT_BYTES} bytes"
            )
        redaction_values = tuple(
            sorted(
                {value for values in snapshot.values() for value in values.values() if value},
                key=lambda item: (-len(item), item),
            )
        )
        return WorkerSecretSnapshot(payload, redaction_values, scopes)

    def clear(self) -> None:
        """Erase configured values and grants from the current process."""
        with self._lock:
            for values in self._values.values():
                for key in list(values):
                    values[key] = b""
            self._values.clear()
            self._job_scopes.clear()


def decode_worker_secret_payload(payload: bytes) -> dict[str, dict[str, bytes]]:
    """Decode and validate the one-shot pipe payload in a worker process."""
    if not payload or len(payload) > MAX_SECRET_SNAPSHOT_BYTES:
        raise SecretConfigurationError("worker secret payload is empty or exceeds its limit")
    try:
        raw = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SecretConfigurationError("worker secret payload is invalid JSON") from error
    if not isinstance(raw, dict) or set(raw) != {"scopes"} or not isinstance(raw["scopes"], dict):
        raise SecretConfigurationError("worker secret payload has an invalid shape")
    if len(raw["scopes"]) > MAX_SECRET_SCOPES_PER_JOB:
        raise SecretConfigurationError("worker secret payload has too many scopes")
    result: dict[str, dict[str, bytes]] = {}
    for scope, entries in raw["scopes"].items():
        normalized_scope = _validate_identifier(scope, "scope")
        if not isinstance(entries, dict) or len(entries) > MAX_SECRET_KEYS_PER_SCOPE:
            raise SecretConfigurationError("worker secret scope has invalid key metadata")
        values: dict[str, bytes] = {}
        for key, encoded in entries.items():
            normalized_key = _validate_identifier(key, "key")
            if not isinstance(encoded, str):
                raise SecretConfigurationError("worker secret value encoding is invalid")
            try:
                value = base64.b64decode(encoded, validate=True)
            except (ValueError, binascii.Error) as error:
                raise SecretConfigurationError("worker secret value encoding is invalid") from error
            if len(value) > MAX_SECRET_VALUE_BYTES:
                raise SecretConfigurationError("worker secret value exceeds its limit")
            values[normalized_key] = value
        result[normalized_scope] = values
    return result


def _validate_identifier(value: str, label: str) -> str:
    if not isinstance(value, str) or _SCOPE_KEY_RE.fullmatch(value) is None:
        raise SecretConfigurationError(f"secret {label} is invalid")
    return value
