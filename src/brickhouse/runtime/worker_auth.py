"""Versioned, attempt-scoped authentication for private worker service messages."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import secrets
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, cast

WORKER_TOKEN_ENV = "BRICKHOUSE_WORKER_TOKEN"
WORKER_PROTOCOL_VERSION = 1
MAX_WORKER_MESSAGE_BYTES = 64 * 1024
# Worker Arrow writes use base64 inside the authenticated JSON envelope. Keep
# their raw payload well below the frame limit so identity and protocol fields
# still fit in the same bounded request.
MAX_WORKER_ARROW_BYTES = 32 * 1024
MAX_WORKER_ARROW_ROWS = 100_000
MAX_WORKER_TOKEN_TTL_SECONDS = 24 * 60 * 60 + 60
_IDENTIFIER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
_BEARER_PREFIX = "Bearer "


class WorkerProtocolError(ValueError):
    """A private worker message is malformed or uses an unsupported protocol."""


class WorkerAuthenticationError(PermissionError):
    """A private worker call has no valid credential for its attempt scope."""


@dataclass(frozen=True, slots=True)
class WorkerCallMessage:
    """Versioned private request envelope; its bearer credential is a header value."""

    protocol_version: int
    run_id: str
    attempt_id: str
    request_id: str
    service: str
    operation: str
    payload: Mapping[str, object]

    def __post_init__(self) -> None:
        if type(self.protocol_version) is not int:
            raise WorkerProtocolError("worker protocol_version must be an integer")
        if self.protocol_version != WORKER_PROTOCOL_VERSION:
            raise WorkerProtocolError("unsupported worker protocol version")
        for label, value in (
            ("run_id", self.run_id),
            ("attempt_id", self.attempt_id),
            ("request_id", self.request_id),
            ("service", self.service),
            ("operation", self.operation),
        ):
            _validate_identifier(value, label)
        if not isinstance(self.payload, Mapping):
            raise WorkerProtocolError("worker payload must be a JSON object")
        if not _is_json_value(dict(self.payload)):
            raise WorkerProtocolError("worker payload must contain finite JSON values")
        try:
            _canonical_json(dict(self.payload))
        except (TypeError, ValueError) as error:
            raise WorkerProtocolError("worker payload must contain finite JSON values") from error

    def to_json(self) -> bytes:
        """Encode the public request envelope without any credential material."""
        value = {
            "attempt_id": self.attempt_id,
            "operation": self.operation,
            "payload": dict(self.payload),
            "protocol_version": self.protocol_version,
            "request_id": self.request_id,
            "run_id": self.run_id,
            "service": self.service,
        }
        encoded = _canonical_json(value).encode("utf-8")
        if len(encoded) > MAX_WORKER_MESSAGE_BYTES:
            raise WorkerProtocolError(
                f"worker message exceeds the {MAX_WORKER_MESSAGE_BYTES}-byte limit"
            )
        return encoded

    @classmethod
    def from_json(cls, raw: bytes | str) -> WorkerCallMessage:
        """Decode a strict, bounded message and reject duplicate/unknown fields."""
        if isinstance(raw, bytes):
            if len(raw) > MAX_WORKER_MESSAGE_BYTES:
                raise WorkerProtocolError(
                    f"worker message exceeds the {MAX_WORKER_MESSAGE_BYTES}-byte limit"
                )
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError as error:
                raise WorkerProtocolError("worker message must be UTF-8 JSON") from error
        elif isinstance(raw, str):
            try:
                encoded = raw.encode("utf-8")
            except UnicodeEncodeError as error:
                raise WorkerProtocolError("worker message must be UTF-8 JSON") from error
            if len(encoded) > MAX_WORKER_MESSAGE_BYTES:
                raise WorkerProtocolError(
                    f"worker message exceeds the {MAX_WORKER_MESSAGE_BYTES}-byte limit"
                )
            text = raw
        else:
            raise WorkerProtocolError("worker message must be bytes or text")
        try:
            decoded = json.loads(text, object_pairs_hook=_unique_object)
        except (json.JSONDecodeError, ValueError) as error:
            raise WorkerProtocolError("worker message must be valid JSON") from error
        if not isinstance(decoded, dict):
            raise WorkerProtocolError("worker message must be a JSON object")
        expected = {
            "attempt_id",
            "operation",
            "payload",
            "protocol_version",
            "request_id",
            "run_id",
            "service",
        }
        if set(decoded) != expected:
            raise WorkerProtocolError("worker message fields do not match protocol version 1")
        payload = decoded["payload"]
        if not isinstance(payload, dict):
            raise WorkerProtocolError("worker payload must be a JSON object")
        try:
            return cls(
                protocol_version=decoded["protocol_version"],
                run_id=decoded["run_id"],
                attempt_id=decoded["attempt_id"],
                request_id=decoded["request_id"],
                service=decoded["service"],
                operation=decoded["operation"],
                payload=cast(dict[str, object], payload),
            )
        except (TypeError, WorkerProtocolError) as error:
            if isinstance(error, WorkerProtocolError):
                raise
            raise WorkerProtocolError("worker message field types are invalid") from error


@dataclass(frozen=True, slots=True)
class WorkerCredential:
    """Bearer token bound to one run/attempt and hidden from object reprs."""

    protocol_version: int
    run_id: str
    attempt_id: str
    expires_at: float
    _token: str = field(repr=False)

    @property
    def authorization_header(self) -> str:
        """Return the transport header value; callers must not log or put it in a URL."""
        return f"{_BEARER_PREFIX}{self._token}"

    def worker_environment(self) -> dict[str, str]:
        """Return the single private credential variable supplied to this child."""
        return {WORKER_TOKEN_ENV: self._token}


@dataclass(frozen=True, slots=True)
class _CredentialRecord:
    digest: bytes
    run_id: str
    attempt_id: str
    expires_at: float


class WorkerAuthenticator:
    """Issue, validate, and revoke private bearer credentials for worker attempts."""

    def __init__(self, *, clock: Callable[[], float] = time.time) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._credentials: dict[bytes, _CredentialRecord] = {}

    def issue(self, run_id: str, attempt_id: str, *, ttl_seconds: int = 300) -> WorkerCredential:
        """Create an unguessable credential scoped to one live run attempt."""
        _validate_identifier(run_id, "run_id")
        _validate_identifier(attempt_id, "attempt_id")
        if type(ttl_seconds) is not int or not 1 <= ttl_seconds <= MAX_WORKER_TOKEN_TTL_SECONDS:
            raise WorkerProtocolError(
                f"ttl_seconds must be an integer from 1 to {MAX_WORKER_TOKEN_TTL_SECONDS}"
            )
        token = secrets.token_urlsafe(32)
        digest = _token_digest(token)
        expires_at = self._clock() + ttl_seconds
        record = _CredentialRecord(digest, run_id, attempt_id, expires_at)
        with self._lock:
            self._credentials[digest] = record
        return WorkerCredential(WORKER_PROTOCOL_VERSION, run_id, attempt_id, expires_at, token)

    def authorize(self, authorization: str | None, message: WorkerCallMessage) -> None:
        """Reject missing, expired, revoked, or cross-attempt calls without fallback."""
        if message.protocol_version != WORKER_PROTOCOL_VERSION:
            raise WorkerAuthenticationError("unsupported worker protocol version")
        if (
            authorization is None
            or not authorization.startswith(_BEARER_PREFIX)
            or len(authorization) > 512
        ):
            raise WorkerAuthenticationError("worker authorization is missing or invalid")
        token = authorization[len(_BEARER_PREFIX) :]
        if not token or any(character.isspace() for character in token):
            raise WorkerAuthenticationError("worker authorization is missing or invalid")
        digest = _token_digest(token)
        now = self._clock()
        with self._lock:
            record = self._credentials.get(digest)
            if record is not None and now >= record.expires_at:
                self._credentials.pop(digest, None)
                record = None
        if record is None or not hmac.compare_digest(digest, record.digest):
            raise WorkerAuthenticationError("worker authorization is expired or revoked")
        if record.run_id != message.run_id or record.attempt_id != message.attempt_id:
            raise WorkerAuthenticationError("worker authorization does not match attempt scope")

    def revoke(self, credential: WorkerCredential) -> None:
        """Revoke one credential when its worker attempt leaves scope."""
        digest = _token_digest(credential._token)
        with self._lock:
            record = self._credentials.get(digest)
            if record is not None and record.run_id == credential.run_id:
                if record.attempt_id == credential.attempt_id:
                    self._credentials.pop(digest, None)

    def revoke_attempt(self, run_id: str, attempt_id: str) -> None:
        """Revoke all credentials issued for a completed run attempt."""
        with self._lock:
            stale = [
                digest
                for digest, record in self._credentials.items()
                if record.run_id == run_id and record.attempt_id == attempt_id
            ]
            for digest in stale:
                self._credentials.pop(digest, None)

    def revoke_all(self) -> None:
        """Invalidate all process-local tokens when their owner service shuts down."""
        with self._lock:
            self._credentials.clear()

    @property
    def active_credential_count(self) -> int:
        """Expose a count for lifecycle diagnostics without exposing token values."""
        now = self._clock()
        with self._lock:
            expired = [
                digest for digest, record in self._credentials.items() if now >= record.expires_at
            ]
            for digest in expired:
                self._credentials.pop(digest, None)
            return len(self._credentials)


def _validate_identifier(value: object, label: str) -> None:
    if not isinstance(value, str) or _IDENTIFIER_RE.fullmatch(value) is None:
        raise WorkerProtocolError(f"worker {label} is invalid")


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON object key")
        value[key] = item
    return value


def _is_json_value(value: object) -> bool:
    if value is None or type(value) in {str, bool, int}:
        return True
    if type(value) is float:
        return value == value and value not in (float("inf"), float("-inf"))
    if type(value) is list:
        return all(_is_json_value(item) for item in value)
    if type(value) is dict:
        return all(type(key) is str and _is_json_value(item) for key, item in value.items())
    return False


def _token_digest(token: str) -> bytes:
    return hashlib.sha256(token.encode("utf-8")).digest()
