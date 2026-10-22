"""Attempt-scoped duplex worker calls over a local socket pair."""

from __future__ import annotations

import json
import os
import socket
import struct
import threading
import uuid
from collections.abc import Callable, Mapping
from typing import Any

from brickhouse.runtime.worker_auth import (
    MAX_WORKER_MESSAGE_BYTES,
    WORKER_TOKEN_ENV,
    WorkerAuthenticationError,
    WorkerAuthenticator,
    WorkerCallMessage,
    WorkerCredential,
    WorkerProtocolError,
)

WORKER_SOCKET_ENV = "BRICKHOUSE_WORKER_SOCKET_FD"
_FRAME_HEADER_BYTES = 4


class WorkerServiceError(RuntimeError):
    """A scoped worker service call failed with an explicit local error."""


WorkerCallHandler = Callable[[WorkerCallMessage], object]


class WorkerServiceSession:
    """Parent side of one private, authenticated worker request channel."""

    def __init__(
        self,
        run_id: str,
        attempt_id: str,
        handler: WorkerCallHandler,
        *,
        authenticator: WorkerAuthenticator,
        credential: WorkerCredential,
    ) -> None:
        if os.name != "posix":
            raise WorkerServiceError(
                "private worker services require POSIX socket descriptor passing"
            )
        if credential.run_id != run_id or credential.attempt_id != attempt_id:
            raise WorkerServiceError("worker service credential does not match attempt scope")
        self.authenticator = authenticator
        self.credential = credential
        self._run_id = run_id
        self._attempt_id = attempt_id
        self._handler = handler
        self._parent_socket, self._child_socket = socket.socketpair()
        self._parent_socket.settimeout(0.1)
        self._child_socket.settimeout(30)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._started = False
        self._closed = False

    def worker_environment(self) -> dict[str, str]:
        """Return the descriptor reference; the bearer travels in its own header."""
        if self._closed or self._child_socket.fileno() < 0:
            raise WorkerServiceError("worker service session is closed")
        return {WORKER_SOCKET_ENV: str(self._child_socket.fileno())}

    def child_pass_fds(self) -> tuple[int, ...]:
        """Return the only parent-owned descriptor inherited by this worker."""
        if self._closed or self._child_socket.fileno() < 0:
            raise WorkerServiceError("worker service session is closed")
        return (self._child_socket.fileno(),)

    def start(self) -> None:
        """Close the parent's child-descriptor copy and start the request handler."""
        if self._started or self._closed:
            raise WorkerServiceError("worker service session cannot be started")
        self._started = True
        self._child_socket.close()
        self._thread = threading.Thread(
            target=self._serve,
            name=f"brickhouse-worker-service-{self._run_id}-{self._attempt_id[:8]}",
            daemon=True,
        )
        self._thread.start()

    def close(self) -> None:
        """Stop dispatch, close both local endpoints and revoke the attempt token."""
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        try:
            self._parent_socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._parent_socket.close()
        try:
            self._child_socket.close()
        except OSError:
            pass
        thread = self._thread
        if thread is not None:
            thread.join(timeout=1)
        self.authenticator.revoke(self.credential)

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                body = _receive_frame(self._parent_socket)
            except TimeoutError:
                continue
            except (OSError, EOFError, WorkerProtocolError):
                return
            try:
                authorization, message = _decode_request(body)
                self.authenticator.authorize(authorization, message)
                if message.run_id != self._run_id or message.attempt_id != self._attempt_id:
                    raise WorkerAuthenticationError("worker call does not match its attempt")
                result = self._handler(message)
                response = _response_bytes({"ok": True, "result": result})
            except WorkerAuthenticationError:
                response = _error_response("permission_denied", "worker call was denied")
            except WorkerProtocolError:
                response = _error_response("invalid_request", "worker call was invalid")
            except PermissionError:
                response = _error_response("permission_denied", "worker call was denied")
            except KeyError as error:
                message_text = str(error.args[0]) if error.args else "not found"
                response = _error_response("not_found", message_text)
            except ValueError as error:
                response = _error_response("invalid_request", str(error))
            except Exception:
                response = _error_response("internal_error", "worker service operation failed")
            try:
                _send_frame(self._parent_socket, response)
            except (OSError, WorkerProtocolError):
                return


class WorkerServiceClient:
    """Synchronous child-side client for the attempt's authenticated service socket."""

    def __init__(self, run_id: str, attempt_id: str, token: str, file_descriptor: int) -> None:
        self._run_id = run_id
        self._attempt_id = attempt_id
        self._token = token
        self._socket = socket.socket(fileno=file_descriptor)
        self._socket.settimeout(30)
        self._lock = threading.Lock()
        self._counter = 0

    def __repr__(self) -> str:
        return f"WorkerServiceClient(run_id={self._run_id!r}, attempt_id={self._attempt_id!r})"

    @classmethod
    def from_environment(cls, run_id: str, attempt_id: str) -> WorkerServiceClient:
        """Consume and remove private transport values before user code executes."""
        try:
            token = os.environ.pop(WORKER_TOKEN_ENV)
            raw_descriptor = os.environ.pop(WORKER_SOCKET_ENV)
            descriptor = int(raw_descriptor)
        except (KeyError, ValueError) as error:
            raise WorkerServiceError("private worker service configuration is missing") from error
        if not token or descriptor < 0:
            raise WorkerServiceError("private worker service configuration is invalid")
        return cls(run_id, attempt_id, token, descriptor)

    def call(self, service: str, operation: str, payload: Mapping[str, object]) -> object:
        """Send one bounded authenticated operation and return its JSON result."""
        with self._lock:
            self._counter += 1
            message = WorkerCallMessage(
                protocol_version=1,
                run_id=self._run_id,
                attempt_id=self._attempt_id,
                request_id=f"req-{self._counter}-{uuid.uuid4().hex[:8]}",
                service=service,
                operation=operation,
                payload=payload,
            )
            encoded_message = json.loads(message.to_json())
            request = _canonical_json(
                {
                    "authorization": f"Bearer {self._token}",
                    "message": encoded_message,
                }
            ).encode("utf-8")
            _send_frame(self._socket, request)
            raw_response = _receive_frame(self._socket)
        try:
            response = json.loads(raw_response, object_pairs_hook=_unique_object)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise WorkerServiceError("private worker response is invalid") from error
        if not isinstance(response, dict) or type(response.get("ok")) is not bool:
            raise WorkerServiceError("private worker response has an invalid shape")
        if response["ok"]:
            return response.get("result")
        response_error = response.get("error")
        if not isinstance(response_error, dict):
            raise WorkerServiceError("private worker error response is malformed")
        code = response_error.get("code")
        text = response_error.get("message")
        if not isinstance(text, str):
            text = "private worker service operation failed"
        if code == "permission_denied":
            raise PermissionError(text)
        if code == "not_found":
            raise KeyError(text)
        if code == "invalid_request":
            raise ValueError(text)
        raise WorkerServiceError(text)

    def duplicate_environment_for_child(self) -> tuple[dict[str, str], int]:
        """Duplicate this attempt's private channel for one synchronous child kernel."""
        if self._socket.fileno() < 0 or not self._token:
            raise WorkerServiceError("private worker service session is closed")
        descriptor = os.dup(self._socket.fileno())
        return (
            {WORKER_TOKEN_ENV: self._token, WORKER_SOCKET_ENV: str(descriptor)},
            descriptor,
        )

    def close(self) -> None:
        """Close the local child endpoint after the user script returns."""
        self._socket.close()
        self._token = ""


def _receive_frame(connection: socket.socket) -> bytes:
    header = _receive_exact(connection, _FRAME_HEADER_BYTES)
    length = struct.unpack(">I", header)[0]
    if length < 1 or length > MAX_WORKER_MESSAGE_BYTES:
        raise WorkerProtocolError("worker frame length is invalid")
    return _receive_exact(connection, length)


def _receive_exact(connection: socket.socket, length: int) -> bytes:
    result = bytearray()
    while len(result) < length:
        try:
            chunk = connection.recv(length - len(result))
        except TimeoutError:
            if result:
                raise WorkerProtocolError("worker frame timed out before completion") from None
            raise
        if not chunk:
            raise EOFError("worker socket closed before frame completed")
        result.extend(chunk)
    return bytes(result)


def _send_frame(connection: socket.socket, body: bytes) -> None:
    if not 0 < len(body) <= MAX_WORKER_MESSAGE_BYTES:
        raise WorkerProtocolError("worker frame exceeds its size limit")
    connection.sendall(struct.pack(">I", len(body)) + body)


def _decode_request(body: bytes) -> tuple[str | None, WorkerCallMessage]:
    try:
        decoded = json.loads(body, object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise WorkerProtocolError("worker request must be valid JSON") from error
    if not isinstance(decoded, dict) or set(decoded) != {"authorization", "message"}:
        raise WorkerProtocolError("worker request envelope has invalid fields")
    authorization = decoded["authorization"]
    if authorization is not None and not isinstance(authorization, str):
        raise WorkerProtocolError("worker authorization header must be text")
    raw_message = decoded["message"]
    if not isinstance(raw_message, dict):
        raise WorkerProtocolError("worker request message must be a JSON object")
    return authorization, WorkerCallMessage.from_json(_canonical_json(raw_message))


def _response_bytes(value: object) -> bytes:
    try:
        encoded = _canonical_json(value).encode("utf-8")
    except (TypeError, ValueError):
        return _error_response("invalid_request", "worker response is not finite JSON")
    if len(encoded) > MAX_WORKER_MESSAGE_BYTES:
        return _error_response("invalid_request", "worker response exceeds its size limit")
    return encoded


def _error_response(code: str, message: str) -> bytes:
    return _response_bytes({"ok": False, "error": {"code": code, "message": message}})


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
