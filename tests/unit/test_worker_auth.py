from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from brickhouse.runtime import (
    MAX_WORKER_MESSAGE_BYTES,
    WORKER_PROTOCOL_VERSION,
    WORKER_TOKEN_ENV,
    WorkerAuthenticationError,
    WorkerAuthenticator,
    WorkerCallMessage,
    WorkerContext,
    WorkerProtocolError,
    run_python_worker,
)


def message(run_id: str = "run-a", attempt_id: str = "attempt-1") -> WorkerCallMessage:
    return WorkerCallMessage(
        protocol_version=WORKER_PROTOCOL_VERSION,
        run_id=run_id,
        attempt_id=attempt_id,
        request_id="request-1",
        service="tables",
        operation="read_snapshot",
        payload={"table": "main.default.orders"},
    )


def test_worker_message_round_trips_without_credentials() -> None:
    auth = WorkerAuthenticator()
    credential = auth.issue("run-a", "attempt-1")
    encoded = message().to_json()

    assert WorkerCallMessage.from_json(encoded) == message()
    assert credential._token.encode() not in encoded
    assert credential._token not in repr(credential)
    assert credential.authorization_header.startswith("Bearer ")


@pytest.mark.parametrize(
    "raw",
    [
        b"not-json",
        b"[]",
        b'{"protocol_version":1,"protocol_version":1}',
        b'{"attempt_id":"a","operation":"read","payload":{},"protocol_version":2,"request_id":"r","run_id":"x","service":"tables"}',
        b'{"attempt_id":"a","operation":"read","payload":{},"protocol_version":true,"request_id":"r","run_id":"x","service":"tables"}',
        b'{"attempt_id":"a","operation":"read","payload":{},"protocol_version":1,"request_id":"r","run_id":"x","service":"tables","token":"secret"}',
    ],
)
def test_worker_message_rejects_malformed_versions_and_extra_fields(raw: bytes) -> None:
    with pytest.raises(WorkerProtocolError):
        WorkerCallMessage.from_json(raw)


def test_worker_message_bounds_utf8_payload_and_rejects_invalid_payload() -> None:
    with pytest.raises(WorkerProtocolError, match="limit"):
        WorkerCallMessage.from_json(b" " * (MAX_WORKER_MESSAGE_BYTES + 1))
    with pytest.raises(WorkerProtocolError, match="finite JSON"):
        WorkerCallMessage(
            WORKER_PROTOCOL_VERSION,
            "run-a",
            "attempt-1",
            "request-1",
            "tables",
            "write",
            {"value": float("nan")},
        )
    with pytest.raises(WorkerProtocolError, match="finite JSON"):
        WorkerCallMessage(
            WORKER_PROTOCOL_VERSION,
            "run-a",
            "attempt-1",
            "request-1",
            "tables",
            "write",
            {"value": {1: "non-string JSON object key"}},  # type: ignore[dict-item]
        )
    with pytest.raises(WorkerProtocolError, match="run_id"):
        WorkerCallMessage(
            WORKER_PROTOCOL_VERSION,
            "../run",
            "attempt-1",
            "request-1",
            "tables",
            "read",
            {},
        )


def test_worker_authentication_is_scoped_and_revoked_per_attempt() -> None:
    now = [100.0]
    auth = WorkerAuthenticator(clock=lambda: now[0])
    first = auth.issue("run-a", "attempt-1", ttl_seconds=30)
    second = auth.issue("run-a", "attempt-2", ttl_seconds=30)

    assert auth.active_credential_count == 2
    auth.authorize(first.authorization_header, message("run-a", "attempt-1"))
    auth.authorize(second.authorization_header, message("run-a", "attempt-2"))
    with pytest.raises(WorkerAuthenticationError, match="scope"):
        auth.authorize(first.authorization_header, message("run-a", "attempt-2"))
    with pytest.raises(WorkerAuthenticationError, match="scope"):
        auth.authorize(second.authorization_header, message("run-a", "attempt-1"))

    auth.revoke_attempt("run-a", "attempt-1")
    assert auth.active_credential_count == 1
    with pytest.raises(WorkerAuthenticationError, match="revoked"):
        auth.authorize(first.authorization_header, message("run-a", "attempt-1"))
    auth.authorize(second.authorization_header, message("run-a", "attempt-2"))


def test_worker_authentication_rejects_missing_invalid_and_expired_tokens() -> None:
    now = [100.0]
    auth = WorkerAuthenticator(clock=lambda: now[0])
    credential = auth.issue("run-a", "attempt-1", ttl_seconds=5)

    for authorization in (None, "Basic abc", "Bearer ", "Bearer wrong token"):
        with pytest.raises(WorkerAuthenticationError):
            auth.authorize(authorization, message())
    now[0] = 105.0
    with pytest.raises(WorkerAuthenticationError, match="expired"):
        auth.authorize(credential.authorization_header, message())
    assert auth.active_credential_count == 0


@pytest.mark.parametrize("ttl", [0, -1, 86_461, True, 1.5])
def test_worker_authentication_rejects_invalid_ttl(ttl: object) -> None:
    auth = WorkerAuthenticator()
    with pytest.raises(WorkerProtocolError, match="ttl_seconds"):
        auth.issue("run-a", "attempt-1", ttl_seconds=ttl)  # type: ignore[arg-type]


def test_worker_token_is_separate_from_json_context_and_redacted_from_results(
    tmp_path: Path,
) -> None:
    auth = WorkerAuthenticator()
    credential = auth.issue("run-a", "attempt-1")
    context = WorkerContext.from_mapping(
        {"run_id": "run-a", "attempt_id": "attempt-1", "parameters": {"value": 7}}
    )
    script = tmp_path / "worker.py"
    script.write_text(
        "import hashlib, json, os\n"
        "print(json.dumps({"
        "'token_present': bool(os.environ.get('BRICKHOUSE_WORKER_TOKEN')), "
        "'token_digest': hashlib.sha256("
        "os.environ['BRICKHOUSE_WORKER_TOKEN'].encode()).hexdigest(), "
        "'context': json.loads(os.environ['BRICKHOUSE_RUN_CONTEXT_JSON'])"
        "}))\n"
        "print(os.environ['BRICKHOUSE_WORKER_TOKEN'])\n",
        encoding="utf-8",
    )

    result = run_python_worker(
        script,
        cwd=tmp_path,
        context=context,
        credential=credential,
        authenticator=auth,
    )

    observed = json.loads(result.stdout.splitlines()[0])
    assert result.exit_code == 0
    assert observed["token_present"] is True
    assert observed["token_digest"] == hashlib.sha256(credential._token.encode()).hexdigest()
    assert observed["context"]["run_id"] == "run-a"
    assert observed["context"]["attempt_id"] == "attempt-1"
    assert credential._token not in result.stdout + result.stderr
    assert "[worker token redacted]" in result.stdout
    assert credential._token not in repr(result)
    assert WORKER_TOKEN_ENV not in context.serialized
    assert auth.active_credential_count == 0


def test_worker_launch_rejects_credential_from_another_context_before_start(
    tmp_path: Path,
) -> None:
    auth = WorkerAuthenticator()
    credential = auth.issue("run-a", "attempt-1")
    context = WorkerContext.from_mapping({"run_id": "run-b", "attempt_id": "attempt-1"})
    script = tmp_path / "worker.py"
    script.write_text("raise SystemExit(99)\n", encoding="utf-8")

    with pytest.raises(ValueError, match="run_id"):
        run_python_worker(
            script,
            cwd=tmp_path,
            context=context,
            credential=credential,
            authenticator=auth,
        )
