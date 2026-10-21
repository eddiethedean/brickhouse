from __future__ import annotations

import json
from pathlib import Path

import pytest

from brickhouse.runtime import (
    SecretConfigurationError,
    SecretProvider,
    WorkerContext,
    run_python_worker,
)
from brickhouse.runtime.secrets import decode_worker_secret_payload


def test_secret_provider_snapshots_only_job_granted_scopes() -> None:
    provider = SecretProvider()
    provider.put("approved", "api-key", "synthetic-secret-value")
    provider.put("approved", "binary", b"\x00\xff")
    provider.put("unrelated", "hidden", "unrelated-secret")
    provider.grant_job(7, "approved")

    snapshot = provider.snapshot_for_job(7)
    decoded = decode_worker_secret_payload(snapshot.payload)
    assert snapshot.scope_names == ("approved",)
    assert decoded == {"approved": {"api-key": b"synthetic-secret-value", "binary": b"\x00\xff"}}
    assert b"unrelated-secret" not in snapshot.payload
    assert snapshot.redaction_values == (b"synthetic-secret-value", b"\x00\xff")
    assert decode_worker_secret_payload(provider.snapshot_for_job(8).payload) == {}


def test_secret_provider_rejects_invalid_and_oversized_values() -> None:
    provider = SecretProvider()
    with pytest.raises(SecretConfigurationError, match="scope is invalid"):
        provider.put("../secret", "key", "value")
    with pytest.raises(SecretConfigurationError, match="cannot exceed"):
        provider.put("scope", "key", "x" * 4097)
    with pytest.raises(SecretConfigurationError, match="positive integer"):
        provider.grant_job(0, "scope")


def test_worker_reads_secret_payload_from_private_stdin_and_redacts_output(tmp_path: Path) -> None:
    provider = SecretProvider()
    secret = "synthetic-worker-secret"
    provider.put("approved", "token", secret)
    provider.grant_job(12, "approved")
    snapshot = provider.snapshot_for_job(12)
    script = tmp_path / "read_secret.py"
    script.write_text(
        "import os\n"
        "print('SCOPES=' + ','.join(scope.name for scope in dbutils.secrets.listScopes()))\n"
        "print('SECRET=' + dbutils.secrets.get('approved', 'token'))\n"
        "print('CONTEXT=' + os.environ['BRICKHOUSE_RUN_CONTEXT_JSON'])\n",
        encoding="utf-8",
    )
    context = WorkerContext.from_mapping(
        {
            "run_id": "12",
            "attempt_id": "attempt-12",
            "dbutils": {
                "dbfs_root": str(tmp_path / "dbfs"),
                "widget_parameters": {},
            },
        }
    )

    result = run_python_worker(
        script,
        cwd=tmp_path,
        context=context,
        secret_payload=snapshot.payload,
        secret_redactions=snapshot.redaction_values,
    )

    assert result.exit_code == 0
    assert f"SECRET={'*' * len(secret)}" in result.stdout
    assert secret not in result.stdout + result.stderr + context.serialized
    assert "SCOPES=approved" in result.stdout
    assert not result.stdout_truncated


def test_secret_values_are_not_added_to_immutable_worker_context() -> None:
    provider = SecretProvider()
    secret = "never-in-context"
    provider.put("scope", "token", secret)
    provider.grant_job(3, "scope")
    context = WorkerContext.from_mapping(
        {
            "run_id": "3",
            "attempt_id": "attempt-3",
            "dbutils": {"dbfs_root": "/tmp/workspace/files/dbfs", "widget_parameters": {}},
        }
    )
    assert secret not in json.dumps(context.to_mapping())
    provider.revoke_job(3)
    assert decode_worker_secret_payload(provider.snapshot_for_job(3).payload) == {}
    provider.clear()
    assert provider.snapshot_for_job(3).scope_names == ()
