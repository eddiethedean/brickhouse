from __future__ import annotations

import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from brickhouse.runtime import (
    WorkerAuthenticationError,
    WorkerAuthenticator,
    WorkerCallMessage,
    WorkerContext,
    run_python_worker,
)
from brickhouse.tables import InputFormat, SchemaDescriptor, StagingArea
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace


def _staged_input(tmp_path: Path, context: WorkspaceContext, name: str):
    source = tmp_path / f"{name}.parquet"
    pq.write_table(pa.table({"id": pa.array([1], type=pa.int64())}), source)
    return StagingArea(context).stage_file(
        source,
        SchemaDescriptor(
            '{"type":"struct","fields":[{"name":"id","type":"long",'
            '"nullable":false,"metadata":{}}]}'
        ),
        InputFormat.PARQUET,
        operation_id=name,
        row_count=1,
    )


def _pid_running(pid: int) -> bool:
    if sys.platform.startswith("linux"):
        stat_file = Path(f"/proc/{pid}/stat")
        if not stat_file.exists():
            return False
        return stat_file.read_text().split()[2] != "Z"
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def test_timeout_kills_reaps_worker_and_persists_actual_exit_and_cleanup(
    tmp_path: Path,
) -> None:
    initialized = initialize_workspace(tmp_path / "workspace")
    script = tmp_path / "worker.py"
    script.write_text("import time\ntime.sleep(60)\n", encoding="utf-8")
    authenticator = WorkerAuthenticator()
    credential = authenticator.issue("run-timeout", "attempt-1")

    with WorkspaceContext.local(initialized.path) as context:
        staged = _staged_input(tmp_path, context, "timeout")
        staging = StagingArea(context)
        worker_context = WorkerContext.from_mapping(
            {"run_id": "run-timeout", "attempt_id": "attempt-1"}
        )
        result = run_python_worker(
            script,
            cwd=tmp_path,
            context=worker_context,
            credential=credential,
            authenticator=authenticator,
            timeout_seconds=0.15,
            termination_grace_seconds=0.05,
            staging_cleanup=lambda: staging.cleanup(staged),
            outcome_journal=context.worker_outcomes,
        )

        outcome = context.worker_outcomes.get("run-timeout", "attempt-1")
        assert result.termination_reason == "timeout"
        assert result.exit_code < 0
        assert staged.path.exists() is False
        assert authenticator.active_credential_count == 0
        assert outcome is not None
        assert outcome.state == "timed_out"
        assert outcome.termination_reason == "timeout"
        assert outcome.exit_code == result.exit_code
        assert outcome.finished_at is not None
        assert outcome.cleanup_error_type is None
        assert outcome.worker_pid is not None and outcome.worker_pid > 0


@pytest.mark.skipif(os.name != "posix", reason="self-SIGKILL crash check is POSIX-scoped")
def test_crashed_worker_exit_status_is_persisted(tmp_path: Path) -> None:
    initialized = initialize_workspace(tmp_path / "workspace")
    script = tmp_path / "crash.py"
    script.write_text("import os, signal\nos.kill(os.getpid(), signal.SIGKILL)\n", encoding="utf-8")
    worker_context = WorkerContext.from_mapping({"run_id": "run-crash", "attempt_id": "attempt-1"})

    with WorkspaceContext.local(initialized.path) as context:
        result = run_python_worker(
            script,
            cwd=tmp_path,
            context=worker_context,
            outcome_journal=context.worker_outcomes,
        )
        outcome = context.worker_outcomes.get("run-crash", "attempt-1")
        assert result.exit_code == -signal.SIGKILL
        assert outcome is not None
        assert outcome.state == "failed"
        assert outcome.exit_code == result.exit_code
        assert outcome.worker_pid is not None and outcome.worker_pid > 0


def test_attempts_do_not_share_environment_import_or_context_state(tmp_path: Path) -> None:
    script = tmp_path / "isolated.py"
    script.write_text(
        "import json, os, sys\n"
        "assert os.environ.get('BRICKHOUSE_PRIVATE_ATTEMPT_STATE') is None\n"
        "assert 'brickhouse_attempt_leak_probe' not in sys.modules\n"
        "context = json.loads(os.environ['BRICKHOUSE_RUN_CONTEXT_JSON'])\n"
        "print(json.dumps(context, sort_keys=True))\n"
        "os.environ['BRICKHOUSE_PRIVATE_ATTEMPT_STATE'] = context['attempt_id']\n"
        "sys.modules['brickhouse_attempt_leak_probe'] = object()\n",
        encoding="utf-8",
    )

    first = run_python_worker(
        script,
        cwd=tmp_path,
        context=WorkerContext.from_mapping({"run_id": "run-isolation", "attempt_id": "attempt-1"}),
    )
    second = run_python_worker(
        script,
        cwd=tmp_path,
        context=WorkerContext.from_mapping({"run_id": "run-isolation", "attempt_id": "attempt-2"}),
    )

    assert first.exit_code == second.exit_code == 0
    assert '"attempt_id": "attempt-1"' in first.stdout
    assert '"attempt_id": "attempt-2"' in second.stdout


@pytest.mark.skipif(os.name != "posix", reason="process-group descendant cleanup is POSIX-scoped")
def test_cancellation_kills_descendants_revokes_token_and_reopens_outcome(
    tmp_path: Path,
) -> None:
    initialized = initialize_workspace(tmp_path / "workspace")
    script = tmp_path / "worker.py"
    child_pid_file = tmp_path / "descendant.pid"
    descendant_ready = tmp_path / "descendant.ready"
    descendant_stopped = tmp_path / "descendant.stopped"
    descendant_code = (
        "import pathlib, signal, time\n"
        f"ready = pathlib.Path({str(descendant_ready)!r})\n"
        f"stopped = pathlib.Path({str(descendant_stopped)!r})\n"
        "def stop(*_):\n"
        "    stopped.write_text('stopped')\n"
        "    raise SystemExit(0)\n"
        "signal.signal(signal.SIGTERM, stop)\n"
        "ready.write_text('ready')\n"
        "while True: time.sleep(1)\n"
    )
    script.write_text(
        "import pathlib, signal, subprocess, sys, time\n"
        f"child = subprocess.Popen([sys.executable, '-c', {descendant_code!r}])\n"
        f"pathlib.Path({str(child_pid_file)!r}).write_text(str(child.pid))\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "print('worker-ready', flush=True)\n"
        "while True: time.sleep(1)\n",
        encoding="utf-8",
    )
    authenticator = WorkerAuthenticator()
    credential = authenticator.issue("run-cancel", "attempt-1")
    cancel_event = threading.Event()

    with WorkspaceContext.local(initialized.path) as context:
        staged = _staged_input(tmp_path, context, "cancel")
        staging = StagingArea(context)
        worker_context = WorkerContext.from_mapping(
            {"run_id": "run-cancel", "attempt_id": "attempt-1"}
        )
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                run_python_worker,
                script,
                cwd=tmp_path,
                context=worker_context,
                credential=credential,
                authenticator=authenticator,
                timeout_seconds=10,
                cancel_event=cancel_event,
                termination_grace_seconds=0.1,
                staging_cleanup=lambda: staging.cleanup(staged),
                outcome_journal=context.worker_outcomes,
            )
            deadline = time.monotonic() + 5
            while not child_pid_file.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            assert child_pid_file.exists(), "worker did not start its descendant"
            while not descendant_ready.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            assert descendant_ready.exists(), "descendant did not install its signal handler"
            child_pid = int(child_pid_file.read_text())
            cancel_event.set()
            result = future.result(timeout=5)

        assert result.termination_reason == "cancelled"
        assert result.exit_code < 0
        assert "worker-ready" in result.stdout
        assert descendant_stopped.read_text() == "stopped"
        deadline = time.monotonic() + 3
        while _pid_running(child_pid) and time.monotonic() < deadline:
            time.sleep(0.02)
        assert not _pid_running(child_pid)
        assert not staged.path.exists()
        assert authenticator.active_credential_count == 0
        with pytest.raises(WorkerAuthenticationError, match="revoked"):
            authenticator.authorize(
                credential.authorization_header,
                WorkerCallMessage(
                    1,
                    "run-cancel",
                    "attempt-1",
                    "request-1",
                    "tables",
                    "read",
                    {},
                ),
            )

    # Reopening the workspace preserves the observed terminal result.
    with WorkspaceContext.local(initialized.path) as reopened:
        outcome = reopened.worker_outcomes.get("run-cancel", "attempt-1")
        assert outcome is not None
        assert outcome.state == "cancelled"
        assert outcome.exit_code == result.exit_code
        assert outcome.cleanup_error_type is None
