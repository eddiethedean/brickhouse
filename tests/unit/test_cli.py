from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

import pytest

from brickhouse.workspace.ownership import (
    WorkspaceInUseError,
    acquire_workspace_owner,
)

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"


def run_module(
    *args: str,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(SRC)
    if extra_env is not None:
        env.update(extra_env)
    return subprocess.run(
        [sys.executable, "-m", "brickhouse", *args],
        cwd=ROOT,
        env=env,
        capture_output=True,
        check=False,
        text=True,
        timeout=10,
    )


def test_help_exits_without_starting_service() -> None:
    result = run_module("--help")

    assert result.returncode == 0
    assert "usage: brickhouse" in result.stdout
    assert "--version" in result.stdout
    assert "init" in result.stdout
    assert "serve" in result.stdout
    assert result.stderr == ""


def test_version_reports_release_version() -> None:
    result = run_module("--version")

    assert result.returncode == 0
    assert result.stdout == "brickhouse 0.1.0\n"
    assert result.stderr == ""


def test_import_does_not_load_optional_integrations() -> None:
    script = """
import builtins
blocked = {"databricks", "duckdb", "delta", "deltalake", "pyarrow", "pyspark", "mlflow", "fastapi"}
original_import = builtins.__import__
def guarded_import(name, *args, **kwargs):
    if name.split(".", 1)[0] in blocked:
        raise AssertionError(f"optional integration imported: {name}")
    return original_import(name, *args, **kwargs)
builtins.__import__ = guarded_import
import brickhouse
import brickhouse.tables
assert brickhouse.__version__ == "0.1.0"
assert brickhouse.Workspace is not None
assert brickhouse.tables.DeltaRsTableService is not None
"""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(SRC)
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        env=env,
        capture_output=True,
        check=False,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == ""


def test_init_creates_workspace_and_cli_capabilities_are_truthful(tmp_path: Path) -> None:
    workspace_path = tmp_path / "workspace"
    initialized = run_module("init", "--workspace", str(workspace_path))

    assert initialized.returncode == 0, initialized.stderr
    assert "(main.default)" in initialized.stdout
    assert (workspace_path / "metadata.sqlite").is_file()

    capabilities = run_module(
        "capabilities",
        "--format",
        "json",
        extra_env={
            "DATABRICKS_TOKEN": "brickhouse-test-token-sentinel",
            "DATABRICKS_HOST": "https://credentials-must-not-be-read.invalid",
        },
    )
    assert capabilities.returncode == 0, capabilities.stderr
    assert "brickhouse-test-token-sentinel" not in capabilities.stdout + capabilities.stderr
    report = json.loads(capabilities.stdout)
    assert report["capabilities"]["workspace_initialization"] == "supported"
    assert report["capabilities"]["catalog_metadata_repository"] == "partial_local_persistence"
    assert report["capabilities"]["table_registration_metadata"] == "partial_local_persistence"
    assert report["capabilities"]["catalog_api"] == "partial_local_rest_profile"
    assert report["capabilities"]["table_provider_integration"] == "partial_local_delta"
    assert report["capabilities"]["python_job_execution"] == "partial_local_sdk_python_script_dag"
    assert (
        report["capabilities"]["databricks_cli_bundle"]
        == "partial_local_classic_single_task_python"
    )
    assert report["capabilities"]["sdk_job_deletion"] == "soft_delete_with_history_retention"
    assert (
        report["capabilities"]["dbutils_task_values"]
        == "partial_local_attempt_scoped_python_script_jobs"
    )
    assert report["providers"]["sparkless"]["integrated"] is False
    assert report["providers"]["delta-rs"]["integrated"] is True

    doctor = run_module("doctor", "--workspace", str(workspace_path))
    assert doctor.returncode == 0, doctor.stderr
    diagnostics = json.loads(doctor.stdout)
    assert diagnostics["doctor"]["workspace_validated"] is True
    assert diagnostics["workspace"]["default_catalog"] == "main"
    assert diagnostics["workspace"]["metadata_schema_version"] == 21


def test_run_fails_explicitly_without_starting_script(tmp_path: Path) -> None:
    marker = tmp_path / "script-started"
    script = tmp_path / "job.py"
    script.write_text(
        f"from pathlib import Path; Path({str(marker)!r}).touch()\n", encoding="utf-8"
    )

    result = run_module("run", str(script))

    assert result.returncode == 2
    assert "no script was started" in result.stderr
    assert "B013" in result.stderr
    assert not marker.exists()


def test_diagnostic_server_rejects_nonloopback_bind(tmp_path: Path) -> None:
    workspace_path = tmp_path / "workspace"
    initialized = run_module("init", "--workspace", str(workspace_path))
    assert initialized.returncode == 0, initialized.stderr

    result = run_module(
        "serve",
        "--workspace",
        str(workspace_path),
        "--host",
        "0.0.0.0",
    )

    assert result.returncode == 2
    assert "must bind to IPv4 loopback" in result.stderr


def test_diagnostic_server_owns_workspace_until_sigterm_shutdown(tmp_path: Path) -> None:
    if os.name != "posix":
        pytest.skip("workspace ownership locking is currently POSIX-only")

    workspace_path = tmp_path / "workspace"
    initialized = run_module("init", "--workspace", str(workspace_path))
    assert initialized.returncode == 0, initialized.stderr
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]

    env = os.environ.copy()
    env["PYTHONPATH"] = str(SRC)
    server = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "brickhouse",
            "serve",
            "--workspace",
            str(workspace_path),
            "--port",
            str(port),
        ],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 5
        response: dict[str, object] | None = None
        while time.monotonic() < deadline and server.poll() is None:
            try:
                with urlopen(f"http://127.0.0.1:{port}/health", timeout=0.25) as result:
                    response = json.load(result)
                    break
            except (OSError, URLError):
                time.sleep(0.05)
        assert response == {"status": "ok", "workspace_format_version": 1}
        with pytest.raises(WorkspaceInUseError):
            acquire_workspace_owner(workspace_path)
        with urlopen(f"http://127.0.0.1:{port}/capabilities", timeout=1) as result:
            report = json.load(result)
        assert report["capabilities"]["catalog_api"] == "partial_local_rest_profile"

        server.send_signal(signal.SIGTERM)
        stdout, stderr = server.communicate(timeout=5)
        assert server.returncode == 0, stderr
        assert f"127.0.0.1:{port}" in stdout
        owner = acquire_workspace_owner(workspace_path)
        owner.release()
    finally:
        if server.poll() is None:
            server.kill()
            server.communicate(timeout=5)
