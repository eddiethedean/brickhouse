from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from threading import Thread
from typing import Any

import pytest

from brickhouse.diagnostic_server import create_diagnostic_server
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace

_CLI_VERSION = "Databricks CLI v1.21.0"
_LOCAL_TOKEN = "brickhouse-local-cli-fixture-token"


@pytest.mark.skipif(
    not os.environ.get("DATABRICKS_CLI"),
    reason="set DATABRICKS_CLI to the pinned Databricks CLI v1.21.0 executable",
)
def test_cli_v1_21_0_workspace_file_and_python_job_flow() -> None:
    cli = Path(os.environ["DATABRICKS_CLI"]).expanduser().resolve()
    assert cli.is_file(), f"DATABRICKS_CLI does not name a file: {cli}"
    version = subprocess.run(
        [str(cli), "version"],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.strip()
    assert version == _CLI_VERSION

    with tempfile.TemporaryDirectory(prefix="brickhouse-cli-v1-21-0-") as temporary:
        root = Path(temporary)
        home = root / "home"
        home.mkdir()
        config_file = root / "databrickscfg"
        config_file.write_text("", encoding="utf-8")
        source_file = root / "echo.py"
        source_file.write_text("print('CLI_V1_21_0_LOCAL_OK')\n", encoding="utf-8")
        job_file = root / "job.json"

        descriptor = initialize_workspace(root / "workspace")
        with WorkspaceContext.local(descriptor.path) as context:
            server = create_diagnostic_server(context, "127.0.0.1", 0)
            thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
            thread.start()
            environment = {
                "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
                "HOME": str(home),
                "TMPDIR": str(root),
                "DATABRICKS_HOST": f"http://127.0.0.1:{server.server_port}",
                "DATABRICKS_TOKEN": _LOCAL_TOKEN,
                "DATABRICKS_CONFIG_FILE": str(config_file),
            }

            def invoke(*arguments: str, succeeds: bool = True) -> subprocess.CompletedProcess[str]:
                result = subprocess.run(
                    [str(cli), *arguments],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=40,
                    env=environment,
                )
                if succeeds:
                    assert result.returncode == 0, result.stderr
                else:
                    assert result.returncode != 0, result.stdout
                assert _LOCAL_TOKEN not in result.stdout + result.stderr
                return result

            try:
                imported_path = "/Workspace/Shared/cli-v1-21-0-echo.py"
                invoke(
                    "workspace",
                    "import",
                    imported_path,
                    "--file",
                    str(source_file),
                    "--format",
                    "SOURCE",
                    "--language",
                    "PYTHON",
                    "-o",
                    "json",
                )

                duplicate_import = invoke(
                    "workspace",
                    "import",
                    imported_path,
                    "--file",
                    str(source_file),
                    "--format",
                    "SOURCE",
                    "--language",
                    "PYTHON",
                    "-o",
                    "json",
                    succeeds=False,
                )
                assert "already exists" in duplicate_import.stderr.casefold()

                missing_source_job = {
                    "name": "cli-v1-21-0-missing-source",
                    "tasks": [
                        {
                            "task_key": "missing",
                            "spark_python_task": {
                                "python_file": "/Workspace/Shared/not-present.py",
                                "parameters": [],
                            },
                            "new_cluster": {
                                "spark_version": "spark35-local-v1",
                                "data_security_mode": "DATA_SECURITY_MODE_STANDARD",
                                "num_workers": 0,
                            },
                        }
                    ],
                }
                job_file.write_text(json.dumps(missing_source_job), encoding="utf-8")
                missing_job = invoke(
                    "jobs",
                    "create",
                    "--json",
                    f"@{job_file}",
                    "-o",
                    "json",
                    succeeds=False,
                )
                assert "source does not exist" in missing_job.stderr.casefold()

                job = {
                    "name": "cli-v1-21-0-local-python",
                    "tasks": [
                        {
                            "task_key": "echo",
                            "spark_python_task": {
                                "python_file": imported_path,
                                "parameters": [],
                            },
                            "new_cluster": {
                                "spark_version": "spark35-local-v1",
                                "data_security_mode": "DATA_SECURITY_MODE_STANDARD",
                                "num_workers": 0,
                            },
                        }
                    ],
                }
                job_file.write_text(json.dumps(job), encoding="utf-8")
                created = invoke("jobs", "create", "--json", f"@{job_file}", "-o", "json")
                job_id = _json_object(created.stdout)["job_id"]
                assert isinstance(job_id, int)

                submitted = invoke(
                    "jobs",
                    "run-now",
                    str(job_id),
                    "--timeout",
                    "30s",
                    "-o",
                    "json",
                )
                run = _json_object(submitted.stdout)
                run_id = run["run_id"]
                assert isinstance(run_id, int)
                assert run["state"]["life_cycle_state"] == "TERMINATED"
                assert run["state"]["result_state"] == "SUCCESS"

                status = invoke("jobs", "get-run", str(run_id), "-o", "json")
                assert _json_object(status.stdout)["state"]["result_state"] == "SUCCESS"

                output = invoke("jobs", "get-run-output", str(run_id), "-o", "json")
                assert "CLI_V1_21_0_LOCAL_OK" in _json_object(output.stdout)["logs"]
            finally:
                server.shutdown()
                thread.join(timeout=5)
                server.server_close()

        workspace_root = root / "workspace"
        shutil.rmtree(workspace_root)
        assert not workspace_root.exists()


def _json_object(raw: str) -> dict[str, Any]:
    decoded: object = json.loads(raw)
    assert isinstance(decoded, dict)
    return decoded
