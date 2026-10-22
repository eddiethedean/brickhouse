from __future__ import annotations

import json
import os
import shutil
import subprocess
import urllib.error
import urllib.request
from pathlib import Path
from threading import Thread
from typing import Any

import pytest

from brickhouse.compute.models import ComputeResourceKind, ComputeState
from brickhouse.diagnostic_server import create_diagnostic_server
from brickhouse.jobs.repository import JobNotFoundError
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace

_CLI_VERSION = "Databricks CLI v1.21.0"
_LOCAL_TOKEN = "brickhouse-local-bundle-fixture-token"


@pytest.mark.skipif(
    not os.environ.get("DATABRICKS_CLI"),
    reason="set DATABRICKS_CLI to the pinned Databricks CLI v1.21.0 executable",
)
def test_cli_bundle_v1_21_0_validate_deploy_and_run_local_python_job(tmp_path: Path) -> None:
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

    project = tmp_path / "project"
    source_directory = project / "src"
    source_directory.mkdir(parents=True)
    source = source_directory / "echo.py"
    source.write_text(
        "import json, os\n"
        "context = json.loads(os.environ['BRICKHOUSE_RUN_CONTEXT_JSON'])\n"
        "print('BUNDLE_LOCAL_OK')\n"
        "print('SIMULATED_COMPUTE=' + str(context.get('compute_simulated')))\n",
        encoding="utf-8",
    )
    (project / "databricks.yml").write_text(
        """bundle:
  name: brickhouse_b0203_local
sync:
  paths:
    - src
targets:
  dev:
    default: true
    mode: development
resources:
  jobs:
    smoke:
      name: brickhouse-b0203-local
      tasks:
        - task_key: smoke
          spark_python_task:
            python_file: ./src/echo.py
            parameters: []
          new_cluster:
            spark_version: spark35-local-v1
            data_security_mode: DATA_SECURITY_MODE_STANDARD
            num_workers: 1
""",
        encoding="utf-8",
    )

    descriptor = initialize_workspace(tmp_path / "workspace")
    config_file = tmp_path / "databrickscfg"
    config_file.write_text("", encoding="utf-8")
    home = tmp_path / "home"
    home.mkdir()

    with WorkspaceContext.local(descriptor.path) as context:
        server = create_diagnostic_server(context, "127.0.0.1", 0)
        requests: list[tuple[str, str]] = []
        handler = server.RequestHandlerClass
        original_get = handler.do_GET
        original_mutation = handler._dispatch_mutation
        original_send = handler._send_payload

        def record_get(instance: Any) -> None:
            requests.append(("GET", instance.path))
            original_get(instance)

        def record_mutation(instance: Any, method: str) -> None:
            requests.append((method, instance.path))
            original_mutation(instance, method)

        def record_response(instance: Any, response: Any) -> None:
            requests.append((f"HTTP {response.status}", instance.path))
            original_send(instance, response)

        handler.do_GET = record_get
        handler._dispatch_mutation = record_mutation
        handler._send_payload = record_response
        thread = Thread(
            target=server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )
        thread.start()
        host = f"http://127.0.0.1:{server.server_port}"
        environment = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": str(home),
            "TMPDIR": str(tmp_path),
            "DATABRICKS_HOST": host,
            "DATABRICKS_TOKEN": _LOCAL_TOKEN,
            "DATABRICKS_CONFIG_FILE": str(config_file),
        }

        def invoke(*arguments: str) -> subprocess.CompletedProcess[str]:
            result = subprocess.run(
                [str(cli), *arguments],
                cwd=project,
                env=environment,
                check=False,
                capture_output=True,
                text=True,
                timeout=90,
            )
            assert _LOCAL_TOKEN not in result.stdout + result.stderr
            assert result.returncode == 0, result.stderr
            return result

        try:
            validation = invoke("bundle", "validate", "-o", "json")
            validated = json.loads(validation.stdout)
            assert validated["workspace"]["current_user"]["id"] == "brickhouse-local-user"
            assert "spark35-local-v1" in validation.stdout

            deployed = invoke("bundle", "deploy", "--auto-approve", "-o", "json")
            assert "Created jobs.smoke" in deployed.stderr

            run_result = invoke("bundle", "run", "smoke", "-o", "json")
            outputs = json.loads(run_result.stdout)["task_outputs"]
            assert len(outputs) == 1
            output = outputs[0]["Output"]["logs"]
            assert "BUNDLE_LOCAL_OK" in output
            assert "SIMULATED_COMPUTE=True" in output

            jobs = context.jobs.list_jobs()
            assert len(jobs) == 1
            job = jobs[0]
            assert job.settings["format"] == "MULTI_TASK"
            assert job.settings["deployment"]["kind"] == "BUNDLE"
            runs = context.jobs.list_runs(job.job_id)
            assert len(runs) == 1
            consumer = context.compute.get_run_consumer(runs[0].run_id, "smoke")
            assert consumer is not None and consumer.state == "released"
            compute = context.compute.get_resource(consumer.compute_id)
            assert compute.resource_kind is ComputeResourceKind.JOB_NEW
            assert compute.state is ComputeState.TERMINATED
            assert compute.is_simulated is True

            unsupported_task = _post_json(
                host,
                "/api/2.2/jobs/create",
                {
                    "name": "unsupported-notebook-task",
                    "tasks": [
                        {
                            "task_key": "unsupported",
                            "spark_jar_task": {"main_class_name": "example.Main"},
                            "new_cluster": {
                                "spark_version": "spark35-local-v1",
                                "data_security_mode": "DATA_SECURITY_MODE_STANDARD",
                                "num_workers": 1,
                            },
                        }
                    ],
                },
            )
            assert unsupported_task[0] == 400
            assert unsupported_task[1]["message"] == "task type spark_jar_task is unsupported"

            unsupported_resource = _post_json(host, "/api/2.0/pipelines", {})
            assert unsupported_resource[0] == 400
            assert "pipelines resources are unsupported" in unsupported_resource[1]["message"]

            destroy = subprocess.run(
                [str(cli), "bundle", "destroy", "--auto-approve", "-o", "json"],
                cwd=project,
                env=environment,
                check=False,
                capture_output=True,
                text=True,
                timeout=90,
            )
            assert _LOCAL_TOKEN not in destroy.stdout + destroy.stderr
            assert destroy.returncode == 0, destroy.stderr
            assert "Destroy: 1 deleted" in destroy.stderr
            assert ("HTTP 200", "/api/2.2/jobs/delete") in requests
            with pytest.raises(JobNotFoundError):
                context.jobs.get_active_job(job.job_id)
            assert context.jobs.get_job(job.job_id).definition_sha256 == job.definition_sha256
            assert context.jobs.get_run(runs[0].run_id).state.value == "succeeded"
            assert not (
                descriptor.path
                / "files"
                / "Users"
                / "brickhouse-local@invalid"
                / ".bundle"
                / "brickhouse_b0203_local"
                / "dev"
            ).exists()
        finally:
            server.shutdown()
            thread.join(timeout=5)
            server.server_close()

    shutil.rmtree(descriptor.path)
    assert not descriptor.path.exists()


def _post_json(host: str, path: str, payload: dict[str, object]) -> tuple[int, dict[str, Any]]:
    request = urllib.request.Request(
        f"{host}{path}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read())
