"""Opt-in conformance test for the pinned Terraform local resource profile."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Mapping
from pathlib import Path
from threading import Thread
from typing import cast

import pytest

from brickhouse.diagnostic_server import create_diagnostic_server
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace

TERRAFORM_VERSION = "1.16.6"
PROVIDER_VERSION = "1.137.0"
_LOCKFILE = (
    Path(__file__).parents[1] / "fixtures" / "terraform" / PROVIDER_VERSION / ".terraform.lock.hcl"
)


def test_terraform_v1_16_6_databricks_provider_v1_137_0_lifecycle(tmp_path: Path) -> None:
    terraform_value = os.environ.get("BRICKHOUSE_TERRAFORM")
    plugin_cache_value = os.environ.get("BRICKHOUSE_TERRAFORM_PLUGIN_CACHE")
    if not terraform_value or not plugin_cache_value:
        pytest.skip(
            "set BRICKHOUSE_TERRAFORM and BRICKHOUSE_TERRAFORM_PLUGIN_CACHE to run "
            "the pinned Terraform provider conformance test"
        )
    terraform = Path(terraform_value).expanduser().resolve()
    plugin_cache = Path(plugin_cache_value).expanduser().resolve()
    if not terraform.is_file() or not plugin_cache.is_dir():
        pytest.skip("the configured Terraform binary or provider cache is unavailable")

    root = tmp_path / "terraform-provider-profile"
    root.mkdir()
    workspace = initialize_workspace(root / "workspace")
    terraform_dir = root / "config"
    terraform_dir.mkdir()
    _write_terraform_configuration(terraform_dir)
    shutil.copyfile(_LOCKFILE, terraform_dir / ".terraform.lock.hcl")
    source_path = terraform_dir / "source.py"
    source_path.write_text(
        "import sys\nprint('TERRAFORM=' + sys.argv[-1])\n",
        encoding="utf-8",
    )

    with WorkspaceContext.local(workspace.path) as context:
        server = create_diagnostic_server(context, "127.0.0.1", 0)
        server_thread = Thread(
            target=server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )
        server_thread.start()
        cluster_id: str | None = None
        try:
            base_url = f"http://127.0.0.1:{server.server_port}"
            cluster_id = cast(
                str,
                _api_json(
                    base_url,
                    "POST",
                    "/api/2.1/clusters/create",
                    {
                        "cluster_name": "terraform-provider-fixture",
                        "spark_version": "spark35-local-v1",
                        "data_security_mode": "DATA_SECURITY_MODE_STANDARD",
                        "num_workers": 0,
                    },
                )["cluster_id"],
            )
            environment = _terraform_environment(terraform_dir, plugin_cache, base_url, cluster_id)
            version = json.loads(
                _run(terraform, terraform_dir, environment, "version", "-json").stdout
            )
            assert version["terraform_version"] == TERRAFORM_VERSION

            _run(terraform, terraform_dir, environment, "init", "-input=false", "-backend=false")
            _run(terraform, terraform_dir, environment, "validate")
            environment["TF_VAR_argument"] = "first"
            _run(terraform, terraform_dir, environment, "apply", "-auto-approve", "-input=false")
            state = json.loads(_run(terraform, terraform_dir, environment, "show", "-json").stdout)
            resources = {
                item["address"]: item["values"]
                for item in state["values"]["root_module"]["resources"]
            }
            assert set(resources) == {"databricks_job.python", "databricks_workspace_file.source"}
            job_id = int(resources["databricks_job.python"]["id"])

            _run(
                terraform,
                terraform_dir,
                environment,
                "apply",
                "-refresh-only",
                "-auto-approve",
                "-input=false",
            )
            no_drift = _run(
                terraform,
                terraform_dir,
                environment,
                "plan",
                "-detailed-exitcode",
                "-input=false",
                check=False,
            )
            assert no_drift.returncode == 0, no_drift.stdout + no_drift.stderr

            source_path.write_text(
                "import sys\nprint('TERRAFORM=' + sys.argv[-1])\nprint('REVISION=two')\n",
                encoding="utf-8",
            )
            environment["TF_VAR_argument"] = "second"
            _run(
                terraform,
                terraform_dir,
                environment,
                "apply",
                "-auto-approve",
                "-input=false",
            )
            _run(
                terraform,
                terraform_dir,
                environment,
                "apply",
                "-refresh-only",
                "-auto-approve",
                "-input=false",
            )

            failed_update_environment = dict(environment)
            failed_update_environment["TF_VAR_cluster_id"] = "missing-local-cluster"
            failed_update = _run(
                terraform,
                terraform_dir,
                failed_update_environment,
                "apply",
                "-auto-approve",
                "-input=false",
                check=False,
            )
            assert failed_update.returncode != 0
            assert "existing_cluster_id does not resolve to a local" in (
                failed_update.stdout + failed_update.stderr
            )
            current_job = _api_json(base_url, "GET", f"/api/2.0/jobs/get?job_id={job_id}")
            assert current_job["settings"]["existing_cluster_id"] == cluster_id
            no_drift_after_failure = _run(
                terraform,
                terraform_dir,
                environment,
                "plan",
                "-detailed-exitcode",
                "-input=false",
                check=False,
            )
            assert no_drift_after_failure.returncode == 0, (
                no_drift_after_failure.stdout + no_drift_after_failure.stderr
            )
            _run(
                terraform,
                terraform_dir,
                environment,
                "apply",
                "-refresh-only",
                "-auto-approve",
                "-input=false",
            )
            refreshed_state = json.loads(
                _run(terraform, terraform_dir, environment, "show", "-json").stdout
            )
            refreshed_resources = {
                item["address"]: item["values"]
                for item in refreshed_state["values"]["root_module"]["resources"]
            }
            assert refreshed_resources["databricks_job.python"]["existing_cluster_id"] == cluster_id

            accepted = _api_json(base_url, "POST", "/api/2.2/jobs/run-now", {"job_id": job_id})
            run_id = int(accepted["run_id"])
            for _ in range(200):
                run = _api_json(base_url, "GET", f"/api/2.2/jobs/runs/get?run_id={run_id}")
                if run["state"]["life_cycle_state"] not in {"PENDING", "QUEUED", "RUNNING"}:
                    break
                time.sleep(0.05)
            assert run["state"]["result_state"] == "SUCCESS"
            output = _api_json(
                base_url,
                "GET",
                f"/api/2.2/jobs/runs/get-output?run_id={run_id}",
            )
            assert output["logs"] == "TERRAFORM=second\nREVISION=two\n"

            _run(
                terraform,
                terraform_dir,
                environment,
                "destroy",
                "-auto-approve",
                "-input=false",
            )
            assert (
                _api_json_status(
                    base_url,
                    "GET",
                    "/api/2.0/jobs/get?job_id=" + str(job_id),
                )[0]
                == 404
            )
            assert (
                _api_json_status(
                    base_url,
                    "GET",
                    "/api/2.0/workspace/get-status?path=%2FShared%2Fbrickhouse-terraform.py",
                )[0]
                == 404
            )
        finally:
            try:
                _cleanup_terraform(
                    terraform,
                    terraform_dir,
                    _terraform_environment(
                        terraform_dir,
                        plugin_cache,
                        f"http://127.0.0.1:{server.server_port}",
                        cluster_id or "missing",
                    ),
                )
                if cluster_id is not None:
                    _api_json(
                        f"http://127.0.0.1:{server.server_port}",
                        "POST",
                        "/api/2.1/clusters/delete",
                        {"cluster_id": cluster_id},
                    )
                    _api_json(
                        f"http://127.0.0.1:{server.server_port}",
                        "POST",
                        "/api/2.1/clusters/permanent-delete",
                        {"cluster_id": cluster_id},
                    )
            finally:
                server.shutdown()
                server_thread.join(timeout=5)
                server.server_close()


def _write_terraform_configuration(directory: Path) -> None:
    (directory / "main.tf").write_text(
        """terraform {
  required_version = "= 1.16.6"
  required_providers {
    databricks = {
      source = "databricks/databricks"
      version = "= 1.137.0"
    }
  }
}

provider "databricks" {
  host = var.host
  token = var.token
}

variable "host" { type = string }
variable "token" {
  type = string
  sensitive = true
}
variable "cluster_id" { type = string }
variable "argument" { type = string }

resource "databricks_workspace_file" "source" {
  source = "${path.module}/source.py"
  path = "/Shared/brickhouse-terraform.py"
}

resource "databricks_job" "python" {
  name = "brickhouse terraform job"
  existing_cluster_id = var.cluster_id
  max_concurrent_runs = 1
  spark_python_task {
    python_file = "/Workspace/Shared/brickhouse-terraform.py"
    parameters = [var.argument]
  }
  depends_on = [databricks_workspace_file.source]
}
""",
        encoding="utf-8",
    )


def _terraform_environment(
    directory: Path,
    plugin_cache: Path,
    host: str,
    cluster_id: str,
) -> dict[str, str]:
    environment = {
        "PATH": os.defpath,
        "HOME": str(directory / "home"),
        "TF_PLUGIN_CACHE_DIR": str(plugin_cache),
        "TF_IN_AUTOMATION": "1",
        "TF_VAR_host": host,
        "TF_VAR_token": "brickhouse-local-test-token",
        "TF_VAR_cluster_id": cluster_id,
        "TF_VAR_argument": "first",
    }
    Path(environment["HOME"]).mkdir(parents=True, exist_ok=True)
    return environment


def _run(
    terraform: Path,
    directory: Path,
    environment: dict[str, str],
    *arguments: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [str(terraform), *arguments],
        cwd=directory,
        env=environment,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if check and result.returncode != 0:
        raise AssertionError(result.stdout + result.stderr)
    return result


def _cleanup_terraform(terraform: Path, directory: Path, environment: dict[str, str]) -> None:
    if not (directory / "terraform.tfstate").exists():
        return
    _run(terraform, directory, environment, "destroy", "-auto-approve", "-input=false", check=False)


def _api_json(
    base_url: str,
    method: str,
    path: str,
    body: Mapping[str, object] | None = None,
) -> dict[str, object]:
    status, payload = _api_json_status(base_url, method, path, body)
    if status != 200:
        raise AssertionError(f"local API returned {status}: {payload}")
    return cast(dict[str, object], payload)


def _api_json_status(
    base_url: str,
    method: str,
    path: str,
    body: Mapping[str, object] | None = None,
) -> tuple[int, object]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        base_url + path,
        data=data,
        headers={"Content-Type": "application/json"} if data is not None else {},
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            raw = response.read()
            return response.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as error:
        raw = error.read()
        return error.code, json.loads(raw) if raw else {}
