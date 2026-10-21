from __future__ import annotations

import time
from pathlib import Path
from typing import cast

import pytest

from brickhouse.api.jobs import JobsHttpApi
from brickhouse.jobs.terraform import (
    TerraformJobNotFoundError,
    TerraformJobRegistry,
)
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.files import WorkspaceFileService
from brickhouse.workspace.lifecycle import initialize_workspace


def test_terraform_job_v2_lifecycle_keeps_stable_id_and_versions_definitions(
    tmp_path: Path,
) -> None:
    descriptor = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(descriptor.path) as context:
        cluster = context.compute.create_cluster(
            {
                "cluster_name": "terraform-job-test",
                "spark_version": "spark35-local-v1",
                "data_security_mode": "DATA_SECURITY_MODE_STANDARD",
                "num_workers": 0,
            }
        )
        WorkspaceFileService(context).import_python(
            "/Workspace/Shared/terraform-job.py",
            b"import sys, time\n"
            b"if sys.argv[-1] == 'slow': time.sleep(2)\n"
            b"print('PARAM=' + sys.argv[-1])\n",
        )
        api = JobsHttpApi(context)
        registry = TerraformJobRegistry(context)
        settings = _settings(cluster.compute_id, "first")

        created = api.dispatch("POST", "/api/2.0/jobs/create", body=settings)
        assert created is not None and created.status == 200
        external_id = cast(dict[str, object], created.payload)["job_id"]
        assert isinstance(external_id, int)
        created_resource = registry.get(external_id)
        assert created_resource.job_id == external_id
        assert created_resource.definition_job_id == external_id
        assert created_resource.revision == 1

        read = api.dispatch("GET", f"/api/2.0/jobs/get?job_id={external_id}")
        assert read is not None and read.status == 200
        read_settings = cast(dict[str, object], read.payload)["settings"]
        assert read_settings == settings

        updated_settings = _settings(cluster.compute_id, "second")
        updated = api.dispatch(
            "POST",
            "/api/2.0/jobs/reset",
            body={"job_id": external_id, "new_settings": updated_settings},
        )
        assert updated is not None and updated.status == 200
        active_resource = registry.get(external_id)
        assert active_resource.job_id == external_id
        assert active_resource.definition_job_id != external_id
        assert active_resource.revision == 2
        sdk_read = api.dispatch("GET", f"/api/2.2/jobs/get?job_id={external_id}")
        assert sdk_read is not None and sdk_read.status == 200
        sdk_settings = cast(dict[str, object], sdk_read.payload)["settings"]
        assert isinstance(sdk_settings, dict)
        sdk_tasks = sdk_settings["tasks"]
        assert isinstance(sdk_tasks, list)
        assert sdk_tasks[0]["spark_python_task"]["parameters"] == ["second"]

        accepted = api.dispatch("POST", "/api/2.2/jobs/run-now", body={"job_id": external_id})
        assert accepted is not None and accepted.status == 200
        run_id = cast(dict[str, object], accepted.payload)["run_id"]
        assert isinstance(run_id, int)
        for _ in range(200):
            status = api.dispatch("GET", f"/api/2.2/jobs/runs/get?run_id={run_id}")
            assert status is not None and status.status == 200
            state = cast(dict[str, object], status.payload)["state"]
            assert isinstance(state, dict)
            if state.get("life_cycle_state") not in {"PENDING", "QUEUED", "RUNNING"}:
                break
            time.sleep(0.01)
        assert state["result_state"] == "SUCCESS"
        output = api.dispatch("GET", f"/api/2.2/jobs/runs/get-output?run_id={run_id}")
        assert output is not None and output.status == 200
        assert cast(dict[str, object], output.payload)["logs"] == "PARAM=second\n"

        active_run = api.dispatch(
            "POST",
            "/api/2.2/jobs/run-now",
            body={"job_id": external_id, "python_params": ["slow"]},
        )
        assert active_run is not None and active_run.status == 200
        active_run_id = cast(dict[str, object], active_run.payload)["run_id"]
        assert isinstance(active_run_id, int)
        delete_conflict = api.dispatch("POST", "/api/2.2/jobs/delete", body={"job_id": external_id})
        assert delete_conflict is not None and delete_conflict.status == 409
        for _ in range(300):
            active_status = api.dispatch("GET", f"/api/2.2/jobs/runs/get?run_id={active_run_id}")
            assert active_status is not None and active_status.status == 200
            active_state = cast(dict[str, object], active_status.payload)["state"]
            assert isinstance(active_state, dict)
            if active_state.get("life_cycle_state") not in {
                "PENDING",
                "QUEUED",
                "RUNNING",
            }:
                break
            time.sleep(0.01)
        assert active_state["result_state"] == "SUCCESS"

        deleted = api.dispatch("POST", "/api/2.2/jobs/delete", body={"job_id": external_id})
        assert deleted is not None and deleted.status == 200
        missing_sdk_job = api.dispatch("GET", f"/api/2.2/jobs/get?job_id={external_id}")
        assert missing_sdk_job is not None and missing_sdk_job.status == 404
        missing_terraform_job = api.dispatch("GET", f"/api/2.0/jobs/get?job_id={external_id}")
        assert missing_terraform_job is not None and missing_terraform_job.status == 404
        with pytest.raises(TerraformJobNotFoundError):
            registry.get(external_id)
        assert context.jobs.get_job(external_id).name == "terraform job"
        assert context.jobs.get_job(active_resource.definition_job_id).name == "terraform job"
        api.close()


def test_terraform_job_routes_reject_unknown_settings(tmp_path: Path) -> None:
    descriptor = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(descriptor.path) as context:
        api = JobsHttpApi(context)
        response = api.dispatch(
            "POST",
            "/api/2.0/jobs/create",
            body={"name": "unsupported", "notebook_task": {"notebook_path": "/n"}},
        )
        assert response is not None and response.status == 400
        payload = cast(dict[str, object], response.payload)
        assert "does not implement" in str(payload["message"])
        api.close()


def _settings(cluster_id: str, parameter: str) -> dict[str, object]:
    return {
        "name": "terraform job",
        "existing_cluster_id": cluster_id,
        "max_concurrent_runs": 1,
        "spark_python_task": {
            "python_file": "/Workspace/Shared/terraform-job.py",
            "parameters": [parameter],
        },
    }
