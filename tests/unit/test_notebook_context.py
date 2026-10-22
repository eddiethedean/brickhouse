from __future__ import annotations

import sys
import tomllib
from pathlib import Path
from typing import Any

import nbformat
import pytest

from brickhouse.notebooks import (
    NotebookEnvironmentError,
    execute_ipynb,
)
from brickhouse.notebooks.runner import _REQUIRED_KERNEL_DISTRIBUTIONS
from brickhouse.runtime.worker_session import WorkerSparkSessionConfigurationError


def _execution_context(profile_name: str) -> dict[str, object]:
    if profile_name == "serverless":
        return {
            "compute_mode": "serverless",
            "profile": "serverless-local-v1",
            "runtime_profile": None,
            "access_mode": None,
            "environment_profile": (
                f"serverless-python{sys.version_info.major}{sys.version_info.minor}-local-v1"
            ),
            "environment_version": "1",
            "dependencies": ["deltalake==1.2.1", "pyarrow==23.0.1"],
            "effective_settings": {
                "spark.sql.ansi.enabled": "true",
                "spark.sql.session.timeZone": "UTC",
            },
        }
    if profile_name in {"classic-standard", "classic-dedicated"}:
        profile = (
            "classic-standard-local-v1"
            if profile_name == "classic-standard"
            else "classic-dedicated-local-v1"
        )
        access_mode = (
            "DATA_SECURITY_MODE_STANDARD"
            if profile_name == "classic-standard"
            else "DATA_SECURITY_MODE_DEDICATED"
        )
        return {
            "compute_mode": "classic",
            "profile": profile,
            "runtime_profile": "spark35-local-v1",
            "access_mode": access_mode,
            "environment_profile": None,
            "environment_version": None,
            "dependencies": [],
            "effective_settings": {
                "spark.sql.ansi.enabled": "false",
                "spark.sql.session.timeZone": "America/Los_Angeles",
            },
        }
    raise AssertionError(f"unknown profile test fixture: {profile_name}")


def _context(
    tmp_path: Path,
    suffix: str,
    profile_name: str,
) -> dict[str, object]:
    dbfs_root = tmp_path / f"dbfs-{suffix}"
    dbfs_root.mkdir()
    return {
        "run_id": f"notebook-{suffix}",
        "attempt_id": f"attempt-{suffix}",
        **_execution_context(profile_name),
        **(
            {
                "compute_id": f"cluster-{suffix}",
                "compute_generation": 7,
                "compute_state": "RUNNING",
                "compute_simulated": True,
            }
            if profile_name != "serverless"
            else {}
        ),
        "dbutils": {
            "dbfs_root": str(dbfs_root),
            "widget_parameters": {"widget_value": "run-now-wins"},
            "task_values_enabled": False,
        },
    }


def _save_notebook(path: Path, cells: list[Any]) -> Path:
    notebook = nbformat.v4.new_notebook(
        cells=cells,
        metadata={
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python"},
        },
    )
    nbformat.write(notebook, path)
    return path


def test_notebook_kernel_sparkless_pin_matches_core_dependency() -> None:
    project = tomllib.loads((Path(__file__).parents[2] / "pyproject.toml").read_text())
    runtime_dependencies = project["project"]["dependencies"]
    sparkless_pins = [item for item in runtime_dependencies if item.startswith("sparkless==")]

    assert sparkless_pins == [f"sparkless=={_REQUIRED_KERNEL_DISTRIBUTIONS['sparkless']}"]


@pytest.mark.parametrize(
    "profile_name",
    ("serverless", "classic-standard", "classic-dedicated"),
)
def test_profile_context_retains_cell_and_session_state_then_resets(
    tmp_path: Path,
    profile_name: str,
) -> None:
    first_source = _save_notebook(
        tmp_path / f"{profile_name}.ipynb",
        [
            nbformat.v4.new_code_cell(
                "batch_label = 'notebook-default'\n",
                metadata={"tags": ["parameters"]},
            ),
            nbformat.v4.new_code_cell(
                "dbutils.widgets.text('widget_value', 'notebook-widget-default')\n"
                "assert dbutils.widgets.get('widget_value') == 'run-now-wins'\n"
                "assert batch_label == 'job-parameter-override'\n"
                "cell_state = ['first']\n"
                "assert spark.conf.get('spark.sql.ansi.enabled') == EXPECTED_ANSI\n"
                "assert spark.conf.get('spark.sql.session.timeZone') == EXPECTED_TIMEZONE\n"
                "assert spark.conf.get('spark.sql.caseSensitive') == 'false'\n"
            ),
            nbformat.v4.new_code_cell(
                "cell_state.append('second')\n"
                "assert cell_state == ['first', 'second']\n"
                "spark.conf.set('spark.sql.caseSensitive', 'true')\n"
            ),
            nbformat.v4.new_code_cell(
                "assert cell_state == ['first', 'second']\n"
                "assert spark.conf.get('spark.sql.caseSensitive') == 'true'\n"
                "print('profile-kernel-state-complete')\n"
            ),
        ],
    )
    reset_source = _save_notebook(
        tmp_path / f"{profile_name}-reset.ipynb",
        [
            nbformat.v4.new_code_cell(
                "assert 'cell_state' not in globals()\n"
                "assert spark.conf.get('spark.sql.caseSensitive') == 'false'\n"
                "print('profile-kernel-reset-complete')\n"
            )
        ],
    )
    profile_context = _context(tmp_path, profile_name, profile_name)
    expected_ansi = "true" if profile_name == "serverless" else "false"
    expected_timezone = "UTC" if profile_name == "serverless" else "America/Los_Angeles"
    parameters = {
        "batch_label": "job-parameter-override",
        "EXPECTED_ANSI": expected_ansi,
        "EXPECTED_TIMEZONE": expected_timezone,
    }

    first = execute_ipynb(
        first_source,
        artifact_path=tmp_path / f"{profile_name}-executed.ipynb",
        worker_context=profile_context,
        parameters=parameters,
    )
    second = execute_ipynb(
        reset_source,
        artifact_path=tmp_path / f"{profile_name}-reset-output.ipynb",
        worker_context=profile_context,
    )

    assert first.kernel_id != second.kernel_id
    assert first.executed_code_cells == 4
    assert second.executed_code_cells == 1
    first_output = nbformat.read(first.artifact_path, as_version=4)
    second_output = nbformat.read(second.artifact_path, as_version=4)
    assert any(
        output.output_type == "stream" and "profile-kernel-state-complete" in output.text
        for cell in first_output.cells
        for output in cell.get("outputs", [])
    )
    assert any(
        output.output_type == "stream" and "profile-kernel-reset-complete" in output.text
        for cell in second_output.cells
        for output in cell.get("outputs", [])
    )


@pytest.mark.parametrize(
    ("profile_name", "invalid_field", "invalid_value", "error_type"),
    [
        (
            "serverless",
            "environment_profile",
            "serverless-python999-local-v1",
            NotebookEnvironmentError,
        ),
        (
            "serverless",
            "dependencies",
            ["uninstalled-package==1.0"],
            NotebookEnvironmentError,
        ),
        (
            "serverless",
            "dependencies",
            ["pyarrow==0.0.1"],
            NotebookEnvironmentError,
        ),
        (
            "serverless",
            "effective_settings",
            {"spark.executor.memory": "8g"},
            WorkerSparkSessionConfigurationError,
        ),
        (
            "classic-standard",
            "effective_settings",
            {"spark.sql.ansi.enabled": "false"},
            WorkerSparkSessionConfigurationError,
        ),
    ],
)
def test_invalid_environment_and_config_are_rejected_before_artifact_or_kernel(
    tmp_path: Path,
    profile_name: str,
    invalid_field: str,
    invalid_value: object,
    error_type: type[Exception],
) -> None:
    source = _save_notebook(
        tmp_path / "invalid-context.ipynb",
        [nbformat.v4.new_code_cell("raise AssertionError('kernel should not start')")],
    )
    context = _context(tmp_path, f"invalid-{invalid_field}", profile_name)
    context[invalid_field] = invalid_value
    artifact = tmp_path / "must-not-exist.ipynb"

    with pytest.raises(error_type):
        execute_ipynb(source, artifact_path=artifact, worker_context=context)

    assert not artifact.exists()
