from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import nbformat
import pytest

from brickhouse.notebooks import NotebookExecutionError, execute_ipynb


def _context(tmp_path: Path, suffix: str = "run") -> dict[str, object]:
    dbfs_root = tmp_path / f"dbfs-{suffix}"
    dbfs_root.mkdir()
    return {
        "run_id": f"notebook-{suffix}",
        "attempt_id": f"attempt-{suffix}",
        "execution_context": {
            "compute_mode": "serverless",
            "profile": "serverless-local-v1",
            "runtime_profile": None,
            "access_mode": None,
            "environment_profile": (
                f"serverless-python{sys.version_info.major}{sys.version_info.minor}-local-v1"
            ),
            "environment_version": "1",
            "dependencies": [],
            "effective_settings": {
                "spark.sql.ansi.enabled": "true",
                "spark.sql.session.timeZone": "UTC",
            },
        },
        "dbutils": {
            "dbfs_root": str(dbfs_root),
            "widget_parameters": {"greeting": "hello from job"},
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


def test_execute_ipynb_in_fresh_kernel_injects_parameters_spark_and_dbutils(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABRICKS_TOKEN", "parent-process-sentinel")
    notebook_path = _save_notebook(
        tmp_path / "source.ipynb",
        [
            nbformat.v4.new_code_cell(
                "greeting = 'default'\nunused_default = True\n",
                metadata={"tags": ["parameters"]},
            ),
            nbformat.v4.new_code_cell(
                "import os\n"
                "from IPython.display import display\n"
                "assert greeting == 'overridden by caller'\n"
                "assert 'DATABRICKS_TOKEN' not in os.environ\n"
                "dbutils.widgets.text('greeting', 'widget default')\n"
                "assert dbutils.widgets.get('greeting') == 'hello from job'\n"
                "assert spark.createDataFrame([(1,), (2,)], ['value']).count() == 2\n"
                "assert dbutils.fs.mkdirs('dbfs:/notes')\n"
                "assert dbutils.fs.put('dbfs:/notes/message.txt', greeting, overwrite=True)\n"
                "assert dbutils.fs.head('dbfs:/notes/message.txt') == greeting\n"
                "print('kernel-output:' + greeting)\n"
                "display({'result': greeting})\n"
            ),
        ],
    )
    artifact_path = tmp_path / "artifacts" / "executed.ipynb"

    result = execute_ipynb(
        notebook_path,
        artifact_path=artifact_path,
        worker_context=_context(tmp_path),
        parameters={"greeting": "overridden by caller"},
    )

    assert result.artifact_path == artifact_path
    assert result.executed_code_cells == 2
    executed = nbformat.read(artifact_path, as_version=4)
    assert len(executed.cells) == 3
    outputs = executed.cells[-1].outputs
    assert any(
        output.output_type == "stream" and "kernel-output:" in output.text for output in outputs
    )
    assert any(output.output_type == "display_data" for output in outputs)
    assert (tmp_path / "dbfs-run" / "notes" / "message.txt").read_text() == "overridden by caller"


def test_execute_ipynb_uses_a_new_kernel_for_every_invocation(tmp_path: Path) -> None:
    first_path = _save_notebook(
        tmp_path / "first.ipynb",
        [nbformat.v4.new_code_cell("only_in_first_kernel = 'private state'\nprint('first')")],
    )
    second_path = _save_notebook(
        tmp_path / "second.ipynb",
        [
            nbformat.v4.new_code_cell(
                "assert 'only_in_first_kernel' not in globals()\nprint('second')"
            )
        ],
    )

    first = execute_ipynb(
        first_path,
        artifact_path=tmp_path / "first-output.ipynb",
        worker_context=_context(tmp_path, "first"),
    )
    second = execute_ipynb(
        second_path,
        artifact_path=tmp_path / "second-output.ipynb",
        worker_context=_context(tmp_path, "second"),
    )

    assert first.kernel_id != second.kernel_id
    assert "first" in nbformat.read(first.artifact_path, as_version=4).cells[0].outputs[0].text
    assert "second" in nbformat.read(second.artifact_path, as_version=4).cells[0].outputs[0].text


def test_execute_ipynb_propagates_cell_errors_and_saves_failure_artifact(tmp_path: Path) -> None:
    notebook_path = _save_notebook(
        tmp_path / "failure.ipynb",
        [nbformat.v4.new_code_cell("raise ValueError('fixture failure')")],
    )
    artifact_path = tmp_path / "failure-output.ipynb"

    with pytest.raises(NotebookExecutionError) as raised:
        execute_ipynb(
            notebook_path,
            artifact_path=artifact_path,
            worker_context=_context(tmp_path),
        )

    assert raised.value.artifact_path == artifact_path
    assert artifact_path.is_file()
    executed = nbformat.read(artifact_path, as_version=4)
    assert executed.cells[0].outputs[0].output_type == "error"
    assert executed.cells[0].outputs[0].ename == "ValueError"


def test_execute_ipynb_rejects_non_mapping_parameters_before_kernel_start(tmp_path: Path) -> None:
    notebook_path = _save_notebook(
        tmp_path / "invalid-parameters.ipynb",
        [nbformat.v4.new_code_cell("print('unreachable')")],
    )
    artifact_path = tmp_path / "should-not-exist.ipynb"

    with pytest.raises(ValueError, match="parameters must be a mapping"):
        execute_ipynb(
            notebook_path,
            artifact_path=artifact_path,
            worker_context=_context(tmp_path),
            parameters=[],  # type: ignore[arg-type]
        )

    assert not artifact_path.exists()


def test_execute_ipynb_parameter_injection_preserves_json_types(tmp_path: Path) -> None:
    notebook_path = _save_notebook(
        tmp_path / "json-parameters.ipynb",
        [
            nbformat.v4.new_code_cell(
                "import json\n"
                "assert options == {'enabled': True, 'items': [1, 2]}\n"
                "print(json.dumps(options))"
            )
        ],
    )
    result = execute_ipynb(
        notebook_path,
        artifact_path=tmp_path / "json-parameters-output.ipynb",
        worker_context=_context(tmp_path),
        parameters={"options": {"enabled": True, "items": [1, 2]}},
    )

    output = nbformat.read(result.artifact_path, as_version=4).cells[-1].outputs[0].text
    assert json.loads(output.strip()) == {"enabled": True, "items": [1, 2]}


def test_dbutils_notebook_run_uses_a_fresh_kernel_and_returns_exit_value(
    tmp_path: Path,
) -> None:
    _save_notebook(
        tmp_path / "child.ipynb",
        [
            nbformat.v4.new_code_cell(
                "dbutils.widgets.text('name', 'child-default')\n"
                "assert dbutils.widgets.get('name') == 'child-argument'\n"
                "child_only = 'not shared with the caller'\n"
                "print('child-only-output')\n"
                "dbutils.notebook.exit('completed:' + dbutils.widgets.get('name'))\n"
                "raise AssertionError('execution must stop at notebook.exit')\n"
            ),
            nbformat.v4.new_code_cell("raise AssertionError('later cells must not run')"),
        ],
    )
    parent_path = _save_notebook(
        tmp_path / "parent.ipynb",
        [
            nbformat.v4.new_code_cell(
                "child_result = dbutils.notebook.run(\n"
                "    'child', timeout_seconds=20, arguments={'name': 'child-argument'}\n"
                ")\n"
                "assert child_result == 'completed:child-argument'\n"
                "assert 'child_only' not in globals()\n"
                "print(child_result)\n"
            )
        ],
    )

    result = execute_ipynb(
        parent_path,
        artifact_path=tmp_path / "parent-output.ipynb",
        worker_context=_context(tmp_path),
    )

    executed = nbformat.read(result.artifact_path, as_version=4)
    assert result.executed_code_cells == 1
    assert any(
        output.output_type == "stream" and "completed:child-argument" in output.text
        for output in executed.cells[-1].outputs
    )
    assert all(
        output.output_type != "stream" or "child-only-output" not in output.text
        for output in executed.cells[-1].outputs
    )


def test_dbutils_notebook_exit_returns_from_top_level_execution(tmp_path: Path) -> None:
    notebook_path = _save_notebook(
        tmp_path / "exit.ipynb",
        [
            nbformat.v4.new_code_cell("dbutils.notebook.exit('finished')"),
            nbformat.v4.new_code_cell("raise AssertionError('execution must stop')"),
        ],
    )

    result = execute_ipynb(
        notebook_path,
        artifact_path=tmp_path / "exit-output.ipynb",
        worker_context=_context(tmp_path),
    )

    assert result.exit_value == "finished"
    assert result.executed_code_cells == 1
    executed = nbformat.read(result.artifact_path, as_version=4)
    assert len(executed.cells) == 2
    assert executed.cells[0].outputs == []
    assert executed.cells[1].execution_count is None


def test_dbutils_notebook_run_accepts_marked_python_source_export(tmp_path: Path) -> None:
    (tmp_path / "child.py").write_text(
        "# Databricks notebook source\n"
        "# COMMAND ----------\n"
        "print('source-child-output')\n"
        "# COMMAND ----------\n"
        "dbutils.notebook.exit('source-child-finished')\n",
        encoding="utf-8",
    )
    parent = _save_notebook(
        tmp_path / "parent.ipynb",
        [
            nbformat.v4.new_code_cell(
                "assert dbutils.notebook.run('child.py', timeout_seconds=20) "
                "== 'source-child-finished'"
            )
        ],
    )

    result = execute_ipynb(
        parent,
        artifact_path=tmp_path / "parent-source-child-output.ipynb",
        worker_context=_context(tmp_path),
    )

    executed = nbformat.read(result.artifact_path, as_version=4)
    assert all(
        output.output_type != "stream" or "source-child-output" not in output.text
        for output in executed.cells[-1].outputs
    )


def test_child_notebook_run_failure_is_propagated(tmp_path: Path) -> None:
    _save_notebook(
        tmp_path / "child.ipynb",
        [nbformat.v4.new_code_cell("raise ValueError('child fixture failure')")],
    )
    parent_path = _save_notebook(
        tmp_path / "parent.ipynb",
        [nbformat.v4.new_code_cell("dbutils.notebook.run('child', timeout_seconds=20)")],
    )

    with pytest.raises(NotebookExecutionError) as raised:
        execute_ipynb(
            parent_path,
            artifact_path=tmp_path / "parent-failure.ipynb",
            worker_context=_context(tmp_path),
        )

    assert raised.value.artifact_path.is_file()
    executed = nbformat.read(raised.value.artifact_path, as_version=4)
    errors = [output for output in executed.cells[-1].outputs if output.output_type == "error"]
    assert errors[0].ename == "NotebookRunError"
    assert "CellExecutionError" in errors[0].evalue


def test_child_notebook_timeout_reaps_kernel_and_descendant(tmp_path: Path) -> None:
    marker = tmp_path / "child-processes.txt"
    _save_notebook(
        tmp_path / "child.ipynb",
        [
            nbformat.v4.new_code_cell(
                "import os, subprocess, sys, time\n"
                "descendant = subprocess.Popen([sys.executable, '-c', "
                "'import time; time.sleep(60)'])\n"
                f"open({str(marker)!r}, 'w').write(f'{{os.getpid()}},{{descendant.pid}}')\n"
                "time.sleep(60)\n"
            )
        ],
    )
    parent = _save_notebook(
        tmp_path / "parent.ipynb",
        [nbformat.v4.new_code_cell("dbutils.notebook.run('child', timeout_seconds=2)")],
    )

    with pytest.raises(NotebookExecutionError) as raised:
        execute_ipynb(
            parent,
            artifact_path=tmp_path / "timeout-output.ipynb",
            worker_context=_context(tmp_path),
        )

    assert marker.is_file()
    child_pid, descendant_pid = (int(value) for value in marker.read_text().split(","))
    executed = nbformat.read(raised.value.artifact_path, as_version=4)
    errors = [output for output in executed.cells[-1].outputs if output.output_type == "error"]
    assert errors[0].ename == "NotebookRunTimeoutError"
    _assert_processes_stopped((child_pid, descendant_pid))


def test_parent_cell_timeout_cancels_child_notebook_and_descendants(tmp_path: Path) -> None:
    marker = tmp_path / "parent-cancel-processes.txt"
    _save_notebook(
        tmp_path / "child.ipynb",
        [
            nbformat.v4.new_code_cell(
                "import os, subprocess, sys, time\n"
                "descendant = subprocess.Popen([sys.executable, '-c', "
                "'import time; time.sleep(60)'])\n"
                f"open({str(marker)!r}, 'w').write(f'{{os.getpid()}},{{descendant.pid}}')\n"
                "time.sleep(60)\n"
            )
        ],
    )
    parent = _save_notebook(
        tmp_path / "parent.ipynb",
        [nbformat.v4.new_code_cell("dbutils.notebook.run('child', timeout_seconds=0)")],
    )

    with pytest.raises(NotebookExecutionError):
        execute_ipynb(
            parent,
            artifact_path=tmp_path / "parent-cancel-output.ipynb",
            worker_context=_context(tmp_path),
            cell_timeout_seconds=3,
        )

    assert marker.is_file()
    child_pid, descendant_pid = (int(value) for value in marker.read_text().split(","))
    _assert_processes_stopped((child_pid, descendant_pid))


def _assert_processes_stopped(process_ids: tuple[int, ...]) -> None:
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        living = []
        for process_id in process_ids:
            try:
                os.kill(process_id, 0)
            except ProcessLookupError:
                continue
            living.append(process_id)
        if not living:
            return
        time.sleep(0.1)
    raise AssertionError(f"notebook processes were not reaped: {living}")
