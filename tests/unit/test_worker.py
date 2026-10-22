from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from brickhouse.runtime import (
    CONTEXT_ENV,
    MAX_CAPTURE_LIMIT_BYTES,
    MAX_CONTEXT_BYTES,
    WorkerConfigurationError,
    WorkerContext,
    run_python_worker,
)


def test_worker_runs_absolute_interpreter_with_snapshot_context_and_explicit_cwd(
    tmp_path: Path,
) -> None:
    working_directory = tmp_path / "work"
    working_directory.mkdir()
    script = tmp_path / "worker.py"
    script.write_text(
        "import json, os, pathlib, sys\n"
        f"print(json.dumps({{'cwd': str(pathlib.Path.cwd()), "
        f"'prefix': sys.prefix, "
        f"'context': json.loads(os.environ[{CONTEXT_ENV!r}])}}))\n",
        encoding="utf-8",
    )
    parameters: dict[str, object] = {"amount": 12.5, "nested": [1, 2]}
    context = WorkerContext.from_mapping(
        {"run_id": "run-a", "attempt_id": "attempt-1", "parameters": parameters}
    )
    parameters["amount"] = 99

    result = run_python_worker(script, cwd=working_directory, context=context)

    assert result.exit_code == 0
    assert result.interpreter == Path(sys.executable).absolute()
    assert result.script == script.resolve()
    assert result.cwd == working_directory.resolve()
    assert json.loads(result.stdout) == {
        "cwd": str(working_directory.resolve()),
        "prefix": sys.prefix,
        "context": {
            "attempt_id": "attempt-1",
            "parameters": {"amount": 12.5, "nested": [1, 2]},
            "run_id": "run-a",
        },
    }
    assert result.stderr == ""
    assert not result.stdout_truncated
    assert not result.stderr_truncated


def test_worker_bounds_output_while_draining_both_pipes(tmp_path: Path) -> None:
    script = tmp_path / "worker.py"
    script.write_text(
        "import sys\nsys.stdout.write('o' * 200000)\nsys.stderr.write('e' * 200000)\n",
        encoding="utf-8",
    )
    result = run_python_worker(
        script,
        cwd=tmp_path,
        context=WorkerContext.from_mapping({"run_id": "large-output"}),
        stdout_limit_bytes=128,
        stderr_limit_bytes=64,
    )

    assert result.exit_code == 0
    assert result.stdout == "o" * 128
    assert result.stderr == "e" * 64
    assert result.stdout_truncated
    assert result.stderr_truncated
    assert result.stdout_limit_bytes == 128
    assert result.stderr_limit_bytes == 64


def test_worker_passes_bounded_task_arguments_without_a_shell(tmp_path: Path) -> None:
    script = tmp_path / "worker.py"
    script.write_text("import json, sys\nprint(json.dumps(sys.argv[1:]))\n", encoding="utf-8")

    result = run_python_worker(
        script,
        cwd=tmp_path,
        context=WorkerContext.from_mapping({"run_id": "arguments"}),
        arguments=("two words", "$(echo not-a-shell)", "--count=3"),
    )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == ["two words", "$(echo not-a-shell)", "--count=3"]


def test_worker_does_not_inherit_credentials_or_import_context(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DATABRICKS_TOKEN", "test-token-sentinel")
    monkeypatch.setenv("PYTHONPATH", "/must-not-leak")
    monkeypatch.setenv("PYTHONSTARTUP", "/must-not-leak")
    script = tmp_path / "worker.py"
    script.write_text(
        "import json, os\n"
        "print(json.dumps({"
        "'token': os.environ.get('DATABRICKS_TOKEN'), "
        "'pythonpath': os.environ.get('PYTHONPATH'), "
        "'startup': os.environ.get('PYTHONSTARTUP'), "
        f"'context': os.environ[{CONTEXT_ENV!r}]"
        "}))\n",
        encoding="utf-8",
    )

    result = run_python_worker(
        script,
        cwd=tmp_path,
        context=WorkerContext.from_mapping({"attempt_id": "attempt-2"}),
    )

    assert result.exit_code == 0
    observed = json.loads(result.stdout)
    assert observed["token"] is None
    assert observed["pythonpath"] is None
    assert observed["startup"] is None
    assert json.loads(observed["context"]) == {"attempt_id": "attempt-2"}
    assert "test-token-sentinel" not in result.stdout + result.stderr


@pytest.mark.parametrize(
    "context",
    [
        "[]",
        "{not-json}",
        "NaN",
        json.dumps({"payload": "x" * MAX_CONTEXT_BYTES}),
    ],
)
def test_worker_context_rejects_invalid_or_oversized_values(context: str) -> None:
    with pytest.raises(WorkerConfigurationError):
        WorkerContext(context)


def test_worker_context_rejects_non_json_values() -> None:
    with pytest.raises(WorkerConfigurationError, match="finite JSON"):
        WorkerContext.from_mapping({"value": object()})
    with pytest.raises(WorkerConfigurationError, match="finite JSON"):
        WorkerContext.from_mapping({"value": float("nan")})


def test_worker_rejects_path_and_limit_errors_before_launch(tmp_path: Path) -> None:
    script = tmp_path / "worker.py"
    script.write_text("raise SystemExit(0)\n", encoding="utf-8")
    context = WorkerContext.from_mapping({"run_id": "validation"})

    with pytest.raises(WorkerConfigurationError, match="interpreter"):
        run_python_worker(
            script,
            cwd=tmp_path,
            context=context,
            interpreter=tmp_path / "missing-python",
        )
    with pytest.raises(WorkerConfigurationError, match="cwd"):
        run_python_worker(script, cwd=script, context=context)
    with pytest.raises(WorkerConfigurationError, match="stdout_limit_bytes"):
        run_python_worker(
            script,
            cwd=tmp_path,
            context=context,
            stdout_limit_bytes=MAX_CAPTURE_LIMIT_BYTES + 1,
        )
    with pytest.raises(WorkerConfigurationError, match="stderr_limit_bytes"):
        run_python_worker(
            script,
            cwd=tmp_path,
            context=context,
            stderr_limit_bytes=True,
        )
    with pytest.raises(WorkerConfigurationError, match="worker arguments"):
        run_python_worker(script, cwd=tmp_path, context=context, arguments=("bad\x00arg",))


def test_worker_returns_nonzero_exit_status_and_bounded_diagnostics(tmp_path: Path) -> None:
    script = tmp_path / "worker.py"
    script.write_text(
        "import sys\n"
        "print('started')\n"
        "print('failure-detail', file=sys.stderr)\n"
        "raise SystemExit(7)\n",
        encoding="utf-8",
    )
    result = run_python_worker(
        script,
        cwd=tmp_path,
        context=WorkerContext.from_mapping({"run_id": "failure"}),
        stdout_limit_bytes=32,
        stderr_limit_bytes=8,
    )

    assert result.exit_code == 7
    assert result.stdout == "started\n"
    assert result.stderr == "failure-"
    assert result.stderr_truncated
