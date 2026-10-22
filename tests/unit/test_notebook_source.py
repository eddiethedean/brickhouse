from __future__ import annotations

import sys
from pathlib import Path

import nbformat
import pytest

from brickhouse.notebooks import (
    NotebookSourceFormatError,
    execute_databricks_python_source,
)


def _context(tmp_path: Path, suffix: str) -> dict[str, object]:
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
            "widget_parameters": {"greeting": "hello from source"},
            "task_values_enabled": False,
        },
    }


def _write_source(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def test_source_export_runs_python_sql_markdown_and_shared_scope_run(
    tmp_path: Path,
) -> None:
    source = _write_source(
        tmp_path / "main.py",
        "# Databricks notebook source\n"
        "# COMMAND ----------\n"
        "base_value = 40\n"
        "# COMMAND ----------\n"
        "# MAGIC %python\n"
        "# MAGIC local_value = 1\n"
        "# COMMAND ----------\n"
        "# MAGIC %sql\n"
        "# MAGIC SELECT 2 AS answer\n"
        "# COMMAND ----------\n"
        "# MAGIC %md\n"
        "# MAGIC ## Local source notebook\n"
        "# COMMAND ----------\n"
        "# MAGIC %run ./shared\n"
        "# COMMAND ----------\n"
        "assert shared_value == 41\n"
        'assert _sqldf.collect()[0]["answer"] == 2\n'
        "print('source-handlers-complete')\n",
    )
    _write_source(
        tmp_path / "shared.py",
        "# Databricks notebook source\n"
        "# COMMAND ----------\n"
        "shared_value = base_value + local_value\n",
    )
    original = source.read_text(encoding="utf-8")
    artifact = tmp_path / "artifacts" / "executed.ipynb"

    result = execute_databricks_python_source(
        source,
        artifact_path=artifact,
        worker_context=_context(tmp_path, "source"),
    )

    executed = nbformat.read(artifact, as_version=4)
    assert result.artifact_path == artifact
    assert result.executed_code_cells == 5
    assert source.read_text(encoding="utf-8") == original
    assert any(cell.cell_type == "markdown" for cell in executed.cells)
    assert any(
        output.output_type == "stream" and "source-handlers-complete" in output.text
        for cell in executed.cells
        for output in cell.get("outputs", [])
    )
    assert any(
        output.output_type == "display_data"
        for cell in executed.cells
        for output in cell.get("outputs", [])
    )


@pytest.mark.parametrize(
    ("directive", "body", "message"),
    [
        ("%r", "x <- 1", "unsupported notebook language"),
        ("%scala", "val x = 1", "unsupported notebook language"),
        ("%pip install", "package-name", "package installation"),
        ("%uv pip install", "package-name", "package installation"),
        ("%sh", "echo unsupported", "unsupported notebook magic"),
        ("%unknown", "ignored", "unsupported notebook magic"),
    ],
)
def test_source_export_rejects_unsupported_magics_before_kernel_start(
    tmp_path: Path,
    directive: str,
    body: str,
    message: str,
) -> None:
    source = _write_source(
        tmp_path / "unsupported.py",
        f"# Databricks notebook source\n# MAGIC {directive}\n# MAGIC {body}\n",
    )
    artifact = tmp_path / "must-not-exist.ipynb"

    with pytest.raises(NotebookSourceFormatError, match=message):
        execute_databricks_python_source(
            source,
            artifact_path=artifact,
            worker_context=_context(tmp_path, "unsupported"),
        )

    assert not artifact.exists()


@pytest.mark.parametrize(
    ("target", "message"),
    [
        ("../outside", "escapes"),
        ("missing", "does not exist"),
    ],
)
def test_source_export_preflights_invalid_run_targets(
    tmp_path: Path,
    target: str,
    message: str,
) -> None:
    source = _write_source(
        tmp_path / "notebook-dir" / "main.py",
        f"# Databricks notebook source\n# COMMAND ----------\n# MAGIC %run {target}\n",
    )
    if target == "../outside":
        _write_source(tmp_path / "outside.py", "# Databricks notebook source\n")

    with pytest.raises(NotebookSourceFormatError, match=message):
        execute_databricks_python_source(
            source,
            artifact_path=tmp_path / "must-not-exist.ipynb",
            worker_context=_context(tmp_path, "invalid-run"),
        )


def test_source_export_rejects_run_cycles_before_kernel_start(tmp_path: Path) -> None:
    source = _write_source(
        tmp_path / "main.py",
        "# Databricks notebook source\n# MAGIC %run ./shared\n",
    )
    _write_source(
        tmp_path / "shared.py",
        "# Databricks notebook source\n# MAGIC %run ./main\n",
    )

    with pytest.raises(NotebookSourceFormatError, match="cannot contain a cycle"):
        execute_databricks_python_source(
            source,
            artifact_path=tmp_path / "must-not-exist.ipynb",
            worker_context=_context(tmp_path, "run-cycle"),
        )


def test_source_export_rejects_unmarked_or_non_python_exports(tmp_path: Path) -> None:
    unmarked = _write_source(tmp_path / "unmarked.py", "print('not a Databricks export')\n")
    sql_export = _write_source(
        tmp_path / "not-python.sql",
        "-- Databricks notebook source\nSELECT 1;\n",
    )

    for source in (unmarked, sql_export):
        with pytest.raises(NotebookSourceFormatError):
            execute_databricks_python_source(
                source,
                artifact_path=tmp_path / f"{source.stem}-output.ipynb",
                worker_context=_context(tmp_path, source.stem),
            )
