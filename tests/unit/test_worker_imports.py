from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

from brickhouse.runtime.worker import WorkerContext, run_python_worker


def _dbutils_context(tmp_path: Path, suffix: str) -> WorkerContext:
    dbfs_root = tmp_path / f"dbfs-{suffix}"
    dbfs_root.mkdir()
    return WorkerContext.from_mapping(
        {
            "run_id": f"run-{suffix}",
            "attempt_id": f"attempt-{suffix}",
            "dbutils": {"dbfs_root": str(dbfs_root), "widget_parameters": {}},
        }
    )


def _write_script(tmp_path: Path, name: str, source: str) -> Path:
    script = tmp_path / name
    script.write_text(source, encoding="utf-8")
    return script


def test_pyspark_sql_imports_are_backed_by_sparkless_in_isolated_workers(
    tmp_path: Path,
) -> None:
    parent_modules = {
        name: module
        for name, module in sys.modules.items()
        if name == "pyspark" or name.startswith("pyspark.")
    }
    first = _write_script(
        tmp_path,
        "first.py",
        "import json, os, pyspark\n"
        "import sparkless.sql as sparkless_sql\n"
        "import sparkless.sql.functions as sparkless_functions\n"
        "import sparkless.sql.types as sparkless_types\n"
        "import sparkless.sql.window as sparkless_window\n"
        "from pyspark.sql import DataFrame, SparkSession\n"
        "from pyspark.sql import functions as F\n"
        "from pyspark.sql.functions import col, lit\n"
        "from pyspark.sql.types import IntegerType, StructField, StructType\n"
        "from pyspark.sql.window import Window\n"
        "assert pyspark.sql.functions is F\n"
        "F.worker_marker = 'first-worker-only'\n"
        "print(json.dumps({'pid': os.getpid(), 'dataframe': DataFrame is sparkless_sql.DataFrame, "
        "'session_facade': SparkSession.__module__ == 'brickhouse.runtime.worker_session', "
        "'column_fn': col is sparkless_functions.col, "
        "'literal_fn': lit is sparkless_functions.lit, "
        "'window': Window is sparkless_window.Window, "
        "'schema': StructType is sparkless_types.StructType, "
        "'field': StructField is sparkless_types.StructField, "
        "'integer': IntegerType is sparkless_types.IntegerType}))\n",
    )
    first_result = run_python_worker(
        first,
        cwd=tmp_path,
        context=_dbutils_context(tmp_path, "first"),
    )

    second = _write_script(
        tmp_path,
        "second.py",
        "import json, os\n"
        "from pyspark.sql import functions as F\n"
        "print(json.dumps({'pid': os.getpid(), 'marker': getattr(F, 'worker_marker', None)}))\n",
    )
    second_result = run_python_worker(
        second,
        cwd=tmp_path,
        context=_dbutils_context(tmp_path, "second"),
    )

    assert first_result.exit_code == 0, first_result.stderr
    assert second_result.exit_code == 0, second_result.stderr
    first_output = json.loads(first_result.stdout)
    second_output = json.loads(second_result.stdout)
    assert all(
        first_output[key]
        for key in (
            "dataframe",
            "session_facade",
            "column_fn",
            "literal_fn",
            "window",
            "schema",
            "field",
            "integer",
        )
    )
    assert first_output["pid"] != os.getpid()
    assert second_output["pid"] != first_output["pid"]
    assert second_output["marker"] is None
    assert {
        name: module
        for name, module in sys.modules.items()
        if name == "pyspark" or name.startswith("pyspark.")
    } == parent_modules


@pytest.mark.parametrize(
    "module_name",
    ("pyspark.rdd", "pyspark.ml", "pyspark.java_gateway"),
)
def test_worker_rejects_unsupported_pyspark_namespaces(tmp_path: Path, module_name: str) -> None:
    script = _write_script(tmp_path, "unsupported.py", f"import {module_name}\n")

    result = run_python_worker(
        script,
        cwd=tmp_path,
        context=_dbutils_context(tmp_path, module_name.rsplit(".", 1)[-1]),
    )

    assert result.exit_code != 0
    assert "UnsupportedPySparkImportError" in result.stderr
    assert module_name in result.stderr


def test_worker_rejects_a_conflicting_pyspark_package(tmp_path: Path) -> None:
    conflicting_package = tmp_path / "pyspark"
    conflicting_package.mkdir()
    (conflicting_package / "__init__.py").write_text("raise AssertionError('must not execute')\n")
    script = _write_script(tmp_path, "conflicting.py", "import pyspark\n")

    result = run_python_worker(
        script,
        cwd=tmp_path,
        context=_dbutils_context(tmp_path, "conflict"),
    )

    assert result.exit_code != 0
    assert "ConflictingPySparkPackageError" in result.stderr
    assert "refuses installed PySpark package" in result.stderr


def test_delta_table_import_is_worker_scoped_and_rejects_unsupported_namespaces(
    tmp_path: Path,
) -> None:
    parent_modules = {
        name: module
        for name, module in sys.modules.items()
        if name == "delta" or name.startswith("delta.")
    }
    script = _write_script(
        tmp_path,
        "delta-import.py",
        "import json, delta\n"
        "from delta import tables\n"
        "from delta.tables import DeltaTable\n"
        "from brickhouse.runtime.worker_delta import DeltaTable as WorkerDeltaTable\n"
        "assert delta.tables is tables\n"
        "print(json.dumps({'identity': DeltaTable is WorkerDeltaTable, "
        "'module': DeltaTable.__module__}))\n",
    )

    result = run_python_worker(
        script,
        cwd=tmp_path,
        context=_dbutils_context(tmp_path, "delta-import"),
    )

    assert result.exit_code == 0, result.stderr
    assert json.loads(result.stdout) == {
        "identity": True,
        "module": "brickhouse.runtime.worker_delta",
    }
    assert {
        name: module
        for name, module in sys.modules.items()
        if name == "delta" or name.startswith("delta.")
    } == parent_modules


def test_worker_rejects_unsupported_delta_namespaces(tmp_path: Path) -> None:
    script = _write_script(tmp_path, "unsupported-delta.py", "import delta.connect\n")

    result = run_python_worker(
        script,
        cwd=tmp_path,
        context=_dbutils_context(tmp_path, "unsupported-delta"),
    )

    assert result.exit_code != 0
    assert "UnsupportedPySparkImportError" in result.stderr
    assert "delta.connect" in result.stderr


def test_worker_rejects_a_conflicting_delta_spark_package(tmp_path: Path) -> None:
    conflicting_package = tmp_path / "delta"
    conflicting_package.mkdir()
    (conflicting_package / "__init__.py").write_text("raise AssertionError('must not execute')\n")
    script = _write_script(tmp_path, "conflicting-delta.py", "import delta.tables\n")

    result = run_python_worker(
        script,
        cwd=tmp_path,
        context=_dbutils_context(tmp_path, "conflicting-delta"),
    )

    assert result.exit_code != 0
    assert "ConflictingDeltaPackageError" in result.stderr
    assert "refuses installed Delta Lake package" in result.stderr
