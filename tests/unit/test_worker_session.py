from __future__ import annotations

import json
from pathlib import Path

import pytest

from brickhouse.runtime.worker import WorkerContext, run_python_worker


def _context(tmp_path: Path, suffix: str, profile: dict[str, object]) -> WorkerContext:
    dbfs_root = tmp_path / f"dbfs-{suffix}"
    dbfs_root.mkdir()
    return WorkerContext.from_mapping(
        {
            **profile,
            "run_id": f"run-{suffix}",
            "attempt_id": f"attempt-{suffix}",
            "dbutils": {"dbfs_root": str(dbfs_root), "widget_parameters": {}},
        }
    )


def _write_script(tmp_path: Path, source: str) -> Path:
    script = tmp_path / "worker.py"
    script.write_text(source, encoding="utf-8")
    return script


def _serverless_profile() -> dict[str, object]:
    return {
        "compute_mode": "serverless",
        "profile": "serverless-local-v1",
        "runtime_profile": None,
        "access_mode": None,
        "effective_settings": {
            "spark.sql.ansi.enabled": "true",
            "spark.sql.session.timeZone": "UTC",
        },
    }


def _classic_profile(
    *,
    runtime_profile: str = "spark35-local-v1",
    profile: str = "classic-standard-local-v1",
    access_mode: str = "DATA_SECURITY_MODE_STANDARD",
) -> dict[str, object]:
    return {
        "compute_mode": "classic",
        "profile": profile,
        "runtime_profile": runtime_profile,
        "access_mode": access_mode,
        "effective_settings": {
            "spark.sql.ansi.enabled": "false",
            "spark.sql.session.timeZone": "America/Los_Angeles",
        },
    }


@pytest.mark.parametrize(
    ("profile", "expected_ansi", "expected_timezone"),
    (
        (_serverless_profile(), "true", "UTC"),
        (_classic_profile(), "false", "America/Los_Angeles"),
    ),
    ids=("serverless", "classic-standard"),
)
def test_worker_spark_session_applies_profile_and_runs_dataframe_work(
    tmp_path: Path,
    profile: dict[str, object],
    expected_ansi: str,
    expected_timezone: str,
) -> None:
    script = _write_script(
        tmp_path,
        "import json\n"
        "from pyspark.sql import SparkSession, SparkSessionBuilder\n"
        "from pyspark.sql.functions import col\n"
        "assert isinstance(SparkSession.builder, SparkSessionBuilder)\n"
        "spark = SparkSession.builder.config('spark.sql.caseSensitive', 'true').getOrCreate()\n"
        "count = (spark.createDataFrame([(1, 'a'), (2, 'b')], ['key', 'value'])\n"
        "    .filter(col('key') > 1).select('value').count())\n"
        "print(json.dumps({'count': count, 'ansi': spark.conf.get('spark.sql.ansi.enabled'), "
        "'timezone': spark.conf.get('spark.sql.session.timeZone'), "
        "'case_sensitive': spark.conf.get('spark.sql.caseSensitive')}))\n"
        "spark.stop()\n",
    )

    result = run_python_worker(
        script,
        cwd=tmp_path,
        context=_context(tmp_path, "profiled", profile),
    )

    assert result.exit_code == 0, result.stderr
    assert json.loads(result.stdout) == {
        "count": 1,
        "ansi": expected_ansi,
        "timezone": expected_timezone,
        "case_sensitive": "true",
    }


@pytest.mark.parametrize(
    ("expression", "error_name", "message"),
    (
        (
            "spark.sparkContext",
            "UnsupportedWorkerSparkOperationError",
            "SparkContext and RDD execution are unavailable",
        ),
        (
            "spark.range(1).rdd",
            "UnsupportedWorkerSparkOperationError",
            "DataFrame.rdd is unsupported",
        ),
        (
            "SparkSession.builder.config('spark.driver.memory', '1g')",
            "WorkerSparkSessionConfigurationError",
            "worker Spark setting is unsupported",
        ),
        (
            "SparkSession.builder.master('local[*]')",
            "UnsupportedWorkerSparkOperationError",
            "builder.master is unsupported",
        ),
    ),
)
def test_worker_rejects_unmapped_session_configuration_and_jvm_apis(
    tmp_path: Path, expression: str, error_name: str, message: str
) -> None:
    script = _write_script(
        tmp_path,
        "from pyspark.sql import SparkSession\n"
        "spark = SparkSession.builder.getOrCreate()\n"
        f"{expression}\n",
    )

    result = run_python_worker(
        script,
        cwd=tmp_path,
        context=_context(tmp_path, "unsupported", _serverless_profile()),
    )

    assert result.exit_code != 0
    assert error_name in result.stderr
    assert message in result.stderr


def test_worker_rejects_unmapped_classic_runtime_before_script_execution(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "script-started"
    script = _write_script(
        tmp_path,
        "from pyspark.sql import SparkSession\n"
        f"from pathlib import Path\nPath({str(marker)!r}).touch()\n",
    )

    result = run_python_worker(
        script,
        cwd=tmp_path,
        context=_context(
            tmp_path,
            "unmapped-runtime",
            _classic_profile(runtime_profile="spark36-local-v1"),
        ),
    )

    assert result.exit_code != 0
    assert "WorkerSparkSessionConfigurationError" in result.stderr
    assert "not mapped to spark35-local-v1" in result.stderr
    assert not marker.exists()


def test_worker_session_rejects_cross_thread_use_and_cleans_up_on_owner_thread(
    tmp_path: Path,
) -> None:
    script = _write_script(
        tmp_path,
        "import json, threading\n"
        "from pyspark.sql import SparkSession\n"
        "spark = SparkSession.builder.getOrCreate()\n"
        "errors = []\n"
        "def use_from_another_thread():\n"
        "    try:\n"
        "        spark.range(1)\n"
        "    except Exception as error:\n"
        "        errors.append(type(error).__name__)\n"
        "thread = threading.Thread(target=use_from_another_thread)\n"
        "thread.start()\n"
        "thread.join()\n"
        "assert spark.range(1).count() == 1\n"
        "print(json.dumps(errors))\n",
    )

    result = run_python_worker(
        script,
        cwd=tmp_path,
        context=_context(tmp_path, "thread", _serverless_profile()),
    )

    assert result.exit_code == 0, result.stderr
    assert json.loads(result.stdout) == ["UnsupportedWorkerSparkOperationError"]


def test_worker_spark_session_rejects_table_reads_without_authenticated_service(
    tmp_path: Path,
) -> None:
    script = _write_script(
        tmp_path,
        "from pyspark.sql import SparkSession\n"
        "spark = SparkSession.builder.getOrCreate()\n"
        "spark.table('main.default.orders')\n",
    )

    result = run_python_worker(
        script,
        cwd=tmp_path,
        context=_context(tmp_path, "no-table-service", _serverless_profile()),
    )

    assert result.exit_code != 0
    assert "UnsupportedWorkerSparkOperationError" in result.stderr
    assert "requires an authenticated job worker service" in result.stderr
