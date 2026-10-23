from __future__ import annotations

import json
import os

import pytest

from scripts.benchmark_emulators import (
    BenchmarkError,
    clean_environment,
    normalize_rows,
    summarize,
    workspace_payload,
)


def test_benchmark_statistics_use_median_and_nearest_rank_p95() -> None:
    summary = summarize([float(value) for value in range(20, 0, -1)])
    assert summary == {"count": 20, "median": 10.5, "p95": 19, "min": 1, "max": 20}
    assert summarize([2.5])["p95"] == 2.5


@pytest.mark.parametrize("samples", [[], [-1.0], [float("inf")], [float("nan")]])
def test_benchmark_statistics_reject_missing_or_invalid_samples(samples: list[float]) -> None:
    with pytest.raises(ValueError):
        summarize(samples)


def test_file_payload_has_exact_size_and_changes_between_samples() -> None:
    first = workspace_payload(1, 65536)
    second = workspace_payload(2, 65536)
    assert len(first) == len(second) == 65536
    assert first != second
    assert first.startswith(b"# local-emulator-benchmark revision=1\n")
    with pytest.raises(ValueError):
        workspace_payload(0, 1)


def test_sql_normalization_accepts_exact_integer_rows_only() -> None:
    assert normalize_rows(
        {"status": {"state": "SUCCEEDED"}, "result": {"data_array": [["0", "66", "30000"]]}}
    ) == [[0, 66, 30000]]


def test_go_sql_normalization_uses_explicit_agent_envelope() -> None:
    response = {
        "status": {"state": "SUCCEEDED"},
        "result": {"text": json.dumps({"schema": {}, "data": [[0, 66, 30000]]})},
    }
    assert normalize_rows(response, agent_text=True) == [[0, 66, 30000]]
    with pytest.raises(BenchmarkError):
        normalize_rows(response)
    with pytest.raises(BenchmarkError):
        normalize_rows({"status": {"state": "SUCCEEDED"}, "result": {}}, agent_text=True)


@pytest.mark.parametrize("text", ["not JSON", "[]", "null", '{"data": []}'])
def test_go_sql_normalization_rejects_invalid_or_empty_envelopes(text: str) -> None:
    with pytest.raises(BenchmarkError):
        normalize_rows(
            {"status": {"state": "SUCCEEDED"}, "result": {"text": text}}, agent_text=True
        )
    assert normalize_rows(
        {"status": {"state": "SUCCEEDED"}, "result": {"data_array": [[0, 66, 30000]]}}
    ) == [[0, 66, 30000]]


@pytest.mark.parametrize(
    "rows",
    [[], [[0, 1]], [[0, 1, 2, 3]], [[0, True, 2]], [[0, 1.5, 2]], [[0, None, 2]], [[0, "1.0", 2]]],
)
def test_sql_normalization_rejects_empty_or_invalid_results(rows: list[object]) -> None:
    with pytest.raises(BenchmarkError):
        normalize_rows({"status": {"state": "SUCCEEDED"}, "result": {"data_array": rows}})


@pytest.mark.parametrize("state", ["RUNNING", "FAILED", "CANCELED"])
def test_sql_normalization_refuses_non_successful_statements(state: str) -> None:
    with pytest.raises(BenchmarkError):
        normalize_rows({"status": {"state": state}, "result": {"data_array": [[0, 1, 2]]}})


def test_benchmark_children_do_not_inherit_account_selectors_or_dependency_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for key in [
        "DATABRICKS_TOKEN",
        "DATABRICKS_HOST",
        "DATABRICKS_CONFIG_FILE",
        "AWS_SECRET_ACCESS_KEY",
        "AZURE_CLIENT_SECRET",
        "PYTHONPATH",
        "SPARK_REMOTE",
    ]:
        monkeypatch.setenv(key, "synthetic-test-value")
    environment = clean_environment()
    assert environment.get("HOME") == os.environ.get("HOME")
    assert environment["PYTHONNOUSERSITE"] == "1"
    assert not any(key.startswith(("DATABRICKS_", "AWS_", "AZURE_")) for key in environment)
    assert "PYTHONPATH" not in environment
    assert "SPARK_REMOTE" not in environment
