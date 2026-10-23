from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from scripts.summarize_emulator_benchmarks import PRODUCTS, combine, render_table


def recorded_runs() -> list[dict[str, Any]]:
    root = Path(__file__).resolve().parents[2] / "artifacts/benchmarks/2026-10-22-local-emulators"
    return [
        json.loads((root / directory / "results.json").read_text())
        for directory in ("initial-measurement", "measured")
    ]


def test_complete_recorded_runs_pool_every_trial_and_timed_sample() -> None:
    combined = combine(recorded_runs())
    assert combined["total_trials"] == 30
    for product in PRODUCTS:
        summary = combined["summary"][product]
        assert summary["passed_trials"] == 10
        assert summary["fresh_workspace_to_verified_file_ms"]["count"] == 10
        assert summary["workspace_round_trip_ms"]["count"] == 250
        assert summary["delta_aggregate_ms"]["count"] == 250
        assert summary["core_server_rss_mib"]["count"] == 10
    table = render_table(combined)
    assert "10,000 Delta rows" in table
    assert "Idle core API server RSS" in table
    assert len(table.strip().splitlines()) == 6


def test_pooling_refuses_empty_input_and_duplicate_runs() -> None:
    with pytest.raises(ValueError, match="At least one"):
        combine([])
    run = recorded_runs()[0]
    with pytest.raises(ValueError, match="same benchmark run"):
        combine([run, copy.deepcopy(run)])


@pytest.mark.parametrize("key", ["configuration", "provenance", "system"])
def test_pooling_refuses_different_profiles_or_hardware(key: str) -> None:
    reports = recorded_runs()
    reports[1][key]["synthetic_changed_field"] = "different"
    with pytest.raises(ValueError, match=f"different {key}"):
        combine(reports)


@pytest.mark.parametrize(
    "change",
    [
        "failed_trial",
        "unmeasured_sql",
        "missing_sample",
        "duplicate_round",
        "mutated_fixture",
        "wrong_expected_rows",
        "missing_verification",
        "restart_failed",
        "cleanup_missing",
    ],
)
def test_pooling_refuses_incomplete_or_invalid_evidence(change: str) -> None:
    reports = recorded_runs()
    run = reports[1]
    trial = run["trials"][0]
    if change == "failed_trial":
        trial["status"] = "failed"
    elif change == "unmeasured_sql":
        trial["sql_status"] = "not_configured"
    elif change == "missing_sample":
        trial["delta_aggregate_ms"].pop()
    elif change == "duplicate_round":
        trial["round"] = 5
    elif change == "mutated_fixture":
        run["fixture_unchanged"] = False
    elif change == "wrong_expected_rows":
        run["fixture"]["expected_sha256"] = "incorrect"
    elif change == "missing_verification":
        trial["verified_sql_results"] -= 1
    elif change == "restart_failed":
        trial["workspace_file_survived_restart"] = False
    else:
        run["temporary_state_removed"] = False
    with pytest.raises(ValueError):
        combine(reports)
