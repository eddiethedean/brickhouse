from __future__ import annotations

import gzip
import json
import statistics
from pathlib import Path
from typing import Any

import pytest

from scripts.profile_sql import Recorder, partition_spans
from scripts.summarize_sql_profile import analyze


def span(name: str, category: str, start: int, end: int, thread: int, depth: int) -> dict[str, Any]:
    return dict(
        name=name,
        category=category,
        start=start * 1_000_000,
        end=end * 1_000_000,
        thread=thread,
        depth=depth,
    )


def test_wall_partition_counts_worker_once_while_http_thread_waits() -> None:
    spans = [
        span("http.request", "http", 0, 100, 1, -1),
        span("worker", "lifecycle", 10, 90, 2, 0),
        span("engine", "engine", 20, 70, 2, 1),
        span("metadata", "metadata", 30, 40, 2, 2),
    ]
    assert partition_spans(spans) == dict(http=20, lifecycle=30, engine=40, metadata=10)


def test_wall_partition_clips_hooks_to_http_request() -> None:
    spans = [span("http.request", "http", 10, 90, 1, -1), span("worker", "worker", 0, 100, 2, 0)]
    assert partition_spans(spans) == {"worker": 80}


def test_wall_partition_has_no_cost_for_zero_duration_hooks() -> None:
    spans = [span("http.request", "http", 0, 100, 1, -1), span("empty", "empty", 50, 50, 1, 0)]
    assert partition_spans(spans) == {"http": 100}


def test_recorder_preserves_return_exception_and_nesting(tmp_path: Path) -> None:
    recorder = Recorder("spans", tmp_path / "trace", 0)

    class Boundary:
        def inner(self, fail: bool) -> int:
            if fail:
                raise ValueError("synthetic boundary failure")
            return 42

        def outer(self, fail: bool) -> int:
            return self.inner(fail)

    recorder.patch(Boundary, "inner", "inner")
    recorder.patch(Boundary, "outer", "outer")
    recorder.active = []
    assert Boundary().outer(False) == 42
    with pytest.raises(ValueError, match="synthetic boundary failure"):
        Boundary().outer(True)
    assert [item["depth"] for item in recorder.active] == [1, 0, 1, 0]
    assert recorder.local.depth == 0
    assert all(item["end"] >= item["start"] for item in recorder.active)


def test_recorder_excludes_warmups_from_detailed_profile(tmp_path: Path) -> None:
    recorder = Recorder("cprofile", tmp_path / "trace", 2)

    class Handler:
        path = "/api/2.0/sql/statements"

        def do_POST(self) -> None:
            sum(range(10))

    recorder.install(Handler)
    # Restore the actual service hook immediately: the test exercises only HTTP
    # profiling; it must not leave a wrapper on a shared runtime class.
    from brickhouse.sql.statements import SqlStatementExecutionService

    wrapped = SqlStatementExecutionService._execute_claimed
    try:
        Handler().do_POST()
        Handler().do_POST()
        assert recorder.profiles == {}
        Handler().do_POST()
        assert set(recorder.profiles) == {"http"}
        assert recorder.requests == 3
        assert recorder.active is None
    finally:
        SqlStatementExecutionService._execute_claimed = wrapped.__wrapped__  # type: ignore[attr-defined]


def test_retained_profile_samples_recompute_the_published_breakdown() -> None:
    root = Path(__file__).resolve().parents[2] / "artifacts/benchmarks/2026-10-22-sql-profile"
    report = json.loads((root / "results.json").read_text())
    config = report["configuration"]
    assert report["fixture_unchanged"]
    assert report["temporary_state_removed"]
    assert all(trial["process_exit_code"] == 0 for trial in report["trials"])
    assert sum(trial["verified_results"] for trial in report["trials"]) == 495
    for mode in ("baseline", "spans", "cprofile"):
        trials = [trial for trial in report["trials"] if trial["mode"] == mode]
        samples = [value for trial in trials for value in trial["client_ms"]]
        assert len(samples) == 150
        assert report["summaries"][mode]["client"]["median"] == statistics.median(samples)
        for trial in trials:
            if mode == "baseline":
                assert trial["breakdowns"] == []
                continue
            trace = root / f"{trial['trial']}-{mode}.jsonl.gz"
            records = [
                json.loads(line) for line in gzip.decompress(trace.read_bytes()).splitlines()
            ]
            assert len(records) == config["warmups"] + config["iterations"]
            for spans, recorded in zip(
                records[config["warmups"] :], trial["breakdowns"], strict=True
            ):
                parts = partition_spans(spans)
                assert parts == recorded["exclusive_wall_ms"]
                assert sum(parts.values()) == pytest.approx(recorded["server_ms"], abs=1e-9)
                assert sum(parts.values()) + recorded[
                    "client_transport_residual_ms"
                ] == pytest.approx(recorded["client_ms"], abs=1e-9)


def test_profile_analysis_recomputes_call_counts_and_published_groups() -> None:
    root = Path(__file__).resolve().parents[2] / "artifacts/benchmarks/2026-10-22-sql-profile"
    actual = analyze(root)
    recorded = json.loads((root / "analysis.json").read_text())
    recorded.pop("analysis_tool_sha256")
    assert actual == recorded
    assert actual["calls_per_request"]["sqlite_connect"] == 34
    assert actual["calls_per_request"]["sqlite_execute"] == 114
    assert actual["calls_per_request"]["sqlite_commit"] == 10
    assert sum(row["percent"] for row in actual["groups"]) == pytest.approx(100)


@pytest.mark.parametrize("failed_flag", ["fixture_unchanged", "temporary_state_removed"])
def test_profile_analysis_rejects_failed_integrity_or_cleanup(
    tmp_path: Path, failed_flag: str
) -> None:
    report = {"configuration": {}, "fixture_unchanged": True, "temporary_state_removed": True}
    report[failed_flag] = False
    (tmp_path / "results.json").write_text(json.dumps(report))
    with pytest.raises(ValueError, match="integrity or cleanup"):
        analyze(tmp_path)
