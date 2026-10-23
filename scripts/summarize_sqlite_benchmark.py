"""Validate and summarize the explicitly invoked SQLite metadata experiment."""

from __future__ import annotations

import argparse
import gzip
import json
import math
from pathlib import Path
from typing import Any

from scripts.benchmark_emulators import digest
from scripts.benchmark_sqlite import STORAGE_MODES, aggregate
from scripts.profile_sql import partition_spans


def analyze(root: Path) -> dict[str, Any]:
    report = json.loads((root / "results.json").read_text())
    config = report["configuration"]
    if not report["fixture_unchanged"] or not report["temporary_state_removed"]:
        raise ValueError("Fixture or cleanup check failed")
    if min(config["rounds"], config["iterations"], config["warmups"]) < 1:
        raise ValueError("Invalid measurement counts")
    expected = {
        (n, storage, mode)
        for n in range(config["rounds"])
        for storage in STORAGE_MODES
        for mode in ("baseline", "spans")
    }
    actual = [(t["trial"], t["storage"], t["mode"]) for t in report["trials"]]
    if len(actual) != len(expected) or set(actual) != expected:
        raise ValueError("Missing or duplicate trial")
    total_queries = config["warmups"] + config["iterations"]
    call_counts: dict[str, dict[str, int]] = {storage: {} for storage in STORAGE_MODES}
    for trial in report["trials"]:
        metadata = trial["metadata"]
        memory = trial["storage"] == "memory"
        if trial["process_exit_code"] or trial["verified_results"] != total_queries:
            raise ValueError("Failed query/process verification")
        if len(trial["client_ms"]) != config["iterations"]:
            raise ValueError("Missing client samples")
        if any(not math.isfinite(v) or v < 0 for v in trial["client_ms"]):
            raise ValueError("Invalid latency sample")
        if metadata["disk_statement_count"] != (0 if memory else total_queries):
            raise ValueError("Incorrect disk persistence")
        if metadata["journal_mode"] != ("memory" if memory else "delete"):
            raise ValueError("Unexpected SQLite journal mode")
        if metadata["synchronous"] != 2 or metadata["read_uncommitted"] != 0:
            raise ValueError("Unexpected durability/isolation PRAGMA")
        observations = metadata["fresh_thread_observations"]
        if len(observations) != 2 or observations[0] != observations[1]:
            raise ValueError("Connections did not share metadata")
        if observations[0]["statement_states"] != [["SUCCEEDED", total_queries]]:
            raise ValueError("Incorrect metadata statement states")
        for observed in (metadata, *observations):
            main = next(row for row in observed["database_list"] if row[1] == "main")
            if (main[2] == "") != memory:
                raise ValueError("SQLite database location differs from declared mode")
        if memory and (
            metadata["seed_sha256"] != metadata["seed_after_sha256"]
            or metadata["objects_after_last_close"] != 0
        ):
            raise ValueError("Memory storage modified disk seed or survived final close")
        if trial["mode"] == "baseline":
            if trial["partitions"]:
                raise ValueError("Unexpected instrumentation in baseline")
            continue
        trace = root / f"{trial['label']}.jsonl.gz"
        records = [json.loads(line) for line in gzip.decompress(trace.read_bytes()).splitlines()]
        if len(records) != total_queries:
            raise ValueError("Missing raw spans")
        for elapsed, spans, recorded in zip(
            trial["client_ms"], records[config["warmups"] :], trial["partitions"], strict=True
        ):
            parts = partition_spans(spans)
            if parts != recorded["wall_ms"]:
                raise ValueError("Stored partition differs from spans")
            if not math.isclose(
                sum(parts.values()) + recorded["transport_residual_ms"], elapsed, abs_tol=1e-9
            ):
                raise ValueError("Non-additive request accounting")
            for span in spans:
                if span["category"].startswith("sqlite_"):
                    key = span["category"]
                    counts = call_counts[trial["storage"]]
                    counts[key] = counts.get(key, 0) + 1
    summaries = aggregate(report["trials"])
    if summaries != report["summaries"]:
        raise ValueError("Stored summaries differ from raw samples")
    total_samples = config["rounds"] * config["iterations"]
    disk_median = summaries["disk"]["baseline"]["client"]["median"]
    comparisons = []
    for storage in STORAGE_MODES:
        baseline, spans = (summaries[storage][mode] for mode in ("baseline", "spans"))
        sqlite_ms = sum(
            value for key, value in spans["wall_mean_ms"].items() if key.startswith("sqlite_")
        )
        comparisons.append(
            dict(
                storage=storage,
                baseline=baseline["client"],
                mean_baseline_ms=baseline["mean_ms"],
                spans=spans["client"],
                mean_spans_ms=spans["mean_ms"],
                speedup_vs_disk_median=disk_median / baseline["client"]["median"],
                observed_mean_span_difference_percent=(spans["mean_ms"] / baseline["mean_ms"] - 1)
                * 100,
                sqlite_mean_span_ms=sqlite_ms,
                sqlite_calls_per_request={
                    key: value / total_samples for key, value in call_counts[storage].items()
                },
            )
        )
    return dict(
        results_sha256=digest(root / "results.json"),
        comparisons=comparisons,
        trial_count=len(actual),
        verified_queries=len(actual) * total_queries,
        timed_queries=len(actual) * config["iterations"],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    result = analyze(args.root)
    result["analysis_tool_sha256"] = digest(Path(__file__))
    (args.root / "analysis.json").write_text(json.dumps(result, indent=2) + "\n")
    lines = [
        "| Metadata configuration | Median ms | p95 ms | Speedup vs disk |",
        "| --- | ---: | ---: | ---: |",
    ]
    lines.extend(
        f"| {row['storage']} | {row['baseline']['median']:.2f} | "
        f"{row['baseline']['p95']:.2f} | {row['speedup_vs_disk_median']:.2f}× |"
        for row in result["comparisons"]
    )
    (args.root / "comparison.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
