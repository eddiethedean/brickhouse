"""Validate complete runs and publish pooled statistics without discarding trials.

Run from the repository root with python -m scripts.summarize_emulator_benchmarks.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any

from scripts.benchmark_emulators import summarize

PRODUCTS = ("brickhouse", "minilake", "databricks-emulator")
METRICS = (
    "fresh_workspace_to_verified_file_ms",
    "workspace_round_trip_ms",
    "delta_aggregate_ms",
    "core_server_rss_mib",
)


def combine(reports: list[dict[str, Any]]) -> dict[str, Any]:
    if not reports:
        raise ValueError("At least one complete benchmark run is required")
    fingerprints = {json.dumps(report, sort_keys=True) for report in reports}
    if len(fingerprints) != len(reports):
        raise ValueError("The same benchmark run cannot be pooled twice")
    baseline = reports[0]
    for report in reports:
        for key in ("configuration", "provenance", "system"):
            if report[key] != baseline[key]:
                raise ValueError(f"Cannot pool runs with different {key}")
        if report["fixture"]["expected_sha256"] != baseline["fixture"]["expected_sha256"]:
            raise ValueError("Cannot pool different SQL fixture expectations")
        if report["result"] != "passed" or report["fixture_unchanged"] is not True:
            raise ValueError("A failed benchmark or mutated fixture cannot be published")
        if report["temporary_state_removed"] is not True:
            raise ValueError("Benchmark temporary state cleanup was not confirmed")
        configuration = report["configuration"]
        rounds = configuration["rounds"]
        iterations = configuration["iterations"]
        warmups = configuration["warmups"]
        if len(report["trials"]) != rounds * len(PRODUCTS):
            raise ValueError("Missing or extra benchmark trials")
        for product in PRODUCTS:
            trials = [trial for trial in report["trials"] if trial["product"] == product]
            if len(trials) != rounds or {t["round"] for t in trials} != set(range(1, rounds + 1)):
                raise ValueError("Missing or duplicate product rounds")
            for trial in trials:
                if trial["status"] != "passed" or trial["sql_status"] != "measured":
                    raise ValueError("Every published trial must pass both measured workloads")
                if trial["workspace_file_survived_restart"] is not True:
                    raise ValueError("Workspace file did not survive restart")
                if trial["verified_sql_results"] != iterations + warmups:
                    raise ValueError("Incomplete SQL correctness verification")
                if trial["verified_file_round_trips"] != iterations + warmups + 1:
                    raise ValueError("Incomplete file correctness verification")
                for metric in ("workspace_round_trip_ms", "delta_aggregate_ms"):
                    if len(trial[metric]) != iterations:
                        raise ValueError("Missing timed samples")
    combined: dict[str, Any] = {
        "schema_version": 1,
        "result": "passed",
        "runs": len(reports),
        "configuration_per_run": baseline["configuration"],
        "system": baseline["system"],
        "provenance": baseline["provenance"],
        "expected_sql_result_sha256": baseline["fixture"]["expected_sha256"],
        "total_trials": sum(len(report["trials"]) for report in reports),
        "summary": {},
    }
    for product in PRODUCTS:
        trials = [t for report in reports for t in report["trials"] if t["product"] == product]
        summary: dict[str, Any] = {"passed_trials": len(trials)}
        for metric in METRICS:
            samples = [
                sample
                for trial in trials
                for sample in (
                    trial[metric] if isinstance(trial[metric], list) else [trial[metric]]
                )
            ]
            summary[metric] = summarize(samples)
        combined["summary"][product] = summary
    return combined


def render_table(combined: dict[str, Any]) -> str:
    lines = [
        "| Workload | BrickHouse | MiniLake | Go emulator |",
        "| --- | --- | --- | --- |",
    ]
    labels = {
        "fresh_workspace_to_verified_file_ms": "Fresh workspace → first verified 64 KiB file",
        "workspace_round_trip_ms": "Warm 64 KiB file import + export",
        "delta_aggregate_ms": "Warm filtered/grouped SQL over 10,000 Delta rows",
        "core_server_rss_mib": "Idle core API server RSS (median only)",
    }
    for metric in METRICS:
        values = []
        for product in PRODUCTS:
            stats = combined["summary"][product][metric]
            value = (
                f"{stats['median']:.2f} MiB"
                if metric == "core_server_rss_mib"
                else (f"{stats['median']:.2f} / {stats['p95']:.2f} ms")
            )
            values.append(value)
        lines.append("| " + " | ".join([labels[metric], *values]) + " |")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="append", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    reports = [json.loads(path.read_text()) for path in args.run]
    combined = combine(reports)
    combined["source_runs"] = [
        {
            "path": os.path.relpath(path, args.output_dir),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "harness_sha256": report["harness_sha256"],
            "native_agent_launcher_sha256": report["native_agent_launcher_sha256"],
        }
        for path, report in zip(args.run, reports, strict=True)
    ]
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "combined-results.json").write_text(json.dumps(combined, indent=2) + "\n")
    (args.output_dir / "results-table.md").write_text(render_table(combined))
    print(render_table(combined))


if __name__ == "__main__":
    main()
