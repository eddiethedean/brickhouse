"""Recompute a local SQL wall profile from retained, sanitized traces."""

from __future__ import annotations

import argparse
import gzip
import json
import math
import pstats
from collections import defaultdict
from pathlib import Path
from typing import Any

from scripts.benchmark_emulators import digest
from scripts.profile_sql import partition_spans


def analyze(root: Path) -> dict[str, Any]:
    report = json.loads((root / "results.json").read_text())
    config = report["configuration"]
    if not report["fixture_unchanged"] or not report["temporary_state_removed"]:
        raise ValueError("Fixture integrity or cleanup verification failed")
    trials = [trial for trial in report["trials"] if trial["mode"] == "spans"]
    sample_count = 0
    totals: dict[str, float] = defaultdict(float)
    counts: dict[str, int] = defaultdict(int)
    templates: dict[str, list[float]] = defaultdict(list)
    for trial in trials:
        if trial["process_exit_code"] != 0:
            raise ValueError("Failed profiling trial")
        trace = root / f"{trial['trial']}-spans.jsonl.gz"
        records = [json.loads(line) for line in gzip.decompress(trace.read_bytes()).splitlines()]
        if len(records) != config["warmups"] + config["iterations"]:
            raise ValueError("Missing request trace")
        for spans, recorded in zip(records[config["warmups"] :], trial["breakdowns"], strict=True):
            parts = partition_spans(spans)
            if parts != recorded["exclusive_wall_ms"]:
                raise ValueError("Stored wall partition differs from raw spans")
            if not math.isclose(sum(parts.values()), recorded["server_ms"], abs_tol=1e-9):
                raise ValueError("Wall partition does not reconcile")
            sample_count += 1
            for key, value in parts.items():
                totals[key] += value
            totals["client_transport_residual"] += recorded["client_transport_residual_ms"]
            for span in spans:
                counts[span["category"]] += 1
                if "sql_template" in span:
                    templates[span["sql_template"]].append((span["end"] - span["start"]) / 1e6)
    means = {key: value / sample_count for key, value in totals.items()}
    groups = {
        "SQLite calls": sum(value for key, value in means.items() if key.startswith("sqlite_")),
        "Sparkless SQL boundary": means["engine_sql"],
        "Sparkless Arrow boundary": means["engine_arrow"],
        "SQL planning, catalog and Delta metadata (outside SQLite)": sum(
            means[key] for key in ("sql_planning", "catalog", "delta_metadata")
        ),
        "Result limits and JSON": means["result_limits"] + means["result_json"],
        "Session open and close (outside SQLite)": means["session_open"] + means["session_close"],
    }
    groups["Other lifecycle, HTTP and client residual"] = sum(means.values()) - sum(groups.values())
    baseline = report["summaries"]["baseline"]["mean_client_ms"]
    span_mean = report["summaries"]["spans"]["mean_client_ms"]
    if not math.isclose(sum(groups.values()), span_mean, abs_tol=1e-9):
        raise ValueError("Grouped breakdown differs from measured client mean")
    detailed = {}
    for role in ("worker", "http"):
        files = sorted(root.glob(f"*-cprofile.{role}.pstats"))
        if len(files) != config["rounds"]:
            raise ValueError("Missing detailed profile")
        stats = pstats.Stats(str(files[0]))
        for path in files[1:]:
            stats.add(str(path))
        # Keep unrounded native pstats times; typeshed omits this data dictionary.
        raw_stats: dict[Any, Any] = vars(stats)["stats"]
        detailed[role] = [
            dict(
                function=f"{key[0]}:{key[1]}({key[2]})",
                calls=value[1],
                self_ms_per_request=value[2] * 1000 / sample_count,
                cumulative_ms_per_request=value[3] * 1000 / sample_count,
            )
            for key, value in sorted(raw_stats.items(), key=lambda item: -item[1][2])[:30]
        ]
    return dict(
        results_sha256=digest(root / "results.json"),
        timed_span_samples=sample_count,
        mean_client_ms=span_mean,
        groups=[
            dict(stage=key, mean_ms=value, percent=value / span_mean * 100)
            for key, value in groups.items()
        ],
        mean_ms=means,
        calls_per_request={key: value / sample_count for key, value in sorted(counts.items())},
        sql_templates=[
            dict(
                sql_template=key,
                calls_per_request=len(values) / sample_count,
                inclusive_mean_ms_per_request=sum(values) / sample_count,
            )
            for key, values in sorted(templates.items(), key=lambda item: -sum(item[1]))
        ],
        observed_mean_overhead_percent=dict(
            spans=(span_mean / baseline - 1) * 100,
            cprofile=(report["summaries"]["cprofile"]["mean_client_ms"] / baseline - 1) * 100,
        ),
        detailed_profile_top_self=detailed,
        caveats=[
            "Mean wall partitions add; component medians and cumulative profiles do not.",
            "cProfile excludes warmups and profiles HTTP plus claimed execution; the worker "
            "continuation before/after _execute_claimed is covered by spans only.",
            "cProfile HTTP lock waiting overlaps worker execution and is not added to it.",
            "Sparkless boundaries include native planning, scan and Arrow work; they are "
            "not a native Rust/C++ internal CPU breakdown.",
        ],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    result = analyze(args.root)
    result["analysis_tool_sha256"] = digest(Path(__file__))
    (args.root / "analysis.json").write_text(json.dumps(result, indent=2) + "\n")
    lines = ["| Stage | Mean ms/request | Share |", "| --- | ---: | ---: |"]
    lines.extend(
        f"| {row['stage']} | {row['mean_ms']:.2f} | {row['percent']:.1f}% |"
        for row in result["groups"]
    )
    lines.append(f"| **Total, instrumented** | **{result['mean_client_ms']:.2f}** | **100%** |")
    (args.root / "breakdown.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
