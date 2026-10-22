"""Render the point-in-time capability registry as a reviewable Markdown report."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


def markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ").strip()


def render(source: Path, generated_date: str) -> str:
    manifest = json.loads(source.read_text(encoding="utf-8"))
    capabilities: list[dict[str, Any]] = manifest["capabilities"]
    counts = Counter(str(item["status"]) for item in capabilities)
    statuses = ", ".join(f"{status}: {count}" for status, count in sorted(counts.items()))
    lines = [
        "# BrickHouse capability snapshot",
        "",
        f"Generated: **{generated_date}** from `{source.relative_to(ROOT)}`.",
        "",
        f"Registry entries: **{len(capabilities)}** ({statuses}). No entry is fully supported.",
        "",
        "This report describes the selected local profiles recorded in the registry. A `partial`",
        "entry is limited to its exact tested behavior and versions. `simulated` describes local",
        "infrastructure metadata, not a real Databricks resource. `planned` is not implemented.",
        "The complete test IDs, versions, evidence references, profiles, and limitations are in",
        "the adjacent `capabilities.json` snapshot.",
        "",
        "| ID | Domain | Capability | State | Target scope | Current limits |",
        "|---|---|---|---|---|---|",
    ]
    for item in capabilities:
        limits = "<br>".join(markdown_cell(str(limit)) for limit in item.get("limitations", []))
        lines.append(
            "| {id} | {domain} | {name} | {status} | {scope} | {limits} |".format(
                id=markdown_cell(str(item["id"])),
                domain=markdown_cell(str(item["domain"])),
                name=markdown_cell(str(item["name"])),
                status=markdown_cell(str(item["status"])),
                scope=markdown_cell(str(item["target_scope"])),
                limits=limits,
            )
        )
    lines.extend(
        [
            "",
            "## Release interpretation",
            "",
            "Capability states do not establish broad Databricks compatibility. The versioned",
            "release bundle's `scope.json`, `versions.json`, and hash-matched qualification",
            "reports define the exact artifact and OS/Python/client profiles that passed.",
            "Historical development reports do not qualify a different artifact.",
            "No remote Databricks runtime, cloud compute,",
            "or caller-authentication compatibility is claimed.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path("planning/capabilities.json"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--date", required=True, help="Point-in-time date in YYYY-MM-DD form")
    args = parser.parse_args()
    source = (ROOT / args.source).resolve() if not args.source.is_absolute() else args.source
    output = (ROOT / args.output).resolve() if not args.output.is_absolute() else args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render(source, args.date), encoding="utf-8")
    print(f"Wrote {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
