"""Capability reports defer artifact/platform claims to measured release evidence."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_capability_report_uses_versioned_qualification_for_platform_claims() -> None:
    spec = importlib.util.spec_from_file_location(
        "brickhouse_capability_report", ROOT / "scripts/render_capability_report.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    report = module.render(ROOT / "planning/capabilities.json", "2026-10-22")
    interpretation = report.split("## Release interpretation", 1)[1]
    assert "`scope.json`" in interpretation
    assert "`versions.json`" in interpretation
    assert "hash-matched qualification" in interpretation
    assert "Linux x86_64 remains unqualified" not in interpretation
    assert "3.11.15" not in interpretation and "3.12.13" not in interpretation
    assert "Historical development reports do not qualify a different artifact" in interpretation
