"""Regression checks for the emulator-first deployment planning boundary."""

from __future__ import annotations

import copy
import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def validator() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "brickhouse_planning_validator", ROOT / "scripts/validate_planning.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_current_plan_keeps_emulator_and_deployment_tracks_separate(validator: ModuleType) -> None:
    report = validator.validate()
    assert report["release_phase_count"] == 9
    assert report["post_stability_stage_count"] == 7
    assert report["optional_cluster_stage_count"] == 6
    assert report["emulator_stability_task"] == "B049"


@pytest.mark.parametrize(
    ("task_id", "field", "value", "error"),
    [
        ("B050", "depends_on", ["B042"], "bypasses emulator stability"),
        ("B049", "depends_on", ["B048"], "must follow consolidated emulator conformance"),
        ("B050", "status", "in_progress", "started before emulator stability"),
        ("B042", "extra_dependency", "B050", "cycle"),
        ("B039", "extra_dependency", "B040", "scheduled after"),
        ("B057", "depends_on", ["B056"], "bypasses emulator stability"),
        ("B058", "status", "in_progress", "started before emulator stability"),
        ("B055", "extra_dependency", "B062", "scheduled after"),
        (
            "B062",
            "depends_on",
            ["B061"],
            "requires qualified engine B061 and remote execution B053",
        ),
        (
            "B062",
            "depends_on",
            ["B053"],
            "requires qualified engine B061 and remote execution B053",
        ),
    ],
)
def test_invalid_track_dependencies_are_rejected(
    validator: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    task_id: str,
    field: str,
    value: Any,
    error: str,
) -> None:
    read_json = validator.read_json
    manifest = copy.deepcopy(read_json("planning/tasks.json"))
    task = next(task for task in manifest["tasks"] if task["id"] == task_id)
    if field == "extra_dependency":
        task["depends_on"].append(value)
    else:
        task[field] = value
    monkeypatch.setattr(
        validator,
        "read_json",
        lambda path: manifest if path == "planning/tasks.json" else read_json(path),
    )
    with pytest.raises(validator.ValidationError, match=error):
        validator.validate()


@pytest.mark.parametrize(
    ("before", "after", "error"),
    [
        (
            "| SH.1 | Unassigned |",
            "| SH.1 | 1.0.0 |",
            "versions require a separate scope decision",
        ),
        ("| SH.6 |", "| SH.7 |", "Expected post-stability stages"),
        ("| B055 | B054 |", "| B050 | B054 |", "Duplicate release assignment"),
        (
            "| SC.0 | Unassigned |",
            "| SC.0 | 1.0.0 |",
            "Cluster versions require a separate scope decision",
        ),
        ("| SC.5 |", "| SC.6 |", "Expected optional cluster stages"),
        ("| B062 | B061, B053 |", "| B057 | B061, B053 |", "Duplicate release assignment"),
    ],
)
def test_invalid_deployment_roadmap_assignments_are_rejected(
    validator: ModuleType, before: str, after: str, error: str
) -> None:
    roadmap = (ROOT / "ROADMAP.md").read_text(encoding="utf-8")
    assert before in roadmap
    manifest = validator.read_json("planning/tasks.json")
    parents = {task["id"] for task in manifest["tasks"] if "parent_id" not in task}
    with pytest.raises(validator.ValidationError, match=error):
        validator.task_release_assignments(roadmap.replace(before, after), parents)


@pytest.mark.parametrize("parent_id", ["B050", "B057"])
def test_future_deployment_and_cluster_children_inherit_their_later_stage(
    validator: ModuleType, monkeypatch: pytest.MonkeyPatch, parent_id: str
) -> None:
    read_json = validator.read_json
    manifest = copy.deepcopy(read_json("planning/tasks.json"))
    parent = next(task for task in manifest["tasks"] if task["id"] == parent_id)
    assert not parent["children"], "Update this fixture after the future epic is decomposed"
    child = copy.deepcopy(parent)
    child.update(
        id=f"{parent_id}.1",
        kind="task",
        parent_id=parent_id,
        parent_criteria=list(range(1, len(parent["acceptance_criteria"]) + 1)),
    )
    parent["children"] = [child["id"]]
    manifest["tasks"].append(child)
    monkeypatch.setattr(
        validator,
        "read_json",
        lambda path: manifest if path == "planning/tasks.json" else read_json(path),
    )
    # The fixture tests phase inheritance, not a nonexistent future Markdown task section.
    monkeypatch.setattr(validator, "check_backlog", lambda tasks: None)
    original = read_json("planning/tasks.json")
    original_children = sum("parent_id" in task for task in original["tasks"])
    report = validator.validate()
    assert report["child_task_count"] == original_children + 1
    assert report["phase_0_1_child_task_count"] < report["child_task_count"]


@pytest.mark.parametrize(
    "mutation", ["missing-family", "missing-selector", "changed-requirement", "missing-gate"]
)
def test_incomplete_release_requirements_are_rejected(
    validator: ModuleType, monkeypatch: pytest.MonkeyPatch, mutation: str
) -> None:
    read_json = validator.read_json
    manifest = copy.deepcopy(read_json("planning/release-requirements.json"))
    if mutation == "missing-family":
        manifest["families"].pop()
    elif mutation == "missing-selector":
        manifest["groups"]["G4"].append("tests/unit/test_cli.py::test_nonexistent")
    elif mutation == "changed-requirement":
        manifest["families"][0]["requirement"] = "weakened requirement"
    else:
        del manifest["groups"]["G4"]
    monkeypatch.setattr(
        validator,
        "read_json",
        lambda path: manifest if path == "planning/release-requirements.json" else read_json(path),
    )
    with pytest.raises(validator.ValidationError):
        validator.check_release_requirements()
