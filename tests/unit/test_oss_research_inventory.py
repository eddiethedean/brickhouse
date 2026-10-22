"""Check offline research provenance and observed package constraints."""

from __future__ import annotations

import copy
import importlib.util
from pathlib import Path
from types import ModuleType

import pytest
from packaging.requirements import Requirement

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def validator() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "brickhouse_oss_planning_validator", ROOT / "scripts/validate_planning.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_saved_constraints_match_real_requirement_specifiers(validator: ModuleType) -> None:
    inventory = validator.read_json("planning/oss-reuse.json")
    conflicts = set()
    for comparison in inventory["constraint_comparisons"]:
        requirement = Requirement(comparison["requires_dist"])
        actual = requirement.specifier.contains(comparison["compared_version"])
        assert actual is comparison["specifier_satisfied"]
        if not actual:
            conflicts.add((comparison["candidate"], comparison["dependency"]))
            assert comparison["environment_strategy"] == "separate_client_environment"
    assert conflicts == {
        ("dbt-databricks", "databricks-sdk"),
        ("dbt-databricks", "databricks-sql-connector"),
    }


@pytest.mark.parametrize(
    ("case", "error"),
    [
        ("runtime_promotion", "cannot promote a candidate"),
        ("missing_hash", "Invalid OSS provenance SHA-256"),
        ("artifact_mismatch", "differs from registry provenance"),
        ("unknown_owner", "Unknown or missing OSS qualification owner"),
        ("shared_conflicting_client", "require a separate environment"),
        ("changed_requirement", "differs from recorded package metadata"),
        ("unpinned_source", "Missing pinned OSS source commit"),
        ("native_runtime_promotion", "Native OSS source research cannot establish runtime support"),
    ],
)
def test_research_inventory_rejects_false_or_unverifiable_claims(
    validator: ModuleType, monkeypatch: pytest.MonkeyPatch, case: str, error: str
) -> None:
    read_json = validator.read_json
    inventory = copy.deepcopy(read_json("planning/oss-reuse.json"))
    candidate = next(
        item for item in inventory["package_candidates"] if item["package"] == "mlflow"
    )
    if case == "runtime_promotion":
        candidate["runtime_qualification"] = "supported"
    elif case == "missing_hash":
        candidate["metadata_json_sha256"] = ""
    elif case == "artifact_mismatch":
        candidate["inspection"]["artifact_sha256"] = "0" * 64
    elif case == "unknown_owner":
        candidate["owner_task_ids"] = ["B999"]
    elif case == "shared_conflicting_client":
        conflict = next(
            item for item in inventory["constraint_comparisons"] if not item["specifier_satisfied"]
        )
        conflict["environment_strategy"] = "coinstall_in_core"
    elif case == "changed_requirement":
        inventory["constraint_comparisons"][0]["requires_dist"] = "databricks-sdk>=99"
    elif case == "unpinned_source":
        source = next(item for item in inventory["native_source_candidates"] if "commit" in item)
        source["commit"] = "main"
    else:
        inventory["native_source_candidates"][0]["runtime_qualification"] = "supported"
    monkeypatch.setattr(
        validator,
        "read_json",
        lambda path: inventory if path == "planning/oss-reuse.json" else read_json(path),
    )
    tasks = read_json("planning/tasks.json")["tasks"]
    with pytest.raises(validator.ValidationError, match=error):
        validator.check_oss_inventory({task["id"]: task for task in tasks})
