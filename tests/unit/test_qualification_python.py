from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def provisioner() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "qualification_python", ROOT / "scripts/provision_qualification_python.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(("minor", "expected"), [("3.11", "3.11.17"), ("3.12", "3.12.15")])
def test_qualification_uses_explicit_native_python_versions(
    provisioner: ModuleType, minor: str, expected: str
) -> None:
    manifest = json.loads((ROOT / "planning/client-artifacts.json").read_text())
    assert provisioner.select_version(minor, manifest["python_provisioning"]) == expected


def test_unqualified_python_minor_is_rejected(provisioner: ModuleType) -> None:
    with pytest.raises(ValueError, match="Unqualified Python minor"):
        provisioner.select_version("3.13", {"python_versions": {"3.11": "3.11.17"}})


def test_malformed_python_version_is_rejected(provisioner: ModuleType) -> None:
    with pytest.raises(ValueError, match="Invalid exact CPython version"):
        provisioner.select_version("3.11", {"python_versions": {"3.11": "3.12.15"}})
