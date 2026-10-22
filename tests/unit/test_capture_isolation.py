from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
PROBE = ROOT / "artifacts" / "probes" / "B005.3-free-edition-sql-reference.py"
FIXTURE = ROOT / "tests" / "fixtures" / "oracles" / "databricks" / "B005.3-sql-literals.json"


def load_probe_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("brickhouse_b0053_probe", PROBE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_sanitized_reference_fixture_loads_without_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for key in (
        "DATABRICKS_TOKEN",
        "DATABRICKS_HOST",
        "DATABRICKS_CONFIG_PROFILE",
        "DATABRICKS_CLIENT_ID",
        "DATABRICKS_CLIENT_SECRET",
    ):
        monkeypatch.delenv(key, raising=False)

    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))

    assert fixture["capture_id"] == "B005.3-sql-literals"
    assert fixture["result"]["schema"][1]["type_text"] == "DECIMAL(12,2)"
    assert fixture["result"]["data_array"][0][4] is None
    assert "token value not retained" in fixture["source"]["authentication_source"]
    assert "dbc-" not in json.dumps(fixture)


def test_normal_cli_ignores_inherited_databricks_credentials_and_profiles(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    hooks = tmp_path / "hooks"
    home.mkdir()
    hooks.mkdir()
    (home / ".env").write_text("DATABRICKS_TOKEN=sentinel-not-a-secret\n", encoding="utf-8")
    (home / ".databrickscfg").write_text(
        "[personal]\nhost = https://personal-workspace.invalid\ntoken = sentinel-not-a-secret\n",
        encoding="utf-8",
    )
    (hooks / "sitecustomize.py").write_text(
        """
import builtins
import os
import pathlib
import socket

_open = builtins.open
def guarded_open(file, *args, **kwargs):
    if pathlib.Path(file).name in {".env", ".databrickscfg"}:
        raise AssertionError("ordinary CLI read personal credentials")
    return _open(file, *args, **kwargs)
builtins.open = guarded_open

_path_open = pathlib.Path.open
def guarded_path_open(self, *args, **kwargs):
    if self.name in {".env", ".databrickscfg"}:
        raise AssertionError("ordinary CLI read personal credentials")
    return _path_open(self, *args, **kwargs)
pathlib.Path.open = guarded_path_open

def blocked_connect(self, address):
    raise AssertionError("ordinary CLI attempted network access")
socket.socket.connect = blocked_connect
""",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env.update(
        {
            "HOME": str(home),
            "PYTHONPATH": os.pathsep.join((str(hooks), str(SRC))),
            "DATABRICKS_HOST": "https://personal-workspace.invalid",
            "DATABRICKS_TOKEN": "sentinel-not-a-secret",
            "DATABRICKS_CONFIG_PROFILE": "personal",
        }
    )
    result = subprocess.run(
        [sys.executable, "-m", "brickhouse", "--version"],
        cwd=home,
        env=env,
        capture_output=True,
        check=False,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == "brickhouse 0.1.0\n"
    assert result.stderr == ""


def test_explicit_capture_fails_without_token_before_network(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.setattr(
        sys,
        "argv",
        [str(PROBE), "--host", "https://test-workspace.invalid"],
    )
    probe = load_probe_module()

    assert probe.main() == 1
    output = capsys.readouterr().out
    assert '"stage": "token_missing"' in output
    assert "sentinel" not in output


def test_capture_reads_only_the_explicit_token_environment_variable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    probe = load_probe_module()
    env_file = tmp_path / ".env"
    env_file.write_text("DATABRICKS_TOKEN=file-token\n", encoding="utf-8")
    monkeypatch.setenv("DATABRICKS_TOKEN", "process-token")

    assert probe._load_token(env_file) == "process-token"

    monkeypatch.delenv("DATABRICKS_TOKEN")
    assert probe._load_token(env_file) == "file-token"


def test_explicit_capture_requires_a_workspace_host(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sys, "argv", [str(PROBE)])
    probe = load_probe_module()

    with pytest.raises(SystemExit) as error:
        probe.main()

    assert error.value.code == 2
    assert "--host" in capsys.readouterr().err
