"""Provision an exact native CPython build before offline qualification."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


def select_version(minor: str, configuration: dict[str, Any]) -> str:
    versions = configuration["python_versions"]
    if minor not in versions:
        raise ValueError(f"Unqualified Python minor: {minor}")
    version = str(versions[minor])
    if re.fullmatch(re.escape(minor) + r"\.\d+", version) is None:
        raise ValueError(f"Invalid exact CPython version: {version}")
    return version


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python-version", required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--uv", type=Path)
    args = parser.parse_args()
    configuration = json.loads((ROOT / "planning/client-artifacts.json").read_text())[
        "python_provisioning"
    ]
    version = select_version(args.python_version, configuration)
    uv = str(args.uv) if args.uv else shutil.which("uv")
    if uv is None:
        raise SystemExit("Install requirements/qualification-tools.txt before provisioning CPython")
    tool = subprocess.check_output([uv, "--version"], text=True).strip()
    if tool.split()[1] != configuration["version"]:
        raise SystemExit(f"Unexpected Python provisioning tool: {tool}")
    destination = args.destination.resolve()
    destination.mkdir(parents=True, exist_ok=True)
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("DATABRICKS_", "UV_", "JAVA_", "SPARK_", "PYSPARK_"))
    }
    environment.pop("PYTHONPATH", None)
    environment["UV_PYTHON_INSTALL_DIR"] = str(destination / "installations")
    environment["UV_PYTHON_BIN_DIR"] = str(destination / "bin")
    environment["UV_CACHE_DIR"] = str(destination / "cache")
    subprocess.run([uv, "--no-config", "python", "install", version], env=environment, check=True)
    executable = Path(
        subprocess.check_output(
            [uv, "--no-config", "python", "find", "--managed-python", version],
            env=environment,
            text=True,
        ).strip()
    ).resolve()
    if not executable.is_relative_to(destination):
        raise SystemExit("Managed Python escaped its qualification directory")
    facts = json.loads(
        subprocess.check_output(
            [
                str(executable),
                "-I",
                "-c",
                "import json,platform,sys; print(json.dumps({"
                "'version':platform.python_version(),'implementation':platform.python_implementation(),"
                "'build':sys.version}))",
            ],
            env=environment,
            text=True,
        )
    )
    if facts["version"] != version or facts["implementation"] != "CPython":
        raise SystemExit(f"Unexpected qualification interpreter: {facts}")
    result = {
        "provider": configuration["provider"],
        "tool": tool,
        "tool_metadata": configuration["pypi_json_url"],
        **facts,
        "executable": str(executable),
        "executable_sha256": hashlib.sha256(executable.read_bytes()).hexdigest(),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    if github_environment := os.environ.get("GITHUB_ENV"):
        with Path(github_environment).open("a") as stream:
            stream.write(f"QUALIFICATION_PYTHON={executable}\n")
    print(f"Provisioned CPython {version}: {executable}", flush=True)


if __name__ == "__main__":
    main()
