"""Provision hash-verified client tools before the offline qualification phase."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import stat
import subprocess
import sys
import urllib.request
from pathlib import Path
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    destination = args.destination.resolve()
    destination.mkdir(parents=True, exist_ok=True)
    machine = platform.machine().lower()
    if sys.platform == "darwin" and machine == "arm64":
        tag = "darwin_arm64"
    elif sys.platform == "linux" and machine in {"amd64", "x86_64"}:
        tag = "linux_amd64"
    else:
        raise SystemExit(f"Unqualified client platform: {sys.platform}/{machine}")
    manifest = json.loads((ROOT / "planning/client-artifacts.json").read_text())
    executables: dict[str, Path] = {}
    verified = []
    for name in ("cli", "terraform", "provider"):
        artifact = manifest[name][tag]
        archive_path = destination / artifact["filename"]
        if not archive_path.is_file():
            with urllib.request.urlopen(artifact["url"], timeout=90) as response:
                archive_path.write_bytes(response.read())
        actual = hashlib.sha256(archive_path.read_bytes()).hexdigest()
        if actual != artifact["sha256"]:
            raise SystemExit(f"Client artifact checksum mismatch: {artifact['filename']}")
        output = destination / name
        if name == "provider":
            output = (
                destination
                / "provider-mirror/registry.terraform.io/databricks/databricks"
                / "1.137.0"
                / tag
            )
        output.mkdir(parents=True, exist_ok=True)
        with ZipFile(archive_path) as archive:
            for member in archive.infolist():
                target = (output / member.filename).resolve()
                if not target.is_relative_to(output):
                    raise SystemExit("Client archive path escapes its provisioned directory")
                if stat.S_ISLNK(member.external_attr >> 16):
                    raise SystemExit("Client archive contains a symbolic link")
            archive.extractall(output)
        prefix = {
            "cli": "databricks",
            "terraform": "terraform",
            "provider": "terraform-provider-databricks",
        }[name]
        candidates = [path for path in output.rglob(prefix + "*") if path.is_file()]
        if len(candidates) != 1:
            raise SystemExit(f"Expected one {name} executable, found {candidates}")
        candidates[0].chmod(0o755)
        executables[name] = candidates[0]
        verified.append({"name": name, **artifact, "sha256_verified": True})
    connector = destination / "connector"
    subprocess.run([sys.executable, "-m", "venv", str(connector)], check=True)
    python = connector / "bin/python"
    subprocess.run(
        [
            str(python),
            "-m",
            "pip",
            "install",
            "--require-hashes",
            "-r",
            str(ROOT / "requirements/sql-connector.txt"),
        ],
        check=True,
    )
    subprocess.run([str(python), "-m", "pip", "check"], check=True)
    result = {
        "platform": tag,
        "verified_artifacts": verified,
        "environment": {
            "DATABRICKS_CLI": str(executables["cli"]),
            "BRICKHOUSE_TERRAFORM": str(executables["terraform"]),
            "BRICKHOUSE_TERRAFORM_PLUGIN_CACHE": str(destination / "provider-mirror"),
            "BRICKHOUSE_SQL_CONNECTOR_PYTHON": str(python),
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(f"Provisioned pinned clients for {tag}: {args.output}")


if __name__ == "__main__":
    main()
