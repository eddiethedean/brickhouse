"""Qualify exact built wheels in separate offline base and notebook environments."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


def run(command: list[str], environment: dict[str, str]) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        cwd=ROOT,
        env=environment,
        check=False,
        text=True,
        capture_output=True,
        timeout=1200,
    )
    if result.returncode:
        print(result.stdout[-12000:])
        print(result.stderr[-4000:])
        raise RuntimeError(f"Qualification command failed ({result.returncode}): {command}")
    return result


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", type=Path, default=ROOT / "dist")
    parser.add_argument("--clients", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--full-source", action="store_true")
    args = parser.parse_args()
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    wheels = list(args.artifact_dir.glob("*.whl"))
    sources = list(args.artifact_dir.glob("*.tar.gz"))
    if len(wheels) != 1 or len(sources) != 1:
        raise SystemExit("Qualification requires exactly one wheel and source distribution")
    wheel, sdist = wheels[0].resolve(), sources[0].resolve()
    requirements = json.loads((ROOT / "planning/release-requirements.json").read_text())
    clients = json.loads(args.clients.read_text())
    environment = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("DATABRICKS_", "JAVA_", "SPARK_", "PYSPARK_", "PYTEST_"))
    }
    environment.pop("PYTHONPATH", None)
    environment.update(clients["environment"])
    environment["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    environment["RUST_LOG"] = "error"
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout.strip()
    provenance = {
        "candidate_wheel_sha256": sha256(wheel),
        "source_distribution_sha256": sha256(sdist),
        "source_commit": revision,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "databricks_account_used": False,
    }
    reports: list[dict[str, Any]] = []
    summary: dict[str, Any] = {**provenance, "result": "failed", "groups": reports}
    try:
        with tempfile.TemporaryDirectory(prefix="brickhouse-qualification-") as temporary:
            work = Path(temporary)
            environment["HOME"] = str(work / "home")
            Path(environment["HOME"]).mkdir()
            for lane in ("base", "notebooks"):
                venv = work / lane
                run([sys.executable, "-m", "venv", str(venv)], environment)
                python = venv / "bin/python"
                candidate = str(wheel) + ("[notebooks]" if lane == "notebooks" else "")
                run([str(python), "-m", "pip", "install", candidate], environment)
                run([str(python), "-m", "pip", "check"], environment)
                run([str(venv / "bin/brickhouse"), "--version"], environment)
                run(
                    [
                        str(python),
                        "-I",
                        "-c",
                        "import brickhouse,importlib.metadata as m,importlib.util as u; "
                        "assert brickhouse.__version__ == m.version('brickhouse'); "
                        "names=('ipykernel','jupyter_client','nbclient','nbformat'); "
                        f"assert all((u.find_spec(n) is not None) == {lane == 'notebooks'} "
                        "for n in names)",
                    ],
                    environment,
                )
                run(
                    [
                        str(python),
                        "-m",
                        "pip",
                        "install",
                        "--require-hashes",
                        "-r",
                        str(ROOT / "requirements/dev.txt"),
                    ],
                    environment,
                )
                if lane == "base":
                    run(
                        [
                            str(python),
                            "-m",
                            "pip",
                            "uninstall",
                            "-y",
                            "ipykernel",
                            "jupyter-client",
                            "nbclient",
                            "nbformat",
                        ],
                        environment,
                    )
                run([str(python), "-m", "pip", "install", "--no-deps", str(wheel)], environment)
                run([str(python), "-m", "pip", "check"], environment)
                installed = json.loads(
                    run(
                        [
                            str(python),
                            "-I",
                            "-c",
                            "import brickhouse,importlib.metadata as m,json; "
                            "print(json.dumps({'brickhouse':m.version('brickhouse'),"
                            "'sparkless':m.version('sparkless'),'import':brickhouse.__file__}))",
                        ],
                        environment,
                    ).stdout
                )
                for name, selectors in requirements["groups"].items():
                    if name == "Notebook" and lane == "base":
                        continue
                    selectors = list(selectors)
                    if name == "G4" and lane == "notebooks":
                        selectors += requirements["notebook_extra_g4_selectors"]
                    report_path = output / f"{lane}-{name.lower().replace('+', '-')}.json"
                    command = [
                        sys.executable,
                        str(ROOT / "scripts/verify_g1_local_only.py"),
                        "--python",
                        str(python),
                        "--lane",
                        lane,
                        "--artifact",
                        str(report_path),
                        "--tests",
                        *selectors,
                    ]
                    run(command, environment)
                    report = json.loads(report_path.read_text())
                    report.update(provenance)
                    report.update({"group": name, "installed_versions": installed})
                    report_path.write_text(json.dumps(report, indent=2) + "\n")
                    if report["result"] != "passed" or report["skipped_cases"]:
                        raise RuntimeError(
                            f"Required offline group failed or skipped: {lane}/{name}"
                        )
                    reports.append(
                        {
                            "lane": lane,
                            "group": name,
                            "passed_cases": report["passed_cases"],
                            "report": report_path.name,
                        }
                    )
                    print(f"PASS {lane} {name}: {report['passed_cases']}", flush=True)
                if lane == "notebooks" and args.full_source:
                    source_environment = {**environment, "PYTHONPATH": str(ROOT / "src")}
                    xml_path = output / "full-source.xml"
                    result = run(
                        [
                            str(python),
                            "-m",
                            "pytest",
                            "-q",
                            "--tb=short",
                            f"--junitxml={xml_path}",
                            "tests",
                        ],
                        source_environment,
                    )
                    xml = ET.parse(xml_path)
                    counts = {
                        key: sum(int(x.get(key, "0")) for x in xml.getroot().iter("testsuite"))
                        for key in ("tests", "errors", "failures", "skipped")
                    }
                    for suite in xml.getroot().iter("testsuite"):
                        suite.attrib.pop("hostname", None)
                    xml.write(xml_path, encoding="unicode")
                    if counts["skipped"] or counts["failures"] or counts["errors"]:
                        raise RuntimeError(f"Full required suite failed or skipped: {counts}")
                    summary["full_source_suite"] = {"counts": counts, "stdout": result.stdout}
                    print(f"PASS full source: {counts['tests']}", flush=True)
        summary["result"] = "passed"
    except (RuntimeError, subprocess.SubprocessError) as error:
        summary["error"] = str(error)
        print(str(error), file=sys.stderr)
    summary["matrix_passed_cases"] = sum(r["passed_cases"] for r in reports)
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return 0 if summary["result"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
