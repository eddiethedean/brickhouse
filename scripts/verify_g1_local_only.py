from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
GUARDED_TEST = "tests/integration/test_g1_shared_table_restart.py"
FORBIDDEN_DISTRIBUTIONS = {"pyspark", "delta-spark"}
NOTEBOOK_DISTRIBUTIONS = {
    "ipykernel": "7.4.0",
    "jupyter-client": "8.10.0",
    "nbclient": "0.11.0",
    "nbformat": "5.11.1",
}
SAFE_DATABRICKS_ENVIRONMENT_KEYS = {"DATABRICKS_CLI"}
JAVA_SPARK_ENVIRONMENT_KEYS = {
    "JAVA_HOME",
    "JRE_HOME",
    "SPARK_HOME",
    "PYSPARK_HOME",
    "SPARK_LOCAL_IP",
}


def run_isolated(
    command: list[str], environment: dict[str, str], guard_dir: Path
) -> subprocess.CompletedProcess[str]:
    """Run in an OS network boundary; Linux tests drop privileges before execution."""
    if sys.platform == "darwin":
        sandbox_exec = shutil.which("sandbox-exec")
        if sandbox_exec is None:
            raise RuntimeError("macOS qualification requires sandbox-exec")
        profile = (
            "(version 1) (allow default) (deny network-outbound) "
            '(allow network-outbound (remote ip "localhost:*")) '
            '(allow network-outbound (remote ip "127.0.0.1:*")) '
            f'(allow network-outbound (subpath "{guard_dir.resolve()}"))'
        )
        return subprocess.run(
            [sandbox_exec, "-p", profile, *command],
            cwd=ROOT,
            env=environment,
            check=False,
            text=True,
            capture_output=True,
        )
    if sys.platform != "linux":
        raise RuntimeError("offline qualification supports Linux and macOS")
    sudo, unshare, ip = (shutil.which(name) for name in ("sudo", "unshare", "ip"))
    if not sudo or not unshare or not ip or os.getuid() == 0:
        raise RuntimeError(
            "Linux qualification requires a non-root user and passwordless sudo/unshare/ip"
        )
    # The root-only launcher configures a fresh network namespace, then restores
    # the invoking user's groups/UID before loading any candidate or test code.
    launcher = (
        "import json,os,subprocess,sys; p=json.load(sys.stdin); "
        "subprocess.run([p['ip'],'link','set','lo','up'],check=True); "
        "os.setgroups(p['groups']); os.setgid(p['gid']); os.setuid(p['uid']); "
        "os.execve(p['command'][0],p['command'],p['environment'])"
    )
    return subprocess.run(
        [sudo, "-n", unshare, "--net", "--", sys.executable, "-I", "-c", launcher],
        cwd=ROOT,
        env={"PATH": os.defpath},
        input=json.dumps(
            {
                "ip": ip,
                "groups": os.getgroups(),
                "gid": os.getgid(),
                "uid": os.getuid(),
                "command": command,
                "environment": environment,
            }
        ),
        check=False,
        text=True,
        capture_output=True,
    )


def _guard_source() -> str:
    return r"""from __future__ import annotations

import ipaddress
import json
import os
import sys

_LOG = os.environ["BRICKHOUSE_LOCAL_GUARD_LOG"]
_FORBIDDEN_IMPORTS = {"pyspark", "delta_spark", "deltaspark", "delta"}
_FORBIDDEN_PROCESSES = {
    "java",
    "java.exe",
    "spark-submit",
    "spark-submit.cmd",
    "docker",
    "docker.exe",
}
_FORBIDDEN_ENV_FILES = {".env", ".env.local", ".env.development", ".env.production"}

def _record(event, **details):
    with open(_LOG, "a", encoding="utf-8") as output:
        output.write(json.dumps({"event": event, **details}, sort_keys=True) + "\n")

def _loopback(host):
    if host is None:
        return False
    value = str(host).lower().rstrip(".")
    if value == "localhost":
        return True
    try:
        return ipaddress.ip_address(value.split("%", 1)[0]).is_loopback
    except ValueError:
        return False

def _audit(event, args):
    if event == "import":
        name = str(args[0]).split(".", 1)[0]
        if name in _FORBIDDEN_IMPORTS:
            _record("forbidden_import", module=name)
            raise RuntimeError("local G1 guard forbids full Spark/PySpark imports: " + name)
        if name == "brickhouse":
            filename = args[1] if len(args) > 1 else None
            _record("brickhouse_import", filename=str(filename))
    elif event == "open":
        path = args[0]
        if isinstance(path, (str, bytes, os.PathLike)):
            name = os.path.basename(os.fsdecode(path)).lower()
            if name in _FORBIDDEN_ENV_FILES or name.startswith(".env."):
                _record("blocked_env_file", filename=name)
                raise OSError("offline candidate guard forbids loading .env files")
    elif event == "socket.getaddrinfo":
        host = args[0]
        if not _loopback(host):
            _record("blocked_dns", host=str(host))
            raise OSError("local G1 guard permits loopback name resolution only")
        _record("loopback_resolution", host=str(host))
    elif event == "socket.connect":
        address = args[1]
        host = address[0] if isinstance(address, tuple) and address else str(address)
        port = address[1] if isinstance(address, tuple) and len(address) > 1 else None
        if not _loopback(host):
            _record("blocked_connection", host=str(host), port=port)
            raise OSError("local G1 guard permits loopback connections only")
        _record("loopback_connection", host=str(host), port=port)
    elif event == "subprocess.Popen":
        executable = args[0]
        command = args[1]
        names = []
        command_name = command[0] if isinstance(command, (tuple, list)) and command else None
        for candidate in (executable, command_name):
            if candidate is not None:
                names.append(os.path.basename(os.fsdecode(candidate)).lower())
        _record("subprocess", executable=names[0] if names else "unknown")
        if any(name in _FORBIDDEN_PROCESSES for name in names):
            _record("forbidden_process", executables=names)
            raise OSError("local G1 guard forbids Java, spark-submit, and Docker processes")

sys.addaudithook(_audit)
"""


def _read_events(log_path: Path) -> list[dict[str, Any]]:
    if not log_path.exists():
        return []
    return [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run an installed BrickHouse candidate with loopback-only networking."
    )
    parser.add_argument(
        "--python",
        type=Path,
        default=Path(sys.executable),
        help="Python executable containing the candidate wheel and test drivers",
    )
    parser.add_argument(
        "--tests",
        nargs="+",
        default=[GUARDED_TEST],
        help="pytest paths or node IDs to run (defaults to the G1 restart workflow)",
    )
    parser.add_argument(
        "--lane",
        choices=("base", "notebooks"),
        default="base",
        help="require a base install without notebook packages or the exact notebooks extra",
    )
    parser.add_argument(
        "--artifact",
        type=Path,
        default=ROOT / "artifacts/conformance/B012.2.3-native-only-network-guard.json",
        help="JSON evidence output path",
    )
    return parser.parse_args()


def main() -> int:
    arguments = _arguments()
    if sys.platform not in {"darwin", "linux"}:
        print("G1 local-only verification requires macOS sandbox-exec or Linux network namespaces")
        return 2

    # Preserve the virtualenv entrypoint path: resolving its symlink would bypass
    # the venv's pyvenv.cfg and therefore its installed wheel/site-packages.
    python = arguments.python.expanduser().absolute()
    if not python.is_file():
        print(f"Candidate Python executable does not exist: {python}")
        return 2

    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("DATABRICKS_") and not key.startswith("PYTEST_")
    }
    removed_java_spark_environment_keys = sorted(
        key for key in environment if key in JAVA_SPARK_ENVIRONMENT_KEYS
    )
    for key in JAVA_SPARK_ENVIRONMENT_KEYS:
        environment.pop(key, None)
    for key in SAFE_DATABRICKS_ENVIRONMENT_KEYS:
        if key in os.environ:
            environment[key] = os.environ[key]

    with tempfile.TemporaryDirectory(prefix="brickhouse-g1-guard-") as temporary:
        guard_dir = Path(temporary)
        home_dir = guard_dir / "home"
        home_dir.mkdir()
        safe_bin = guard_dir / "bin"
        safe_bin.mkdir()
        for executable_name in ("openssl", "ps"):
            executable_path = shutil.which(executable_name)
            if executable_path is not None:
                (safe_bin / executable_name).symlink_to(Path(executable_path).resolve())
        log_path = guard_dir / "audit.jsonl"
        (guard_dir / "sitecustomize.py").write_text(_guard_source(), encoding="utf-8")
        environment["BRICKHOUSE_LOCAL_GUARD_LOG"] = str(log_path)
        environment["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
        environment["PYTHONPATH"] = str(guard_dir)
        environment["HOME"] = str(home_dir)
        environment["TMPDIR"] = str(guard_dir)
        environment["PATH"] = os.pathsep.join((str(python.parent), str(safe_bin)))
        environment.pop("PYTHONHOME", None)
        environment.pop("PYTEST_ADDOPTS", None)
        network_probe = run_isolated(
            [
                str(python),
                "-I",
                "-c",
                "import json,os,socket\n"
                "try:\n socket.create_connection(('192.0.2.1',9),timeout=0.25)\n"
                "except OSError:\n"
                " print(json.dumps({'non_loopback_denied':True,'uid':os.getuid()}))\n"
                "else:\n raise SystemExit('OS network isolation did not block egress')\n",
            ],
            environment,
            guard_dir,
        )
        if network_probe.returncode != 0:
            print(network_probe.stderr)
            return 1
        preflight_code = (
            "import importlib.metadata, importlib.util, json, brickhouse, sys; "
            "names = {(d.metadata.get('Name') or '').lower().replace('_', '-') "
            "for d in importlib.metadata.distributions()}; "
            "forbidden = sorted(names & {'pyspark', 'delta-spark'}); "
            "notebook_names = {'ipykernel', 'jupyter-client', 'nbclient', 'nbformat'}; "
            "notebook = {n: d.version for d in importlib.metadata.distributions() "
            "for n in [(d.metadata.get('Name') or '').lower().replace('_', '-')] "
            "if n in notebook_names}; "
            "modules = {name: importlib.util.find_spec(name) is not None for name in "
            "('pyspark', 'delta_spark', 'deltaspark', 'delta')}; "
            "print(json.dumps({'brickhouse_file': brickhouse.__file__, "
            "'brickhouse_version': importlib.metadata.version('brickhouse'), "
            "'forbidden_distributions': forbidden, 'notebook_distributions': notebook, "
            "'forbidden_importable_modules': [name for name, found in modules.items() if found], "
            "'python': sys.version}))"
        )
        preflight = run_isolated([str(python), "-c", preflight_code], environment, guard_dir)
        try:
            candidate = json.loads(preflight.stdout.strip())
        except (json.JSONDecodeError, TypeError):
            candidate = {}
        package_file = Path(str(candidate.get("brickhouse_file", ""))).resolve()
        source_package = (ROOT / "src" / "brickhouse").resolve()
        candidate_installed = bool(candidate) and not package_file.is_relative_to(ROOT.resolve())
        found_forbidden = candidate.get("forbidden_distributions", [])
        found_notebook = candidate.get("notebook_distributions", {})
        expected_notebook = NOTEBOOK_DISTRIBUTIONS if arguments.lane == "notebooks" else {}
        notebook_lane_matches = found_notebook == expected_notebook
        importable_forbidden = candidate.get("forbidden_importable_modules", [])
        if (
            preflight.returncode != 0
            or not candidate_installed
            or found_forbidden
            or not notebook_lane_matches
            or importable_forbidden
        ):
            print(
                json.dumps(
                    {
                        "result": "failed",
                        "reason": "candidate preflight failed",
                        "candidate": candidate,
                        "preflight_return_code": preflight.returncode,
                        "stderr": preflight.stderr.strip(),
                    },
                    indent=2,
                )
            )
            return 1

        artifact_path = arguments.artifact.expanduser().absolute()
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        junit_path = artifact_path.with_suffix(".xml")
        command = [
            str(python),
            "-m",
            "pytest",
            "-c",
            "/dev/null",
            "--rootdir",
            str(ROOT),
            "-q",
            f"--junitxml={junit_path}",
            *arguments.tests,
        ]
        result = run_isolated(command, environment, guard_dir)
        counts = {key: 0 for key in ("tests", "failures", "errors", "skipped")}
        if junit_path.is_file():
            junit = ET.parse(junit_path)
            for suite in junit.getroot().iter("testsuite"):
                suite.attrib.pop("hostname", None)
                for key in counts:
                    counts[key] += int(suite.get(key, "0"))
            junit.write(junit_path, encoding="unicode")
        events = _read_events(log_path)
        forbidden_events = [
            event
            for event in events
            if event["event"]
            in {
                "blocked_dns",
                "blocked_connection",
                "blocked_env_file",
                "forbidden_import",
                "forbidden_process",
            }
        ]
        connections = [event for event in events if event["event"] == "loopback_connection"]
        resolutions = [event for event in events if event["event"] == "loopback_resolution"]
        processes = [event for event in events if event["event"] == "subprocess"]
        imports = [event for event in events if event["event"] == "brickhouse_import"]
        forbidden_processes = [event for event in events if event["event"] == "forbidden_process"]
        passed = (
            result.returncode == 0
            and counts["tests"] > 0
            and not any(counts[key] for key in ("failures", "errors", "skipped"))
            and not forbidden_events
            and bool(connections)
            and bool(imports)
            and all(
                not Path(event["filename"]).resolve().is_relative_to(source_package)
                for event in imports
                if event.get("filename") not in {"None", ""}
            )
        )

        report = {
            "result": "passed" if passed else "failed",
            "lane": arguments.lane,
            "command": (
                ("sandbox-exec" if sys.platform == "darwin" else "unshare [network namespace]")
                + " [loopback-only] "
                + python.name
                + " -m pytest -c /dev/null -q "
                + " ".join(arguments.tests)
            ),
            "return_code": result.returncode,
            "argv": command,
            "os_network_probe": json.loads(network_probe.stdout),
            "counts": counts,
            "passed_cases": counts["tests"]
            - counts["failures"]
            - counts["errors"]
            - counts["skipped"],
            "skipped_cases": counts["skipped"],
            "databricks_credentials_present": False,
            "inherited_databricks_environment_key_count": sum(
                key.startswith("DATABRICKS_") and key not in SAFE_DATABRICKS_ENVIRONMENT_KEYS
                for key in os.environ
            ),
            "dotenv_loads": [event for event in events if event["event"] == "blocked_env_file"],
            "candidate_python": candidate.get("python"),
            "candidate_version": candidate.get("brickhouse_version"),
            "candidate_import": candidate.get("brickhouse_file"),
            "candidate_wheel_installed": candidate_installed,
            "forbidden_distributions_installed": found_forbidden,
            "notebook_distributions_installed": found_notebook,
            "forbidden_importable_modules": importable_forbidden,
            "java_spark_environment_keys_removed": removed_java_spark_environment_keys,
            "forbidden_events": forbidden_events,
            "network_policy": (
                (
                    "macOS sandbox denies outbound networking except localhost/127.0.0.1; "
                    if sys.platform == "darwin"
                    else (
                        "Linux network namespace contains only an enabled loopback interface; "
                        "tests run as the original non-root user; "
                    )
                )
                + "local Unix-domain sockets are permitted; Python audit hook rejects "
                "non-loopback resolution/connections"
            ),
            "sandbox_profile": (
                (
                    "deny network-outbound; permit localhost IP sockets and Unix-domain sockets "
                    if sys.platform == "darwin"
                    else (
                        "isolated Linux network namespace; permit loopback and Unix-domain sockets "
                    )
                )
                + (
                    "under the unique per-run temporary directory"
                    if sys.platform == "darwin"
                    else "(filesystem access remains that of the invoking user)"
                )
            ),
            "process_policy": (
                "test PATH exposes only the candidate wheel environment and whitelisted local "
                "utilities; Python audit hook rejects Java, spark-submit, Docker, and full "
                "PySpark/Delta Spark imports"
            ),
            "loopback_connections": {
                "count": len(connections),
                "hosts": sorted({event["host"] for event in connections}),
            },
            "loopback_resolutions": {
                "count": len(resolutions),
                "hosts": sorted({event["host"] for event in resolutions}),
            },
            "subprocesses": {
                "count": len(processes),
                "executables": sorted({event["executable"] for event in processes}),
            },
            "forbidden_processes": forbidden_processes,
            "blocked_process_names": ["java", "spark-submit", "docker"],
            "blocked_import_roots": ["pyspark", "delta_spark", "deltaspark", "delta"],
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
        }
        artifact_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2))
        try:
            display_path = artifact_path.relative_to(ROOT)
        except ValueError:
            display_path = artifact_path
        print(f"Evidence: {display_path}")
        return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
