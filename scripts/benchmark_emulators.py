"""Opt-in local emulator comparison; never imports or contacts a real workspace.

Competitor installation/builds are explicit preparation steps. No benchmark is
part of ordinary test discovery. See docs/benchmarks/local-emulators.md.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import http.client
import json
import math
import os
import platform
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode


class BenchmarkError(RuntimeError):
    """A measured operation failed correctness or readiness checks."""


def summarize(samples: list[float]) -> dict[str, float | int]:
    if not samples or any(not math.isfinite(value) or value < 0 for value in samples):
        raise ValueError("Samples must be a nonempty list of finite nonnegative values")
    ordered = sorted(samples)
    return {
        "count": len(samples),
        "median": statistics.median(ordered),
        "p95": ordered[math.ceil(0.95 * len(ordered)) - 1],
        "min": ordered[0],
        "max": ordered[-1],
    }


def workspace_payload(revision: int, size: int) -> bytes:
    prefix = f"# local-emulator-benchmark revision={revision}\n".encode()
    if size < len(prefix):
        raise ValueError("Payload size is too small for its revision marker")
    repeated = b"# deterministic synthetic workspace file\n"
    return (
        prefix + (repeated * math.ceil((size - len(prefix)) / len(repeated)))[: size - len(prefix)]
    )


def normalize_rows(response: dict[str, Any], *, agent_text: bool = False) -> list[list[int]]:
    if response.get("status", {}).get("state") != "SUCCEEDED":
        raise BenchmarkError(f"Statement did not succeed: {response.get('status')}")
    if agent_text:
        text = response.get("result", {}).get("text")
        if not isinstance(text, str):
            raise BenchmarkError("Expected the Go server's agent JSON text envelope")
        try:
            envelope = json.loads(text)
        except ValueError as error:
            raise BenchmarkError("Invalid agent JSON result envelope") from error
        if not isinstance(envelope, dict):
            raise BenchmarkError("Agent JSON result envelope must be an object")
        rows = envelope.get("data")
    else:
        rows = response.get("result", {}).get("data_array")
    if not isinstance(rows, list) or not rows:
        raise BenchmarkError("Statement did not return a nonempty data_array")
    normalized = []
    for row in rows:
        if not isinstance(row, list) or len(row) != 3:
            raise BenchmarkError("Aggregate result must have three columns")
        if any(isinstance(value, bool) or not isinstance(value, (str, int)) for value in row):
            raise BenchmarkError("Expected exact integer aggregate values")
        try:
            normalized.append([int(value) for value in row])
        except ValueError as error:
            raise BenchmarkError("Invalid integer aggregate value") from error
    return normalized


def clean_environment() -> dict[str, str]:
    # Do not inherit cloud credentials, real host selectors, PYTHONPATH, or an
    # optional dependency override. Child CWDs are fresh temporary directories.
    allowed = {"PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "SYSTEMROOT"}
    environment = {key: value for key, value in os.environ.items() if key in allowed}
    environment["PYTHONNOUSERSITE"] = "1"
    return environment


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def free_port() -> int:
    with socket.socket() as candidate:
        candidate.bind(("127.0.0.1", 0))
        return int(candidate.getsockname()[1])


class Client:
    def __init__(self, port: int, token: str = "local-benchmark") -> None:
        self.port = port
        self.token = token

    def call(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=30)
        payload = json.dumps(body).encode() if body is not None else None
        try:
            connection.request(
                method,
                path,
                payload,
                {
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json",
                    "Connection": "close",
                },
            )
            response = connection.getresponse()
            raw = response.read()
            parsed = json.loads(raw) if raw else {}
            if not 200 <= response.status < 300:
                raise BenchmarkError(f"HTTP {response.status} on {method} {path}: {parsed}")
            return parsed
        finally:
            connection.close()

    def round_trip(self, payload: bytes) -> None:
        path = "/Workspace/Shared/emulator_benchmark.py"
        self.call(
            "POST",
            "/api/2.0/workspace/import",
            {
                "path": path,
                "format": "SOURCE",
                "language": "PYTHON",
                "overwrite": True,
                "content": base64.b64encode(payload).decode(),
            },
        )
        self.verify_export(payload)

    def verify_export(self, expected: bytes) -> None:
        query = urlencode({"path": "/Workspace/Shared/emulator_benchmark.py", "format": "AUTO"})
        exported = self.call("GET", "/api/2.0/workspace/export?" + query)
        actual = base64.b64decode(exported["content"], validate=True)
        if actual != expected:
            raise BenchmarkError("Workspace export did not match the imported bytes")

    def statement(self, warehouse: str, sql: str) -> dict[str, Any]:
        result: dict[str, Any] = self.call(
            "POST",
            "/api/2.0/sql/statements",
            {
                "warehouse_id": warehouse,
                "statement": sql,
                "catalog": "bench",
                "schema": "default",
                "wait_timeout": "30s",
                "format": "JSON_ARRAY",
                "disposition": "INLINE",
            },
        )
        deadline = time.monotonic() + 60
        while result.get("status", {}).get("state") in {"PENDING", "RUNNING"}:
            if time.monotonic() >= deadline:
                raise BenchmarkError("Statement did not become terminal within 60 seconds")
            time.sleep(0.01)
            result = self.call("GET", "/api/2.0/sql/statements/" + result["statement_id"])
        if result.get("status", {}).get("state") != "SUCCEEDED":
            raise BenchmarkError(f"Statement failed: {result.get('status')}")
        return result


@dataclass(frozen=True)
class Product:
    name: str
    executable: Path
    source: str
    health: str
    sql_enabled: bool
    extension_dir: Path | None = None
    agent_url: str | None = None

    def commands(self, state: Path, port: int) -> tuple[list[str] | None, list[str]]:
        if self.name == "brickhouse":
            return (
                [str(self.executable), "init", "--workspace", str(state / "workspace")],
                [
                    str(self.executable),
                    "serve",
                    "--workspace",
                    str(state / "workspace"),
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(port),
                ],
            )
        if self.name == "minilake":
            return None, [str(self.executable), "--host", "127.0.0.1", "--port", str(port)]
        command = [
            str(self.executable),
            "-addr",
            f"127.0.0.1:{port}",
            "-data-dir",
            str(state / "data"),
            "-disable-tls",
            "-public-url",
            f"http://127.0.0.1:{port}",
        ]
        if self.agent_url is not None:
            command += ["-spark-connect-url", self.agent_url]
        return None, command

    def environment(self, state: Path) -> dict[str, str]:
        environment = clean_environment()
        if self.name == "minilake":
            environment.update(
                {
                    "MINILAKE_DATA_DIR": str(state / "data"),
                    "MINILAKE_PERSIST": "1",
                    "MINILAKE_NOTEBOOK": "0",
                    "MINILAKE_SPARK_PREWARM": "0",
                }
            )
            if self.extension_dir is not None:
                environment["MINILAKE_DUCKDB_EXTENSION_DIR"] = str(self.extension_dir)
        return environment


def start(
    product: Product, state: Path, *, initialize: bool
) -> tuple[subprocess.Popen[bytes], Client]:
    port = free_port()
    setup, command = product.commands(state, port)
    environment = product.environment(state)
    if initialize and setup is not None:
        subprocess.run(
            setup,
            cwd=state,
            env=environment,
            check=True,
            timeout=30,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    process = subprocess.Popen(
        command,
        cwd=state,
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    client = Client(port)
    try:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise BenchmarkError(f"{product.name} exited during startup: {process.returncode}")
            try:
                client.call("GET", product.health)
                if product.name == "databricks-emulator":
                    client.token = (state / "data/admin.pat").read_text().strip()
                return process, client
            except (OSError, http.client.HTTPException):
                time.sleep(0.005)
        raise BenchmarkError(f"{product.name} was not ready after 60 seconds")
    except BaseException:
        stop(process)
        raise


def stop(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def start_sail(state: Path, args: argparse.Namespace) -> tuple[list[subprocess.Popen[bytes]], str]:
    """Start fresh native Sail and pinned agent processes outside timed queries."""
    sail_port, agent_port = free_port(), free_port()
    environment = clean_environment()
    environment["SAIL_SPARK__SESSION_TIMEOUT_SECS"] = "3600"
    processes = []
    try:
        with (state / "sail.log").open("wb") as log:
            sail = subprocess.Popen(
                [
                    str(args.sail_bin),
                    "spark",
                    "server",
                    "--ip",
                    "127.0.0.1",
                    "--port",
                    str(sail_port),
                    "--directory",
                    str(state),
                ],
                cwd=state,
                env=environment,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        processes.append(sail)
        deadline = time.monotonic() + 90
        while True:
            if sail.poll() is not None:
                raise BenchmarkError("Native Sail exited: " + (state / "sail.log").read_text())
            try:
                with socket.create_connection(("127.0.0.1", sail_port), timeout=0.25):
                    break
            except OSError:
                if time.monotonic() >= deadline:
                    raise BenchmarkError("Native Sail did not become ready") from None
                time.sleep(0.02)
        with (state / "agent.log").open("wb") as log:
            agent = subprocess.Popen(
                [
                    str(args.sail_python),
                    str(Path(__file__).with_name("benchmark_sail_agent.py")),
                    "--agent-file",
                    str(args.spark_agent_source),
                    "--port",
                    str(agent_port),
                    "--spark-remote",
                    f"sc://127.0.0.1:{sail_port}",
                ],
                cwd=state,
                env=environment,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        processes.append(agent)
        client = Client(agent_port)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if agent.poll() is not None:
                raise BenchmarkError("Native agent exited: " + (state / "agent.log").read_text())
            try:
                client.call("GET", "/health")
                return processes, f"http://127.0.0.1:{agent_port}"
            except (OSError, http.client.HTTPException):
                time.sleep(0.02)
        raise BenchmarkError("Native statement agent did not become ready")
    except BaseException:
        for process in reversed(processes):
            stop(process)
        raise


def create_fixture(root: Path, count: int) -> dict[str, Any]:
    import pyarrow as pa  # type: ignore[import-untyped]
    from deltalake import write_deltalake

    location = root / "delta-fixture"
    data = {
        "group_key": pa.array([i % 100 for i in range(count)], type=pa.int64()),
        "amount": pa.array([i % 1000 for i in range(count)], type=pa.int64()),
        "active": pa.array([i % 3 != 0 for i in range(count)], type=pa.bool_()),
    }
    write_deltalake(location, pa.table(data))
    aggregates: dict[int, list[int]] = defaultdict(lambda: [0, 0])
    for i in range(count):
        if i % 3 != 0:
            aggregates[i % 100][0] += 1
            aggregates[i % 100][1] += i % 1000
    expected = [[key, *values] for key, values in sorted(aggregates.items())]
    return {
        "location": location,
        "rows": count,
        "schema": "group_key BIGINT, amount BIGINT, active BOOLEAN",
        "expected": expected,
        "expected_sha256": hashlib.sha256(json.dumps(expected).encode()).hexdigest(),
        "files": [
            {
                "path": str(p.relative_to(location)),
                "sha256": digest(p),
                "size_bytes": p.stat().st_size,
            }
            for p in sorted(location.rglob("*"))
            if p.is_file()
        ],
    }


def prepare_sql(
    client: Client, fixture: dict[str, Any], *, register_through_sql: bool
) -> tuple[str, str]:
    catalogs = client.call("GET", "/api/2.1/unity-catalog/catalogs").get("catalogs", [])
    if "bench" not in {item["name"] for item in catalogs}:
        client.call("POST", "/api/2.1/unity-catalog/catalogs", {"name": "bench"})
    schemas = client.call("GET", "/api/2.1/unity-catalog/schemas?catalog_name=bench").get(
        "schemas", []
    )
    if "default" not in {item["name"] for item in schemas}:
        client.call(
            "POST", "/api/2.1/unity-catalog/schemas", {"catalog_name": "bench", "name": "default"}
        )
    warehouse = client.call(
        "POST",
        "/api/2.0/sql/warehouses",
        {
            "name": "local-benchmark",
            "cluster_size": "2X-Small",
            "warehouse_type": "PRO",
            "enable_serverless_compute": True,
            "auto_stop_mins": 0,
        },
    )["id"]
    if register_through_sql:
        client.statement(
            warehouse,
            f"CREATE TABLE bench.default.emulator_bench ({fixture['schema']}) "
            f"USING DELTA LOCATION '{fixture['location']}'",
        )
    return warehouse, (
        "SELECT group_key, COUNT(*) AS n, SUM(amount) AS total "
        "FROM bench.default.emulator_bench WHERE active = true "
        "GROUP BY group_key ORDER BY group_key"
    )


def rss_mib(pid: int) -> float:
    value = subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], text=True).strip()
    return int(value) / 1024


def trial(
    product: Product, state: Path, fixture: dict[str, Any], args: argparse.Namespace
) -> dict[str, Any]:
    state.mkdir()
    measured: dict[str, Any] = {"product": product.name, "status": "failed"}
    process = None
    backends: list[subprocess.Popen[bytes]] = []
    try:
        before = time.perf_counter()
        process, client = start(product, state, initialize=True)
        latest = workspace_payload(0, args.payload_bytes)
        client.round_trip(latest)
        measured["fresh_workspace_to_verified_file_ms"] = (time.perf_counter() - before) * 1000
        measured["core_server_rss_mib"] = rss_mib(process.pid)
        file_samples = []
        for iteration in range(args.warmups + args.iterations):
            latest = workspace_payload(iteration + 1, args.payload_bytes)
            before = time.perf_counter()
            client.round_trip(latest)
            duration = (time.perf_counter() - before) * 1000
            if iteration >= args.warmups:
                file_samples.append(duration)
        measured["workspace_round_trip_ms"] = file_samples
        measured["verified_file_round_trips"] = 1 + args.warmups + args.iterations
        if product.sql_enabled:
            if product.name == "brickhouse":
                # External table registration is an embedded BrickHouse surface,
                # not its SQL LOCATION or table-creation HTTP profile. Close the
                # server owner before opening the documented workspace context.
                stop(process)
                process = None
                setup = (
                    "import sys\n"
                    "from brickhouse.workspace.context import WorkspaceContext\n"
                    "with WorkspaceContext.local(sys.argv[1]) as context:\n"
                    "    context.catalogs.create('bench')\n"
                    "    context.tables.register_external(\n"
                    "        'bench', 'default', 'emulator_bench', sys.argv[2])\n"
                )
                registered = subprocess.run(
                    [
                        str(product.executable.parent / "python"),
                        "-c",
                        setup,
                        str(state / "workspace"),
                        str(fixture["location"]),
                    ],
                    cwd=state,
                    env=product.environment(state),
                    timeout=30,
                    capture_output=True,
                    text=True,
                )
                if registered.returncode != 0:
                    raise BenchmarkError(
                        f"Embedded external registration failed: {registered.stderr}"
                    )
                process, client = start(product, state, initialize=False)
                measured["sql_setup"] = "Untimed embedded external registration and server reopen"
            elif product.name == "databricks-emulator":
                stop(process)
                process = None
                backends, agent_url = start_sail(state, args)
                process, client = start(
                    replace(product, agent_url=agent_url), state, initialize=False
                )
                measured["sql_setup"] = (
                    "Fresh native Sail plus pinned statement agent; Delta path read"
                )
            else:
                measured["sql_setup"] = "Untimed SQL external table registration"
            if product.name == "databricks-emulator":
                warehouse = client.call(
                    "POST",
                    "/api/2.0/sql/warehouses",
                    {"name": "local-benchmark", "cluster_size": "2X-Small"},
                )["id"]
                sql = (
                    "SELECT group_key, COUNT(*) AS n, SUM(amount) AS total "
                    f"FROM delta.`{fixture['location']}` WHERE active = true "
                    "GROUP BY group_key ORDER BY group_key"
                )
            else:
                warehouse, sql = prepare_sql(
                    client, fixture, register_through_sql=product.name != "brickhouse"
                )
            sql_samples = []
            for iteration in range(args.warmups + args.iterations):
                before = time.perf_counter()
                actual = normalize_rows(
                    client.statement(warehouse, sql),
                    agent_text=product.name == "databricks-emulator",
                )
                if actual != fixture["expected"]:
                    raise BenchmarkError(
                        "SQL aggregate differs from the deterministic expected rows"
                    )
                duration = (time.perf_counter() - before) * 1000
                if iteration >= args.warmups:
                    sql_samples.append(duration)
            measured["delta_aggregate_ms"] = sql_samples
            measured["verified_sql_results"] = args.warmups + args.iterations
            measured["sql_status"] = "measured"
        else:
            measured["sql_status"] = "not_configured"
            measured["sql_reason"] = (
                "Core Go API only; Sail/statement agent and UC OSS were not provisioned"
            )
        stop(process)
        process = None
        process, client = start(product, state, initialize=False)
        client.verify_export(latest)
        measured["workspace_file_survived_restart"] = True
        measured["status"] = "passed"
    except (BenchmarkError, OSError, ValueError, subprocess.SubprocessError, KeyError) as error:
        measured["error"] = f"{type(error).__name__}: {error}"
    finally:
        if process is not None:
            stop(process)
        for backend in reversed(backends):
            stop(backend)
    return measured


def system_info() -> dict[str, Any]:
    info: dict[str, Any] = {
        "os": platform.platform(),
        "architecture": platform.machine(),
        "controller_python": platform.python_version(),
        "logical_cpus": os.cpu_count(),
    }
    if sys.platform == "darwin":
        for name, key in [("machdep.cpu.brand_string", "cpu"), ("hw.memsize", "memory_bytes")]:
            info[key] = subprocess.check_output(["sysctl", "-n", name], text=True).strip()
    return info


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--brickhouse-bin", type=Path, required=True)
    parser.add_argument("--minilake-bin", type=Path, required=True)
    parser.add_argument("--databricks-emulator-bin", type=Path, required=True)
    parser.add_argument("--minilake-extension-dir", type=Path, required=True)
    parser.add_argument("--sail-bin", type=Path)
    parser.add_argument("--sail-python", type=Path)
    parser.add_argument("--spark-agent-source", type=Path)
    parser.add_argument("--provenance", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--iterations", type=int, default=25)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--payload-bytes", type=int, default=65536)
    parser.add_argument("--rows", type=int, default=10000)
    args = parser.parse_args()
    backend_args = [args.sail_bin, args.sail_python, args.spark_agent_source]
    if any(backend_args) and not all(backend_args):
        parser.error("Native Go SQL measurement requires all three Sail/agent arguments")
    for key in ["sail_bin", "sail_python", "spark_agent_source"]:
        if getattr(args, key) is not None:
            setattr(args, key, getattr(args, key).absolute())
    if min(args.rounds, args.iterations) < 1 or args.warmups < 0 or args.rows < 100:
        parser.error(
            "Require positive rounds/iterations, nonnegative warmups, and at least 100 rows"
        )
    workspace_payload(0, args.payload_bytes)
    provenance = json.loads(args.provenance.read_text())
    products = [
        Product(
            "brickhouse",
            args.brickhouse_bin.resolve(),
            provenance["brickhouse"]["source"],
            "/health",
            True,
        ),
        Product(
            "minilake",
            args.minilake_bin.resolve(),
            provenance["minilake"]["source"],
            "/_minilake/health",
            True,
            args.minilake_extension_dir.resolve(),
        ),
        Product(
            "databricks-emulator",
            args.databricks_emulator_bin.resolve(),
            provenance["databricks-emulator"]["source"],
            "/health",
            all(backend_args),
        ),
    ]
    if any(not product.executable.is_file() for product in products):
        parser.error("All server executables must already exist")
    args.output_dir.mkdir(parents=True, exist_ok=False)
    configuration = {key: value for key, value in vars(args).items() if not isinstance(value, Path)}
    result: dict[str, Any] = {
        "schema_version": 1,
        "started_at": datetime.now(UTC).isoformat(),
        "system": system_info(),
        "configuration": configuration,
        "provenance": provenance,
        "harness_sha256": digest(Path(__file__)),
        "native_agent_launcher_sha256": digest(Path(__file__).with_name("benchmark_sail_agent.py")),
        "trials": [],
        "summary": {},
        "scope": "Serial local native-process core API and shared external Delta read workloads",
        "network": (
            "Explicit IPv4 loopback HTTP only; no pooled connections; TLS disabled for all products"
        ),
        "sql_limitations": (
            "Go SQL uses native Sail and a pinned source statement agent, direct Delta path reads, "
            "and JSON text normalization; core API timings/RSS exclude those engine processes"
            if all(backend_args)
            else "Go SQL engine not provisioned; no timing inferred"
        ),
        "databricks_account_used": False,
    }
    with tempfile.TemporaryDirectory(prefix="brickhouse-emulator-benchmark-") as directory:
        root = Path(directory)
        fixture = create_fixture(root, args.rows)
        result["fixture"] = {key: value for key, value in fixture.items() if key != "location"}
        for round_number in range(args.rounds):
            order = (
                products[round_number % len(products) :] + products[: round_number % len(products)]
            )
            for product in order:
                print(f"round {round_number + 1}/{args.rounds}: {product.name}", flush=True)
                measured = trial(product, root / f"{round_number}-{product.name}", fixture, args)
                measured["round"] = round_number + 1
                result["trials"].append(measured)
                (args.output_dir / "results.json").write_text(json.dumps(result, indent=2) + "\n")
                print(
                    f"  {measured['status']}: {measured.get('error', 'content/results verified')}",
                    flush=True,
                )
        current_files = [
            {
                "path": str(p.relative_to(fixture["location"])),
                "sha256": digest(p),
                "size_bytes": p.stat().st_size,
            }
            for p in sorted(fixture["location"].rglob("*"))
            if p.is_file()
        ]
        result["fixture_unchanged"] = current_files == fixture["files"]
    for product in products:
        trials = [t for t in result["trials"] if t["product"] == product.name]
        summary: dict[str, Any] = {
            "passed_trials": sum(t["status"] == "passed" for t in trials),
            "total_trials": len(trials),
        }
        for metric in [
            "fresh_workspace_to_verified_file_ms",
            "core_server_rss_mib",
            "workspace_round_trip_ms",
            "delta_aggregate_ms",
        ]:
            samples = [
                sample
                for t in trials
                for sample in (
                    t.get(metric, []) if isinstance(t.get(metric, []), list) else [t[metric]]
                )
            ]
            if samples:
                summary[metric] = summarize(samples)
        result["summary"][product.name] = summary
    result["completed_at"] = datetime.now(UTC).isoformat()
    result["temporary_state_removed"] = True
    result["result"] = (
        "passed"
        if result["fixture_unchanged"] and all(t["status"] == "passed" for t in result["trials"])
        else "failed"
    )
    (args.output_dir / "results.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"result": result["result"], "summary": result["summary"]}, indent=2))
    return 0 if result["result"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
