"""Opt-in local B067 SQL profiling; no credentials, cloud calls or runtime changes.

Run with the benchmark's installed BrickHouse interpreter. Internal hooks exist
only in disposable profiler subprocesses. See docs/benchmarks/sql-profile.md.
"""

from __future__ import annotations

import argparse
import cProfile
import functools
import gzip
import importlib.metadata
import json
import pstats
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from scripts.benchmark_emulators import (
    BenchmarkError,
    Client,
    clean_environment,
    create_fixture,
    digest,
    free_port,
    normalize_rows,
    prepare_sql,
    stop,
    summarize,
    system_info,
)


def partition_spans(spans: list[dict[str, Any]]) -> dict[str, float]:
    """Partition root HTTP wall time; worker spans take precedence over waiter.

    Each instant belongs to exactly one category. On each thread the deepest
    hook wins; active worker work wins over HTTP-thread waiting. No subtraction
    of cumulative cProfile times or summation of concurrent thread durations.
    """
    root = next(span for span in spans if span["name"] == "http.request")
    points = sorted(
        {root["start"], root["end"]}
        | {
            max(root["start"], min(root["end"], span[bound]))
            for span in spans
            for bound in ("start", "end")
        }
    )
    totals: dict[str, float] = defaultdict(float)
    for left, right in zip(points, points[1:], strict=False):
        active = [span for span in spans if span["start"] <= left and span["end"] >= right]
        selected = max(
            active,
            key=lambda span: (span["thread"] != root["thread"], span["depth"]),
        )
        totals[selected["category"]] += (right - left) / 1e6
    return dict(totals)


class Recorder:
    def __init__(self, mode: str, output: Path, warmups: int) -> None:
        self.mode = mode
        self.output = output
        self.local = threading.local()
        self.active: list[dict[str, Any]] | None = None
        self.profiles: dict[str, pstats.Stats] = {}
        self.warmups = warmups
        self.requests = 0
        self.capture_profile = False

    def save_profile(self, role: str, profiler: cProfile.Profile) -> None:
        if role not in self.profiles:
            self.profiles[role] = pstats.Stats(profiler)
        else:
            self.profiles[role].add(profiler)

    def patch(self, owner: Any, name: str, category: str, *, worker: bool = False) -> None:
        original = getattr(owner, name)
        label = f"{getattr(owner, '__name__', type(owner).__name__)}.{name}"

        @functools.wraps(original)
        def measured(*args: Any, **kwargs: Any) -> Any:
            active = self.active
            if active is None:
                return original(*args, **kwargs)
            depth = getattr(self.local, "depth", 0)
            self.local.depth = depth + 1
            start = time.perf_counter_ns()
            profiler = cProfile.Profile() if worker and self.capture_profile else None
            try:
                if profiler is not None:
                    profiler.enable()
                return original(*args, **kwargs)
            finally:
                if profiler is not None:
                    profiler.disable()
                end = time.perf_counter_ns()
                self.local.depth = depth
                span = dict(
                    name=label,
                    category=category,
                    start=start,
                    end=end,
                    thread=threading.get_ident(),
                    depth=depth,
                )
                if category in {"sqlite_execute", "sqlite_executemany"}:
                    # Query templates only; bound arguments are deliberately omitted.
                    span["sql_template"] = args[1]
                active.append(span)
                if profiler is not None:
                    self.save_profile("worker", profiler)

        setattr(owner, name, measured)

    def install(self, handler: Any) -> None:
        from brickhouse.sql.statements import SqlStatementExecutionService

        self.patch(SqlStatementExecutionService, "_execute_claimed", "lifecycle", worker=True)
        if self.mode == "spans":
            from brickhouse.catalog import repository
            from brickhouse.catalog.identifiers import NamespaceState
            from brickhouse.engines import sparkless
            from brickhouse.sql import planner, results, service, statements
            from brickhouse.tables.delta_rs import DeltaRsTableService

            class TimedConnection(sqlite3.Connection):
                pass

            for name in ("execute", "executemany", "commit", "rollback", "close"):
                self.patch(TimedConnection, name, f"sqlite_{name}")
            original_connect = sqlite3.connect

            def connect(*args: Any, **kwargs: Any) -> Any:
                kwargs.setdefault("factory", TimedConnection)
                return original_connect(*args, **kwargs)

            sqlite3.connect = connect
            self.patch(sqlite3, "connect", "sqlite_connect")
            for name in ("create", "claim", "succeed", "get"):
                self.patch(statements._SqlStatementRepository, name, "statement_sqlite")
            self.patch(repository.CatalogRepository, "get", "catalog")
            self.patch(repository.SchemaRepository, "get", "catalog")
            for name in ("get", "get_by_id", "get_by_location"):
                self.patch(repository.TableRepository, name, "catalog")
            for name in ("use_catalog", "use_schema", "resolve_table"):
                self.patch(NamespaceState, name, "catalog")
            self.patch(DeltaRsTableService, "read_snapshot", "delta_metadata")
            self.patch(planner.SqlStatementPlanner, "plan", "sql_planning")
            self.patch(sparkless.SparklessSessionFactory, "_open", "session_open")
            self.patch(sparkless.SparklessSession, "close", "session_close")
            self.patch(sparkless._SparklessSessionFacade, "sql", "engine_sql")
            self.patch(sparkless._SparklessDataFrameFacade, "toArrowBatches", "engine_arrow")
            for name in ("__enter__", "__next__", "__exit__"):
                self.patch(sparkless._SparklessArrowBatchIterator, name, "engine_arrow")
            self.patch(results, "collect_bounded_arrow_result", "result_limits")
            # These functions were imported into statements.py at module load.
            self.patch(statements, "encode_json_array", "result_json")
            self.patch(statements, "split_json_array_result", "result_json")
            self.patch(service.SqlStatementService, "_execute_query", "execution_plumbing")

        original = handler.do_POST

        def request(instance: Any) -> None:
            if instance.path != "/api/2.0/sql/statements":
                original(instance)
                return
            if self.active is not None:
                raise RuntimeError("Profiler requires serial SQL requests")
            spans: list[dict[str, Any]] = []
            self.active = spans
            self.capture_profile = self.mode == "cprofile" and self.requests >= self.warmups
            self.requests += 1
            profiler = cProfile.Profile() if self.capture_profile else None
            start = time.perf_counter_ns()
            try:
                if profiler is not None:
                    profiler.enable()
                original(instance)
            finally:
                if profiler is not None:
                    profiler.disable()
                end = time.perf_counter_ns()
                self.active = None
                if profiler is not None:
                    self.save_profile("http", profiler)
                spans.append(
                    dict(
                        name="http.request",
                        category="http_submission",
                        start=start,
                        end=end,
                        thread=threading.get_ident(),
                        depth=-1,
                    )
                )
                with self.output.open("a") as stream:
                    stream.write(json.dumps(spans) + "\n")

        handler.do_POST = request


def serve(args: argparse.Namespace) -> None:
    from brickhouse.diagnostic_server import create_diagnostic_server
    from brickhouse.workspace.context import WorkspaceContext

    recorder = Recorder(args.mode, args.trace, args.warmups)
    with WorkspaceContext.local(args.workspace) as context:
        with create_diagnostic_server(context, "127.0.0.1", args.port) as server:
            if args.mode != "baseline":
                recorder.install(server.RequestHandlerClass)
            try:
                server.serve_forever(poll_interval=0.05)
            except KeyboardInterrupt:
                pass
    for role, profile in recorder.profiles.items():
        profile.dump_stats(str(args.trace.with_suffix(f".{role}.pstats")))


def profile_trial(
    root: Path, fixture: dict[str, Any], output: Path, mode: str, trial: int, args: Any
) -> dict[str, Any]:
    from brickhouse.workspace.context import WorkspaceContext

    state = root / f"{trial}-{mode}"
    state.mkdir()
    environment = clean_environment()
    initialized = subprocess.run(
        [sys.executable, "-m", "brickhouse", "init", "--workspace", str(state / "workspace")],
        env=environment,
        cwd=state,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    if initialized.returncode:
        raise BenchmarkError(initialized.stderr)
    with WorkspaceContext.local(state / "workspace") as context:
        context.catalogs.create("bench")
        context.tables.register_external("bench", "default", "emulator_bench", fixture["location"])
    port = free_port()
    trace = output / f"{trial}-{mode}.jsonl"
    with (state / "server.log").open("w") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import runpy, sys; sys.path.insert(0, sys.argv.pop(1)); "
                "runpy.run_module('scripts.profile_sql', run_name='__main__')",
                str(Path(__file__).resolve().parents[1]),
                "--server",
                "--mode",
                mode,
                "--workspace",
                str(state / "workspace"),
                "--port",
                str(port),
                "--trace",
                str(trace),
                "--warmups",
                str(args.warmups),
            ],
            env=environment,
            cwd=state,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    samples = []
    rows = []
    try:
        client = Client(port)
        deadline = time.monotonic() + 30
        while True:
            if process.poll() is not None:
                raise BenchmarkError((state / "server.log").read_text())
            try:
                client.call("GET", "/health")
                break
            except OSError:
                if time.monotonic() > deadline:
                    raise BenchmarkError("Profiling server startup timed out") from None
                time.sleep(0.01)
        warehouse, sql = prepare_sql(client, fixture, register_through_sql=False)
        for iteration in range(args.warmups + args.iterations):
            before = time.perf_counter_ns()
            actual = normalize_rows(client.statement(warehouse, sql))
            duration = (time.perf_counter_ns() - before) / 1e6
            if actual != fixture["expected"]:
                raise BenchmarkError("Profiled SQL returned incorrect rows")
            if iteration >= args.warmups:
                samples.append(duration)
        # Stop joins request threads and flushes the last trace/profile before reading.
    finally:
        stop(process)
    if process.returncode != 0:
        raise BenchmarkError(f"Profiling server exited {process.returncode}")
    if mode != "baseline":
        records = [json.loads(line) for line in trace.read_text().splitlines()]
        if len(records) != args.warmups + args.iterations:
            raise BenchmarkError("Missing request trace")
        for elapsed, spans in zip(samples, records[args.warmups :], strict=True):
            root_span = next(span for span in spans if span["name"] == "http.request")
            server_ms = (root_span["end"] - root_span["start"]) / 1e6
            parts = partition_spans(spans)
            if abs(sum(parts.values()) - server_ms) > 1e-6:
                raise BenchmarkError("Wall-time partition does not reconcile")
            rows.append(
                dict(
                    client_ms=elapsed,
                    server_ms=server_ms,
                    client_transport_residual_ms=elapsed - server_ms,
                    exclusive_wall_ms=parts,
                )
            )
        trace.with_suffix(".jsonl.gz").write_bytes(gzip.compress(trace.read_bytes(), mtime=0))
        trace.unlink()
    return dict(
        mode=mode,
        trial=trial,
        client_ms=samples,
        summary=summarize(samples),
        breakdowns=rows,
        verified_results=args.warmups + args.iterations,
        process_exit_code=process.returncode,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--iterations", type=int, default=50)
    parser.add_argument("--warmups", type=int, default=5)
    parser.add_argument("--server", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--mode", choices=("baseline", "spans", "cprofile"))
    parser.add_argument("--workspace", type=Path)
    parser.add_argument("--port", type=int)
    parser.add_argument("--trace", type=Path)
    args = parser.parse_args()
    if args.server:
        serve(args)
        return
    if args.output is None or min(args.rounds, args.iterations, args.warmups) < 1:
        parser.error("Require --output and positive rounds/iterations/warmups")
    if args.output.exists():
        parser.error("Output must be a new directory to preserve all runs")
    args.output.mkdir(parents=True)
    trials = []
    with tempfile.TemporaryDirectory(prefix="brickhouse-sql-profile-") as temporary:
        root = Path(temporary)
        fixture = create_fixture(root, 10_000)
        modes = ["baseline", "spans", "cprofile"]
        for trial in range(args.rounds):
            for mode in modes[trial % 3 :] + modes[: trial % 3]:
                result = profile_trial(root, fixture, args.output.resolve(), mode, trial, args)
                trials.append(result)
                print(f"{trial} {mode}: {result['summary']['median']:.3f} ms", flush=True)
        fixture_unchanged = all(
            digest(fixture["location"] / entry["path"]) == entry["sha256"]
            for entry in fixture["files"]
        )
        if not fixture_unchanged:
            raise BenchmarkError("Delta fixture changed")
    summaries = {}
    for mode in modes:
        selected = [trial for trial in trials if trial["mode"] == mode]
        samples = [value for trial in selected for value in trial["client_ms"]]
        breakdowns = [row for trial in selected for row in trial["breakdowns"]]
        categories = {key for row in breakdowns for key in row["exclusive_wall_ms"]}
        summaries[mode] = dict(
            client=summarize(samples),
            mean_client_ms=statistics.mean(samples),
            mean_wall_ms={
                key: statistics.mean(row["exclusive_wall_ms"].get(key, 0) for row in breakdowns)
                for key in sorted(categories)
            },
            mean_transport_residual_ms=(
                statistics.mean(row["client_transport_residual_ms"] for row in breakdowns)
                if breakdowns
                else None
            ),
        )
    from brickhouse import __file__ as source_file

    source_root = Path(source_file).parent
    report = dict(
        schema_version=1,
        system=system_info(),
        executable=sys.executable,
        versions={
            name: importlib.metadata.version(name)
            for name in ("brickhouse", "sparkless", "deltalake", "pyarrow", "sqlglot")
        },
        source_hashes={
            str(path.relative_to(source_root)): digest(path)
            for path in sorted(source_root.rglob("*.py"))
        },
        tool_sha256=digest(Path(__file__)),
        benchmark_tool_sha256=digest(Path(__file__).with_name("benchmark_emulators.py")),
        configuration=dict(rounds=args.rounds, iterations=args.iterations, warmups=args.warmups),
        fixture={key: value for key, value in fixture.items() if key != "location"},
        fixture_unchanged=fixture_unchanged,
        temporary_state_removed=not root.exists(),
        credentials_removed=True,
        trials=trials,
        summaries=summaries,
    )
    (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
