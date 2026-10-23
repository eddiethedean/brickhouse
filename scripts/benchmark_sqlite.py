"""Opt-in SQL experiment with disk and shared-memory metadata; not a runtime mode."""

from __future__ import annotations

import argparse
import gzip
import importlib.metadata
import json
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

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
from scripts.profile_sql import Recorder, partition_spans

STORAGE_MODES = ("disk", "disk-shared", "memory")


class MetadataExperiment:
    """Redirect only one synthetic metadata DB inside the benchmark subprocess.

    Shared-cache connections keep their normal thread ownership and transactions.
    A keeper preserves the named database/cache while repositories close handles.
    The disk-shared control uses the same keeper/cache without removing disk I/O.
    """

    def __init__(self, path: Path, storage: str) -> None:
        if storage not in STORAGE_MODES:
            raise ValueError("Unknown metadata storage experiment")
        self.path = path.resolve()
        self.storage = storage
        self.native_connect: Callable[..., sqlite3.Connection] = sqlite3.connect
        self.keeper: sqlite3.Connection | None = None
        self.uri = (
            f"file:brickhouse-benchmark-{uuid.uuid4().hex}?mode=memory&cache=shared"
            if storage == "memory"
            else self.path.as_uri() + "?cache=shared"
        )
        self.evidence: dict[str, Any] = {}

    def connect(self, database: Any, *args: Any, **kwargs: Any) -> sqlite3.Connection:
        read_only = False
        if isinstance(database, str) and database.startswith("file:"):
            if not kwargs.get("uri"):
                return self.native_connect(database, *args, **kwargs)
            parsed = urlsplit(database)
            target = Path(unquote(parsed.path)).resolve()
            read_only = parse_qs(parsed.query).get("mode") == ["ro"]
        else:
            target = Path(database).resolve()
        if target != self.path:
            return self.native_connect(database, *args, **kwargs)
        kwargs["uri"] = True
        connection = self.native_connect(self.uri, *args, **kwargs)
        if read_only:
            connection.execute("PRAGMA query_only = ON")
        return connection

    def __enter__(self) -> MetadataExperiment:
        self.evidence["seed_sha256"] = digest(self.path)
        if self.storage != "disk":
            self.keeper = self.native_connect(self.uri, uri=True)
            if self.storage == "memory":
                source = self.native_connect(self.path.as_uri() + "?mode=ro", uri=True)
                try:
                    source.backup(self.keeper)
                finally:
                    source.close()
            vars(sqlite3)["connect"] = self.connect
        probe = self.keeper or self.native_connect(self.path)
        try:
            self.evidence.update(
                storage=self.storage,
                sqlite_version=sqlite3.sqlite_version,
                database_list=probe.execute("PRAGMA database_list").fetchall(),
                journal_mode=probe.execute("PRAGMA journal_mode").fetchone()[0],
                synchronous=probe.execute("PRAGMA synchronous").fetchone()[0],
                read_uncommitted=probe.execute("PRAGMA read_uncommitted").fetchone()[0],
            )
        finally:
            if self.keeper is None:
                probe.close()
        return self

    def __exit__(self, *_exception: object) -> None:
        # Recorder may have wrapped the redirector; neither hook escapes this process.
        vars(sqlite3)["connect"] = self.native_connect
        observations: list[dict[str, Any]] = []

        def fresh_connection_probe() -> None:
            connection = self.native_connect(
                self.path if self.storage == "disk" else self.uri,
                uri=self.storage != "disk",
            )
            try:
                observations.append(
                    dict(
                        statement_states=connection.execute(
                            "SELECT state, COUNT(*) FROM sql_statements "
                            "GROUP BY state ORDER BY state"
                        ).fetchall(),
                        schema_objects=connection.execute(
                            "SELECT COUNT(*) FROM sqlite_master"
                        ).fetchone()[0],
                        database_list=connection.execute("PRAGMA database_list").fetchall(),
                    )
                )
            finally:
                connection.close()

        try:
            # Distinct threads open/use/close distinct handles to the same database.
            for _ in range(2):
                thread = threading.Thread(target=fresh_connection_probe)
                thread.start()
                thread.join()
            if len(observations) != 2 or observations[0] != observations[1]:
                raise BenchmarkError("Fresh thread connections did not share the metadata")
            self.evidence["fresh_thread_observations"] = observations
        finally:
            if self.keeper is not None:
                self.keeper.close()
        self.evidence["seed_after_sha256"] = digest(self.path)
        disk = self.native_connect(self.path.as_uri() + "?mode=ro", uri=True)
        try:
            self.evidence["disk_statement_count"] = disk.execute(
                "SELECT COUNT(*) FROM sql_statements"
            ).fetchone()[0]
        finally:
            disk.close()
        if self.storage == "memory":
            fresh = self.native_connect(self.uri, uri=True)
            try:
                self.evidence["objects_after_last_close"] = fresh.execute(
                    "SELECT COUNT(*) FROM sqlite_master"
                ).fetchone()[0]
            finally:
                fresh.close()
            if self.evidence["objects_after_last_close"] != 0:
                raise BenchmarkError("Memory database survived its last connection")
            if self.evidence["seed_sha256"] != self.evidence["seed_after_sha256"]:
                raise BenchmarkError("Memory workload modified its disk seed")


def serve(args: argparse.Namespace) -> None:
    from brickhouse.diagnostic_server import create_diagnostic_server
    from brickhouse.workspace.context import WorkspaceContext

    experiment = MetadataExperiment(args.workspace / "metadata.sqlite", args.storage)
    with experiment:
        with WorkspaceContext.local(args.workspace) as context:
            with create_diagnostic_server(context, "127.0.0.1", args.port) as server:
                if args.mode == "spans":
                    Recorder(args.mode, args.trace, args.warmups).install(
                        server.RequestHandlerClass
                    )
                try:
                    server.serve_forever(poll_interval=0.05)
                except KeyboardInterrupt:
                    pass
    args.trace.with_suffix(".metadata.json").write_text(
        json.dumps(experiment.evidence, indent=2) + "\n"
    )


def trial(
    root: Path,
    fixture: dict[str, Any],
    output: Path,
    storage: str,
    mode: str,
    number: int,
    args: argparse.Namespace,
) -> dict[str, Any]:
    from brickhouse.workspace.context import WorkspaceContext

    label = f"{number}-{storage}-{mode}"
    state = root / label
    state.mkdir()
    environment = clean_environment()
    initialized = subprocess.run(
        [sys.executable, "-m", "brickhouse", "init", "--workspace", str(state / "workspace")],
        env=environment,
        cwd=state,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if initialized.returncode:
        raise BenchmarkError(initialized.stderr)
    with WorkspaceContext.local(state / "workspace") as context:
        context.catalogs.create("bench")
        context.tables.register_external("bench", "default", "emulator_bench", fixture["location"])
    port = free_port()
    trace = output / f"{label}.jsonl"
    with (state / "server.log").open("w") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import runpy, sys; sys.path.insert(0, sys.argv.pop(1)); "
                "runpy.run_module('scripts.benchmark_sqlite', run_name='__main__')",
                str(Path(__file__).resolve().parents[1]),
                "--server",
                "--storage",
                storage,
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
                    raise BenchmarkError("Experiment server startup timed out") from None
                time.sleep(0.01)
        warehouse, sql = prepare_sql(client, fixture, register_through_sql=False)
        for iteration in range(args.warmups + args.iterations):
            before = time.perf_counter_ns()
            actual = normalize_rows(client.statement(warehouse, sql))
            elapsed = (time.perf_counter_ns() - before) / 1e6
            if actual != fixture["expected"]:
                raise BenchmarkError("Wrong SQL result in SQLite storage experiment")
            if iteration >= args.warmups:
                samples.append(elapsed)
    finally:
        stop(process)
    if process.returncode:
        raise BenchmarkError((state / "server.log").read_text())
    metadata = json.loads(trace.with_suffix(".metadata.json").read_text())
    expected_states = [["SUCCEEDED", args.warmups + args.iterations]]
    if any(
        item["statement_states"] != expected_states
        for item in metadata["fresh_thread_observations"]
    ):
        raise BenchmarkError("Query metadata was not shared/persisted within the API process")
    if metadata["disk_statement_count"] != (
        0 if storage == "memory" else args.warmups + args.iterations
    ):
        raise BenchmarkError("Unexpected disk persistence for metadata mode")
    partitions = []
    if mode == "spans":
        records = [json.loads(line) for line in trace.read_text().splitlines()]
        if len(records) != args.warmups + args.iterations:
            raise BenchmarkError("Missing query spans")
        for elapsed, spans in zip(samples, records[args.warmups :], strict=True):
            parts = partition_spans(spans)
            root_span = next(span for span in spans if span["name"] == "http.request")
            server_ms = (root_span["end"] - root_span["start"]) / 1e6
            if abs(sum(parts.values()) - server_ms) > 1e-6:
                raise BenchmarkError("Non-additive wall-time partition")
            partitions.append(dict(wall_ms=parts, transport_residual_ms=elapsed - server_ms))
        trace.with_suffix(".jsonl.gz").write_bytes(gzip.compress(trace.read_bytes(), mtime=0))
        trace.unlink()
    return dict(
        label=label,
        storage=storage,
        mode=mode,
        trial=number,
        client_ms=samples,
        summary=summarize(samples),
        partitions=partitions,
        verified_results=args.warmups + args.iterations,
        metadata=metadata,
        process_exit_code=process.returncode,
    )


def aggregate(trials: list[dict[str, Any]]) -> dict[str, Any]:
    summaries: dict[str, dict[str, Any]] = {}
    for storage in STORAGE_MODES:
        summaries[storage] = {}
        for mode in ("baseline", "spans"):
            chosen = [t for t in trials if t["storage"] == storage and t["mode"] == mode]
            samples = [value for t in chosen for value in t["client_ms"]]
            parts = [part for t in chosen for part in t["partitions"]]
            groups: dict[str, float] = defaultdict(float)
            for part in parts:
                for key, value in part["wall_ms"].items():
                    groups[key] += value / len(parts)
                groups["client_transport_residual"] += part["transport_residual_ms"] / len(parts)
            summaries[storage][mode] = dict(
                client=summarize(samples),
                mean_ms=statistics.mean(samples),
                wall_mean_ms=dict(groups),
            )
    return summaries


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--iterations", type=int, default=50)
    parser.add_argument("--warmups", type=int, default=5)
    parser.add_argument("--server", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--storage", choices=STORAGE_MODES)
    parser.add_argument("--mode", choices=("baseline", "spans"))
    parser.add_argument("--workspace", type=Path)
    parser.add_argument("--port", type=int)
    parser.add_argument("--trace", type=Path)
    args = parser.parse_args()
    if args.server:
        serve(args)
        return
    if args.output is None or min(args.rounds, args.iterations, args.warmups) < 1:
        parser.error("Require a new --output directory and positive sample counts")
    if args.output.exists():
        parser.error("Output must not exist; preserve earlier runs")
    args.output.mkdir(parents=True)
    trials = []
    with tempfile.TemporaryDirectory(prefix="brickhouse-sqlite-experiment-") as temporary:
        root = Path(temporary)
        fixture = create_fixture(root, 10_000)
        for number in range(args.rounds):
            order = STORAGE_MODES[number % 3 :] + STORAGE_MODES[: number % 3]
            for storage in order:
                modes = ("baseline", "spans") if number % 2 == 0 else ("spans", "baseline")
                for mode in modes:
                    result = trial(
                        root, fixture, args.output.resolve(), storage, mode, number, args
                    )
                    trials.append(result)
                    print(f"{result['label']}: {result['summary']['median']:.3f} ms", flush=True)
        unchanged = all(
            digest(fixture["location"] / entry["path"]) == entry["sha256"]
            for entry in fixture["files"]
        )
        if not unchanged:
            raise BenchmarkError("Delta fixture changed")
    from brickhouse import __file__ as source_file

    source_root = Path(source_file).parent
    report = dict(
        schema_version=1,
        system=system_info(),
        executable=sys.executable,
        sqlite_version=sqlite3.sqlite_version,
        versions={
            name: importlib.metadata.version(name)
            for name in ("brickhouse", "sparkless", "deltalake", "pyarrow", "sqlglot")
        },
        source_hashes={
            str(p.relative_to(source_root)): digest(p) for p in sorted(source_root.rglob("*.py"))
        },
        tools={
            str(p): digest(p)
            for p in (
                Path(__file__),
                Path(__file__).with_name("profile_sql.py"),
                Path(__file__).with_name("benchmark_emulators.py"),
            )
        },
        configuration=dict(rounds=args.rounds, iterations=args.iterations, warmups=args.warmups),
        fixture={key: value for key, value in fixture.items() if key != "location"},
        fixture_unchanged=unchanged,
        temporary_state_removed=not root.exists(),
        credentials_removed_from_children=True,
        trials=trials,
        summaries=aggregate(trials),
    )
    (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
