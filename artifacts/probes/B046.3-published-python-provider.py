"""Verify the released Python hook against real BrickHouse Delta commits.

This explicit local probe is outside test discovery. It is not the production
engine adapter and does not load .env or contact a Databricks workspace.
It exercises structs with null fields; parent-null structs are checked by the
separate B046.3-null-struct-repro.py and currently fail in Sparkless 4.14.0.
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import weakref
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from importlib.metadata import version
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc
from deltalake import DeltaTable
from sparkless.external_catalog import ExternalTableDescriptor, ExternalTableWriteResult
from sparkless.sql import SparkSession
from sparkless.sql import types as T

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from brickhouse.catalog import parse_identifier  # noqa: E402
from brickhouse.tables import DeltaRsTableService, SnapshotSelector, WriteMode  # noqa: E402
from brickhouse.workspace.context import WorkspaceContext  # noqa: E402
from brickhouse.workspace.lifecycle import initialize_workspace  # noqa: E402

IDENTIFIER = "main.default.orders"
MAX_IPC_BYTES = 1024 * 1024
MAX_ROWS = 32
SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("amount", pa.decimal128(12, 2)),
        ("items", pa.list_(pa.int64())),
        ("details", pa.struct([("label", pa.string())])),
    ]
)


def data(row_id: int) -> pa.Table:
    return pa.Table.from_pylist(
        [
            {
                "id": row_id,
                "amount": Decimal("12.34"),
                "items": [row_id, row_id + 1],
                "details": {"label": "synthetic"},
            },
            {
                "id": row_id + 1,
                "amount": None,
                "items": None,
                "details": {"label": None},
            },
        ],
        schema=SCHEMA,
    )


def spark_type(value):
    """Translate this probe's declared Delta types using public constructors."""
    if isinstance(value, str):
        if value.startswith("decimal("):
            precision, scale = value.removeprefix("decimal(").removesuffix(")").split(",")
            return T.DecimalType(int(precision), int(scale))
        return {"long": T.LongType, "string": T.StringType}[value]()
    kind = value["type"]
    if kind == "struct":
        return T.StructType(
            [
                T.StructField(field["name"], spark_type(field["type"]), field["nullable"])
                for field in value["fields"]
            ]
        )
    if kind == "array":
        return T.ArrayType(spark_type(value["elementType"]), value["containsNull"])
    raise ValueError(f"unsupported probe schema: {value}")


class ProviderFailure(RuntimeError):
    """A deliberate provider-boundary failure, before any commit."""


class Provider:
    def __init__(self, service, handle, root: Path, *, hold: str | None = None):
        self.service = service
        self.handle = handle
        self.root = root
        self.hold = hold
        self.owner_thread = threading.get_ident()
        self.lookups = []
        self.receipts = []
        self.fail_lookup = False
        self.fail_write = False
        self.pinned_version = None
        self.last_failure = None

    def resolve_table(self, identifier):
        assert threading.get_ident() == self.owner_thread
        self.lookups.append(identifier)
        if self.fail_lookup:
            self.last_failure = ProviderFailure("synthetic catalog unavailable")
            raise self.last_failure
        if identifier != IDENTIFIER:
            return None
        selector = SnapshotSelector(version=self.pinned_version)
        snapshot = self.service.read_snapshot(self.handle, selector)
        return ExternalTableDescriptor(
            table_id=self.handle.table_id,
            canonical_location=str(self.handle.location),
            schema=spark_type(json.loads(snapshot.schema.json_text)),
            provider="brickhouse-local-delta-probe",
            version=snapshot.version,
        )

    def write_table(self, request):
        assert threading.get_ident() == self.owner_thread
        if self.fail_write:
            self.last_failure = ProviderFailure("synthetic commit rejected")
            raise self.last_failure
        if request.options or request.partition_by or request.format not in (None, "delta"):
            raise ProviderFailure("unsupported probe write settings")
        if len(request.data) > MAX_IPC_BYTES:
            raise ProviderFailure("probe IPC byte limit exceeded")
        table = ipc.open_stream(request.data).read_all()
        if table.num_rows > MAX_ROWS:
            raise ProviderFailure("probe row limit exceeded")
        if self.hold == "before":
            self._wait_for_termination("before")
        mode = {"append": WriteMode.APPEND, "overwrite": WriteMode.OVERWRITE}[request.mode]
        result = self.service.write_arrow(
            self.handle, table, mode, operation_id=f"probe-write-{len(self.receipts)}"
        )
        self.receipts.append(result.version)
        if self.hold == "after":
            self._wait_for_termination("after")
        committed = self.resolve_table(IDENTIFIER)
        assert committed.version == result.version
        return ExternalTableWriteResult(committed, result.version)

    def _wait_for_termination(self, phase: str) -> None:
        (self.root / "callback-phase").write_text(phase)
        time.sleep(30)
        raise AssertionError("parent did not terminate the held callback")


@contextmanager
def workspace(root: Path, seed: int = 1):
    root.mkdir(parents=True, exist_ok=True)
    initialized = initialize_workspace(root / "workspace")
    with WorkspaceContext.local(initialized.path) as context:
        context.tables.create_managed("main", "default", "orders")
        service = DeltaRsTableService(context)
        handle = service.resolve(parse_identifier(IDENTIFIER), context.new_namespace_state())
        service.write_arrow(handle, data(seed), WriteMode.CREATE, operation_id="seed")
        yield service, handle


def expect_error(operation, label: str) -> str:
    try:
        operation()
    except Exception as error:
        return type(error).__name__
    raise AssertionError(f"expected an explicit error: {label}")


def check_provider(root: Path) -> list[str]:
    cases = []
    with workspace(root) as (service, handle):
        spark = SparkSession("brickhouse-released-provider")
        provider = Provider(service, handle, root)
        spark.set_external_catalog_provider(provider)
        try:
            expected = [(1, Decimal("12.34")), (2, None)]
            assert spark.table(IDENTIFIER).select("id", "amount").collect() == expected
            assert spark.read.table(IDENTIFIER).count() == 2
            assert (
                spark.sql(f"SELECT id, amount FROM {IDENTIFIER} ORDER BY id").collect() == expected
            )
            assert spark.catalog.tableExists(IDENTIFIER)
            cases.append("native DataFrame/reader/SQL/catalog reads through public Python provider")

            schema = provider.resolve_table(IDENTIFIER).schema
            appended = spark.createDataFrame(data(3), schema)
            appended.write.format("delta").mode("append").saveAsTable(IDENTIFIER)
            assert provider.receipts == [1]
            actual = service.read_table(handle).data.sort_by("id").to_pylist()
            expected_rows = data(1).to_pylist() + data(3).to_pylist()
            assert actual == expected_rows, (actual, expected_rows)
            cases.append(
                "decimal, scalar null, array and struct append uses journaled table service"
            )

            spark.createDataFrame(data(5), schema).createOrReplaceTempView("incoming")
            spark.sql(f"INSERT INTO {IDENTIFIER} SELECT * FROM incoming")
            assert provider.receipts == [1, 2]
            assert service.read_snapshot(handle).version == 2
            assert spark.table(IDENTIFIER).count() == 6
            cases.append("SQL INSERT SELECT delegated through the same commit authority")

            spark.createDataFrame(data(9), schema).write.mode("overwrite").saveAsTable(IDENTIFIER)
            assert provider.receipts == [1, 2, 3]
            assert service.read_table(handle).data.to_pylist() == data(9).to_pylist()
            assert DeltaTable(str(handle.location)).version() == 3
            cases.append("overwrite returns the exact committed Delta version and physical rows")

            provider.pinned_version = 0
            assert spark.table(IDENTIFIER).select("id", "amount").collect() == expected
            provider.pinned_version = None
            cases.append("version-pinned external reads remain distinct from latest")

            other = spark.newSession()
            try:
                expect_error(lambda: other.table(IDENTIFIER), "new session inherits no provider")
                other.set_external_catalog_provider(Provider(service, handle, root))
                assert other.table(IDENTIFIER).count() == 2
            finally:
                other.stop()
            cases.append("fresh session has no provider until explicitly attached")

            provider.fail_write = True
            error_type = expect_error(
                lambda: appended.write.mode("append").saveAsTable(IDENTIFIER), "write failure"
            )
            assert isinstance(provider.last_failure, ProviderFailure)
            assert DeltaTable(str(handle.location)).version() == 3
            provider.fail_write = False
            cases.append(
                f"callback failure surfaced as {error_type}; no mutation or local fallback"
            )

            provider.fail_lookup = True
            expect_error(
                lambda: spark.catalog.tableExists(IDENTIFIER), "lookup error is not a miss"
            )
            provider.fail_lookup = False
            cases.append("catalog lookup failure is explicit rather than false absence")

            expect_error(
                lambda: (
                    appended.write.option("unsupported", "true")
                    .mode("append")
                    .saveAsTable(IDENTIFIER)
                ),
                "unsupported options",
            )
            assert DeltaTable(str(handle.location)).version() == 3
            cases.append("unsupported write options fail before the provider commit")

            reference = weakref.ref(provider)
            reader = spark.read
            del provider
            gc.collect()
            assert reference() is not None
            spark.set_external_catalog_provider(None)
            gc.collect()
            assert reference() is None
            expect_error(lambda: reader.table(IDENTIFIER), "detached reader")
            cases.append(
                "provider strong reference released on detach; existing reader sees removal"
            )
        finally:
            spark.stop()

    with WorkspaceContext.local(root / "workspace") as context:
        service = DeltaRsTableService(context)
        handle = service.resolve(parse_identifier(IDENTIFIER), context.new_namespace_state())
        assert service.read_snapshot(handle).version == 3
        assert service.read_table(handle).data.to_pylist() == data(9).to_pylist()
    cases.append("fresh workspace owner reads the same committed schema/data after reopen")
    return cases


def check_concurrent_owners(root: Path) -> str:
    barrier = threading.Barrier(2)

    def read_owned(seed: int):
        case_root = root / str(seed)
        with workspace(case_root, seed) as (service, handle):
            spark = SparkSession(f"owner-{seed}")
            try:
                spark.set_external_catalog_provider(Provider(service, handle, case_root))
                barrier.wait(timeout=10)
                result = spark.table(IDENTIFIER).select("id").collect()
                assert result == [(seed,), (seed + 1,)]
                return threading.get_ident()
            finally:
                spark.stop()

    with ThreadPoolExecutor(max_workers=2) as executor:
        owners = list(executor.map(read_owned, [100, 200]))
    assert len(set(owners)) == 2
    return "independent owner threads keep sessions, callback context and tables isolated"


def held_callback(root: Path, phase: str) -> None:
    with workspace(root) as (service, handle):
        spark = SparkSession("held-provider-callback")
        try:
            provider = Provider(service, handle, root, hold=phase)
            spark.set_external_catalog_provider(provider)
            schema = provider.resolve_table(IDENTIFIER).schema
            spark.createDataFrame(data(3), schema).write.mode("append").saveAsTable(IDENTIFIER)
        finally:
            spark.stop()


def check_process_interruption(root: Path, phase: str) -> str:
    root.mkdir(parents=True)
    process = subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve()), "--hold", phase, "--root", str(root)],
        env={"PYTHONUNBUFFERED": "1"},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        deadline = time.monotonic() + 15
        while not (root / "callback-phase").exists() and time.monotonic() < deadline:
            if process.poll() is not None:
                output, error = process.communicate()
                raise AssertionError((process.returncode, output, error))
            time.sleep(0.02)
        assert (root / "callback-phase").read_text() == phase
        os.killpg(process.pid, signal.SIGTERM)
        process.communicate(timeout=5)
        assert process.returncode < 0
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate(timeout=5)
    with WorkspaceContext.local(root / "workspace") as context:
        service = DeltaRsTableService(context)
        handle = service.resolve(parse_identifier(IDENTIFIER), context.new_namespace_state())
        assert service.read_snapshot(handle).version == (0 if phase == "before" else 1)
        expected = data(1).to_pylist()
        if phase == "after":
            expected += data(3).to_pylist()
        assert service.read_table(handle).data.sort_by("id").to_pylist() == expected
    return f"process termination {phase} commit preserves the actual Delta outcome"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hold", choices=["before", "after"])
    parser.add_argument("--root", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.hold:
        if args.root is None:
            parser.error("--hold requires --root")
        held_callback(args.root, args.hold)
        return
    with tempfile.TemporaryDirectory(prefix="brickhouse-b0463-") as directory:
        root = Path(directory)
        cases = check_provider(root / "provider")
        cases.append(check_concurrent_owners(root / "threads"))
        for phase in ("before", "after"):
            cases.append(check_process_interruption(root / phase, phase))
    report = {
        "result": "passed",
        "date": datetime.now(UTC).date().isoformat(),
        "versions": {
            "python": sys.version.split()[0],
            **{name: version(name) for name in ("sparkless", "deltalake", "pyarrow")},
        },
        "case_count": len(cases),
        "cases": cases,
        "excluded_contracts": {
            "nullable_structs": "B046.3-null-struct-repro.py; upstream issues #1694 and #1695"
        },
        "limits": {"callback_ipc_bytes": MAX_IPC_BYTES, "callback_rows": MAX_ROWS},
        "limitations": [
            "Native writer materializes data before these callback limits apply.",
            "Sessions stay on their owner thread; cross-thread transfer is unsupported.",
            "Synchronous callbacks have process containment, not an async cancellation API.",
            "This explicit probe is not a production adapter or a Databricks compatibility claim.",
        ],
    }
    if args.output:
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
