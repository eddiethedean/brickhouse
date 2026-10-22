"""Recheck distinct nullable-struct contracts in the published Sparkless wheel.

No credentials, network, JVM, or fabricated write receipt is used. The callback
captures actual IPC bytes and deliberately raises before any mutation.
"""

from __future__ import annotations

import argparse
import json
from importlib.metadata import version
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
from deltalake import write_deltalake
from sparkless.external_catalog import ExternalTableDescriptor
from sparkless.sql import SparkSession
from sparkless.sql import types as T


def probe() -> list[dict]:
    expected = [
        {"details": {"label": "present"}},
        {"details": None},
        {"details": {"label": None}},
    ]
    arrow = pa.Table.from_pylist(
        expected, schema=pa.schema([("details", pa.struct([("label", pa.string())]))])
    )
    schema = T.StructType(
        [T.StructField("details", T.StructType([T.StructField("label", T.StringType())]))]
    )
    cases = []
    with TemporaryDirectory(prefix="brickhouse-null-struct-") as directory:
        write_deltalake(directory, arrow)

        class Provider:
            received = None

            def resolve_table(self, identifier):
                if identifier == "main.default.t":
                    return ExternalTableDescriptor("t", directory, schema, "repro", version=0)
                return None

            def write_table(self, request):
                self.received = pa.ipc.open_stream(request.data).read_all().to_pylist()
                raise RuntimeError("intentional capture stop before commit")

        spark = SparkSession("nullable-struct-repro")
        provider = Provider()
        spark.set_external_catalog_provider(provider)
        try:
            sources = {
                "python_rows": lambda: spark.createDataFrame(
                    [({"label": "present"},), (None,), ({"label": None},)], schema
                ),
                "arrow": lambda: spark.createDataFrame(arrow, schema),
                "native_delta": lambda: spark.table("main.default.t"),
            }
            for source, create in sources.items():
                frame = create()
                collected = [{"details": row[0]} for row in frame.collect()]
                cases.append(
                    {
                        "contract": f"{source}: collect distinguishes null struct and null field",
                        "passed": collected == expected,
                        "expected": expected,
                        "actual": collected,
                    }
                )
                provider.received = None
                try:
                    frame.write.mode("append").saveAsTable("main.default.t")
                except Exception:
                    if provider.received is None:
                        raise
                else:
                    raise AssertionError("capture callback must fail before commit")
                cases.append(
                    {
                        "contract": f"{source}: provider IPC preserves parent struct validity",
                        "passed": provider.received == expected,
                        "expected": expected,
                        "actual": provider.received,
                    }
                )
        finally:
            spark.stop()
    return cases


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    cases = probe()
    report = {
        "versions": {name: version(name) for name in ("sparkless", "pyarrow", "deltalake")},
        "result": "failed" if any(not case["passed"] for case in cases) else "passed",
        "cases": cases,
    }
    rendered = json.dumps(report, indent=2) + "\n"
    if args.output:
        args.output.write_text(rendered)
    print(rendered, end="")
    raise SystemExit(1 if report["result"] == "failed" else 0)


if __name__ == "__main__":
    main()
