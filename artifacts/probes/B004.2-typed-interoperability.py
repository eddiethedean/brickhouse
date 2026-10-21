"""Probe local Sparkless/Delta interoperability without Databricks access.

This named research probe is not test-discovered. It checks the published
Sparkless release and local Delta files against regressions found in 4.13.3.
"""

from __future__ import annotations

import tempfile
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import sparkless
from deltalake import DeltaTable, write_deltalake
from sparkless.sql import SparkSession


def verify_decimal_delta_cross_read(root: Path) -> None:
    path = root / "typed-decimal"
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("amount", pa.decimal128(12, 2)),
            ("event_date", pa.date32()),
            ("event_ts", pa.timestamp("us", tz="UTC")),
            ("label", pa.string()),
            ("enabled", pa.bool_()),
            ("tags", pa.list_(pa.string())),
            ("profile", pa.struct([("name", pa.string()), ("rank", pa.int64())])),
        ]
    )
    table = pa.Table.from_pylist(
        [
            {
                "id": 1,
                "amount": Decimal("10.50"),
                "event_date": date(2026, 10, 16),
                "event_ts": datetime(2026, 10, 16, 12, 30, tzinfo=UTC),
                "label": "first",
                "enabled": True,
                "tags": ["a", "b"],
                "profile": {"name": "Ada", "rank": 1},
            },
            {
                "id": 2,
                "amount": None,
                "event_date": None,
                "event_ts": None,
                "label": None,
                "enabled": None,
                "tags": None,
                "profile": None,
            },
        ],
        schema=schema,
    )
    write_deltalake(str(path), table)

    session = SparkSession.builder.appName("b0042-release-decimal-read").getOrCreate()
    try:
        frame = session.read.format("delta").load(str(path))
        expected_schema = (
            "struct<id:bigint,amount:decimal(12,2),event_date:date,event_ts:timestamp,"
            "label:string,enabled:boolean,tags:array<string>,"
            "profile:struct<name:string,rank:bigint>>"
        )
        assert frame.schema.simpleString() == expected_schema
        assert frame.collect() == [
            (
                1,
                Decimal("10.50"),
                date(2026, 10, 16),
                datetime(2026, 10, 16, 12, 30),
                "first",
                True,
                ["a", "b"],
                {"name": "Ada", "rank": 1},
            ),
            (2, None, None, None, None, None, None, None),
        ]

        frame.createOrReplaceTempView("typed_values")
        result = session.sql("SELECT id, amount FROM typed_values ORDER BY id")
        assert result.schema.simpleString() == "struct<id:bigint,amount:decimal(12,2)>"
        assert result.collect() == [(1, Decimal("10.50")), (2, None)]
        print(
            "Delta-to-Sparkless DataFrame and SQL cross-read preserved decimal, "
            "null, date, UTC-valued timestamp, array, and struct"
        )
        print(f"Sparkless typed schema: {frame.schema.simpleString()}")
    finally:
        session.stop()


def verify_arrow_decimal_ingress() -> None:
    table = pa.Table.from_pylist(
        [{"amount": Decimal("10.50")}, {"amount": None}],
        schema=pa.schema([("amount", pa.decimal128(12, 2))]),
    )
    session = SparkSession.builder.appName("b0042-release-arrow-decimal").getOrCreate()
    try:
        frame = session.createDataFrame(table, schema=[("amount", "decimal(12,2)")])
        assert frame.schema.simpleString() == "struct<amount:decimal(12,2)>"
        assert frame.collect() == [(Decimal("10.50"),), (None,)]
        print("PyArrow decimal ingress preserved decimal(12,2), exact value, and null")
    finally:
        session.stop()


def verify_delta_path_writer(root: Path) -> None:
    path = root / "delta-writer-output"
    session = SparkSession.builder.appName("b0042-release-delta-writer").getOrCreate()
    try:
        frame = session.createDataFrame([(1, "alpha"), (2, "beta")], ["id", "name"])
        frame.write.format("delta").mode("overwrite").save(str(path))
        assert path.is_dir()
        assert (path / "_delta_log").is_dir()
        assert DeltaTable(str(path)).version() == 0
        readback = session.read.format("delta").load(str(path))
        assert readback.schema.simpleString() == "struct<id:bigint,name:string>"
        assert readback.collect() == [(1, "alpha"), (2, "beta")]
        print("format('delta').save(path) created and read back a real Delta table")
    finally:
        session.stop()


def main() -> None:
    root = Path(tempfile.mkdtemp(prefix="brickhouse-b0042-release-"))
    print(f"Sparkless release: {sparkless.__version__}")
    verify_decimal_delta_cross_read(root)
    verify_arrow_decimal_ingress()
    verify_delta_path_writer(root)


if __name__ == "__main__":
    main()
