"""Temporary feasibility probe; not production code or a CI test.

Run only in an isolated environment with the documented Sparkless source
revision, deltalake, and PyArrow installed. Writes synthetic Delta tables
under a temporary directory and makes no network or Databricks calls.
"""

from __future__ import annotations

import tempfile
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from sparkless.sql import SparkSession


def bind_external_delta(session: Any, name: str, path: Path) -> Any:
    """Temporary Python-side name binding through a Sparkless session view."""
    frame = session.read.format("delta").load(str(path))
    frame.createOrReplaceTempView(name)
    return frame


def delegate_write(frame: Any, path: Path) -> None:
    """Temporary writer proxy; collects rows, then calls the Delta provider."""
    records = [row.asDict() for row in frame.collect()]
    # This primitive-type probe deliberately does not claim decimal/nested safety.
    table = pa.Table.from_pylist(records)
    write_deltalake(str(path), table, mode="overwrite")


def main() -> None:
    root = Path(tempfile.mkdtemp(prefix="brickhouse-b0032-"))
    source = root / "source"
    target = root / "delegated"
    input_table = pa.table(
        {
            "order_id": pa.array([1, 2, 3, 4], type=pa.int64()),
            "customer": pa.array(["alice", "alice", "bob", "bob"], type=pa.string()),
            "amount": pa.array([10.5, 5.25, None, 20.0], type=pa.float64()),
            "active": pa.array([True, True, True, False], type=pa.bool_()),
        }
    )
    write_deltalake(str(source), input_table)
    assert DeltaTable(str(source)).version() == 0

    session = (
        SparkSession.builder.appName("brickhouse-b0032-delta-bridge")
        .config("spark.sql.warehouse.dir", str(root / "warehouse"))
        .getOrCreate()
    )
    try:
        external_frame = bind_external_delta(session, "main.b003.orders", source)
        assert external_frame.count() == 4
        result = session.sql(
            "SELECT customer, COUNT(order_id) AS order_count, "
            "SUM(amount) AS total_amount "
            "FROM main.b003.orders "
            "WHERE active = true AND amount IS NOT NULL "
            "GROUP BY customer ORDER BY customer"
        )
        assert result.collect() == [("alice", 2, 15.75)]
        delegate_write(result, target)

        delta_result = DeltaTable(str(target)).to_pyarrow_table()
        sparkless_result = session.read.format("delta").load(str(target))
        delta_rows = delta_result.to_pylist()
        print(f"delta-rs result rows: {delta_rows!r}")
        assert delta_rows == [{"customer": "alice", "order_count": 2, "total_amount": 15.75}]
        assert sparkless_result.collect() == [("alice", 2, 15.75)]

        decimal_input = pa.Table.from_pylist(
            [{"amount": Decimal("10.50")}],
            schema=pa.schema([("amount", pa.decimal128(12, 2))]),
        )
        decimal_frame = session.createDataFrame(decimal_input, schema=[("amount", "decimal(12,2)")])
        decimal_type_after_ingress = decimal_frame.schema.simpleString()
        assert "amount:double" in decimal_type_after_ingress

        print(f"source delta version: {DeltaTable(str(source)).version()}")
        print(f"delegated delta version: {DeltaTable(str(target)).version()}")
        print(f"source rows read through Sparkless: {external_frame.count()}")
        print(f"aggregate rows: {result.collect()}")
        print(f"schema after PyArrow decimal ingress: {decimal_type_after_ingress}")
        print("result read through delta-rs and Sparkless: passed")
        print("temporary adapter boundary: Python materialization; primitive types only")
    finally:
        session.stop()


if __name__ == "__main__":
    main()
