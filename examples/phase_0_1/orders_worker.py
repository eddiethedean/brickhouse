"""Worker source uploaded by run_orders.py for a real local SDK job."""

from __future__ import annotations

import sys
from decimal import Decimal

from pyspark.sql import SparkSession  # type: ignore[import-not-found]  # worker-injected API
from pyspark.sql.types import (  # type: ignore[import-not-found]  # worker-injected API
    BooleanType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("expected one registered table name")
    table_name = sys.argv[1]
    if not table_name.startswith("orders_") or not table_name[7:].isalnum():
        raise SystemExit("table name must be a generated orders_<hex> identifier")

    schema = StructType(
        [
            StructField("order_id", LongType(), nullable=True),
            StructField("customer", StringType(), nullable=True),
            StructField("amount", DecimalType(12, 2), nullable=True),
            StructField("active", BooleanType(), nullable=True),
        ]
    )
    rows = [
        (1, "alice", Decimal("10.50"), True),
        (2, "alice", Decimal("5.25"), True),
        (3, "bob", None, True),
        (4, "bob", Decimal("20.00"), False),
    ]
    frame = SparkSession.builder.getOrCreate().createDataFrame(rows, schema)
    frame.write.format("delta").mode("append").saveAsTable(f"main.default.{table_name}")


if __name__ == "__main__":
    main()
