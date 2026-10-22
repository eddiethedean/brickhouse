"""The same real aggregate runs in Python jobs, ipynb, and source notebooks."""

import json
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)

dbutils = globals()["dbutils"]
spark = SparkSession.builder.getOrCreate()
for key, default in (
    ("table", ""),
    ("customer", ""),
    ("role", ""),
    ("task_values", "false"),
):
    dbutils.widgets.text(key, default)
table_name = dbutils.widgets.get("table")
customer = dbutils.widgets.get("customer")
role = dbutils.widgets.get("role")
use_task_values = dbutils.widgets.get("task_values") == "true"
assert table_name == "main.default.release_orders"
assert customer == "alice"
assert spark.conf.get("spark.sql.ansi.enabled") == "true"
assert spark.conf.get("spark.sql.session.timeZone") == "UTC"

if role == "produce":
    assert dbutils.secrets.get("release-access", "api-key")
    schema = StructType(
        [
            StructField("order_id", LongType(), True),
            StructField("customer", StringType(), True),
            StructField("amount", DecimalType(12, 2), True),
            StructField("active", BooleanType(), True),
        ]
    )
    frame = spark.createDataFrame(
        [
            (1, "alice", Decimal("10.50"), True),
            (2, "alice", Decimal("5.25"), True),
            (3, "bob", None, True),
            (4, "bob", Decimal("20.00"), False),
        ],
        schema,
    )
    result = (
        frame.filter(
            F.col("active") & F.col("amount").isNotNull() & (F.col("customer") == customer)
        )
        .groupBy("customer")
        .agg(
            F.count("*").cast("long").alias("order_count"),
            F.sum("amount").cast("decimal(22,2)").alias("total_amount"),
        )
    )
    result.write.format("delta").mode("append").saveAsTable(table_name)
    if use_task_values:
        dbutils.jobs.taskValues.set("customer", customer)
elif role == "consume":
    if use_task_values:
        assert dbutils.jobs.taskValues.get(taskKey="produce", key="customer") == customer
else:
    raise ValueError("unknown workload role")

rows = spark.table(table_name).collect()
actual = [(row.customer, row.order_count, str(row.total_amount)) for row in rows]
assert actual == [(customer, 2, "15.75")], actual
print("RELEASE_ORDERS=" + json.dumps(actual))
