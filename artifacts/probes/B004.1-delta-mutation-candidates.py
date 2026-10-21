"""Probe local Delta mutations with delta-rs and reads through Sparkless.

Research evidence only: this is deliberately outside test discovery and makes
no network or Databricks calls.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from sparkless.sql import SparkSession


def read_rows(session: SparkSession, path: Path) -> list[tuple[int, int]]:
    frame = session.read.format("delta").load(str(path))
    return sorted((int(row[0]), int(row[1])) for row in frame.collect())


def main() -> None:
    root = Path(tempfile.mkdtemp(prefix="brickhouse-b0041-"))
    table_path = root / "orders"
    initial = pa.table({"id": [1, 2, 3], "amount": [10, 20, 30]})
    write_deltalake(str(table_path), initial)
    assert DeltaTable(str(table_path)).version() == 0

    session = (
        SparkSession.builder.appName("brickhouse-b0041-delta-candidates")
        .config("spark.sql.warehouse.dir", str(root / "warehouse"))
        .getOrCreate()
    )
    try:
        assert read_rows(session, table_path) == [(1, 10), (2, 20), (3, 30)]
        print("create/read: version 0 passed")

        write_deltalake(str(table_path), pa.table({"id": [4], "amount": [40]}), mode="append")
        assert DeltaTable(str(table_path)).version() == 1
        assert read_rows(session, table_path) == [
            (1, 10),
            (2, 20),
            (3, 30),
            (4, 40),
        ]
        print("append: version 1 passed")

        DeltaTable(str(table_path)).update(predicate="id = 2", updates={"amount": "amount + 1"})
        assert DeltaTable(str(table_path)).version() == 2
        assert read_rows(session, table_path) == [
            (1, 10),
            (2, 21),
            (3, 30),
            (4, 40),
        ]
        print("update: version 2 passed")

        DeltaTable(str(table_path)).delete(predicate="id = 3")
        assert DeltaTable(str(table_path)).version() == 3
        assert read_rows(session, table_path) == [(1, 10), (2, 21), (4, 40)]
        print("delete: version 3 passed")

        merge_source = pa.table({"id": [2, 5], "amount": [22, 50]})
        merge_result = (
            DeltaTable(str(table_path))
            .merge(
                source=merge_source,
                predicate="target.id = source.id",
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
        assert DeltaTable(str(table_path)).version() == 4
        expected = [(1, 10), (2, 22), (4, 40), (5, 50)]
        assert read_rows(session, table_path) == expected
        print(f"merge: version 4 passed; metrics={merge_result}")
        print("all local Delta mutations read consistently through Sparkless")
    finally:
        print("stopping Sparkless session", flush=True)
        session.stop()
        print("Sparkless session stopped", flush=True)


if __name__ == "__main__":
    main()
