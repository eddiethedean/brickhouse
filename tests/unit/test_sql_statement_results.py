from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pyarrow as pa
import pytest

from brickhouse.sql.results import (
    MAX_INLINE_RESULT_BYTES,
    MAX_INLINE_RESULT_ROWS,
    SqlResultLimitError,
    encode_json_array,
)


def test_json_array_encoding_matches_sanitized_databricks_literal_fixture() -> None:
    table = pa.table(
        {
            "session_timezone": pa.array(["Etc/UTC"], type=pa.string()),
            "amount": pa.array([Decimal("12.34")], type=pa.decimal128(12, 2)),
            "event_date": pa.array([date(2026, 10, 16)], type=pa.date32()),
            "event_ts": pa.array(
                [datetime(2026, 10, 16, 12, 34, 56, tzinfo=UTC)],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "optional_text": pa.array([None], type=pa.string()),
            "item_ids": pa.array([[1, 2]], type=pa.list_(pa.int32())),
        }
    )

    manifest, result, row_count, _byte_count = encode_json_array(table)

    assert manifest is not None
    assert result is not None
    assert [
        {
            "name": column["name"],
            "type_name": column["type_name"],
            "type_text": column["type_text"],
            **(
                {
                    "type_precision": column["type_precision"],
                    "type_scale": column["type_scale"],
                }
                if "type_precision" in column
                else {}
            ),
        }
        for column in manifest["schema"]["columns"]  # type: ignore[index]
    ] == [
        {"name": "session_timezone", "type_name": "STRING", "type_text": "STRING"},
        {
            "name": "amount",
            "type_name": "DECIMAL",
            "type_text": "DECIMAL(12,2)",
            "type_precision": 12,
            "type_scale": 2,
        },
        {"name": "event_date", "type_name": "DATE", "type_text": "DATE"},
        {"name": "event_ts", "type_name": "TIMESTAMP", "type_text": "TIMESTAMP"},
        {"name": "optional_text", "type_name": "STRING", "type_text": "STRING"},
        {"name": "item_ids", "type_name": "ARRAY", "type_text": "ARRAY<INT>"},
    ]
    assert result["data_array"] == [
        [
            "Etc/UTC",
            "12.34",
            "2026-10-16",
            "2026-10-16T12:34:56.000Z",
            None,
            '["1","2"]',
        ]
    ]
    assert row_count == 1


def test_json_array_timestamp_encoding_preserves_arrow_submillisecond_precision() -> None:
    table = pa.table(
        {
            "millisecond_ts": pa.array(
                [datetime(2026, 10, 21, 12, 0, 0, 123000, tzinfo=UTC)],
                type=pa.timestamp("ms", tz="UTC"),
            ),
            "microsecond_ts": pa.array(
                [datetime(2026, 10, 21, 12, 0, 0, 123456, tzinfo=UTC)],
                type=pa.timestamp("us", tz="UTC"),
            ),
        }
    )

    _manifest, result, _row_count, _byte_count = encode_json_array(table)

    assert result is not None
    assert result["data_array"] == [["2026-10-21T12:00:00.123Z", "2026-10-21T12:00:00.123456Z"]]


def test_json_array_encoding_enforces_row_and_byte_caps() -> None:
    too_many_rows = pa.table({"value": pa.array(range(MAX_INLINE_RESULT_ROWS + 1))})
    with pytest.raises(SqlResultLimitError, match="limited to 10000 rows"):
        encode_json_array(too_many_rows)

    too_many_bytes = pa.table({"value": ["x" * (MAX_INLINE_RESULT_BYTES + 1)]})
    with pytest.raises(SqlResultLimitError, match="encoded bytes"):
        encode_json_array(too_many_bytes)
