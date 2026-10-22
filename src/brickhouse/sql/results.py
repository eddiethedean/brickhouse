"""Typed JSON_ARRAY result encoding for the initial SQL API profile."""

from __future__ import annotations

import base64
import json
import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, cast


class SqlResultLimitError(RuntimeError):
    """A statement result exceeds the initial inline JSON profile limits."""


MAX_INLINE_RESULT_ROWS = 10_000
MAX_INLINE_RESULT_BYTES = 8 * 1024 * 1024
MAX_INLINE_CHUNK_ROWS = 1_000
MAX_RETAINED_SQL_RESULT_BYTES = 64 * 1024 * 1024
SQL_RESULT_RETENTION_SECONDS = 12 * 60 * 60
BATCH_MATERIALIZATION_ROWS = 256


@dataclass(frozen=True, slots=True)
class BoundedArrowResult:
    """A bounded Arrow prefix plus the total rows observed while streaming."""

    table: Any
    total_row_count: int
    truncated: bool


def collect_bounded_arrow_result(
    dataframe: Any,
    *,
    row_limit: int | None = None,
    byte_limit: int | None = None,
) -> BoundedArrowResult:
    """Stream public Sparkless Arrow batches while retaining only the fitting prefix."""
    import pyarrow as pa  # type: ignore[import-untyped]

    _validate_result_limits(row_limit=row_limit, byte_limit=byte_limit)
    effective_row_limit = row_limit or MAX_INLINE_RESULT_ROWS
    effective_byte_limit = byte_limit or MAX_INLINE_RESULT_BYTES
    batch_iterator = dataframe.toArrowBatches(
        max_rows=BATCH_MATERIALIZATION_ROWS,
        max_bytes=MAX_INLINE_RESULT_BYTES,
    )

    retained_batches: list[Any] = []
    result_schema: Any | None = None
    total_row_count = 0
    retained_row_count = 0
    encoded_byte_count = 2
    byte_limit_reached = False
    try:
        with batch_iterator as batches:
            for batch in batches:
                result_schema = batch.schema
                total_row_count += batch.num_rows
                if row_limit is None and total_row_count > MAX_INLINE_RESULT_ROWS:
                    raise SqlResultLimitError(
                        f"inline SQL results are limited to {MAX_INLINE_RESULT_ROWS} rows"
                    )

                retained_in_batch = 0
                for row_index in range(batch.num_rows):
                    if byte_limit_reached or retained_row_count >= effective_row_limit:
                        continue
                    row = [
                        _encode_value(batch.column(index)[row_index].as_py(), field.type)
                        for index, field in enumerate(batch.schema)
                    ]
                    encoded_row = json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode(
                        "utf-8"
                    )
                    next_byte_count = (
                        encoded_byte_count + len(encoded_row) + (1 if retained_row_count else 0)
                    )
                    if next_byte_count > effective_byte_limit:
                        if byte_limit is None:
                            raise SqlResultLimitError(
                                "inline SQL results are limited to "
                                f"{MAX_INLINE_RESULT_BYTES} encoded bytes"
                            )
                        byte_limit_reached = True
                        continue
                    retained_row_count += 1
                    retained_in_batch += 1
                    encoded_byte_count = next_byte_count

                if retained_in_batch:
                    retained_batches.append(batch.slice(0, retained_in_batch))
    except ValueError as error:
        if "max_bytes" in str(error) or "one row" in str(error):
            raise SqlResultLimitError(
                "one result row exceeds the bounded Arrow batch byte limit"
            ) from error
        raise

    if result_schema is None:
        # No batches means there are no rows to materialize. Sparkless' empty
        # Arrow conversion supplies the query's exact schema without retaining
        # a result set.
        empty = dataframe.toArrow()
        return BoundedArrowResult(empty, 0, False)

    table = pa.Table.from_batches(retained_batches, schema=result_schema)
    return BoundedArrowResult(
        table,
        total_row_count,
        total_row_count > retained_row_count or byte_limit_reached,
    )


def encode_json_array(
    table: Any | None,
    *,
    row_limit: int | None = None,
    byte_limit: int | None = None,
    total_row_count: int | None = None,
    result_truncated: bool = False,
) -> tuple[dict[str, object] | None, dict[str, object] | None, int, int]:
    """Convert an Arrow table to the pinned Statement Execution JSON_ARRAY shape."""
    if table is None:
        return None, None, 0, 0
    _validate_result_limits(row_limit=row_limit, byte_limit=byte_limit)
    if table.num_rows > MAX_INLINE_RESULT_ROWS and row_limit is None:
        raise SqlResultLimitError(
            f"inline SQL results are limited to {MAX_INLINE_RESULT_ROWS} rows"
        )

    effective_row_limit = row_limit or MAX_INLINE_RESULT_ROWS
    effective_byte_limit = byte_limit or MAX_INLINE_RESULT_BYTES
    columns = [_column_info(field, index) for index, field in enumerate(table.schema)]
    column_values = [table.column(index).to_pylist() for index in range(len(table.schema))]
    rows: list[list[str | None]] = []
    encoded_byte_count = 2
    actual_total_row_count = table.num_rows if total_row_count is None else total_row_count
    truncated = result_truncated or actual_total_row_count > effective_row_limit
    for row_index in range(min(table.num_rows, effective_row_limit)):
        row = [
            _encode_value(column_values[index][row_index], field.type)
            for index, field in enumerate(table.schema)
        ]
        encoded_row = json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        next_byte_count = encoded_byte_count + len(encoded_row) + (1 if rows else 0)
        if next_byte_count > effective_byte_limit:
            if byte_limit is None:
                raise SqlResultLimitError(
                    f"inline SQL results are limited to {MAX_INLINE_RESULT_BYTES} encoded bytes"
                )
            truncated = True
            break
        encoded_byte_count = next_byte_count
        rows.append(row)

    encoded_rows = json.dumps(rows, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    schema = {"column_count": len(columns), "columns": columns}
    manifest: dict[str, object] = {
        "chunks": [{"chunk_index": 0, "row_count": len(rows), "row_offset": 0}],
        "format": "JSON_ARRAY",
        "schema": schema,
        "total_chunk_count": 1,
        "total_row_count": actual_total_row_count,
        "truncated": truncated,
    }
    result: dict[str, object] = {
        "chunk_index": 0,
        "data_array": rows,
        "row_count": len(rows),
        "row_offset": 0,
    }
    return manifest, result, len(rows), len(encoded_rows)


def _validate_result_limits(*, row_limit: int | None, byte_limit: int | None) -> None:
    if row_limit is not None and not 1 <= row_limit <= MAX_INLINE_RESULT_ROWS:
        raise SqlResultLimitError(f"row_limit must be between 1 and {MAX_INLINE_RESULT_ROWS}")
    if byte_limit is not None and not 1 <= byte_limit <= MAX_INLINE_RESULT_BYTES:
        raise SqlResultLimitError(f"byte_limit must be between 1 and {MAX_INLINE_RESULT_BYTES}")


def split_json_array_result(
    manifest: dict[str, object] | None,
    result: dict[str, object] | None,
) -> tuple[dict[str, object] | None, list[dict[str, object]] | None]:
    """Split bounded inline rows into stable, index-addressable response chunks."""
    if manifest is None or result is None:
        return None, None
    raw_rows = result.get("data_array")
    if not isinstance(raw_rows, list):
        raise SqlResultLimitError("JSON_ARRAY result rows are malformed")
    rows = raw_rows
    chunks: list[dict[str, object]] = []
    chunk_descriptors: list[dict[str, object]] = []
    for offset in range(0, len(rows), MAX_INLINE_CHUNK_ROWS):
        chunk_rows = rows[offset : offset + MAX_INLINE_CHUNK_ROWS]
        index = len(chunks)
        chunks.append(
            {
                "chunk_index": index,
                "data_array": chunk_rows,
                "row_count": len(chunk_rows),
                "row_offset": offset,
            }
        )
        chunk_descriptors.append(
            {"chunk_index": index, "row_count": len(chunk_rows), "row_offset": offset}
        )
    if not chunks:
        chunks.append({"chunk_index": 0, "data_array": [], "row_count": 0, "row_offset": 0})
        chunk_descriptors.append({"chunk_index": 0, "row_count": 0, "row_offset": 0})
    split_manifest = dict(manifest)
    split_manifest["chunks"] = chunk_descriptors
    split_manifest["total_chunk_count"] = len(chunks)
    return split_manifest, chunks


def _column_info(field: Any, position: int) -> dict[str, object]:
    import pyarrow as pa

    data_type = field.type
    if pa.types.is_dictionary(data_type):
        data_type = data_type.value_type
    type_name, type_text, precision, scale = _sql_type(data_type)
    column: dict[str, object] = {
        "name": field.name,
        "position": position,
        "type_name": type_name,
        "type_text": type_text,
    }
    if precision is not None:
        column["type_precision"] = precision
    if scale is not None:
        column["type_scale"] = scale
    return column


def _sql_type(data_type: Any) -> tuple[str, str, int | None, int | None]:
    import pyarrow as pa

    if pa.types.is_null(data_type):
        return "NULL", "VOID", None, None
    if pa.types.is_boolean(data_type):
        return "BOOLEAN", "BOOLEAN", None, None
    if pa.types.is_int8(data_type):
        return "BYTE", "TINYINT", None, None
    if pa.types.is_int16(data_type):
        return "SHORT", "SMALLINT", None, None
    if pa.types.is_int32(data_type):
        return "INT", "INT", None, None
    if pa.types.is_int64(data_type):
        return "LONG", "BIGINT", None, None
    if pa.types.is_float32(data_type):
        return "FLOAT", "FLOAT", None, None
    if pa.types.is_float64(data_type):
        return "DOUBLE", "DOUBLE", None, None
    if pa.types.is_decimal(data_type):
        return (
            "DECIMAL",
            f"DECIMAL({data_type.precision},{data_type.scale})",
            data_type.precision,
            data_type.scale,
        )
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return "STRING", "STRING", None, None
    if (
        pa.types.is_binary(data_type)
        or pa.types.is_large_binary(data_type)
        or pa.types.is_fixed_size_binary(data_type)
    ):
        return "BINARY", "BINARY", None, None
    if pa.types.is_date(data_type):
        return "DATE", "DATE", None, None
    if pa.types.is_timestamp(data_type):
        return "TIMESTAMP", "TIMESTAMP", None, None
    if (
        pa.types.is_list(data_type)
        or pa.types.is_large_list(data_type)
        or pa.types.is_fixed_size_list(data_type)
    ):
        value_type = data_type.value_type
        return "ARRAY", f"ARRAY<{_sql_type(value_type)[1]}>", None, None
    if pa.types.is_struct(data_type):
        fields = ",".join(f"{field.name}:{_sql_type(field.type)[1]}" for field in data_type)
        return "STRUCT", f"STRUCT<{fields}>", None, None
    if pa.types.is_map(data_type):
        key_type = _sql_type(data_type.key_type)[1]
        value_type = _sql_type(data_type.item_type)[1]
        return "MAP", f"MAP<{key_type},{value_type}>", None, None
    raise SqlResultLimitError(f"Arrow result type is unsupported by JSON_ARRAY: {data_type}")


def _encode_value(value: object, data_type: Any) -> str | None:
    import pyarrow as pa

    if value is None:
        return None
    if pa.types.is_dictionary(data_type):
        return _encode_value(value, data_type.value_type)
    if pa.types.is_boolean(data_type):
        return "true" if value else "false"
    if pa.types.is_integer(data_type):
        return str(value)
    if pa.types.is_floating(data_type):
        number = float(cast(float, value))
        if math.isnan(number):
            return "NaN"
        if math.isinf(number):
            return "Infinity" if number > 0 else "-Infinity"
        return repr(number)
    if pa.types.is_decimal(data_type):
        return format(value, "f") if isinstance(value, Decimal) else str(value)
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return str(value)
    if (
        pa.types.is_binary(data_type)
        or pa.types.is_large_binary(data_type)
        or pa.types.is_fixed_size_binary(data_type)
    ):
        return base64.b64encode(bytes(cast(bytes, value))).decode("ascii")
    if pa.types.is_date(data_type):
        return value.isoformat() if isinstance(value, date) else str(value)
    if pa.types.is_timestamp(data_type):
        if not isinstance(value, datetime):
            return str(value)
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        value = value.astimezone(UTC)
        # SQL TIMESTAMP has microsecond precision. Keep the established compact
        # millisecond representation when it is exact, while retaining sub-ms
        # digits carried by Arrow's microsecond timestamp type.
        timespec = (
            "microseconds"
            if getattr(data_type, "unit", "ms") in {"us", "ns"} and value.microsecond % 1_000 != 0
            else "milliseconds"
        )
        return value.isoformat(timespec=timespec).replace("+00:00", "Z")
    if (
        pa.types.is_list(data_type)
        or pa.types.is_large_list(data_type)
        or pa.types.is_fixed_size_list(data_type)
    ):
        nested = [
            _encode_nested(item, data_type.value_type) for item in cast(Sequence[object], value)
        ]
        return json.dumps(nested, ensure_ascii=False, separators=(",", ":"))
    if pa.types.is_struct(data_type):
        nested_value = value if isinstance(value, dict) else {}
        nested_struct = {
            field.name: _encode_nested(nested_value.get(field.name), field.type)
            for field in data_type
        }
        return json.dumps(nested_struct, ensure_ascii=False, separators=(",", ":"))
    if pa.types.is_map(data_type):
        nested_map: dict[str, object] = {}
        for key, item in cast(Sequence[tuple[object, object]], value):
            encoded_key = _encode_nested(key, data_type.key_type)
            key_text = (
                encoded_key
                if isinstance(encoded_key, str)
                else json.dumps(encoded_key, ensure_ascii=False, separators=(",", ":"))
            )
            nested_map["null" if encoded_key is None else key_text] = _encode_nested(
                item, data_type.item_type
            )
        return json.dumps(nested_map, ensure_ascii=False, separators=(",", ":"))
    raise SqlResultLimitError(f"Arrow result type is unsupported by JSON_ARRAY: {data_type}")


def _encode_nested(value: object, data_type: Any) -> str | None | list[object] | dict[str, object]:
    """Encode one nested SQL value while retaining JSON null and string leaves."""
    if value is None:
        return None
    encoded = _encode_value(value, data_type)
    if encoded is None:
        return None
    if data_type is not None:
        import pyarrow as pa

        if (
            pa.types.is_list(data_type)
            or pa.types.is_large_list(data_type)
            or pa.types.is_fixed_size_list(data_type)
            or pa.types.is_struct(data_type)
            or pa.types.is_map(data_type)
        ):
            return cast(str | list[object] | dict[str, object], json.loads(encoded))
    return encoded
