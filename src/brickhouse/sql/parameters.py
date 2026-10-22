"""Typed binding for Databricks SQL's named scalar parameter markers."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import cast

import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel, ParseError

from brickhouse.sql.errors import SqlStatementError
from brickhouse.sql.render import render_sparkless_sql


class SqlStatementParameterError(SqlStatementError, ValueError):
    """A statement parameter request is malformed or cannot be bound safely."""


@dataclass(frozen=True, slots=True)
class SqlStatementParameter:
    """One named Databricks Statement Execution scalar parameter."""

    name: str
    value: str | None = None
    type: str | None = None


_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DECIMAL_TYPE_RE = re.compile(r"^DECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$", re.IGNORECASE)
_SUPPORTED_TYPES = {
    "BOOL": "BOOLEAN",
    "BOOLEAN": "BOOLEAN",
    "BYTE": "TINYINT",
    "TINYINT": "TINYINT",
    "SHORT": "SMALLINT",
    "SMALLINT": "SMALLINT",
    "INT": "INT",
    "INTEGER": "INT",
    "LONG": "BIGINT",
    "BIGINT": "BIGINT",
    "FLOAT": "FLOAT",
    "DOUBLE": "DOUBLE",
    "STRING": "STRING",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
}
_INTEGER_RANGES = {
    "TINYINT": (-(2**7), 2**7 - 1),
    "SMALLINT": (-(2**15), 2**15 - 1),
    "INT": (-(2**31), 2**31 - 1),
    "BIGINT": (-(2**63), 2**63 - 1),
}


def bind_statement_parameters(
    statement: str,
    parameters: tuple[SqlStatementParameter, ...],
) -> str:
    """Replace named markers by typed AST literals, never by text interpolation."""
    expression = _parse_statement(statement)
    bind_expression_parameters(expression, parameters)
    return render_sparkless_sql(expression)


def validate_statement_parameters(
    statement: str,
    parameters: tuple[SqlStatementParameter, ...],
) -> None:
    """Validate marker/value correspondence before persisting a statement request."""
    try:
        parsed = sqlglot.parse(statement, read="databricks", error_level=ErrorLevel.RAISE)
    except ParseError:
        if parameters:
            raise SqlStatementParameterError(
                "cannot bind parameters in a statement that does not parse"
            ) from None
        return
    if len(parsed) != 1 or parsed[0] is None:
        if parameters:
            raise SqlStatementParameterError(
                "parameters require exactly one non-empty SQL statement"
            )
        return
    bind_expression_parameters(cast(exp.Expression, parsed[0]), parameters)


def serialize_statement_parameters(
    parameters: tuple[SqlStatementParameter, ...],
) -> str | None:
    """Encode typed parameter values for durable asynchronous statement execution."""
    if not parameters:
        return None
    return json.dumps(
        [
            {"name": parameter.name, "type": parameter.type, "value": parameter.value}
            for parameter in parameters
        ],
        separators=(",", ":"),
        ensure_ascii=False,
    )


def deserialize_statement_parameters(value: str | None) -> tuple[SqlStatementParameter, ...]:
    """Restore the immutable parameter set retained with a pending statement."""
    if value is None:
        return ()
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as error:
        raise SqlStatementParameterError("stored SQL statement parameters are corrupt") from error
    if not isinstance(decoded, list):
        raise SqlStatementParameterError("stored SQL statement parameters are corrupt")
    parameters: list[SqlStatementParameter] = []
    for item in decoded:
        if not isinstance(item, dict):
            raise SqlStatementParameterError("stored SQL statement parameters are corrupt")
        name, parameter_type, parameter_value = (
            item.get("name"),
            item.get("type"),
            item.get("value"),
        )
        if (
            not isinstance(name, str)
            or (parameter_type is not None and not isinstance(parameter_type, str))
            or (parameter_value is not None and not isinstance(parameter_value, str))
        ):
            raise SqlStatementParameterError("stored SQL statement parameters are corrupt")
        parameters.append(SqlStatementParameter(name, parameter_value, parameter_type))
    return tuple(parameters)


def bind_expression_parameters(
    expression: exp.Expression,
    parameters: tuple[SqlStatementParameter, ...],
) -> None:
    """Replace parameter nodes in a parsed AST with checked typed literals."""
    markers = list(expression.find_all(exp.Placeholder))
    if not markers and not parameters:
        return
    if not markers and parameters:
        raise SqlStatementParameterError("statement parameters were provided but no markers exist")

    named_markers: list[tuple[exp.Placeholder, str]] = []
    for marker in markers:
        name = marker.this
        if not isinstance(name, str) or not name:
            raise SqlStatementParameterError(
                "positional parameter markers are not supported; use named :markers"
            )
        if _NAME_RE.fullmatch(name) is None:
            raise SqlStatementParameterError(f"invalid SQL parameter marker name: {name}")
        named_markers.append((marker, name.casefold()))

    parameters_by_name: dict[str, SqlStatementParameter] = {}
    for parameter in parameters:
        if not isinstance(parameter.name, str) or _NAME_RE.fullmatch(parameter.name) is None:
            raise SqlStatementParameterError("parameter names must be simple SQL marker names")
        key = parameter.name.casefold()
        if key in parameters_by_name:
            raise SqlStatementParameterError(f"duplicate SQL parameter: {parameter.name}")
        if parameter.value is not None and not isinstance(parameter.value, str):
            raise SqlStatementParameterError("parameter values must be strings or null")
        if parameter.type is not None and not isinstance(parameter.type, str):
            raise SqlStatementParameterError("parameter types must be strings")
        parameters_by_name[key] = parameter

    marker_names = {name for _, name in named_markers}
    provided_names = set(parameters_by_name)
    missing = sorted(marker_names - provided_names)
    if missing:
        raise SqlStatementParameterError(f"missing SQL parameter(s): {', '.join(missing)}")
    unused = sorted(provided_names - marker_names)
    if unused:
        raise SqlStatementParameterError(f"unused SQL parameter(s): {', '.join(unused)}")

    for marker, name in named_markers:
        marker.replace(_parameter_expression(parameters_by_name[name]))


def _parse_statement(
    statement: str,
) -> exp.Expression:
    try:
        parsed = sqlglot.parse(statement, read="databricks", error_level=ErrorLevel.RAISE)
    except ParseError:
        raise SqlStatementParameterError(
            "cannot bind parameters in a statement that does not parse"
        ) from None
    if len(parsed) != 1 or parsed[0] is None:
        raise SqlStatementParameterError("parameters require exactly one non-empty SQL statement")
    return cast(exp.Expression, parsed[0])


def _parameter_expression(parameter: SqlStatementParameter) -> exp.Expression:
    value_type, decimal_shape = _normalize_type(parameter.type)
    if parameter.value is None:
        expression: exp.Expression = exp.Null()
    elif value_type == "STRING":
        expression = exp.Literal.string(parameter.value)
    elif value_type == "BOOLEAN":
        normalized = parameter.value.strip().casefold()
        if normalized not in {"true", "false"}:
            raise SqlStatementParameterError(
                f"parameter {parameter.name} is not a valid BOOLEAN value"
            )
        expression = exp.Boolean(this=normalized == "true")
    elif value_type in _INTEGER_RANGES:
        try:
            number = int(parameter.value, 10)
        except ValueError:
            raise SqlStatementParameterError(
                f"parameter {parameter.name} is not a valid {value_type} value"
            ) from None
        lower, upper = _INTEGER_RANGES[value_type]
        if not lower <= number <= upper:
            raise SqlStatementParameterError(
                f"parameter {parameter.name} is outside the range for {value_type}"
            )
        expression = exp.Literal.number(str(number))
    elif value_type in {"FLOAT", "DOUBLE"}:
        try:
            floating_number = Decimal(parameter.value)
        except InvalidOperation:
            raise SqlStatementParameterError(
                f"parameter {parameter.name} is not a valid {value_type} value"
            ) from None
        if not floating_number.is_finite():
            raise SqlStatementParameterError(
                f"parameter {parameter.name} must be a finite {value_type} value"
            )
        expression = exp.Literal.number(parameter.value)
    elif value_type == "DECIMAL":
        assert decimal_shape is not None
        precision, scale = decimal_shape
        try:
            decimal_number = Decimal(parameter.value)
            quantum = Decimal(1).scaleb(-scale)
            normalized_number = decimal_number.quantize(quantum)
        except (InvalidOperation, ValueError):
            raise SqlStatementParameterError(
                f"parameter {parameter.name} is not a valid DECIMAL value"
            ) from None
        if not decimal_number.is_finite() or normalized_number != decimal_number:
            raise SqlStatementParameterError(
                f"parameter {parameter.name} exceeds DECIMAL scale {scale}"
            )
        integer_digits = max(0, normalized_number.adjusted() + 1) if normalized_number else 0
        if integer_digits + scale > precision:
            raise SqlStatementParameterError(
                f"parameter {parameter.name} exceeds DECIMAL precision {precision}"
            )
        expression = exp.Literal.number(format(normalized_number, f".{scale}f"))
    elif value_type == "DATE":
        try:
            date.fromisoformat(parameter.value)
        except ValueError:
            raise SqlStatementParameterError(
                f"parameter {parameter.name} is not a valid DATE value"
            ) from None
        expression = exp.Literal.string(parameter.value)
    elif value_type == "TIMESTAMP":
        try:
            timestamp = datetime.fromisoformat(parameter.value.replace("Z", "+00:00"))
        except ValueError:
            raise SqlStatementParameterError(
                f"parameter {parameter.name} is not a valid TIMESTAMP value"
            ) from None
        if timestamp.tzinfo is not None:
            timestamp = timestamp.astimezone(UTC).replace(tzinfo=None)
        timespec = "microseconds" if timestamp.microsecond else "seconds"
        expression = exp.Literal.string(timestamp.isoformat(sep=" ", timespec=timespec))
    else:
        raise AssertionError(f"unhandled SQL parameter type: {value_type}")

    if value_type in {"STRING", "BOOLEAN"}:
        return expression
    cast_type = (
        f"DECIMAL({decimal_shape[0]},{decimal_shape[1]})"
        if decimal_shape is not None
        else value_type
    )
    return exp.Cast(
        this=expression,
        to=exp.DataType.build(cast_type, dialect="spark"),
    )


def _normalize_type(value: str | None) -> tuple[str, tuple[int, int] | None]:
    if value is None or not value.strip():
        return "STRING", None
    normalized = " ".join(value.strip().upper().split())
    decimal_match = _DECIMAL_TYPE_RE.fullmatch(normalized)
    if decimal_match is not None:
        precision, scale = int(decimal_match.group(1)), int(decimal_match.group(2))
        if not 1 <= precision <= 38 or not 0 <= scale <= precision:
            raise SqlStatementParameterError(
                "DECIMAL precision or scale is outside the supported range"
            )
        return "DECIMAL", (precision, scale)
    if normalized == "DECIMAL":
        raise SqlStatementParameterError("DECIMAL parameters require explicit precision and scale")
    result = _SUPPORTED_TYPES.get(normalized)
    if result is None:
        raise SqlStatementParameterError(f"SQL parameter type is unsupported: {value}")
    return result, None
