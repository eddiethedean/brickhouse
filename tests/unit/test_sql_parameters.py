from __future__ import annotations

import pytest

from brickhouse.sql.parameters import (
    SqlStatementParameter,
    SqlStatementParameterError,
    bind_statement_parameters,
)


def test_named_scalar_parameters_bind_through_sql_ast_without_text_substitution() -> None:
    statement = "SELECT :label AS label, :count AS total, :label AS repeated"
    bound = bind_statement_parameters(
        statement,
        (
            SqlStatementParameter("label", "a'; DROP TABLE orders; --"),
            SqlStatementParameter("count", "12", "INT"),
        ),
    )

    assert "DROP TABLE" in bound
    assert "AS label" in bound
    assert "CAST(12 AS INT)" in bound
    assert "'a''; DROP TABLE orders; --'" in bound
    assert ":label" not in bound
    assert ":count" not in bound


@pytest.mark.parametrize(
    ("parameter", "expected"),
    [
        (SqlStatementParameter("value", None), "SELECT NULL AS result"),
        (SqlStatementParameter("value", "true", "BOOLEAN"), "SELECT TRUE AS result"),
        (SqlStatementParameter("value", "12.30", "DECIMAL(10,2)"), "CAST(12.30 AS DECIMAL(10, 2))"),
        (SqlStatementParameter("value", "2026-10-20", "DATE"), "CAST('2026-10-20' AS DATE)"),
    ],
)
def test_supported_parameter_types_and_null(
    parameter: SqlStatementParameter,
    expected: str,
) -> None:
    bound = bind_statement_parameters("SELECT :value AS result", (parameter,))
    assert expected in bound


@pytest.mark.parametrize(
    ("statement", "parameters", "message"),
    [
        ("SELECT ? AS value", (), "positional"),
        ("SELECT :value AS value", (), "missing SQL parameter"),
        (
            "SELECT :value AS value",
            (SqlStatementParameter("other", "x"),),
            "missing SQL parameter",
        ),
        (
            "SELECT :value AS value",
            (SqlStatementParameter("value", "x"), SqlStatementParameter("VALUE", "y")),
            "duplicate",
        ),
        (
            "SELECT :value AS value",
            (SqlStatementParameter("value", "x"), SqlStatementParameter("other", "y")),
            "unused",
        ),
        (
            "SELECT :value AS value",
            (SqlStatementParameter("value", "1", "ARRAY<INT>"),),
            "unsupported",
        ),
        (
            "SELECT :value AS value",
            (SqlStatementParameter("value", "999", "TINYINT"),),
            "range",
        ),
        (
            "SELECT :value AS value",
            (SqlStatementParameter("value", "1.234", "DECIMAL(4,2)"),),
            "scale",
        ),
    ],
)
def test_invalid_parameter_requests_are_rejected(statement, parameters, message) -> None:
    with pytest.raises(SqlStatementParameterError, match=message):
        bind_statement_parameters(statement, parameters)


def test_marker_like_text_inside_string_literal_is_not_treated_as_parameter() -> None:
    statement = "SELECT ':not_a_marker' AS literal, :value AS result"
    bound = bind_statement_parameters(
        statement,
        (SqlStatementParameter("value", "ok"),),
    )
    assert "':not_a_marker'" in bound
    assert "'ok'" in bound
