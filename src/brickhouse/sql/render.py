"""SQL serialization compatible with the selected Sparkless parser profile."""

from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.databricks import Databricks


class _SparklessDatabricksDialect(Databricks):
    class Generator(Databricks.Generator):  # type: ignore[misc, valid-type]
        def escape_str(
            self,
            text: str,
            escape_backslash: bool = True,
            delimiter: str | None = None,
            escaped_delimiter: str | None = None,
            is_byte_string: bool = False,
            is_bytes: bool = False,
        ) -> str:
            del escape_backslash, is_byte_string, is_bytes
            selected_delimiter = delimiter or self.dialect.QUOTE_END
            selected_escape = escaped_delimiter or selected_delimiter * 2
            return str(self._replace_line_breaks(text)).replace(
                selected_delimiter,
                selected_escape,
            )


def render_sparkless_sql(expression: exp.Expression) -> str:
    """Render a validated Databricks AST using Sparkless-compatible string escaping."""
    return expression.sql(dialect=_SparklessDatabricksDialect)
