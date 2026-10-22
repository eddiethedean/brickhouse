"""Specific errors for the selected local SQL profile."""


class SqlStatementError(Exception):
    """Base class for a request that the local SQL profile cannot execute."""


class SqlParseError(SqlStatementError, ValueError):
    """A SQL string is empty, malformed, or contains multiple statements."""


class UnsupportedSqlStatementError(SqlStatementError, NotImplementedError):
    """A valid statement is outside the selected SQL profile."""
