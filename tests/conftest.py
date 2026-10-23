from __future__ import annotations

import sqlite3
from collections.abc import Callable
from typing import Any

import pytest


def _assert_report_matches(actual: Any, expected: Any, path: str = "report") -> None:
    if isinstance(expected, dict):
        assert isinstance(actual, dict), f"{path}: expected a mapping"
        assert actual.keys() == expected.keys(), f"{path}: report keys differ"
        for key, expected_value in expected.items():
            _assert_report_matches(actual[key], expected_value, f"{path}.{key}")
        return
    if isinstance(expected, list):
        assert isinstance(actual, list), f"{path}: expected a list"
        assert len(actual) == len(expected), f"{path}: list lengths differ"
        for index, (actual_value, expected_value) in enumerate(zip(actual, expected, strict=True)):
            _assert_report_matches(actual_value, expected_value, f"{path}[{index}]")
        return
    if isinstance(expected, float):
        assert isinstance(actual, (int, float)) and not isinstance(actual, bool), (
            f"{path}: expected a number"
        )
        assert actual == pytest.approx(expected, rel=1e-12, abs=1e-12), (
            f"{path}: calculated and recorded values differ"
        )
        return
    assert actual == expected, f"{path}: calculated and recorded values differ"


@pytest.fixture
def assert_report_matches() -> Callable[[Any, Any], None]:
    """Compare retained reports exactly except for insignificant float rounding."""
    return _assert_report_matches


@pytest.fixture
def downgrade_from_v21() -> Callable[[sqlite3.Connection], None]:
    """Downgrade schema 21 queue metadata and schema 20 worker-log expiry."""

    def downgrade(connection: sqlite3.Connection) -> None:
        connection.execute("DROP INDEX IF EXISTS job_runs_admission_state_idx")
        connection.execute("ALTER TABLE job_runs DROP COLUMN dispatched_at")
        connection.execute("ALTER TABLE job_runs DROP COLUMN admission_key")
        connection.execute("DELETE FROM schema_migrations WHERE version = 21")
        connection.execute("DROP TRIGGER job_attempt_outputs_are_immutable")
        connection.execute("ALTER TABLE job_task_attempt_outputs DROP COLUMN expired_at")
        connection.execute(
            "CREATE TRIGGER job_attempt_outputs_are_immutable "
            "BEFORE UPDATE ON job_task_attempt_outputs "
            "BEGIN SELECT RAISE(ABORT, 'job task attempt output is immutable'); END"
        )
        connection.execute("DELETE FROM schema_migrations WHERE version = 20")

    return downgrade


@pytest.fixture
def downgrade_workspace_from_v13(
    downgrade_from_v21: Callable[[sqlite3.Connection], None],
) -> Callable[[sqlite3.Connection], None]:
    """Remove the latest SQL metadata objects to construct an authentic v12 fixture."""

    def downgrade(connection: sqlite3.Connection) -> None:
        connection.execute("PRAGMA foreign_keys = OFF")
        downgrade_from_v21(connection)
        connection.execute("DROP INDEX IF EXISTS table_operation_journal_pending_idx")
        connection.execute(
            "ALTER TABLE table_operation_journal RENAME TO table_operation_journal_v18"
        )
        connection.execute(
            "CREATE TABLE table_operation_journal ("
            "table_id TEXT NOT NULL, operation_id TEXT NOT NULL, "
            "fingerprint TEXT NOT NULL CHECK(length(fingerprint) = 64), "
            "mode TEXT NOT NULL CHECK(mode IN ('create', 'append', 'overwrite')), "
            "schema_fingerprint TEXT NOT NULL CHECK(length(schema_fingerprint) = 64), "
            "input_sha256 TEXT NOT NULL CHECK(length(input_sha256) = 64), "
            "transaction_version INTEGER NOT NULL CHECK(transaction_version >= 0), "
            "base_version INTEGER NOT NULL CHECK(base_version >= -1), "
            "row_count INTEGER NOT NULL CHECK(row_count >= 0), "
            "state TEXT NOT NULL CHECK(state IN "
            "('prepared', 'committing', 'committed', 'uncertain')), "
            "commit_version INTEGER CHECK(commit_version IS NULL OR commit_version >= 0), "
            "error_code TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, "
            "PRIMARY KEY(table_id, operation_id), UNIQUE(table_id, transaction_version), "
            "CHECK((state = 'committed' AND commit_version IS NOT NULL) OR "
            "(state != 'committed' AND commit_version IS NULL)), "
            "FOREIGN KEY (table_id) REFERENCES table_registrations(table_id) ON DELETE RESTRICT"
            ")"
        )
        connection.execute(
            "INSERT INTO table_operation_journal "
            "SELECT table_id, operation_id, fingerprint, mode, schema_fingerprint, input_sha256, "
            "transaction_version, base_version, row_count, state, commit_version, error_code, "
            "created_at, updated_at FROM table_operation_journal_v18 "
            "WHERE mode IN ('create', 'append', 'overwrite') "
            "AND state IN ('prepared', 'committing', 'committed', 'uncertain')"
        )
        connection.execute("DROP TABLE table_operation_journal_v18")
        connection.execute(
            "CREATE INDEX table_operation_journal_pending_idx "
            "ON table_operation_journal(table_id, state, transaction_version)"
        )
        connection.execute("DELETE FROM schema_migrations WHERE version = 19")
        connection.execute("DELETE FROM schema_migrations WHERE version = 18")
        connection.execute("PRAGMA user_version = 17")
        for trigger in (
            "sql_statement_result_identity_is_immutable",
            "sql_statement_result_close_is_one_way",
            "sql_statement_result_chunk_update_is_forbidden",
            "sql_statement_result_chunk_delete_requires_close",
        ):
            connection.execute(f'DROP TRIGGER IF EXISTS "{trigger}"')
        connection.execute('DROP INDEX IF EXISTS "sql_statement_results_expiry_idx"')
        connection.execute('DROP TABLE IF EXISTS "sql_statement_result_chunks"')
        connection.execute('DROP TABLE IF EXISTS "sql_statement_results"')
        connection.execute('DROP TRIGGER IF EXISTS "sql_statement_identity_is_immutable"')
        connection.execute("ALTER TABLE sql_statements DROP COLUMN byte_limit")
        connection.execute("ALTER TABLE sql_statements DROP COLUMN row_limit")
        connection.execute("DELETE FROM schema_migrations WHERE version = 17")
        for trigger in (
            "sql_statement_identity_is_immutable",
            "sql_statement_state_transitions_are_valid",
            "sql_terminal_statements_are_immutable",
            "sql_statements_cannot_be_deleted",
        ):
            connection.execute(f'DROP TRIGGER IF EXISTS "{trigger}"')
        connection.execute('DROP TABLE IF EXISTS "sql_statements"')
        connection.execute("DELETE FROM schema_migrations WHERE version = 15")
        connection.execute("DELETE FROM schema_migrations WHERE version = 16")
        connection.execute("DELETE FROM schema_migrations WHERE version = 14")
        connection.execute("PRAGMA user_version = 13")
        for trigger in (
            "sql_warehouse_identity_is_immutable",
            "sql_warehouse_delete_is_terminal",
            "sql_warehouse_events_are_immutable",
            "sql_warehouse_events_cannot_be_deleted",
            "sql_statement_lease_identity_is_immutable",
            "sql_statement_lease_terminal_is_immutable",
            "sql_statement_leases_cannot_be_deleted",
        ):
            connection.execute(f'DROP TRIGGER IF EXISTS "{trigger}"')
        for table in (
            "sql_statement_admission_leases",
            "sql_warehouse_lifecycle_events",
            "sql_warehouses",
        ):
            connection.execute(f'DROP TABLE IF EXISTS "{table}"')
        connection.execute("DELETE FROM schema_migrations WHERE version = 13")
        connection.execute("PRAGMA user_version = 12")
        connection.execute("PRAGMA foreign_keys = ON")

    return downgrade
