from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

from brickhouse.workspace import WorkspaceConfig, initialize_workspace, open_workspace
from brickhouse.workspace.config import (
    InvalidWorkspaceConfigError,
    UnsupportedConfigVersionError,
    UnsupportedWorkspaceFormatError,
)
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import (
    SUPPORTED_METADATA_SCHEMA_VERSION,
    WorkspaceCorruptError,
    WorkspaceExistsError,
    WorkspaceNotFoundError,
)


def test_initialize_and_reopen_workspace_with_explicit_default_namespace(
    tmp_path: Path,
) -> None:
    root = tmp_path / "workspace"

    created = initialize_workspace(root)
    reopened = open_workspace(root)

    assert created.path == root
    assert created.workspace_id == reopened.workspace_id
    assert created.config == WorkspaceConfig()
    assert created.config.default_catalog == "main"
    assert created.config.default_schema == "default"
    assert created.metadata_path.is_file()
    assert (root / "brickhouse.toml").is_file()
    directories = (
        "tables",
        "files",
        "staging",
        "results",
        "runs",
        "checkpoints",
        "integrations",
    )
    for directory in directories:
        assert (root / directory).is_dir()

    with sqlite3.connect(f"file:{created.metadata_path}?mode=ro", uri=True) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (
            SUPPORTED_METADATA_SCHEMA_VERSION,
        )
        assert connection.execute("SELECT name, is_default FROM catalogs").fetchall() == [
            ("main", 1)
        ]
        assert connection.execute(
            "SELECT catalog_name, name, is_default FROM schemas"
        ).fetchall() == [
            ("main", "default", 1),
            ("main", "information_schema", 0),
        ]
        assert connection.execute("SELECT count(*) FROM table_registrations").fetchone() == (0,)


def test_initialize_uses_configured_bootstrap_namespace(tmp_path: Path) -> None:
    config = WorkspaceConfig(default_catalog="analytics", default_schema="landing")

    workspace = initialize_workspace(tmp_path / "custom", config=config)

    assert open_workspace(workspace.path).config == config
    with sqlite3.connect(f"file:{workspace.metadata_path}?mode=ro", uri=True) as connection:
        assert connection.execute("SELECT name FROM catalogs").fetchall() == [("analytics",)]
        assert set(connection.execute("SELECT catalog_name, name FROM schemas").fetchall()) == {
            ("analytics", "landing"),
            ("analytics", "information_schema"),
        }


def test_schema_nine_migration_preserves_and_interrupts_orphan_worker_outcome(
    tmp_path: Path,
    downgrade_workspace_from_v13,
) -> None:
    descriptor = initialize_workspace(tmp_path / "workspace")
    started_at = "2026-10-20T00:00:00+00:00"
    with sqlite3.connect(descriptor.metadata_path) as connection:
        downgrade_workspace_from_v13(connection)
        for trigger in (
            "job_deletions_are_immutable",
            "job_deletions_cannot_be_deleted",
        ):
            connection.execute(f'DROP TRIGGER "{trigger}"')
        connection.execute("DROP TABLE job_deletions")
        connection.execute("DELETE FROM schema_migrations WHERE version = 12")
        connection.execute("DROP INDEX worker_attempt_outcomes_state_idx")
        connection.execute(
            "CREATE TABLE worker_attempt_outcomes_v8 ("
            "run_id TEXT NOT NULL, attempt_id TEXT NOT NULL, "
            "state TEXT NOT NULL CHECK(state IN "
            "('running', 'succeeded', 'failed', 'timed_out', 'cancelled')), "
            "exit_code INTEGER, termination_reason TEXT NOT NULL CHECK(termination_reason IN "
            "('running', 'completed', 'timeout', 'cancelled', 'launch_failed')), "
            "started_at TEXT NOT NULL, finished_at TEXT, interpreter TEXT NOT NULL, "
            "script TEXT NOT NULL, cwd TEXT NOT NULL, stdout_bytes INTEGER NOT NULL DEFAULT 0, "
            "stderr_bytes INTEGER NOT NULL DEFAULT 0, stdout_truncated INTEGER NOT NULL DEFAULT 0, "
            "stderr_truncated INTEGER NOT NULL DEFAULT 0, cleanup_error_type TEXT, "
            "PRIMARY KEY(run_id, attempt_id), "
            "CHECK((state = 'running' AND finished_at IS NULL AND exit_code IS NULL) OR "
            "(state != 'running' AND finished_at IS NOT NULL)))"
        )
        connection.execute(
            "INSERT INTO worker_attempt_outcomes_v8 "
            "(run_id, attempt_id, state, exit_code, termination_reason, started_at, "
            "finished_at, interpreter, script, cwd) "
            "VALUES ('recovery-run', 'recovery-attempt', 'running', NULL, 'running', ?, NULL, "
            "?, ?, ?)",
            (started_at, sys.executable, __file__, str(tmp_path)),
        )
        connection.execute("DROP TABLE worker_attempt_outcomes")
        connection.execute(
            "ALTER TABLE worker_attempt_outcomes_v8 RENAME TO worker_attempt_outcomes"
        )
        connection.execute(
            "CREATE INDEX worker_attempt_outcomes_state_idx "
            "ON worker_attempt_outcomes(state, started_at)"
        )
        for trigger in (
            "job_task_values_require_running_attempt_insert",
            "job_task_values_require_running_attempt_update",
            "job_task_value_identity_is_immutable",
            "job_task_values_cannot_be_deleted",
        ):
            connection.execute(f'DROP TRIGGER "{trigger}"')
        connection.execute("DROP TABLE job_task_values")
        for trigger in (
            "terraform_job_resource_identity_is_immutable",
            "terraform_job_revision_is_monotonic",
            "terraform_job_resource_delete_is_terminal",
            "terraform_job_revisions_are_immutable",
            "terraform_job_revisions_cannot_be_deleted",
            "terraform_job_resources_cannot_be_deleted",
        ):
            connection.execute(f'DROP TRIGGER "{trigger}"')
        connection.execute("DROP TABLE terraform_job_revisions")
        connection.execute("DROP TABLE terraform_job_resources")
        connection.execute("DELETE FROM schema_migrations WHERE version = 11")
        connection.execute("DELETE FROM schema_migrations WHERE version = 10")
        connection.execute("DELETE FROM schema_migrations WHERE version = 9")
        connection.execute("PRAGMA user_version = 8")

    with WorkspaceContext.local(descriptor.path) as context:
        assert context.workspace.metadata_schema_version == 21
        outcome = context.worker_outcomes.get("recovery-run", "recovery-attempt")
        assert outcome is not None
        assert outcome.state == "running"
        assert outcome.worker_pid is None
        context.worker_outcomes.interrupt_attempt("recovery-run", "recovery-attempt")
        interrupted = context.worker_outcomes.get("recovery-run", "recovery-attempt")
        assert interrupted is not None
        assert interrupted.state == "interrupted"
        assert interrupted.termination_reason == "interrupted"
        assert interrupted.exit_code is None
        assert interrupted.finished_at is not None


def test_schema_twelve_migrates_to_persistent_sql_warehouse_and_statement_contract(
    tmp_path: Path,
    downgrade_workspace_from_v13,
) -> None:
    descriptor = initialize_workspace(tmp_path / "workspace")
    with sqlite3.connect(descriptor.metadata_path) as connection:
        downgrade_workspace_from_v13(connection)

    assert open_workspace(descriptor.path).metadata_schema_version == 12
    with WorkspaceContext.local(descriptor.path) as upgraded:
        assert upgraded.workspace.metadata_schema_version == 21
        with sqlite3.connect(upgraded.workspace.metadata_path) as connection:
            assert connection.execute("PRAGMA user_version").fetchone() == (21,)
            assert connection.execute(
                "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 2"
            ).fetchall() == [(21,), (20,)]
            assert connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'sql_warehouses'"
            ).fetchone() == ("sql_warehouses",)
            assert connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name = 'sql_statement_admission_leases'"
            ).fetchone() == ("sql_statement_admission_leases",)
            assert connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'sql_statements'"
            ).fetchone() == ("sql_statements",)
            columns = {row[1] for row in connection.execute("PRAGMA table_info(sql_statements)")}
            assert "parameters_json" in columns


def test_initialize_never_overwrites_existing_path(tmp_path: Path) -> None:
    root = tmp_path / "existing"
    root.mkdir()
    marker = root / "keep.txt"
    marker.write_text("keep", encoding="utf-8")

    with pytest.raises(WorkspaceExistsError):
        initialize_workspace(root)

    assert marker.read_text(encoding="utf-8") == "keep"


def test_initialize_refuses_a_path_created_after_initial_existence_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "raced"
    absolute_root = root.resolve()
    original_exists = Path.exists

    def create_target_during_existence_check(path: Path) -> bool:
        if path == absolute_root:
            root.mkdir()
            return False
        return original_exists(path)

    monkeypatch.setattr(Path, "exists", create_target_during_existence_check)

    with pytest.raises(WorkspaceExistsError):
        initialize_workspace(root)

    assert original_exists(root)
    assert list(root.iterdir()) == []


def test_open_rejects_uninitialized_directory(tmp_path: Path) -> None:
    with pytest.raises(WorkspaceNotFoundError):
        open_workspace(tmp_path / "missing")


def test_open_handles_special_characters_in_workspace_path(tmp_path: Path) -> None:
    root = tmp_path / "workspace ? # local"

    initialized = initialize_workspace(root)

    assert open_workspace(root).workspace_id == initialized.workspace_id


def test_open_rejects_missing_workspace_directories(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    (workspace.path / "integrations").rmdir()

    with pytest.raises(WorkspaceCorruptError, match="directories are missing"):
        open_workspace(workspace.path)


def test_config_rejects_unknown_and_missing_settings(tmp_path: Path) -> None:
    config = tmp_path / "brickhouse.toml"
    config.write_text(
        "[workspace]\nconfig_version = 1\nformat_version = 1\n"
        'default_catalog = "main"\nunknown = true\n',
        encoding="utf-8",
    )
    with pytest.raises(InvalidWorkspaceConfigError, match="unknown"):
        WorkspaceConfig.from_file(config)

    config.write_text(
        "[workspace]\nconfig_version = 1\nformat_version = 1\n",
        encoding="utf-8",
    )
    with pytest.raises(InvalidWorkspaceConfigError, match="missing"):
        WorkspaceConfig.from_file(config)


def test_config_rejects_unsupported_config_and_format_versions(tmp_path: Path) -> None:
    config = tmp_path / "brickhouse.toml"
    config.write_text(
        "[workspace]\nconfig_version = 2\nformat_version = 1\n"
        'default_catalog = "main"\ndefault_schema = "default"\n',
        encoding="utf-8",
    )
    with pytest.raises(UnsupportedConfigVersionError):
        WorkspaceConfig.from_file(config)

    config.write_text(
        "[workspace]\nconfig_version = 1\nformat_version = 2\n"
        'default_catalog = "main"\ndefault_schema = "default"\n',
        encoding="utf-8",
    )
    with pytest.raises(UnsupportedWorkspaceFormatError):
        WorkspaceConfig.from_file(config)


def test_config_rejects_invalid_identifier_and_boolean_version(tmp_path: Path) -> None:
    config = tmp_path / "brickhouse.toml"
    config.write_text(
        "[workspace]\nconfig_version = true\nformat_version = 1\n"
        'default_catalog = "Main"\ndefault_schema = "default"\n',
        encoding="utf-8",
    )
    with pytest.raises(InvalidWorkspaceConfigError, match="config_version"):
        WorkspaceConfig.from_file(config)

    config.write_text(
        "[workspace]\nconfig_version = 1\nformat_version = 1\n"
        'default_catalog = "Main"\ndefault_schema = "default"\n',
        encoding="utf-8",
    )
    with pytest.raises(InvalidWorkspaceConfigError, match="default_catalog"):
        WorkspaceConfig.from_file(config)


def test_open_rejects_persisted_workspace_format_mismatch(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with sqlite3.connect(workspace.metadata_path) as connection:
        connection.execute("UPDATE workspace_metadata SET value = '2' WHERE key = 'format_version'")

    with pytest.raises(UnsupportedWorkspaceFormatError):
        open_workspace(workspace.path)


def test_open_rejects_newer_metadata_schema(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with sqlite3.connect(workspace.metadata_path) as connection:
        connection.execute(f"PRAGMA user_version = {SUPPORTED_METADATA_SCHEMA_VERSION + 1}")

    with pytest.raises(
        UnsupportedWorkspaceFormatError,
        match=f"schema version {SUPPORTED_METADATA_SCHEMA_VERSION + 1}",
    ):
        open_workspace(workspace.path)


def test_open_rejects_incomplete_task_value_schema(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with sqlite3.connect(workspace.metadata_path) as connection:
        connection.execute("DROP TRIGGER job_task_values_cannot_be_deleted")

    with pytest.raises(WorkspaceCorruptError, match="task value constraints"):
        open_workspace(workspace.path)


def test_open_rejects_config_metadata_namespace_disagreement(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    config_path = workspace.path / "brickhouse.toml"
    config_path.write_text(
        "[workspace]\nconfig_version = 1\nformat_version = 1\n"
        'default_catalog = "other"\ndefault_schema = "default"\n',
        encoding="utf-8",
    )

    with pytest.raises(WorkspaceCorruptError):
        open_workspace(workspace.path)
