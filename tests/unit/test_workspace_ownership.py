from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

from brickhouse.workspace import (
    Migration,
    WorkspaceInUseError,
    WorkspaceMigrationError,
    acquire_workspace_owner,
    initialize_workspace,
)
from brickhouse.workspace.migrations import apply_migrations


def test_owner_lock_blocks_a_second_process_and_recovers_after_crash(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    ready_file = tmp_path / "owner-ready"
    source_root = Path(__file__).resolve().parents[2] / "src"
    child_code = (
        "import sys, time\n"
        "from pathlib import Path\n"
        "from brickhouse.workspace import acquire_workspace_owner\n"
        "owner = acquire_workspace_owner(sys.argv[1])\n"
        "Path(sys.argv[2]).write_text(str(owner.workspace.workspace_id))\n"
        "time.sleep(60)\n"
    )
    environment = os.environ.copy()
    environment.pop("DATABRICKS_TOKEN", None)
    environment["PYTHONPATH"] = os.pathsep.join(
        part for part in (str(source_root), environment.get("PYTHONPATH", "")) if part
    )
    process = subprocess.Popen(
        [sys.executable, "-c", child_code, str(workspace.path), str(ready_file)],
        cwd=source_root.parent,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 10
        while not ready_file.exists() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.02)
        assert ready_file.exists(), "child process did not acquire workspace ownership"
        with pytest.raises(WorkspaceInUseError):
            acquire_workspace_owner(workspace.path)
    finally:
        if process.poll() is None:
            process.terminate()
        process.wait(timeout=10)

    recovered = acquire_workspace_owner(workspace.path)
    try:
        assert recovered.recovered_stale_owner
        assert recovered.previous_owner is not None
        assert recovered.previous_owner["pid"] == process.pid
        assert recovered.previous_owner["workspace_id"] == workspace.workspace_id
    finally:
        recovered.release()


def test_owners_and_metadata_are_independent_between_workspaces(tmp_path: Path) -> None:
    first = initialize_workspace(tmp_path / "first")
    second = initialize_workspace(tmp_path / "second")
    assert first.workspace_id != second.workspace_id

    with acquire_workspace_owner(first.path) as first_owner:
        with acquire_workspace_owner(second.path) as second_owner:
            assert first_owner.is_acquired
            assert second_owner.is_acquired
            with sqlite3.connect(first.metadata_path) as connection:
                connection.execute(
                    "INSERT INTO workspace_metadata(key, value) VALUES ('test_marker', 'first')"
                )
            with sqlite3.connect(second.metadata_path) as connection:
                connection.execute(
                    "INSERT INTO workspace_metadata(key, value) VALUES ('test_marker', 'second')"
                )

    with sqlite3.connect(first.metadata_path) as connection:
        assert connection.execute(
            "SELECT value FROM workspace_metadata WHERE key = 'test_marker'"
        ).fetchone() == ("first",)
    with sqlite3.connect(second.metadata_path) as connection:
        assert connection.execute(
            "SELECT value FROM workspace_metadata WHERE key = 'test_marker'"
        ).fetchone() == ("second",)


def test_owner_context_releases_lock_deterministically(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    lock_path = workspace.path / ".brickhouse-owner.lock"

    with acquire_workspace_owner(workspace.path) as owner:
        assert owner.is_acquired
        assert lock_path.is_file()
        assert lock_path.stat().st_mode & 0o777 == 0o600

    assert not owner.is_acquired
    assert lock_path.read_bytes() == b""
    with acquire_workspace_owner(workspace.path) as reopened:
        assert reopened.is_acquired
        assert not reopened.recovered_stale_owner


def test_migrations_roll_back_schema_history_and_user_version_on_failure(
    tmp_path: Path,
) -> None:
    database = tmp_path / "migration.sqlite"
    connection = sqlite3.connect(database)

    def create_first_schema(transaction: sqlite3.Connection) -> None:
        transaction.execute("CREATE TABLE first_step(value TEXT)")

    def fail_after_partial_migration(transaction: sqlite3.Connection) -> None:
        transaction.execute("CREATE TABLE partial_second_step(value TEXT)")
        raise RuntimeError("injected migration failure")

    try:
        with pytest.raises(WorkspaceMigrationError, match="migration 2 failed"):
            apply_migrations(
                connection,
                (
                    Migration(1, create_first_schema),
                    Migration(2, fail_after_partial_migration),
                ),
            )

        tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall()
        assert tables == []
        assert connection.execute("PRAGMA user_version").fetchone() == (0,)
    finally:
        connection.close()
