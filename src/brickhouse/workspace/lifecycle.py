"""Initial local workspace creation and read-only reopening."""

from __future__ import annotations

import shutil
import sqlite3
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path

from brickhouse.workspace.config import (
    SUPPORTED_FORMAT_VERSION,
    InvalidWorkspaceConfigError,
    UnsupportedWorkspaceFormatError,
    WorkspaceConfig,
)
from brickhouse.workspace.migrations import Migration, WorkspaceMigrationError, apply_migrations

_DIRECTORIES = (
    "tables",
    "files",
    "staging",
    "results",
    "runs",
    "checkpoints",
    "integrations",
)
SUPPORTED_METADATA_SCHEMA_VERSION = 12


class WorkspaceError(RuntimeError):
    """Base error for local workspace lifecycle failures."""


class WorkspaceExistsError(WorkspaceError):
    """The target already exists; initialization never overwrites a workspace."""


class WorkspaceNotFoundError(WorkspaceError):
    """The target is not an initialized BrickHouse workspace."""


class WorkspaceCorruptError(WorkspaceError):
    """Workspace metadata is missing required initial schema records."""


@dataclass(frozen=True, slots=True)
class Workspace:
    """A reopened workspace descriptor; it does not hold live database handles."""

    path: Path
    config: WorkspaceConfig
    workspace_id: str
    metadata_schema_version: int

    @property
    def metadata_path(self) -> Path:
        return self.path / "metadata.sqlite"


def initialize_workspace(
    path: str | Path,
    *,
    config: WorkspaceConfig | None = None,
) -> Workspace:
    """Create a new workspace with an explicit default catalog and schema.

    Initialization is built in a sibling staging directory before it is moved
    into place. Existing paths are never overwritten.
    """
    target = Path(path).expanduser().resolve()
    if target.exists():
        raise WorkspaceExistsError(f"workspace path already exists: {target}")
    if not target.name:
        raise WorkspaceError("workspace path must name a directory")
    effective_config = config or WorkspaceConfig()
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.initializing-", dir=target.parent))
    except OSError as error:
        raise WorkspaceError("could not prepare workspace directory") from error

    workspace_id = str(uuid.uuid4())
    claimed_target = False
    created_entries: list[Path] = []
    completed = False
    try:
        for name in _DIRECTORIES:
            (staging / name).mkdir()
        (staging / "brickhouse.toml").write_text(effective_config.to_toml(), encoding="utf-8")
        _initialize_metadata(staging / "metadata.sqlite", effective_config, workspace_id)
        try:
            # mkdir is the exclusive claim: unlike rename, it cannot replace an
            # empty directory created after the initial existence check.
            target.mkdir()
        except FileExistsError as error:
            raise WorkspaceExistsError(f"workspace path already exists: {target}") from error
        claimed_target = True

        # Move the database last so an observer never sees the workspace as
        # initialized until its config, namespace directories, and metadata exist.
        entry_names = (*_DIRECTORIES, "brickhouse.toml", "metadata.sqlite")
        for name in entry_names:
            entry = target / name
            (staging / name).rename(entry)
            created_entries.append(entry)
        completed = True
    except OSError as error:
        raise WorkspaceError("could not initialize workspace directory") from error
    finally:
        if claimed_target and not completed:
            for entry in reversed(created_entries):
                if entry.is_dir():
                    shutil.rmtree(entry)
                else:
                    entry.unlink(missing_ok=True)
            try:
                target.rmdir()
            except OSError:
                # Preserve any unexpected data rather than deleting a directory
                # another process may have populated during initialization.
                pass
        if staging.exists():
            shutil.rmtree(staging)
    return open_workspace(target)


def _initialize_metadata(
    path: Path,
    config: WorkspaceConfig,
    workspace_id: str,
) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        apply_migrations(connection, _workspace_migrations(config, workspace_id))
    except sqlite3.Error as error:
        if connection.in_transaction:
            connection.rollback()
        raise WorkspaceError("could not initialize workspace metadata") from error
    except BaseException:
        if connection.in_transaction:
            connection.rollback()
        raise
    finally:
        connection.close()


def _workspace_migrations(
    config: WorkspaceConfig,
    workspace_id: str,
) -> tuple[Migration, ...]:
    return (
        Migration(
            version=1,
            apply=lambda transaction: _create_metadata_v1(transaction, config, workspace_id),
        ),
        Migration(version=2, apply=_create_metadata_v2),
        Migration(version=3, apply=_create_metadata_v3),
        Migration(version=4, apply=_create_metadata_v4),
        Migration(version=5, apply=_create_metadata_v5),
        Migration(version=6, apply=_create_metadata_v6),
        Migration(version=7, apply=_create_metadata_v7),
        Migration(version=8, apply=_create_metadata_v8),
        Migration(version=9, apply=_create_metadata_v9),
        Migration(version=10, apply=_create_metadata_v10),
        Migration(version=11, apply=_create_metadata_v11),
        Migration(version=12, apply=_create_metadata_v12),
    )


def _create_metadata_v1(
    connection: sqlite3.Connection,
    config: WorkspaceConfig,
    workspace_id: str,
) -> None:
    connection.execute(
        "CREATE TABLE workspace_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    )
    connection.execute(
        "CREATE TABLE catalogs ("
        "name TEXT PRIMARY KEY, is_default INTEGER NOT NULL "
        "CHECK (is_default IN (0, 1)))"
    )
    connection.execute(
        "CREATE TABLE schemas ("
        "catalog_name TEXT NOT NULL, name TEXT NOT NULL, "
        "is_default INTEGER NOT NULL CHECK (is_default IN (0, 1)), "
        "PRIMARY KEY (catalog_name, name), "
        "FOREIGN KEY (catalog_name) REFERENCES catalogs(name))"
    )
    metadata = {
        "workspace_id": workspace_id,
        "format_version": str(config.format_version),
        "default_catalog": config.default_catalog,
        "default_schema": config.default_schema,
    }
    connection.executemany(
        "INSERT INTO workspace_metadata(key, value) VALUES (?, ?)",
        metadata.items(),
    )
    connection.execute(
        "INSERT INTO catalogs(name, is_default) VALUES (?, 1)",
        (config.default_catalog,),
    )
    connection.execute(
        "INSERT INTO schemas(catalog_name, name, is_default) VALUES (?, ?, 1)",
        (config.default_catalog, config.default_schema),
    )


def _create_metadata_v2(connection: sqlite3.Connection) -> None:
    connection.execute("ALTER TABLE catalogs ADD COLUMN catalog_id TEXT")
    connection.execute("ALTER TABLE schemas ADD COLUMN schema_id TEXT")
    for row in connection.execute("SELECT name FROM catalogs").fetchall():
        connection.execute(
            "UPDATE catalogs SET catalog_id = ? WHERE name = ?",
            (str(uuid.uuid4()), row[0]),
        )
    for row in connection.execute("SELECT catalog_name, name FROM schemas").fetchall():
        connection.execute(
            "UPDATE schemas SET schema_id = ? WHERE catalog_name = ? AND name = ?",
            (str(uuid.uuid4()), row[0], row[1]),
        )
    connection.execute("CREATE UNIQUE INDEX catalogs_catalog_id_unique ON catalogs(catalog_id)")
    connection.execute("CREATE UNIQUE INDEX schemas_schema_id_unique ON schemas(schema_id)")
    connection.execute(
        "CREATE TRIGGER catalogs_require_catalog_id_insert "
        "BEFORE INSERT ON catalogs WHEN NEW.catalog_id IS NULL OR NEW.catalog_id = '' "
        "BEGIN SELECT RAISE(ABORT, 'catalog_id is required'); END"
    )
    connection.execute(
        "CREATE TRIGGER schemas_require_schema_id_insert "
        "BEFORE INSERT ON schemas WHEN NEW.schema_id IS NULL OR NEW.schema_id = '' "
        "BEGIN SELECT RAISE(ABORT, 'schema_id is required'); END"
    )
    connection.execute(
        "CREATE TRIGGER catalogs_require_catalog_id_update "
        "BEFORE UPDATE OF catalog_id ON catalogs "
        "WHEN NEW.catalog_id IS NULL OR NEW.catalog_id = '' "
        "BEGIN SELECT RAISE(ABORT, 'catalog_id is required'); END"
    )
    connection.execute(
        "CREATE TRIGGER schemas_require_schema_id_update "
        "BEFORE UPDATE OF schema_id ON schemas "
        "WHEN NEW.schema_id IS NULL OR NEW.schema_id = '' "
        "BEGIN SELECT RAISE(ABORT, 'schema_id is required'); END"
    )
    connection.execute(
        "CREATE TRIGGER catalogs_catalog_id_immutable "
        "BEFORE UPDATE OF catalog_id ON catalogs WHEN NEW.catalog_id != OLD.catalog_id "
        "BEGIN SELECT RAISE(ABORT, 'catalog_id is immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER schemas_schema_id_immutable "
        "BEFORE UPDATE OF schema_id ON schemas WHEN NEW.schema_id != OLD.schema_id "
        "BEGIN SELECT RAISE(ABORT, 'schema_id is immutable'); END"
    )
    connection.execute(
        "CREATE TABLE table_registrations ("
        "table_id TEXT PRIMARY KEY, "
        "catalog_name TEXT NOT NULL, schema_name TEXT NOT NULL, name TEXT NOT NULL, "
        "table_format TEXT NOT NULL CHECK (table_format = 'delta'), "
        "ownership TEXT NOT NULL CHECK (ownership IN ('managed', 'external')), "
        "canonical_location TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL, "
        "UNIQUE (catalog_name, schema_name, name), "
        "FOREIGN KEY (catalog_name, schema_name) "
        "REFERENCES schemas(catalog_name, name))"
    )
    connection.execute(
        "CREATE INDEX table_registrations_namespace_idx "
        "ON table_registrations(catalog_name, schema_name, name)"
    )


def _create_metadata_v3(connection: sqlite3.Connection) -> None:
    connection.execute("ALTER TABLE catalogs ADD COLUMN comment TEXT")
    connection.execute("ALTER TABLE schemas ADD COLUMN comment TEXT")
    for row in connection.execute("SELECT name FROM catalogs").fetchall():
        connection.execute(
            "INSERT OR IGNORE INTO schemas(schema_id, catalog_name, name, is_default, comment) "
            "VALUES (?, ?, 'information_schema', 0, NULL)",
            (str(uuid.uuid4()), row[0]),
        )


def _create_metadata_v4(connection: sqlite3.Connection) -> None:
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
        "state TEXT NOT NULL CHECK(state IN ('prepared', 'committing', 'committed', 'uncertain')), "
        "commit_version INTEGER CHECK(commit_version IS NULL OR commit_version >= 0), "
        "error_code TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, "
        "PRIMARY KEY(table_id, operation_id), "
        "UNIQUE(table_id, transaction_version), "
        "CHECK((state = 'committed' AND commit_version IS NOT NULL) OR "
        "(state != 'committed' AND commit_version IS NULL)), "
        "FOREIGN KEY (table_id) REFERENCES table_registrations(table_id) ON DELETE RESTRICT"
        ")"
    )
    connection.execute(
        "CREATE INDEX table_operation_journal_pending_idx "
        "ON table_operation_journal(table_id, state, transaction_version)"
    )


def _create_metadata_v5(connection: sqlite3.Connection) -> None:
    connection.execute(
        "CREATE TABLE worker_attempt_outcomes ("
        "run_id TEXT NOT NULL, attempt_id TEXT NOT NULL, "
        "state TEXT NOT NULL CHECK(state IN "
        "('running', 'succeeded', 'failed', 'timed_out', 'cancelled')), "
        "exit_code INTEGER, termination_reason TEXT NOT NULL CHECK(termination_reason IN "
        "('running', 'completed', 'timeout', 'cancelled', 'launch_failed')), "
        "started_at TEXT NOT NULL, finished_at TEXT, "
        "interpreter TEXT NOT NULL, script TEXT NOT NULL, cwd TEXT NOT NULL, "
        "stdout_bytes INTEGER NOT NULL DEFAULT 0 CHECK(stdout_bytes >= 0), "
        "stderr_bytes INTEGER NOT NULL DEFAULT 0 CHECK(stderr_bytes >= 0), "
        "stdout_truncated INTEGER NOT NULL DEFAULT 0 CHECK(stdout_truncated IN (0, 1)), "
        "stderr_truncated INTEGER NOT NULL DEFAULT 0 CHECK(stderr_truncated IN (0, 1)), "
        "cleanup_error_type TEXT, "
        "PRIMARY KEY(run_id, attempt_id), "
        "CHECK((state = 'running' AND finished_at IS NULL AND exit_code IS NULL) OR "
        "(state != 'running' AND finished_at IS NOT NULL))"
        ")"
    )
    connection.execute(
        "CREATE INDEX worker_attempt_outcomes_state_idx "
        "ON worker_attempt_outcomes(state, started_at)"
    )


def _create_metadata_v6(connection: sqlite3.Connection) -> None:
    connection.execute(
        "CREATE TABLE jobs ("
        "job_id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "name TEXT NOT NULL CHECK(length(name) BETWEEN 1 AND 256), "
        "settings_json TEXT NOT NULL, "
        "task_count INTEGER NOT NULL CHECK(task_count BETWEEN 1 AND 10), "
        "definition_sha256 TEXT NOT NULL CHECK(length(definition_sha256) = 64), "
        "created_at TEXT NOT NULL, "
        "UNIQUE(job_id, name)"
        ")"
    )
    connection.execute(
        "CREATE TABLE job_tasks ("
        "job_id INTEGER NOT NULL, task_key TEXT NOT NULL, "
        "task_type TEXT NOT NULL CHECK(task_type = 'python_script'), "
        "task_order INTEGER NOT NULL CHECK(task_order >= 0), "
        "depends_on_json TEXT NOT NULL, settings_json TEXT NOT NULL, "
        "PRIMARY KEY(job_id, task_key), UNIQUE(job_id, task_order), "
        "FOREIGN KEY(job_id) REFERENCES jobs(job_id) ON DELETE RESTRICT"
        ")"
    )
    connection.execute(
        "CREATE TABLE job_runs ("
        "run_id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER NOT NULL, "
        "state TEXT NOT NULL CHECK(state IN "
        "('queued', 'running', 'succeeded', 'failed', 'cancelled', 'interrupted')), "
        "submission_token TEXT UNIQUE CHECK(submission_token IS NULL OR "
        "length(submission_token) BETWEEN 1 AND 64), "
        "request_fingerprint TEXT NOT NULL CHECK(length(request_fingerprint) = 64), "
        "submitted_settings_json TEXT NOT NULL, created_at TEXT NOT NULL, "
        "started_at TEXT, finished_at TEXT, error_code TEXT, "
        "UNIQUE(run_id, job_id), "
        "CHECK((state = 'queued' AND started_at IS NULL AND finished_at IS NULL) OR "
        "(state = 'running' AND started_at IS NOT NULL AND finished_at IS NULL) OR "
        "(state IN ('succeeded', 'failed', 'cancelled', 'interrupted') "
        "AND finished_at IS NOT NULL)), "
        "FOREIGN KEY(job_id) REFERENCES jobs(job_id) ON DELETE RESTRICT"
        ")"
    )
    connection.execute(
        "CREATE INDEX job_runs_job_created_idx ON job_runs(job_id, created_at, run_id)"
    )
    connection.execute(
        "CREATE TABLE job_run_tasks ("
        "run_id INTEGER NOT NULL, job_id INTEGER NOT NULL, task_key TEXT NOT NULL, "
        "task_type TEXT NOT NULL CHECK(task_type = 'python_script'), "
        "task_order INTEGER NOT NULL CHECK(task_order >= 0), "
        "depends_on_json TEXT NOT NULL, settings_json TEXT NOT NULL, "
        "state TEXT NOT NULL CHECK(state IN "
        "('pending', 'queued', 'running', 'succeeded', 'failed', 'cancelled', "
        "'skipped', 'interrupted')), "
        "attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0), "
        "started_at TEXT, finished_at TEXT, error_code TEXT, "
        "CHECK((state = 'pending' AND attempt_count = 0 AND started_at IS NULL "
        "AND finished_at IS NULL) OR (state = 'queued' AND attempt_count > 0 "
        "AND finished_at IS NULL) OR (state = 'running' AND attempt_count > 0 "
        "AND started_at IS NOT NULL AND finished_at IS NULL) OR "
        "(state = 'skipped' AND attempt_count = 0 AND finished_at IS NOT NULL) OR "
        "(state IN ('succeeded', 'failed', 'cancelled', 'interrupted') "
        "AND attempt_count > 0 AND finished_at IS NOT NULL)), "
        "PRIMARY KEY(run_id, task_key), UNIQUE(run_id, job_id, task_key), "
        "FOREIGN KEY(run_id, job_id) REFERENCES job_runs(run_id, job_id) "
        "ON DELETE RESTRICT, "
        "FOREIGN KEY(job_id, task_key) REFERENCES job_tasks(job_id, task_key) "
        "ON DELETE RESTRICT"
        ")"
    )
    connection.execute(
        "CREATE INDEX job_run_tasks_state_idx ON job_run_tasks(run_id, state, task_order)"
    )
    connection.execute(
        "CREATE TABLE job_task_attempts ("
        "run_id INTEGER NOT NULL, task_key TEXT NOT NULL, "
        "attempt_number INTEGER NOT NULL CHECK(attempt_number > 0), "
        "attempt_id TEXT NOT NULL UNIQUE, "
        "state TEXT NOT NULL CHECK(state IN "
        "('queued', 'running', 'succeeded', 'failed', 'cancelled', 'timed_out', 'interrupted')), "
        "termination_reason TEXT NOT NULL CHECK(termination_reason IN "
        "('queued', 'running', 'completed', 'timeout', 'cancelled', 'interrupted', "
        "'launch_failed')), "
        "started_at TEXT, finished_at TEXT, exit_code INTEGER, error_code TEXT, "
        "PRIMARY KEY(run_id, task_key, attempt_number), "
        "CHECK((state = 'queued' AND termination_reason = 'queued' "
        "AND started_at IS NULL AND finished_at IS NULL AND exit_code IS NULL) OR "
        "(state = 'running' AND termination_reason = 'running' "
        "AND started_at IS NOT NULL AND finished_at IS NULL AND exit_code IS NULL) OR "
        "(state = 'succeeded' AND termination_reason = 'completed' AND exit_code = 0 "
        "AND finished_at IS NOT NULL AND error_code IS NULL) OR "
        "(state = 'failed' AND finished_at IS NOT NULL AND "
        "((termination_reason = 'completed' AND exit_code IS NOT NULL AND exit_code != 0) OR "
        "(termination_reason = 'launch_failed' AND exit_code IS NULL))) OR "
        "(state = 'cancelled' AND termination_reason = 'cancelled' "
        "AND exit_code IS NOT NULL AND finished_at IS NOT NULL) OR "
        "(state = 'timed_out' AND termination_reason = 'timeout' "
        "AND exit_code IS NOT NULL AND finished_at IS NOT NULL) OR "
        "(state = 'interrupted' AND termination_reason = 'interrupted' "
        "AND exit_code IS NULL AND finished_at IS NOT NULL)), "
        "FOREIGN KEY(run_id, task_key) REFERENCES job_run_tasks(run_id, task_key) "
        "ON DELETE RESTRICT"
        ")"
    )
    connection.execute(
        "CREATE INDEX job_task_attempts_state_idx "
        "ON job_task_attempts(run_id, state, task_key, attempt_number)"
    )
    connection.execute(
        "CREATE TRIGGER job_definitions_are_immutable "
        "BEFORE UPDATE ON jobs BEGIN SELECT RAISE(ABORT, 'job definitions are immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER job_definitions_cannot_be_deleted "
        "BEFORE DELETE ON jobs BEGIN SELECT RAISE(ABORT, 'job definitions are immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER job_task_definitions_are_immutable "
        "BEFORE UPDATE ON job_tasks "
        "BEGIN SELECT RAISE(ABORT, 'job task definitions are immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER job_task_definitions_cannot_be_deleted "
        "BEFORE DELETE ON job_tasks "
        "BEGIN SELECT RAISE(ABORT, 'job task definitions are immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER job_task_count_cannot_exceed_definition "
        "BEFORE INSERT ON job_tasks WHEN "
        "(SELECT count(*) FROM job_tasks WHERE job_id = NEW.job_id) >= "
        "(SELECT task_count FROM jobs WHERE job_id = NEW.job_id) "
        "BEGIN SELECT RAISE(ABORT, 'job definition task count is immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER job_run_submission_is_immutable "
        "BEFORE UPDATE OF job_id, submission_token, request_fingerprint, "
        "submitted_settings_json, created_at ON job_runs "
        "BEGIN SELECT RAISE(ABORT, 'submitted job settings are immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER job_run_state_transitions_are_valid "
        "BEFORE UPDATE OF state ON job_runs WHEN NEW.state != OLD.state AND NOT ("
        "(OLD.state = 'queued' AND NEW.state IN ('running', 'interrupted')) OR "
        "(OLD.state = 'running' AND NEW.state IN "
        "('succeeded', 'failed', 'cancelled', 'interrupted'))) "
        "BEGIN SELECT RAISE(ABORT, 'invalid job run state transition'); END"
    )
    connection.execute(
        "CREATE TRIGGER job_run_terminal_outcomes_are_valid "
        "BEFORE UPDATE OF state ON job_runs WHEN "
        "(NEW.state = 'succeeded' AND EXISTS (SELECT 1 FROM job_run_tasks "
        "WHERE run_id = NEW.run_id AND state != 'succeeded')) OR "
        "(NEW.state = 'failed' AND (NOT EXISTS (SELECT 1 FROM job_run_tasks "
        "WHERE run_id = NEW.run_id AND state = 'failed') OR EXISTS "
        "(SELECT 1 FROM job_run_tasks WHERE run_id = NEW.run_id "
        "AND state IN ('pending', 'queued', 'running')))) OR "
        "(NEW.state = 'cancelled' AND (NOT EXISTS (SELECT 1 FROM job_run_tasks "
        "WHERE run_id = NEW.run_id AND state = 'cancelled') OR EXISTS "
        "(SELECT 1 FROM job_run_tasks WHERE run_id = NEW.run_id "
        "AND state IN ('pending', 'queued', 'running')))) "
        "BEGIN SELECT RAISE(ABORT, 'job run terminal outcome disagrees with task state'); END"
    )
    connection.execute(
        "CREATE TRIGGER terminal_job_runs_are_immutable "
        "BEFORE UPDATE ON job_runs "
        "WHEN OLD.state IN ('succeeded', 'failed', 'cancelled', 'interrupted') "
        "BEGIN SELECT RAISE(ABORT, 'terminal job runs are immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER job_runs_cannot_be_deleted "
        "BEFORE DELETE ON job_runs "
        "BEGIN SELECT RAISE(ABORT, 'job run history is immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER job_run_task_definition_is_immutable "
        "BEFORE UPDATE OF job_id, task_key, task_type, task_order, depends_on_json, settings_json "
        "ON job_run_tasks "
        "BEGIN SELECT RAISE(ABORT, 'submitted task settings are immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER terminal_job_tasks_are_immutable "
        "BEFORE UPDATE ON job_run_tasks "
        "WHEN OLD.state IN ('succeeded', 'cancelled', 'skipped', 'interrupted') "
        "OR (OLD.state = 'failed' AND NEW.state != 'queued') "
        "BEGIN SELECT RAISE(ABORT, 'terminal task state cannot be reopened'); END"
    )
    connection.execute(
        "CREATE TRIGGER job_task_state_transitions_are_valid "
        "BEFORE UPDATE OF state ON job_run_tasks "
        "WHEN NEW.state != OLD.state AND NOT ("
        "(OLD.state = 'pending' AND NEW.state IN ('queued', 'skipped')) OR "
        "(OLD.state = 'queued' AND NEW.state IN ('running', 'interrupted')) OR "
        "(OLD.state = 'running' AND NEW.state IN "
        "('succeeded', 'failed', 'cancelled', 'interrupted')) OR "
        "(OLD.state = 'failed' AND NEW.state = 'queued')) "
        "BEGIN SELECT RAISE(ABORT, 'invalid job task state transition'); END"
    )
    connection.execute(
        "CREATE TRIGGER job_task_transitions_require_active_run "
        "BEFORE UPDATE OF state ON job_run_tasks WHEN NEW.state != OLD.state AND "
        "(SELECT state FROM job_runs WHERE run_id = OLD.run_id) NOT IN ('queued', 'running') "
        "BEGIN SELECT RAISE(ABORT, 'cannot change tasks in a terminal job run'); END"
    )
    connection.execute(
        "CREATE TRIGGER job_run_tasks_cannot_be_deleted "
        "BEFORE DELETE ON job_run_tasks "
        "BEGIN SELECT RAISE(ABORT, 'submitted task history is immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER job_attempt_identity_is_immutable "
        "BEFORE UPDATE OF run_id, task_key, attempt_number, attempt_id "
        "ON job_task_attempts "
        "BEGIN SELECT RAISE(ABORT, 'job task attempt identity is immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER terminal_job_attempts_are_immutable "
        "BEFORE UPDATE ON job_task_attempts "
        "WHEN OLD.state IN ('succeeded', 'failed', 'cancelled', 'timed_out', 'interrupted') "
        "BEGIN SELECT RAISE(ABORT, 'terminal job task attempts are immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER job_attempt_state_transitions_are_valid "
        "BEFORE UPDATE OF state ON job_task_attempts "
        "WHEN NEW.state != OLD.state AND NOT ("
        "(OLD.state = 'queued' AND NEW.state IN ('running', 'interrupted')) OR "
        "(OLD.state = 'running' AND NEW.state IN "
        "('succeeded', 'failed', 'cancelled', 'timed_out', 'interrupted'))) "
        "BEGIN SELECT RAISE(ABORT, 'invalid job task attempt state transition'); END"
    )
    connection.execute(
        "CREATE TRIGGER job_attempt_transitions_require_active_run "
        "BEFORE UPDATE OF state ON job_task_attempts WHEN NEW.state != OLD.state AND "
        "(SELECT state FROM job_runs WHERE run_id = OLD.run_id) NOT IN ('queued', 'running') "
        "BEGIN SELECT RAISE(ABORT, 'cannot change attempts in a terminal job run'); END"
    )
    connection.execute(
        "CREATE TRIGGER job_attempts_cannot_be_deleted "
        "BEFORE DELETE ON job_task_attempts "
        "BEGIN SELECT RAISE(ABORT, 'job task attempt history is immutable'); END"
    )


def _create_metadata_v7(connection: sqlite3.Connection) -> None:
    connection.execute(
        "CREATE TABLE compute_resources ("
        "compute_id TEXT PRIMARY KEY, "
        "resource_kind TEXT NOT NULL CHECK(resource_kind IN "
        "('all_purpose', 'job_new', 'job_shared')), "
        "profile TEXT NOT NULL CHECK(profile IN "
        "('classic-standard-local-v1', 'classic-dedicated-local-v1')), "
        "state TEXT NOT NULL CHECK(state IN "
        "('PENDING', 'RUNNING', 'RESTARTING', 'TERMINATING', 'TERMINATED', 'ERROR')), "
        "run_id INTEGER, compute_key TEXT, cluster_name TEXT NOT NULL, "
        "spark_version TEXT NOT NULL, access_mode TEXT NOT NULL, "
        "single_user_name TEXT, settings_json TEXT NOT NULL, "
        "autotermination_minutes INTEGER CHECK(autotermination_minutes IS NULL OR "
        "autotermination_minutes = 0 OR autotermination_minutes BETWEEN 10 AND 10000), "
        "created_at TEXT NOT NULL, updated_at TEXT NOT NULL, started_at TEXT, "
        "terminated_at TEXT, last_activity_at TEXT NOT NULL, generation INTEGER NOT NULL "
        "CHECK(generation > 0), deleted_at TEXT, "
        "CHECK((resource_kind = 'all_purpose' AND run_id IS NULL AND compute_key IS NULL) OR "
        "(resource_kind IN ('job_new', 'job_shared') AND run_id IS NOT NULL "
        "AND compute_key IS NOT NULL AND length(compute_key) BETWEEN 1 AND 128)), "
        "CHECK((state IN ('RUNNING', 'RESTARTING', 'PENDING', 'TERMINATING') AND "
        "terminated_at IS NULL) OR (state IN ('TERMINATED', 'ERROR') AND "
        "terminated_at IS NOT NULL)), "
        "CHECK(deleted_at IS NULL OR state = 'TERMINATED'), "
        "UNIQUE(run_id, compute_key), "
        "FOREIGN KEY(run_id) REFERENCES job_runs(run_id) ON DELETE RESTRICT"
        ")"
    )
    connection.execute(
        "CREATE INDEX compute_resources_state_idx "
        "ON compute_resources(resource_kind, state, deleted_at, updated_at)"
    )
    connection.execute(
        "CREATE TABLE compute_consumers ("
        "compute_id TEXT NOT NULL, run_id INTEGER NOT NULL, task_key TEXT NOT NULL, "
        "state TEXT NOT NULL CHECK(state IN ('registered', 'active', 'released')), "
        "registered_at TEXT NOT NULL, started_at TEXT, released_at TEXT, "
        "PRIMARY KEY(compute_id, run_id, task_key), "
        "CHECK((state = 'registered' AND started_at IS NULL AND released_at IS NULL) OR "
        "(state = 'active' AND started_at IS NOT NULL AND released_at IS NULL) OR "
        "(state = 'released' AND released_at IS NOT NULL)), "
        "FOREIGN KEY(compute_id) REFERENCES compute_resources(compute_id) ON DELETE RESTRICT, "
        "FOREIGN KEY(run_id, task_key) REFERENCES job_run_tasks(run_id, task_key) "
        "ON DELETE RESTRICT"
        ")"
    )
    connection.execute(
        "CREATE INDEX compute_consumers_run_state_idx ON compute_consumers(run_id, state, task_key)"
    )
    connection.execute(
        "CREATE TABLE compute_lifecycle_events ("
        "event_id INTEGER PRIMARY KEY AUTOINCREMENT, compute_id TEXT NOT NULL, "
        "event_type TEXT NOT NULL CHECK(event_type IN "
        "('created', 'started', 'restarted', 'edited', 'terminating', 'terminated', "
        "'autoterminating', 'autoterminated', "
        "'deleted', 'consumer_registered', 'consumer_started', 'consumer_released')), "
        "state TEXT NOT NULL CHECK(state IN "
        "('PENDING', 'RUNNING', 'RESTARTING', 'TERMINATING', 'TERMINATED', 'ERROR')), "
        "generation INTEGER NOT NULL CHECK(generation > 0), "
        "invalidates_sessions INTEGER NOT NULL CHECK(invalidates_sessions IN (0, 1)), "
        "occurred_at TEXT NOT NULL, details_json TEXT NOT NULL, "
        "FOREIGN KEY(compute_id) REFERENCES compute_resources(compute_id) ON DELETE RESTRICT"
        ")"
    )
    connection.execute(
        "CREATE INDEX compute_lifecycle_events_resource_idx "
        "ON compute_lifecycle_events(compute_id, event_id)"
    )
    connection.execute(
        "CREATE TRIGGER compute_resource_identity_is_immutable "
        "BEFORE UPDATE OF compute_id, resource_kind, run_id, compute_key, created_at "
        "ON compute_resources "
        "BEGIN SELECT RAISE(ABORT, 'compute resource identity is immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER compute_resources_cannot_be_deleted "
        "BEFORE DELETE ON compute_resources "
        "BEGIN SELECT RAISE(ABORT, 'compute lifecycle history is immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER compute_resource_state_transitions_are_valid "
        "BEFORE UPDATE OF state ON compute_resources WHEN NEW.state != OLD.state AND NOT ("
        "(OLD.state = 'PENDING' AND NEW.state IN ('RUNNING', 'TERMINATING', 'ERROR')) OR "
        "(OLD.state = 'RUNNING' AND NEW.state IN ('RESTARTING', 'TERMINATING', 'ERROR')) OR "
        "(OLD.state = 'RESTARTING' AND NEW.state IN ('RUNNING', 'TERMINATING', 'ERROR')) OR "
        "(OLD.state = 'TERMINATING' AND NEW.state IN ('TERMINATED', 'ERROR')) OR "
        "(OLD.state = 'TERMINATED' AND NEW.state = 'PENDING')) "
        "BEGIN SELECT RAISE(ABORT, 'invalid compute resource state transition'); END"
    )
    connection.execute(
        "CREATE TRIGGER compute_resource_generations_are_monotonic "
        "BEFORE UPDATE OF generation ON compute_resources "
        "WHEN NEW.generation < OLD.generation OR NEW.generation > OLD.generation + 1 "
        "BEGIN SELECT RAISE(ABORT, 'compute generation must increase by at most one'); END"
    )
    connection.execute(
        "CREATE TRIGGER deleted_compute_resources_are_immutable "
        "BEFORE UPDATE ON compute_resources WHEN OLD.deleted_at IS NOT NULL "
        "BEGIN SELECT RAISE(ABORT, 'deleted compute resource is immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER released_compute_consumers_are_immutable "
        "BEFORE UPDATE ON compute_consumers WHEN OLD.state = 'released' "
        "BEGIN SELECT RAISE(ABORT, 'released compute consumers are immutable'); END"
    )


def _create_metadata_v8(connection: sqlite3.Connection) -> None:
    connection.execute(
        "CREATE TABLE job_task_attempt_outputs ("
        "run_id INTEGER NOT NULL, task_key TEXT NOT NULL, "
        "attempt_number INTEGER NOT NULL CHECK(attempt_number > 0), "
        "stdout TEXT NOT NULL, stderr TEXT NOT NULL, "
        "stdout_truncated INTEGER NOT NULL CHECK(stdout_truncated IN (0, 1)), "
        "stderr_truncated INTEGER NOT NULL CHECK(stderr_truncated IN (0, 1)), "
        "PRIMARY KEY(run_id, task_key, attempt_number), "
        "CHECK(length(CAST(stdout AS BLOB)) <= 1048576), "
        "CHECK(length(CAST(stderr AS BLOB)) <= 1048576), "
        "FOREIGN KEY(run_id, task_key, attempt_number) REFERENCES "
        "job_task_attempts(run_id, task_key, attempt_number) ON DELETE RESTRICT"
        ")"
    )
    connection.execute(
        "CREATE TRIGGER job_attempt_outputs_are_immutable "
        "BEFORE UPDATE ON job_task_attempt_outputs "
        "BEGIN SELECT RAISE(ABORT, 'job task attempt output is immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER job_attempt_outputs_cannot_be_deleted "
        "BEFORE DELETE ON job_task_attempt_outputs "
        "BEGIN SELECT RAISE(ABORT, 'job task attempt output is immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER compute_consumer_identity_is_immutable "
        "BEFORE UPDATE OF compute_id, run_id, task_key, registered_at ON compute_consumers "
        "BEGIN SELECT RAISE(ABORT, 'compute consumer identity is immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER compute_consumer_state_transitions_are_valid "
        "BEFORE UPDATE OF state ON compute_consumers WHEN NEW.state != OLD.state AND NOT ("
        "(OLD.state = 'registered' AND NEW.state IN ('active', 'released')) OR "
        "(OLD.state = 'active' AND NEW.state = 'released')) "
        "BEGIN SELECT RAISE(ABORT, 'invalid compute consumer state transition'); END"
    )
    connection.execute(
        "CREATE TRIGGER compute_consumers_cannot_be_deleted "
        "BEFORE DELETE ON compute_consumers "
        "BEGIN SELECT RAISE(ABORT, 'compute consumer history is immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER compute_lifecycle_events_are_immutable "
        "BEFORE UPDATE ON compute_lifecycle_events "
        "BEGIN SELECT RAISE(ABORT, 'compute lifecycle events are immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER compute_lifecycle_events_cannot_be_deleted "
        "BEFORE DELETE ON compute_lifecycle_events "
        "BEGIN SELECT RAISE(ABORT, 'compute lifecycle events are immutable'); END"
    )


def _create_metadata_v9(connection: sqlite3.Connection) -> None:
    """Retain worker process identity and allow unknown restart outcomes."""
    connection.execute("DROP INDEX worker_attempt_outcomes_state_idx")
    connection.execute(
        "CREATE TABLE worker_attempt_outcomes_v9 ("
        "run_id TEXT NOT NULL, attempt_id TEXT NOT NULL, "
        "state TEXT NOT NULL CHECK(state IN "
        "('running', 'succeeded', 'failed', 'timed_out', 'cancelled', 'interrupted')), "
        "exit_code INTEGER, termination_reason TEXT NOT NULL CHECK(termination_reason IN "
        "('running', 'completed', 'timeout', 'cancelled', 'launch_failed', 'interrupted')), "
        "started_at TEXT NOT NULL, finished_at TEXT, "
        "interpreter TEXT NOT NULL, script TEXT NOT NULL, cwd TEXT NOT NULL, "
        "stdout_bytes INTEGER NOT NULL DEFAULT 0 CHECK(stdout_bytes >= 0), "
        "stderr_bytes INTEGER NOT NULL DEFAULT 0 CHECK(stderr_bytes >= 0), "
        "stdout_truncated INTEGER NOT NULL DEFAULT 0 CHECK(stdout_truncated IN (0, 1)), "
        "stderr_truncated INTEGER NOT NULL DEFAULT 0 CHECK(stderr_truncated IN (0, 1)), "
        "cleanup_error_type TEXT, worker_pid INTEGER CHECK(worker_pid IS NULL OR worker_pid > 0), "
        "PRIMARY KEY(run_id, attempt_id), "
        "CHECK((state = 'running' AND finished_at IS NULL AND exit_code IS NULL) OR "
        "(state != 'running' AND finished_at IS NOT NULL)), "
        "CHECK((state = 'interrupted' AND termination_reason = 'interrupted' "
        "AND exit_code IS NULL) OR state != 'interrupted')"
        ")"
    )
    connection.execute(
        "INSERT INTO worker_attempt_outcomes_v9 ("
        "run_id, attempt_id, state, exit_code, termination_reason, started_at, finished_at, "
        "interpreter, script, cwd, stdout_bytes, stderr_bytes, stdout_truncated, "
        "stderr_truncated, cleanup_error_type, worker_pid) "
        "SELECT run_id, attempt_id, state, exit_code, termination_reason, started_at, "
        "finished_at, interpreter, script, cwd, stdout_bytes, stderr_bytes, stdout_truncated, "
        "stderr_truncated, cleanup_error_type, NULL FROM worker_attempt_outcomes"
    )
    connection.execute("DROP TABLE worker_attempt_outcomes")
    connection.execute("ALTER TABLE worker_attempt_outcomes_v9 RENAME TO worker_attempt_outcomes")
    connection.execute(
        "CREATE INDEX worker_attempt_outcomes_state_idx "
        "ON worker_attempt_outcomes(state, started_at)"
    )


def _create_metadata_v10(connection: sqlite3.Connection) -> None:
    """Persist JSON task values under the task attempt that produced them."""
    connection.execute(
        "CREATE TABLE job_task_values ("
        "run_id INTEGER NOT NULL, task_key TEXT NOT NULL, "
        "attempt_number INTEGER NOT NULL CHECK(attempt_number > 0), "
        "value_key TEXT NOT NULL CHECK(length(value_key) BETWEEN 1 AND 256), "
        "value_json TEXT NOT NULL, updated_at TEXT NOT NULL, "
        "PRIMARY KEY(run_id, task_key, attempt_number, value_key), "
        "FOREIGN KEY(run_id, task_key, attempt_number) "
        "REFERENCES job_task_attempts(run_id, task_key, attempt_number) ON DELETE RESTRICT"
        ")"
    )
    connection.execute(
        "CREATE INDEX job_task_values_lookup_idx "
        "ON job_task_values(run_id, task_key, value_key, attempt_number DESC)"
    )
    connection.execute(
        "CREATE TRIGGER job_task_values_require_running_attempt_insert "
        "BEFORE INSERT ON job_task_values WHEN NOT EXISTS ("
        "SELECT 1 FROM job_task_attempts AS attempt "
        "JOIN job_run_tasks AS task USING(run_id, task_key) "
        "JOIN job_runs AS run USING(run_id) "
        "WHERE attempt.run_id = NEW.run_id AND attempt.task_key = NEW.task_key "
        "AND attempt.attempt_number = NEW.attempt_number AND attempt.state = 'running' "
        "AND task.state = 'running' AND run.state = 'running') "
        "BEGIN SELECT RAISE(ABORT, 'task values require the active task attempt'); END"
    )
    connection.execute(
        "CREATE TRIGGER job_task_values_require_running_attempt_update "
        "BEFORE UPDATE ON job_task_values WHEN NOT EXISTS ("
        "SELECT 1 FROM job_task_attempts AS attempt "
        "JOIN job_run_tasks AS task USING(run_id, task_key) "
        "JOIN job_runs AS run USING(run_id) "
        "WHERE attempt.run_id = NEW.run_id AND attempt.task_key = NEW.task_key "
        "AND attempt.attempt_number = NEW.attempt_number AND attempt.state = 'running' "
        "AND task.state = 'running' AND run.state = 'running') "
        "BEGIN SELECT RAISE(ABORT, 'task values require the active task attempt'); END"
    )
    connection.execute(
        "CREATE TRIGGER job_task_value_identity_is_immutable "
        "BEFORE UPDATE OF run_id, task_key, attempt_number, value_key ON job_task_values "
        "BEGIN SELECT RAISE(ABORT, 'task value identity is immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER job_task_values_cannot_be_deleted "
        "BEFORE DELETE ON job_task_values "
        "BEGIN SELECT RAISE(ABORT, 'task value history is immutable'); END"
    )


def _create_metadata_v11(connection: sqlite3.Connection) -> None:
    """Keep Terraform job identities stable while preserving immutable job revisions."""
    connection.execute(
        "CREATE TABLE terraform_job_resources ("
        "job_id INTEGER PRIMARY KEY, "
        "current_revision INTEGER NOT NULL CHECK(current_revision > 0), "
        "created_at TEXT NOT NULL, deleted_at TEXT, "
        "FOREIGN KEY(job_id) REFERENCES jobs(job_id) ON DELETE RESTRICT, "
        "CHECK(deleted_at IS NULL OR length(deleted_at) > 0)"
        ")"
    )
    connection.execute(
        "CREATE TABLE terraform_job_revisions ("
        "job_id INTEGER NOT NULL, revision INTEGER NOT NULL CHECK(revision > 0), "
        "definition_job_id INTEGER NOT NULL, settings_json TEXT NOT NULL, "
        "created_at TEXT NOT NULL, PRIMARY KEY(job_id, revision), "
        "UNIQUE(definition_job_id), "
        "FOREIGN KEY(job_id) REFERENCES terraform_job_resources(job_id) ON DELETE RESTRICT, "
        "FOREIGN KEY(definition_job_id) REFERENCES jobs(job_id) ON DELETE RESTRICT"
        ")"
    )
    connection.execute(
        "CREATE TRIGGER terraform_job_resource_identity_is_immutable "
        "BEFORE UPDATE OF job_id, created_at ON terraform_job_resources "
        "BEGIN SELECT RAISE(ABORT, 'Terraform job resource identity is immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER terraform_job_revision_is_monotonic "
        "BEFORE UPDATE OF current_revision ON terraform_job_resources "
        "WHEN NEW.current_revision != OLD.current_revision + 1 "
        "OR NOT EXISTS (SELECT 1 FROM terraform_job_revisions "
        "WHERE job_id = NEW.job_id AND revision = NEW.current_revision) "
        "BEGIN SELECT RAISE(ABORT, 'Terraform job revisions must advance one step'); END"
    )
    connection.execute(
        "CREATE TRIGGER terraform_job_resource_delete_is_terminal "
        "BEFORE UPDATE OF deleted_at ON terraform_job_resources "
        "WHEN OLD.deleted_at IS NOT NULL OR NEW.deleted_at IS NULL "
        "BEGIN SELECT RAISE(ABORT, 'deleted Terraform jobs cannot be restored'); END"
    )
    connection.execute(
        "CREATE TRIGGER terraform_job_revisions_are_immutable "
        "BEFORE UPDATE ON terraform_job_revisions "
        "BEGIN SELECT RAISE(ABORT, 'Terraform job revision history is immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER terraform_job_revisions_cannot_be_deleted "
        "BEFORE DELETE ON terraform_job_revisions "
        "BEGIN SELECT RAISE(ABORT, 'Terraform job revision history is immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER terraform_job_resources_cannot_be_deleted "
        "BEFORE DELETE ON terraform_job_resources "
        "BEGIN SELECT RAISE(ABORT, 'Terraform job resource history is immutable'); END"
    )


def _create_metadata_v12(connection: sqlite3.Connection) -> None:
    """Tombstone deleted SDK jobs while retaining immutable definitions and run history."""
    connection.execute(
        "CREATE TABLE job_deletions ("
        "job_id INTEGER PRIMARY KEY, deleted_at TEXT NOT NULL CHECK(length(deleted_at) > 0), "
        "FOREIGN KEY(job_id) REFERENCES jobs(job_id) ON DELETE RESTRICT"
        ")"
    )
    connection.execute(
        "CREATE TRIGGER job_deletions_are_immutable "
        "BEFORE UPDATE ON job_deletions "
        "BEGIN SELECT RAISE(ABORT, 'job deletion history is immutable'); END"
    )
    connection.execute(
        "CREATE TRIGGER job_deletions_cannot_be_deleted "
        "BEFORE DELETE ON job_deletions "
        "BEGIN SELECT RAISE(ABORT, 'job deletion history cannot be deleted'); END"
    )


def _migrate_workspace_metadata(workspace: Workspace) -> Workspace:
    """Upgrade a validated workspace while its owner lock is held."""
    connection = sqlite3.connect(workspace.metadata_path, timeout=5.0)
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        apply_migrations(
            connection,
            _workspace_migrations(workspace.config, workspace.workspace_id),
        )
    except sqlite3.Error as error:
        if connection.in_transaction:
            connection.rollback()
        raise WorkspaceError("could not migrate workspace metadata") from error
    except WorkspaceMigrationError:
        raise
    except BaseException:
        if connection.in_transaction:
            connection.rollback()
        raise
    finally:
        connection.close()
    return open_workspace(workspace.path)


def open_workspace(path: str | Path) -> Workspace:
    """Validate persisted configuration, format, and bootstrap namespaces."""
    target = Path(path).expanduser().resolve()
    config_path = target / "brickhouse.toml"
    metadata_path = target / "metadata.sqlite"
    if not target.is_dir() or not config_path.is_file() or not metadata_path.is_file():
        raise WorkspaceNotFoundError("path is not an initialized BrickHouse workspace")
    missing_directories = [name for name in _DIRECTORIES if not (target / name).is_dir()]
    if missing_directories:
        raise WorkspaceCorruptError(
            f"workspace directories are missing: {', '.join(missing_directories)}"
        )
    try:
        config = WorkspaceConfig.from_file(config_path)
    except InvalidWorkspaceConfigError:
        raise

    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(f"{metadata_path.as_uri()}?mode=ro", uri=True)
        connection.execute("PRAGMA foreign_keys = ON")
        user_version = connection.execute("PRAGMA user_version").fetchone()
        if user_version is None:
            raise WorkspaceCorruptError("workspace schema version is missing")
        if user_version[0] < 1 or user_version[0] > SUPPORTED_METADATA_SCHEMA_VERSION:
            raise UnsupportedWorkspaceFormatError(
                f"workspace schema version {user_version[0]} is not supported; "
                f"supported versions are 1 through {SUPPORTED_METADATA_SCHEMA_VERSION}"
            )
        migration_versions = tuple(
            row[0]
            for row in connection.execute("SELECT version FROM schema_migrations ORDER BY version")
        )
        if migration_versions != tuple(range(1, user_version[0] + 1)):
            raise WorkspaceCorruptError(
                "workspace migration history disagrees with SQLite user_version"
            )
        if user_version[0] >= 2:
            for table, column in (("catalogs", "catalog_id"), ("schemas", "schema_id")):
                counts = connection.execute(
                    f"SELECT count(*), count(DISTINCT {column}), "
                    f"sum(CASE WHEN {column} IS NULL OR {column} = '' THEN 1 ELSE 0 END) "
                    f"FROM {table}"
                ).fetchone()
                if counts is None or counts[0] != counts[1] or counts[2] not in (0, None):
                    raise WorkspaceCorruptError(
                        f"workspace {table} identities are missing or duplicated"
                    )
        if user_version[0] >= 3:
            for table in ("catalogs", "schemas"):
                columns = {
                    column[1]
                    for column in connection.execute(f"PRAGMA table_info({table})").fetchall()
                }
                if "comment" not in columns:
                    raise WorkspaceCorruptError(
                        f"workspace {table} metadata is missing its comment field"
                    )
        if user_version[0] >= 4:
            columns = {
                column[1]
                for column in connection.execute(
                    "PRAGMA table_info(table_operation_journal)"
                ).fetchall()
            }
            required = {
                "table_id",
                "operation_id",
                "fingerprint",
                "mode",
                "schema_fingerprint",
                "input_sha256",
                "transaction_version",
                "base_version",
                "row_count",
                "state",
                "commit_version",
                "error_code",
                "created_at",
                "updated_at",
            }
            if not required <= columns:
                raise WorkspaceCorruptError(
                    "workspace table operation journal schema is incomplete"
                )
        if user_version[0] >= 5:
            columns = {
                column[1]
                for column in connection.execute(
                    "PRAGMA table_info(worker_attempt_outcomes)"
                ).fetchall()
            }
            required = {
                "run_id",
                "attempt_id",
                "state",
                "exit_code",
                "termination_reason",
                "started_at",
                "finished_at",
                "interpreter",
                "script",
                "cwd",
                "stdout_bytes",
                "stderr_bytes",
                "stdout_truncated",
                "stderr_truncated",
                "cleanup_error_type",
            }
            if not required <= columns:
                raise WorkspaceCorruptError("workspace worker outcome schema is incomplete")
        if user_version[0] >= 6:
            required_job_columns = {
                "jobs": {
                    "job_id",
                    "name",
                    "settings_json",
                    "task_count",
                    "definition_sha256",
                    "created_at",
                },
                "job_tasks": {
                    "job_id",
                    "task_key",
                    "task_type",
                    "task_order",
                    "depends_on_json",
                    "settings_json",
                },
                "job_runs": {
                    "run_id",
                    "job_id",
                    "state",
                    "submission_token",
                    "request_fingerprint",
                    "submitted_settings_json",
                    "created_at",
                    "started_at",
                    "finished_at",
                },
                "job_run_tasks": {
                    "run_id",
                    "job_id",
                    "task_key",
                    "state",
                    "attempt_count",
                },
                "job_task_attempts": {
                    "run_id",
                    "task_key",
                    "attempt_number",
                    "attempt_id",
                    "state",
                    "exit_code",
                },
            }
            for table, required in required_job_columns.items():
                columns = {
                    column[1]
                    for column in connection.execute(f"PRAGMA table_info({table})").fetchall()
                }
                if not required <= columns:
                    raise WorkspaceCorruptError(f"workspace {table} schema is incomplete")
            triggers = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'trigger'"
                ).fetchall()
            }
            required_triggers = {
                "job_definitions_are_immutable",
                "job_definitions_cannot_be_deleted",
                "job_task_definitions_are_immutable",
                "job_task_definitions_cannot_be_deleted",
                "job_task_count_cannot_exceed_definition",
                "job_run_submission_is_immutable",
                "job_run_state_transitions_are_valid",
                "job_run_terminal_outcomes_are_valid",
                "terminal_job_runs_are_immutable",
                "job_runs_cannot_be_deleted",
                "job_run_task_definition_is_immutable",
                "terminal_job_tasks_are_immutable",
                "job_task_state_transitions_are_valid",
                "job_task_transitions_require_active_run",
                "job_run_tasks_cannot_be_deleted",
                "job_attempt_identity_is_immutable",
                "terminal_job_attempts_are_immutable",
                "job_attempt_state_transitions_are_valid",
                "job_attempt_transitions_require_active_run",
                "job_attempts_cannot_be_deleted",
            }
            if not required_triggers <= triggers:
                raise WorkspaceCorruptError("workspace job state triggers are incomplete")
        if user_version[0] >= 7:
            required_compute_columns = {
                "compute_resources": {
                    "compute_id",
                    "resource_kind",
                    "profile",
                    "state",
                    "run_id",
                    "compute_key",
                    "spark_version",
                    "access_mode",
                    "settings_json",
                    "autotermination_minutes",
                    "generation",
                    "deleted_at",
                },
                "compute_consumers": {
                    "compute_id",
                    "run_id",
                    "task_key",
                    "state",
                    "registered_at",
                    "started_at",
                    "released_at",
                },
                "compute_lifecycle_events": {
                    "event_id",
                    "compute_id",
                    "event_type",
                    "state",
                    "generation",
                    "invalidates_sessions",
                    "occurred_at",
                    "details_json",
                },
            }
            for table, required in required_compute_columns.items():
                columns = {
                    column[1]
                    for column in connection.execute(f"PRAGMA table_info({table})").fetchall()
                }
                if not required <= columns:
                    raise WorkspaceCorruptError(f"workspace {table} schema is incomplete")
            compute_triggers = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'trigger'"
                ).fetchall()
            }
            required_compute_triggers = {
                "compute_resource_identity_is_immutable",
                "compute_resources_cannot_be_deleted",
                "compute_resource_state_transitions_are_valid",
                "compute_resource_generations_are_monotonic",
                "deleted_compute_resources_are_immutable",
                "released_compute_consumers_are_immutable",
            }
            if not required_compute_triggers <= compute_triggers:
                raise WorkspaceCorruptError("workspace compute state triggers are incomplete")
        if user_version[0] >= 8:
            output_columns = {
                column[1]
                for column in connection.execute(
                    "PRAGMA table_info(job_task_attempt_outputs)"
                ).fetchall()
            }
            required_output_columns = {
                "run_id",
                "task_key",
                "attempt_number",
                "stdout",
                "stderr",
                "stdout_truncated",
                "stderr_truncated",
            }
            if not required_output_columns <= output_columns:
                raise WorkspaceCorruptError("workspace job output schema is incomplete")
            required_new_triggers = {
                "job_attempt_outputs_are_immutable",
                "job_attempt_outputs_cannot_be_deleted",
                "compute_consumer_identity_is_immutable",
                "compute_consumer_state_transitions_are_valid",
                "compute_consumers_cannot_be_deleted",
                "compute_lifecycle_events_are_immutable",
                "compute_lifecycle_events_cannot_be_deleted",
            }
            if not required_new_triggers <= compute_triggers:
                raise WorkspaceCorruptError("workspace output/lifecycle triggers are incomplete")
        if user_version[0] >= 9:
            worker_columns = {
                column[1]
                for column in connection.execute(
                    "PRAGMA table_info(worker_attempt_outcomes)"
                ).fetchall()
            }
            if "worker_pid" not in worker_columns:
                raise WorkspaceCorruptError("workspace worker recovery schema is incomplete")
        if user_version[0] >= 10:
            task_value_columns = {
                column[1]
                for column in connection.execute("PRAGMA table_info(job_task_values)").fetchall()
            }
            required_task_value_columns = {
                "run_id",
                "task_key",
                "attempt_number",
                "value_key",
                "value_json",
                "updated_at",
            }
            if not required_task_value_columns <= task_value_columns:
                raise WorkspaceCorruptError("workspace task value schema is incomplete")
            task_value_objects = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type IN ('index', 'trigger')"
                ).fetchall()
            }
            required_task_value_objects = {
                "job_task_values_lookup_idx",
                "job_task_values_require_running_attempt_insert",
                "job_task_values_require_running_attempt_update",
                "job_task_value_identity_is_immutable",
                "job_task_values_cannot_be_deleted",
            }
            if not required_task_value_objects <= task_value_objects:
                raise WorkspaceCorruptError("workspace task value constraints are incomplete")
        if user_version[0] >= 11:
            required_terraform_job_columns = {
                "terraform_job_resources": {
                    "job_id",
                    "current_revision",
                    "created_at",
                    "deleted_at",
                },
                "terraform_job_revisions": {
                    "job_id",
                    "revision",
                    "definition_job_id",
                    "settings_json",
                    "created_at",
                },
            }
            for table, required in required_terraform_job_columns.items():
                columns = {
                    column[1]
                    for column in connection.execute(f"PRAGMA table_info({table})").fetchall()
                }
                if not required <= columns:
                    raise WorkspaceCorruptError(f"workspace {table} schema is incomplete")
            required_terraform_job_triggers = {
                "terraform_job_resource_identity_is_immutable",
                "terraform_job_revision_is_monotonic",
                "terraform_job_resource_delete_is_terminal",
                "terraform_job_revisions_are_immutable",
                "terraform_job_revisions_cannot_be_deleted",
                "terraform_job_resources_cannot_be_deleted",
            }
            terraform_job_triggers = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'trigger'"
                ).fetchall()
            }
            if not required_terraform_job_triggers <= terraform_job_triggers:
                raise WorkspaceCorruptError("workspace Terraform job constraints are incomplete")
        if user_version[0] >= 12:
            deletion_columns = {
                column[1]
                for column in connection.execute("PRAGMA table_info(job_deletions)").fetchall()
            }
            if not {"job_id", "deleted_at"} <= deletion_columns:
                raise WorkspaceCorruptError("workspace job deletion schema is incomplete")
            deletion_triggers = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'trigger'"
                ).fetchall()
            }
            if (
                not {
                    "job_deletions_are_immutable",
                    "job_deletions_cannot_be_deleted",
                }
                <= deletion_triggers
            ):
                raise WorkspaceCorruptError("workspace job deletion constraints are incomplete")
        rows = dict(connection.execute("SELECT key, value FROM workspace_metadata"))
        try:
            stored_format = int(rows["format_version"])
            workspace_id = rows["workspace_id"]
            default_catalog = rows["default_catalog"]
            default_schema = rows["default_schema"]
        except (KeyError, ValueError) as error:
            raise WorkspaceCorruptError("required workspace metadata is missing") from error
        if stored_format != SUPPORTED_FORMAT_VERSION or stored_format != config.format_version:
            raise UnsupportedWorkspaceFormatError(
                f"workspace format {stored_format} is not supported or disagrees with config"
            )
        if default_catalog != config.default_catalog or default_schema != config.default_schema:
            raise WorkspaceCorruptError("configured default namespace disagrees with metadata")
        catalog = connection.execute(
            "SELECT is_default FROM catalogs WHERE name = ?", (default_catalog,)
        ).fetchone()
        schema = connection.execute(
            "SELECT is_default FROM schemas WHERE catalog_name = ? AND name = ?",
            (default_catalog, default_schema),
        ).fetchone()
        if catalog != (1,) or schema != (1,):
            raise WorkspaceCorruptError("configured default namespace is missing")
        default_catalog_count = connection.execute(
            "SELECT count(*) FROM catalogs WHERE is_default = 1"
        ).fetchone()
        default_schema_count = connection.execute(
            "SELECT count(*) FROM schemas WHERE is_default = 1"
        ).fetchone()
        if default_catalog_count != (1,) or default_schema_count != (1,):
            raise WorkspaceCorruptError("workspace must have exactly one default namespace")
        if not workspace_id:
            raise WorkspaceCorruptError("workspace identity is missing")
    except sqlite3.Error as error:
        raise WorkspaceCorruptError("workspace metadata cannot be read") from error
    finally:
        if connection is not None:
            connection.close()
    return Workspace(
        path=target,
        config=config,
        workspace_id=workspace_id,
        metadata_schema_version=user_version[0],
    )
