from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from brickhouse import Workspace
from brickhouse.catalog import (
    CatalogRepositoryError,
    InvalidTableLocationError,
    NamespaceNotFoundError,
    TableAlreadyExistsError,
    TableLocationConflictError,
    TableOwnership,
)
from brickhouse.workspace.lifecycle import initialize_workspace, open_workspace
from brickhouse.workspace.ownership import acquire_workspace_owner


def test_catalog_and_schema_repositories_read_workspace_defaults(tmp_path: Path) -> None:
    descriptor = initialize_workspace(tmp_path / "workspace")

    with Workspace.local(descriptor.path) as context:
        assert context.catalogs is context.catalogs
        assert context.schemas is context.schemas
        catalog = context.catalogs.get("main")
        schema = context.schemas.get("main", "default")
        assert catalog.catalog_id
        assert context.catalogs.list() == [catalog]
        assert catalog.is_default
        assert schema in context.schemas.list("main")
        assert context.schemas.get("main", "information_schema").name == "information_schema"
        assert schema.schema_id
        assert schema.name == "default"
        assert schema.is_default
        with pytest.raises(NamespaceNotFoundError, match="catalog does not exist"):
            context.schemas.list("missing")


def test_managed_table_identity_and_location_survive_reopen(tmp_path: Path) -> None:
    descriptor = initialize_workspace(tmp_path / "workspace")

    with Workspace.local(descriptor.path) as context:
        catalog = context.catalogs.get("main")
        schema = context.schemas.get("main", "default")
        created = context.tables.create_managed("main", "default", "orders")
        assert created.table_id
        assert created.catalog_id == catalog.catalog_id
        assert created.schema_id == schema.schema_id
        assert created.ownership is TableOwnership.MANAGED
        assert created.table_format == "delta"
        assert (
            created.canonical_location == (descriptor.path / "tables" / created.table_id).resolve()
        )
        assert not created.canonical_location.exists()
        assert context.tables.get("main", "default", "orders") == created
        assert context.tables.get_by_id(created.table_id) == created
        assert context.tables.get_by_location(created.canonical_location) == created
        with pytest.raises(TableAlreadyExistsError):
            context.tables.create_managed("main", "default", "orders")
        with pytest.raises(NamespaceNotFoundError):
            context.tables.create_managed("missing", "default", "orphan")

    with Workspace.local(descriptor.path) as reopened:
        assert reopened.workspace.workspace_id == descriptor.workspace_id
        assert reopened.catalogs.get("main").catalog_id == catalog.catalog_id
        assert reopened.schemas.get("main", "default").schema_id == schema.schema_id
        assert reopened.tables.get_by_id(created.table_id) == created


def test_external_symlink_alias_cannot_be_registered_twice(tmp_path: Path) -> None:
    descriptor = initialize_workspace(tmp_path / "workspace")
    external_path = tmp_path / "external-delta"
    external_path.mkdir()
    alias_path = tmp_path / "external-alias"
    try:
        alias_path.symlink_to(external_path, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"test filesystem cannot create symlinks: {error}")

    with Workspace.local(descriptor.path) as context:
        created = context.tables.register_external("main", "default", "source", external_path)
        assert created.ownership is TableOwnership.EXTERNAL
        assert created.canonical_location == external_path.resolve()
        assert context.tables.get_by_location(alias_path) == created
        with pytest.raises(TableLocationConflictError):
            context.tables.register_external("main", "default", "alias_source", alias_path)


def test_repository_rejects_remote_and_managed_root_external_locations(
    tmp_path: Path,
) -> None:
    descriptor = initialize_workspace(tmp_path / "workspace")
    with Workspace.local(descriptor.path) as context:
        with pytest.raises(InvalidTableLocationError, match="local filesystem"):
            context.tables.register_external("main", "default", "remote", "s3://bucket/path")
        with pytest.raises(InvalidTableLocationError, match="managed-table root"):
            context.tables.register_external(
                "main", "default", "managed_alias", descriptor.path / "tables" / "elsewhere"
            )
        with pytest.raises(CatalogRepositoryError, match="lowercase unquoted"):
            context.tables.create_managed("main", "default", "NotQuoted")


def test_owner_upgrade_migrates_schema_v1_before_repositories_open(
    tmp_path: Path,
) -> None:
    descriptor = initialize_workspace(tmp_path / "workspace")
    with sqlite3.connect(descriptor.metadata_path) as connection:
        for trigger in (
            "job_deletions_are_immutable",
            "job_deletions_cannot_be_deleted",
        ):
            connection.execute(f"DROP TRIGGER {trigger}")
        connection.execute("DROP TABLE job_deletions")
        connection.execute("DELETE FROM schema_migrations WHERE version = 12")
        for trigger in (
            "terraform_job_resource_identity_is_immutable",
            "terraform_job_revision_is_monotonic",
            "terraform_job_resource_delete_is_terminal",
            "terraform_job_revisions_are_immutable",
            "terraform_job_revisions_cannot_be_deleted",
            "terraform_job_resources_cannot_be_deleted",
            "compute_resource_state_transitions_are_valid",
            "compute_resource_generations_are_monotonic",
            "deleted_compute_resources_are_immutable",
            "compute_resource_identity_is_immutable",
            "compute_resources_cannot_be_deleted",
            "released_compute_consumers_are_immutable",
            "compute_consumer_identity_is_immutable",
            "compute_consumer_state_transitions_are_valid",
            "compute_consumers_cannot_be_deleted",
            "compute_lifecycle_events_are_immutable",
            "compute_lifecycle_events_cannot_be_deleted",
            "job_task_values_require_running_attempt_insert",
            "job_task_values_require_running_attempt_update",
            "job_task_value_identity_is_immutable",
            "job_task_values_cannot_be_deleted",
        ):
            connection.execute(f"DROP TRIGGER {trigger}")
        connection.execute("DROP TABLE terraform_job_revisions")
        connection.execute("DROP TABLE terraform_job_resources")
        connection.execute("DELETE FROM schema_migrations WHERE version = 11")
        connection.execute("DROP TABLE job_task_values")
        connection.execute("DROP TABLE compute_lifecycle_events")
        connection.execute("DROP TABLE compute_consumers")
        connection.execute("DROP TABLE compute_resources")
        connection.execute("DELETE FROM schema_migrations WHERE version = 10")
        connection.execute("DROP TABLE job_task_attempt_outputs")
        connection.execute("DELETE FROM schema_migrations WHERE version = 9")
        connection.execute("DELETE FROM schema_migrations WHERE version = 8")
        connection.execute("DELETE FROM schema_migrations WHERE version = 7")
        connection.execute(
            "DELETE FROM schemas WHERE catalog_name = 'main' AND name = 'information_schema'"
        )
        connection.execute("ALTER TABLE catalogs DROP COLUMN comment")
        connection.execute("ALTER TABLE schemas DROP COLUMN comment")
        connection.execute("DROP TABLE table_operation_journal")
        connection.execute("DROP TABLE worker_attempt_outcomes")
        connection.execute("DROP TABLE job_task_attempts")
        connection.execute("DROP TABLE job_run_tasks")
        connection.execute("DROP TABLE job_runs")
        connection.execute("DROP TABLE job_tasks")
        connection.execute("DROP TABLE jobs")
        connection.execute("DROP TABLE table_registrations")
        for name in (
            "catalogs_require_catalog_id_insert",
            "schemas_require_schema_id_insert",
            "catalogs_require_catalog_id_update",
            "schemas_require_schema_id_update",
            "catalogs_catalog_id_immutable",
            "schemas_schema_id_immutable",
        ):
            connection.execute(f"DROP TRIGGER {name}")
        for version in (4, 5, 6):
            connection.execute("DELETE FROM schema_migrations WHERE version = ?", (version,))
        connection.execute("DROP INDEX catalogs_catalog_id_unique")
        connection.execute("DROP INDEX schemas_schema_id_unique")
        connection.execute("ALTER TABLE catalogs DROP COLUMN catalog_id")
        connection.execute("ALTER TABLE schemas DROP COLUMN schema_id")
        connection.execute("DELETE FROM schema_migrations WHERE version = 4")
        connection.execute("DELETE FROM schema_migrations WHERE version = 3")
        connection.execute("DELETE FROM schema_migrations WHERE version = 2")
        connection.execute("PRAGMA user_version = 1")

    legacy = open_workspace(descriptor.path)
    assert legacy.metadata_schema_version == 1

    with acquire_workspace_owner(descriptor.path) as owner:
        assert owner.workspace.metadata_schema_version == 12
        with sqlite3.connect(owner.workspace.metadata_path) as connection:
            assert connection.execute("PRAGMA user_version").fetchone() == (12,)
            assert connection.execute(
                "SELECT version FROM schema_migrations ORDER BY version"
            ).fetchall() == [
                (1,),
                (2,),
                (3,),
                (4,),
                (5,),
                (6,),
                (7,),
                (8,),
                (9,),
                (10,),
                (11,),
                (12,),
            ]
            assert connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name = 'table_registrations'"
            ).fetchone() == ("table_registrations",)
            assert connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name = 'table_operation_journal'"
            ).fetchone() == ("table_operation_journal",)
            assert connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'jobs'"
            ).fetchone() == ("jobs",)
            catalog_id = connection.execute(
                "SELECT catalog_id FROM catalogs WHERE name = 'main'"
            ).fetchone()[0]
            schema_id = connection.execute(
                "SELECT schema_id FROM schemas WHERE catalog_name = 'main' AND name = 'default'"
            ).fetchone()[0]
            assert catalog_id
            assert schema_id

    with Workspace.local(descriptor.path) as reopened:
        assert reopened.catalogs.get("main").catalog_id == catalog_id
        assert reopened.schemas.get("main", "default").schema_id == schema_id
