"""Transactional SQLite schema migration primitives for workspaces."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Sequence
from dataclasses import dataclass


class WorkspaceMigrationError(RuntimeError):
    """A workspace schema migration could not be applied safely."""


@dataclass(frozen=True, slots=True)
class Migration:
    """One ordered schema migration, applied inside a SQLite transaction."""

    version: int
    apply: Callable[[sqlite3.Connection], None]

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version < 1:
            raise ValueError("migration version must be a positive integer")


def apply_migrations(
    connection: sqlite3.Connection,
    migrations: Sequence[Migration],
) -> int:
    """Apply every pending migration atomically and return the resulting version."""
    ordered = tuple(sorted(migrations, key=lambda migration: migration.version))
    versions = tuple(migration.version for migration in ordered)
    if len(set(versions)) != len(versions):
        raise WorkspaceMigrationError("migration versions must be unique")
    if versions and versions != tuple(range(1, versions[-1] + 1)):
        raise WorkspaceMigrationError("migration versions must be consecutive starting at 1")
    if not ordered:
        raise WorkspaceMigrationError("at least one workspace migration is required")
    if connection.in_transaction:
        raise WorkspaceMigrationError("cannot migrate inside an existing transaction")

    connection.execute("BEGIN IMMEDIATE")
    current_version = 0
    try:
        has_history = (
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'"
            ).fetchone()
            is not None
        )
        pragma_version = connection.execute("PRAGMA user_version").fetchone()
        if pragma_version is None:
            raise WorkspaceMigrationError("SQLite user_version is unavailable")
        current_version = pragma_version[0]

        if has_history:
            history = connection.execute(
                "SELECT version FROM schema_migrations ORDER BY version"
            ).fetchall()
            history_versions = tuple(row[0] for row in history)
            if history_versions != tuple(range(1, current_version + 1)):
                raise WorkspaceMigrationError(
                    "workspace migration history disagrees with SQLite user_version"
                )
        else:
            if current_version != 0:
                raise WorkspaceMigrationError(
                    "workspace has a SQLite version but no migration history"
                )
            connection.execute(
                "CREATE TABLE schema_migrations ("
                "version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
            )

        if current_version > versions[-1]:
            raise WorkspaceMigrationError(
                f"workspace schema version {current_version} is newer than this migration set"
            )

        for migration in ordered:
            if migration.version <= current_version:
                continue
            if migration.version != current_version + 1:
                raise WorkspaceMigrationError(
                    f"migration sequence skips version {current_version + 1}"
                )
            try:
                migration.apply(connection)
            except Exception as error:
                raise WorkspaceMigrationError(
                    f"workspace migration {migration.version} failed"
                ) from error
            connection.execute(
                "INSERT INTO schema_migrations(version, applied_at) "
                "VALUES (?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
                (migration.version,),
            )
            connection.execute(f"PRAGMA user_version = {migration.version}")
            current_version = migration.version

        connection.commit()
        return current_version
    except BaseException:
        if connection.in_transaction:
            connection.rollback()
        raise
