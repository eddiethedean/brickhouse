"""Persistent Terraform job identities backed by immutable local job revisions."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

from brickhouse.jobs.models import canonical_settings
from brickhouse.workspace.context import WorkspaceContext


class TerraformJobRegistryError(RuntimeError):
    """Base error for the local Terraform job identity registry."""


class TerraformJobNotFoundError(TerraformJobRegistryError):
    """A Terraform job identity is absent or has been destroyed."""


@dataclass(frozen=True, slots=True)
class TerraformJobRecord:
    """The current API settings and immutable definition version for one job identity."""

    job_id: int
    definition_job_id: int
    revision: int
    settings_json: str
    created_at: str

    @property
    def settings(self) -> dict[str, object]:
        """Return a detached copy of the Terraform settings document."""
        value = json.loads(self.settings_json)
        if not isinstance(value, dict):
            raise TerraformJobRegistryError("persisted Terraform job settings are invalid")
        return cast(dict[str, object], value)


class TerraformJobRegistry:
    """Map stable Terraform resource IDs to append-only job definition versions."""

    def __init__(self, context: WorkspaceContext) -> None:
        self._metadata_path = context.workspace.metadata_path

    def create(self, definition_job_id: int, settings: dict[str, object]) -> TerraformJobRecord:
        """Register revision one using the first immutable job ID as the API identity."""
        now = _utc_now()
        settings_json = canonical_settings(settings, label="Terraform job settings")
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "INSERT INTO terraform_job_resources "
                    "(job_id, current_revision, created_at) VALUES (?, 1, ?)",
                    (definition_job_id, now),
                )
                connection.execute(
                    "INSERT INTO terraform_job_revisions "
                    "(job_id, revision, definition_job_id, settings_json, created_at) "
                    "VALUES (?, 1, ?, ?, ?)",
                    (definition_job_id, definition_job_id, settings_json, now),
                )
                connection.commit()
            except (sqlite3.Error, ValueError) as error:
                if connection.in_transaction:
                    connection.rollback()
                raise TerraformJobRegistryError("could not register Terraform job") from error
        return TerraformJobRecord(
            job_id=definition_job_id,
            definition_job_id=definition_job_id,
            revision=1,
            settings_json=settings_json,
            created_at=now,
        )

    def get(self, job_id: int) -> TerraformJobRecord:
        """Return the active settings and definition version for a stable API ID."""
        with self._connection() as connection:
            row = connection.execute(
                "SELECT resource.current_revision, resource.created_at, "
                "resource.deleted_at, revision.definition_job_id, revision.settings_json "
                "FROM terraform_job_resources AS resource "
                "JOIN terraform_job_revisions AS revision "
                "ON revision.job_id = resource.job_id "
                "AND revision.revision = resource.current_revision "
                "WHERE resource.job_id = ?",
                (job_id,),
            ).fetchone()
        if row is None or row[2] is not None:
            raise TerraformJobNotFoundError(f"job {job_id} does not exist")
        return TerraformJobRecord(
            job_id=job_id,
            definition_job_id=row[3],
            revision=row[0],
            settings_json=row[4],
            created_at=row[1],
        )

    def resolve_execution_id(self, job_id: int) -> int | None:
        """Resolve a managed API identity, leaving ordinary SDK job IDs untouched."""
        with self._connection() as connection:
            row = connection.execute(
                "SELECT current_revision, deleted_at FROM terraform_job_resources WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if row is None:
                return None
            if row[1] is not None:
                raise TerraformJobNotFoundError(f"job {job_id} does not exist")
            revision = connection.execute(
                "SELECT definition_job_id FROM terraform_job_revisions "
                "WHERE job_id = ? AND revision = ?",
                (job_id, row[0]),
            ).fetchone()
        if revision is None:
            raise TerraformJobRegistryError("active Terraform job revision is missing")
        return cast(int, revision[0])

    def definition_job_ids(self, job_id: int) -> tuple[int, ...] | None:
        """Return every immutable definition ID for an active Terraform job identity."""
        with self._connection() as connection:
            resource = connection.execute(
                "SELECT deleted_at FROM terraform_job_resources WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if resource is None:
                return None
            if resource[0] is not None:
                raise TerraformJobNotFoundError(f"job {job_id} does not exist")
            rows = connection.execute(
                "SELECT definition_job_id FROM terraform_job_revisions "
                "WHERE job_id = ? ORDER BY revision",
                (job_id,),
            ).fetchall()
        if not rows:
            raise TerraformJobRegistryError("active Terraform job has no definition history")
        return tuple(cast(int, row[0]) for row in rows)

    def advance(
        self,
        job_id: int,
        definition_job_id: int,
        settings: dict[str, object],
    ) -> TerraformJobRecord:
        """Append an updated immutable definition and point the resource to it."""
        now = _utc_now()
        settings_json = canonical_settings(settings, label="Terraform job settings")
        with self._connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT current_revision, created_at, deleted_at "
                    "FROM terraform_job_resources WHERE job_id = ?",
                    (job_id,),
                ).fetchone()
                if row is None or row[2] is not None:
                    raise TerraformJobNotFoundError(f"job {job_id} does not exist")
                revision = cast(int, row[0]) + 1
                connection.execute(
                    "INSERT INTO terraform_job_revisions "
                    "(job_id, revision, definition_job_id, settings_json, created_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (job_id, revision, definition_job_id, settings_json, now),
                )
                connection.execute(
                    "UPDATE terraform_job_resources SET current_revision = ? WHERE job_id = ?",
                    (revision, job_id),
                )
                connection.commit()
            except TerraformJobNotFoundError:
                if connection.in_transaction:
                    connection.rollback()
                raise
            except (sqlite3.Error, ValueError) as error:
                if connection.in_transaction:
                    connection.rollback()
                raise TerraformJobRegistryError("could not update Terraform job") from error
        return TerraformJobRecord(
            job_id=job_id,
            definition_job_id=definition_job_id,
            revision=revision,
            settings_json=settings_json,
            created_at=cast(str, row[1]),
        )

    def delete(self, job_id: int) -> None:
        """Tombstone an API identity while retaining its definition and revision history."""
        try:
            with self._connection() as connection:
                connection.execute("BEGIN IMMEDIATE")
                cursor = connection.execute(
                    "UPDATE terraform_job_resources SET deleted_at = ? "
                    "WHERE job_id = ? AND deleted_at IS NULL",
                    (_utc_now(), job_id),
                )
                if cursor.rowcount != 1:
                    connection.rollback()
                    raise TerraformJobNotFoundError(f"job {job_id} does not exist")
                connection.commit()
        except TerraformJobNotFoundError:
            raise
        except sqlite3.Error as error:
            raise TerraformJobRegistryError("could not delete Terraform job") from error

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self._metadata_path, timeout=5.0)
        try:
            connection.execute("PRAGMA foreign_keys = ON")
            yield connection
        finally:
            connection.close()


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
