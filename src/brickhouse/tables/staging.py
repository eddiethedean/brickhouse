"""Workspace-owned staging for local table inputs."""

from __future__ import annotations

import hashlib
import os
import shutil
import uuid
from datetime import UTC, datetime
from pathlib import Path

from brickhouse.tables.contracts import InputFormat, SchemaDescriptor, StagedInput
from brickhouse.workspace.context import WorkspaceContext


class StagingError(RuntimeError):
    """Base error for staged table input operations."""


class StagingOwnershipError(StagingError):
    """A staged file is outside this workspace's private staging area."""


class StagingArea:
    """Copy local provider input into a private per-artifact workspace directory."""

    def __init__(self, context: WorkspaceContext) -> None:
        self._context = context

    def stage_file(
        self,
        source: str | Path,
        schema: SchemaDescriptor,
        data_format: InputFormat,
        *,
        operation_id: str,
        row_count: int,
    ) -> StagedInput:
        if (
            not operation_id
            or len(operation_id) > 128
            or any(ord(character) < 32 or ord(character) == 127 for character in operation_id)
        ):
            raise StagingError(
                "operation_id must be a non-empty printable value up to 128 characters"
            )
        if row_count < 0:
            raise StagingError("row_count must be zero or greater")
        try:
            source_path = Path(source).expanduser().resolve(strict=True)
        except (OSError, RuntimeError) as error:
            raise StagingError("staging source does not exist or cannot be resolved") from error
        if not source_path.is_file():
            raise StagingError("staging source must be a regular file")
        try:
            selected_format = (
                data_format if isinstance(data_format, InputFormat) else InputFormat(data_format)
            )
        except ValueError as error:
            raise StagingError(f"unsupported staged input format: {data_format!r}") from error

        root = self._staging_root()
        staging_id = str(uuid.uuid4())
        artifact_directory = root / staging_id
        try:
            artifact_directory.mkdir(mode=0o700)
            target = artifact_directory / f"input{selected_format.suffix}"
            file_descriptor = os.open(
                target,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o600,
            )
            digest = hashlib.sha256()
            byte_count = 0
            with os.fdopen(file_descriptor, "wb") as target_file:
                with source_path.open("rb") as source_file:
                    while chunk := source_file.read(1024 * 1024):
                        target_file.write(chunk)
                        digest.update(chunk)
                        byte_count += len(chunk)
                target_file.flush()
                os.fsync(target_file.fileno())
        except OSError as error:
            if artifact_directory.exists():
                shutil.rmtree(artifact_directory, ignore_errors=True)
            raise StagingError("could not copy input into workspace staging") from error
        return StagedInput(
            staging_id=staging_id,
            workspace_id=self._context.workspace.workspace_id,
            operation_id=operation_id,
            path=target.resolve(strict=True),
            data_format=selected_format,
            schema=schema,
            row_count=row_count,
            byte_count=byte_count,
            sha256=digest.hexdigest(),
            created_at=datetime.now(UTC),
        )

    def cleanup(self, staged: StagedInput) -> None:
        """Remove only the artifact named by this staged-input manifest."""
        resolved = self.resolve_owned_path(staged)
        resolved.unlink()
        try:
            resolved.parent.rmdir()
        except OSError:
            # Preserve sibling data if an artifact directory was unexpectedly populated.
            pass

    def resolve_owned_path(self, staged: StagedInput) -> Path:
        """Validate a manifest and return its regular file inside this workspace staging root."""
        if staged.workspace_id != self._context.workspace.workspace_id:
            raise StagingOwnershipError("staged input belongs to a different workspace")
        try:
            canonical_id = str(uuid.UUID(staged.staging_id))
            selected_format = (
                staged.data_format
                if isinstance(staged.data_format, InputFormat)
                else InputFormat(staged.data_format)
            )
        except (AttributeError, TypeError, ValueError) as error:
            raise StagingOwnershipError("staged input has invalid artifact metadata") from error
        if canonical_id != staged.staging_id:
            raise StagingOwnershipError("staged input has a non-canonical artifact ID")
        root = self._staging_root()
        expected = root / canonical_id / f"input{selected_format.suffix}"
        try:
            resolved = staged.path.resolve(strict=True)
            resolved_expected = expected.resolve(strict=True)
        except (OSError, RuntimeError) as error:
            raise StagingOwnershipError("staged input is missing or cannot be resolved") from error
        if (
            resolved != resolved_expected
            or not resolved.is_relative_to(root)
            or not resolved.is_file()
        ):
            raise StagingOwnershipError("staged input path is outside its owned artifact directory")
        return resolved

    def verify_content(self, staged: StagedInput) -> Path:
        """Validate ownership plus the copied byte count and digest before provider use."""
        path = self.resolve_owned_path(staged)
        digest = hashlib.sha256()
        byte_count = 0
        try:
            with path.open("rb") as staged_file:
                while chunk := staged_file.read(1024 * 1024):
                    digest.update(chunk)
                    byte_count += len(chunk)
        except OSError as error:
            raise StagingError("could not verify staged input contents") from error
        if byte_count != staged.byte_count or digest.hexdigest() != staged.sha256:
            raise StagingError("staged input contents do not match its byte count or SHA-256")
        return path

    def _staging_root(self) -> Path:
        workspace = self._context.workspace
        root = (workspace.path / "staging").resolve(strict=True)
        if not root.is_relative_to(workspace.path):
            raise StagingOwnershipError("workspace staging directory resolves outside its root")
        return root
