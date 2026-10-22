"""Quiesced, local workspace snapshots with an explicit external-table policy."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
import uuid
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from brickhouse.workspace.lifecycle import Workspace, WorkspaceError, open_workspace
from brickhouse.workspace.ownership import acquire_workspace_owner

_MANIFEST_NAME = "brickhouse.snapshot.json"
_MANIFEST_VERSION = 2
_EXTERNAL_POLICY = "exclude_bytes_preserve_registration"
_OWNER_LOCK_NAME = ".brickhouse-owner.lock"


class WorkspaceSnapshotError(WorkspaceError):
    """A workspace snapshot could not be created or restored safely."""


class WorkspaceSnapshotBusyError(WorkspaceSnapshotError):
    """A workspace has unfinished work and is not quiescent enough to snapshot."""


class WorkspaceSnapshotCorruptError(WorkspaceSnapshotError):
    """A snapshot manifest or copied workspace state is inconsistent."""


@dataclass(frozen=True, slots=True)
class ExternalTableReference:
    """An external table whose bytes are kept outside the workspace snapshot."""

    table_id: str
    full_name: str
    location: str
    available_at_snapshot: bool


@dataclass(frozen=True, slots=True)
class WorkspaceSnapshot:
    """A completed snapshot and its external-table reference inventory."""

    path: Path
    source_path: Path
    workspace_id: str
    external_tables: tuple[ExternalTableReference, ...]


@dataclass(frozen=True, slots=True)
class WorkspaceRestoreResult:
    """The restored workspace and external references that were not copied."""

    workspace: Workspace
    external_tables: tuple[ExternalTableReference, ...]


def snapshot_workspace(
    source_workspace: str | Path,
    snapshot_path: str | Path,
) -> WorkspaceSnapshot:
    """Copy a quiesced workspace into a new directory, excluding external table bytes.

    The workspace owner lock prevents BrickHouse services from mutating metadata or
    managed tables while the copy is made. Registered external locations are kept
    in metadata and the snapshot manifest, but any such location nested under the
    workspace directory is excluded from the copied tree.
    """
    source = open_workspace(source_workspace).path
    target = _new_target(snapshot_path, "snapshot")
    _require_disjoint(source, target)
    if (source / _MANIFEST_NAME).exists():
        raise WorkspaceSnapshotError(f"workspace reserves {_MANIFEST_NAME} for snapshots")

    temporary: Path | None = None
    try:
        with acquire_workspace_owner(source) as owner:
            workspace = owner.workspace
            external_tables, excluded_paths = _read_and_validate_source(workspace)
            _require_disjoint_from_external(target, external_tables)
            _checkpoint_metadata(workspace.metadata_path)
            _reject_unhandled_symlinks(source, excluded_paths)
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = Path(
                tempfile.mkdtemp(prefix=f".{target.name}.snapshot-", dir=target.parent)
            )
            shutil.copytree(
                source,
                temporary,
                dirs_exist_ok=True,
                ignore=_copy_filter(source, excluded_paths, omit_manifest=False),
                symlinks=False,
            )
            manifest = {
                "format_version": _MANIFEST_VERSION,
                "workspace_id": workspace.workspace_id,
                "source_path": str(source),
                "external_table_policy": _EXTERNAL_POLICY,
                "external_tables": [
                    {
                        "table_id": item.table_id,
                        "full_name": item.full_name,
                        "location": item.location,
                        "available_at_snapshot": item.available_at_snapshot,
                    }
                    for item in external_tables
                ],
                "managed_tables": _managed_table_inventory(workspace.path, temporary),
            }
            (temporary / _MANIFEST_NAME).write_text(
                json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
            snapshot_id = workspace.workspace_id
            source_path = source
        assert temporary is not None
        _publish_directory(temporary, target)
        return WorkspaceSnapshot(target, source_path, snapshot_id, external_tables)
    except BaseException:
        if temporary is not None and temporary.exists():
            shutil.rmtree(temporary, ignore_errors=True)
        raise


def restore_workspace_snapshot(
    snapshot_path: str | Path,
    workspace_path: str | Path,
) -> WorkspaceRestoreResult:
    """Restore a snapshot to a new path and rebase only workspace-managed tables.

    External registrations keep their original absolute locations. Their data is
    never copied; reads fail with the table service's explicit missing-table error
    when the referenced external location is unavailable.
    """
    snapshot = open_workspace(snapshot_path).path
    target = _new_target(workspace_path, "restore")
    _require_disjoint(snapshot, target)
    temporary: Path | None = None
    try:
        with acquire_workspace_owner(snapshot) as owner:
            source_workspace = owner.workspace
            manifest = _read_manifest(snapshot, source_workspace)
            external_tables = _external_references_from_manifest(manifest)
            database_external = _external_tables(source_workspace)
            if _external_identity(external_tables) != _external_identity(database_external):
                raise WorkspaceSnapshotCorruptError(
                    "snapshot external-table manifest does not match its registrations"
                )
            _require_disjoint_from_external(target, external_tables)
            excluded_paths = _external_paths_under_root(snapshot, external_tables)
            _reject_unhandled_symlinks(snapshot, excluded_paths, omit_manifest=True)
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = Path(tempfile.mkdtemp(prefix=f".{target.name}.restore-", dir=target.parent))
            shutil.copytree(
                snapshot,
                temporary,
                dirs_exist_ok=True,
                ignore=_copy_filter(snapshot, excluded_paths, omit_manifest=True),
                symlinks=False,
            )

        assert temporary is not None
        _rebase_managed_table_locations(
            temporary,
            snapshot_root=snapshot,
            original_workspace=Path(cast(str, manifest["source_path"])),
            restored_workspace=target,
            manifest=manifest,
        )
        _publish_directory(temporary, target)
        with acquire_workspace_owner(target) as owner:
            # Opening under the owner lock validates the copied metadata and applies
            # any supported migration before the restore is reported successful.
            restored = owner.workspace
        return WorkspaceRestoreResult(restored, external_tables)
    except BaseException:
        if temporary is not None and temporary.exists():
            shutil.rmtree(temporary, ignore_errors=True)
        raise


def _new_target(path: str | Path, label: str) -> Path:
    raw = Path(path).expanduser()
    if raw.exists() or raw.is_symlink():
        raise WorkspaceSnapshotError(f"{label} path already exists: {raw}")
    target = raw.resolve(strict=False)
    if not target.name:
        raise WorkspaceSnapshotError(f"{label} path must name a directory")
    return target


def _require_disjoint(source: Path, target: Path) -> None:
    if source == target or source.is_relative_to(target) or target.is_relative_to(source):
        raise WorkspaceSnapshotError("source and destination paths must not overlap")


def _require_disjoint_from_external(
    target: Path,
    external_tables: tuple[ExternalTableReference, ...],
) -> None:
    for item in external_tables:
        location = Path(item.location).resolve(strict=False)
        if target == location or target.is_relative_to(location) or location.is_relative_to(target):
            raise WorkspaceSnapshotError(
                f"destination overlaps external table location for {item.full_name}"
            )


def _read_and_validate_source(
    workspace: Workspace,
) -> tuple[tuple[ExternalTableReference, ...], set[Path]]:
    with closing(sqlite3.connect(workspace.metadata_path)) as connection:
        connection.row_factory = sqlite3.Row
        _assert_quiescent(connection)
        rows = connection.execute(
            "SELECT table_id, catalog_name, schema_name, name, canonical_location "
            "FROM table_registrations WHERE ownership = 'external' "
            "ORDER BY catalog_name, schema_name, name"
        ).fetchall()
    external_tables = tuple(
        ExternalTableReference(
            table_id=str(row["table_id"]),
            full_name=f"{row['catalog_name']}.{row['schema_name']}.{row['name']}",
            location=str(Path(row["canonical_location"])),
            available_at_snapshot=Path(row["canonical_location"]).exists(),
        )
        for row in rows
    )
    return external_tables, _external_paths_under_root(workspace.path, external_tables)


def _assert_quiescent(connection: sqlite3.Connection) -> None:
    active_checks = (
        ("table_operation_journal", "state IN ('prepared', 'committing', 'uncertain')"),
        ("worker_attempt_outcomes", "state = 'running'"),
        ("job_runs", "state IN ('queued', 'running')"),
        ("job_task_attempts", "state IN ('queued', 'running')"),
        ("sql_statements", "state IN ('PENDING', 'RUNNING')"),
        ("sql_statement_admission_leases", "state IN ('QUEUED', 'ACTIVE')"),
    )
    for table, predicate in active_checks:
        count = connection.execute(f"SELECT count(*) FROM {table} WHERE {predicate}").fetchone()[0]
        if count:
            raise WorkspaceSnapshotBusyError(
                f"workspace has {count} unfinished record(s) in {table}"
            )


def _external_tables(workspace: Workspace) -> tuple[ExternalTableReference, ...]:
    with closing(sqlite3.connect(workspace.metadata_path)) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            "SELECT table_id, catalog_name, schema_name, name, canonical_location "
            "FROM table_registrations WHERE ownership = 'external' "
            "ORDER BY catalog_name, schema_name, name"
        ).fetchall()
    return tuple(
        ExternalTableReference(
            table_id=str(row["table_id"]),
            full_name=f"{row['catalog_name']}.{row['schema_name']}.{row['name']}",
            location=str(Path(row["canonical_location"])),
            available_at_snapshot=Path(row["canonical_location"]).exists(),
        )
        for row in rows
    )


def _external_identity(
    references: tuple[ExternalTableReference, ...],
) -> tuple[tuple[str, str, str], ...]:
    return tuple((item.table_id, item.full_name, item.location) for item in references)


def _external_paths_under_root(
    root: Path,
    external_tables: tuple[ExternalTableReference, ...],
) -> set[Path]:
    excluded: set[Path] = set()
    for item in external_tables:
        location = Path(item.location).resolve(strict=False)
        if root.is_relative_to(location):
            raise WorkspaceSnapshotError(
                f"external table {item.full_name} refers to the workspace or one of its parents"
            )
        if location.is_relative_to(root):
            relative = location.relative_to(root)
            if not relative.parts:
                raise WorkspaceSnapshotError(
                    f"external table {item.full_name} cannot refer to the workspace root"
                )
            excluded.add(relative)
    return excluded


def _checkpoint_metadata(metadata_path: Path) -> None:
    try:
        with sqlite3.connect(metadata_path, timeout=5.0) as connection:
            mode = connection.execute("PRAGMA journal_mode").fetchone()
            if mode is not None and str(mode[0]).casefold() == "wal":
                busy, log_frames, checkpointed = connection.execute(
                    "PRAGMA wal_checkpoint(TRUNCATE)"
                ).fetchone()
                if busy or log_frames != checkpointed:
                    raise WorkspaceSnapshotBusyError(
                        "workspace SQLite WAL could not be checkpointed for a snapshot"
                    )
    except sqlite3.Error as error:
        raise WorkspaceSnapshotError("could not checkpoint workspace metadata") from error


def _reject_unhandled_symlinks(
    root: Path,
    excluded_paths: set[Path],
    *,
    omit_manifest: bool = False,
) -> None:
    for directory, directory_names, file_names in os.walk(root, followlinks=False):
        parent = Path(directory)
        relative_parent = parent.relative_to(root)
        kept_directories: list[str] = []
        for name in directory_names:
            relative = relative_parent / name
            if relative in excluded_paths:
                continue
            candidate = parent / name
            if candidate.is_symlink():
                raise WorkspaceSnapshotError(
                    f"workspace snapshot does not follow symbolic links: {relative}"
                )
            kept_directories.append(name)
        directory_names[:] = kept_directories
        for name in file_names:
            if relative_parent == Path(".") and name == _OWNER_LOCK_NAME:
                continue
            if omit_manifest and relative_parent == Path(".") and name == _MANIFEST_NAME:
                continue
            relative = relative_parent / name
            if relative in excluded_paths:
                continue
            candidate = parent / name
            if candidate.is_symlink():
                raise WorkspaceSnapshotError(
                    f"workspace snapshot does not follow symbolic links: {relative}"
                )


def _copy_filter(
    root: Path,
    excluded_paths: set[Path],
    *,
    omit_manifest: bool,
) -> Callable[[str, list[str]], set[str]]:
    def ignore(directory: str, names: list[str]) -> set[str]:
        relative_parent = Path(directory).relative_to(root)
        excluded = {name for name in names if relative_parent / name in excluded_paths}
        if relative_parent == Path("."):
            excluded.add(_OWNER_LOCK_NAME)
            if omit_manifest:
                excluded.add(_MANIFEST_NAME)
        return excluded

    return ignore


def _read_manifest(snapshot: Path, workspace: Workspace) -> dict[str, object]:
    manifest_path = snapshot / _MANIFEST_NAME
    try:
        manifest_value: object = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise WorkspaceSnapshotCorruptError("snapshot manifest is missing or invalid") from error
    if not isinstance(manifest_value, dict):
        raise WorkspaceSnapshotCorruptError("snapshot manifest must be a JSON object")
    manifest = cast(dict[str, object], manifest_value)
    if (
        manifest.get("format_version") not in {1, _MANIFEST_VERSION}
        or manifest.get("workspace_id") != workspace.workspace_id
        or manifest.get("external_table_policy") != _EXTERNAL_POLICY
    ):
        raise WorkspaceSnapshotCorruptError("snapshot manifest identity or policy is invalid")
    source_path = manifest.get("source_path")
    if not isinstance(source_path, str) or not Path(source_path).is_absolute():
        raise WorkspaceSnapshotCorruptError("snapshot manifest source path is invalid")
    return manifest


def _external_references_from_manifest(
    manifest: dict[str, object],
) -> tuple[ExternalTableReference, ...]:
    values = manifest.get("external_tables")
    if not isinstance(values, list):
        raise WorkspaceSnapshotCorruptError("snapshot external-table inventory is invalid")
    references: list[ExternalTableReference] = []
    for value in values:
        if not isinstance(value, dict):
            raise WorkspaceSnapshotCorruptError("snapshot external-table entry is invalid")
        table_id = value.get("table_id")
        full_name = value.get("full_name")
        location = value.get("location")
        available = value.get("available_at_snapshot")
        if (
            not isinstance(table_id, str)
            or not isinstance(full_name, str)
            or not isinstance(location, str)
            or not isinstance(available, bool)
            or not Path(location).is_absolute()
        ):
            raise WorkspaceSnapshotCorruptError("snapshot external-table entry is invalid")
        references.append(ExternalTableReference(table_id, full_name, location, available))
    return tuple(references)


def _rebase_managed_table_locations(
    copied_workspace: Path,
    *,
    snapshot_root: Path,
    original_workspace: Path,
    restored_workspace: Path,
    manifest: dict[str, object],
) -> None:
    metadata_path = copied_workspace / "metadata.sqlite"
    try:
        with closing(sqlite3.connect(metadata_path, timeout=5.0)) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                "SELECT table_id, canonical_location FROM table_registrations "
                "WHERE ownership = 'managed'"
            ).fetchall()
            inventory = _managed_inventory_from_manifest(manifest)
            if inventory is not None and set(inventory) != {row[0] for row in rows}:
                raise WorkspaceSnapshotCorruptError(
                    "snapshot managed-table inventory does not match its registrations"
                )
            for table_id, stored_location in rows:
                try:
                    canonical_id = str(uuid.UUID(table_id))
                except (TypeError, ValueError, AttributeError) as error:
                    raise WorkspaceSnapshotCorruptError(
                        "managed table registration has an invalid table ID"
                    ) from error
                if canonical_id != table_id:
                    raise WorkspaceSnapshotCorruptError(
                        "managed table registration has a non-canonical table ID"
                    )
                original_location = (original_workspace / "tables" / canonical_id).resolve(
                    strict=False
                )
                if Path(stored_location) != original_location:
                    raise WorkspaceSnapshotCorruptError(
                        f"managed table location is outside its workspace-owned path: {table_id}"
                    )
                copied_table = snapshot_root / "tables" / canonical_id
                if copied_table.is_symlink():
                    raise WorkspaceSnapshotCorruptError(
                        f"managed table snapshot path is a symbolic link: {table_id}"
                    )
                if inventory is None:
                    if original_location.exists() and not copied_table.exists():
                        raise WorkspaceSnapshotCorruptError(
                            f"managed table data is missing from the snapshot: {table_id}"
                        )
                else:
                    materialized, expected_digest = inventory[canonical_id]
                    if materialized:
                        if not copied_table.is_dir():
                            raise WorkspaceSnapshotCorruptError(
                                f"managed table data is missing from the snapshot: {table_id}"
                            )
                        if _tree_sha256(copied_table) != expected_digest:
                            raise WorkspaceSnapshotCorruptError(
                                f"managed table data does not match its snapshot inventory: "
                                f"{table_id}"
                            )
                    elif copied_table.exists():
                        raise WorkspaceSnapshotCorruptError(
                            f"unmaterialized managed table has unexpected snapshot data: {table_id}"
                        )
                new_location = restored_workspace / "tables" / canonical_id
                connection.execute(
                    "UPDATE table_registrations SET canonical_location = ? WHERE table_id = ?",
                    (str(new_location), table_id),
                )
            connection.commit()
    except WorkspaceSnapshotCorruptError:
        raise
    except sqlite3.Error as error:
        raise WorkspaceSnapshotError("could not rebase managed table locations") from error


def _managed_table_inventory(
    source_workspace: Path,
    snapshot_root: Path,
) -> list[dict[str, object]]:
    """Fingerprint copied managed data independently of the source workspace."""
    try:
        with closing(
            sqlite3.connect(source_workspace / "metadata.sqlite", timeout=5.0)
        ) as connection:
            rows = connection.execute(
                "SELECT table_id, canonical_location FROM table_registrations "
                "WHERE ownership = 'managed' ORDER BY table_id"
            ).fetchall()
    except sqlite3.Error as error:
        raise WorkspaceSnapshotError("could not inventory managed table data") from error

    inventory: list[dict[str, object]] = []
    for table_id_value, location_value in rows:
        table_id = str(table_id_value)
        try:
            canonical_id = str(uuid.UUID(table_id))
        except (TypeError, ValueError, AttributeError) as error:
            raise WorkspaceSnapshotCorruptError(
                "managed table registration has an invalid table ID"
            ) from error
        if canonical_id != table_id:
            raise WorkspaceSnapshotCorruptError(
                "managed table registration has a non-canonical table ID"
            )
        source_table = (source_workspace / "tables" / canonical_id).resolve(strict=False)
        if Path(str(location_value)) != source_table:
            raise WorkspaceSnapshotCorruptError(
                f"managed table location is outside its workspace-owned path: {table_id}"
            )
        copied_table = snapshot_root / "tables" / canonical_id
        if source_table.exists():
            if not source_table.is_dir() or not copied_table.is_dir():
                raise WorkspaceSnapshotCorruptError(
                    f"materialized managed table data is missing from the snapshot: {table_id}"
                )
            inventory.append(
                {
                    "table_id": canonical_id,
                    "materialized": True,
                    "sha256": _tree_sha256(copied_table),
                }
            )
        else:
            if copied_table.exists():
                raise WorkspaceSnapshotCorruptError(
                    f"unmaterialized managed table has unexpected snapshot data: {table_id}"
                )
            inventory.append({"table_id": canonical_id, "materialized": False, "sha256": None})
    return inventory


def _managed_inventory_from_manifest(
    manifest: dict[str, object],
) -> dict[str, tuple[bool, str | None]] | None:
    if manifest.get("format_version") == 1:
        return None
    values = manifest.get("managed_tables")
    if not isinstance(values, list):
        raise WorkspaceSnapshotCorruptError("snapshot managed-table inventory is invalid")
    inventory: dict[str, tuple[bool, str | None]] = {}
    for value in values:
        if not isinstance(value, dict):
            raise WorkspaceSnapshotCorruptError("snapshot managed-table entry is invalid")
        table_id = value.get("table_id")
        materialized = value.get("materialized")
        digest = value.get("sha256")
        try:
            canonical_id = str(uuid.UUID(table_id)) if isinstance(table_id, str) else ""
        except (TypeError, ValueError, AttributeError):
            canonical_id = ""
        if (
            canonical_id != table_id
            or type(materialized) is not bool
            or (materialized and (not isinstance(digest, str) or len(digest) != 64))
            or (not materialized and digest is not None)
            or canonical_id in inventory
        ):
            raise WorkspaceSnapshotCorruptError("snapshot managed-table entry is invalid")
        inventory[canonical_id] = (materialized, digest)
    return inventory


def _tree_sha256(root: Path) -> str:
    digest = hashlib.sha256()
    entries = [root, *sorted(root.rglob("*"), key=lambda path: path.relative_to(root).as_posix())]
    for entry in entries:
        if entry.is_symlink():
            raise WorkspaceSnapshotCorruptError("managed table data contains a symbolic link")
        relative = "." if entry == root else entry.relative_to(root).as_posix()
        encoded = relative.encode("utf-8")
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
        if entry.is_dir():
            digest.update(b"D")
            continue
        if not entry.is_file():
            raise WorkspaceSnapshotCorruptError("managed table data contains a special file")
        digest.update(b"F")
        with entry.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                digest.update(len(chunk).to_bytes(8, "big"))
                digest.update(chunk)
    return digest.hexdigest()


def _publish_directory(temporary: Path, target: Path) -> None:
    """Claim a new destination and move metadata last so it appears initialized last."""
    try:
        target.mkdir()
    except FileExistsError as error:
        raise WorkspaceSnapshotError(f"destination path already exists: {target}") from error
    created_entries: list[Path] = []
    try:
        entries = sorted(
            (entry for entry in temporary.iterdir()),
            key=lambda entry: (entry.name == "metadata.sqlite", entry.name),
        )
        for entry in entries:
            destination = target / entry.name
            entry.rename(destination)
            created_entries.append(destination)
        temporary.rmdir()
    except OSError as error:
        for entry in reversed(created_entries):
            if entry.is_dir():
                shutil.rmtree(entry, ignore_errors=True)
            else:
                entry.unlink(missing_ok=True)
        try:
            target.rmdir()
        except OSError:
            pass
        raise WorkspaceSnapshotError("could not publish workspace snapshot") from error
