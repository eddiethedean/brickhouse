from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from brickhouse.catalog import (
    InvalidTableLocationError,
    TableNotFoundError,
    TableOwnership,
)
from brickhouse.tables import (
    DeltaRsTableService,
    PendingTableOperationError,
    TableHandle,
)
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace

ROOT = Path(__file__).resolve().parents[2]


def test_drop_external_table_removes_metadata_and_preserves_files(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    external_location = tmp_path / "external"
    external_location.mkdir()
    sentinel = external_location / "keep.txt"
    sentinel.write_text("user data", encoding="utf-8")

    with WorkspaceContext.local(workspace.path) as context:
        registration = context.tables.register_external(
            "main", "default", "source", external_location
        )
        assert registration.ownership is TableOwnership.EXTERNAL
        service = DeltaRsTableService(context)
        service.drop_table(TableHandle.from_registration(registration))

        with pytest.raises(TableNotFoundError):
            context.tables.get("main", "default", "source")

    assert sentinel.read_text(encoding="utf-8") == "user data"
    assert external_location.is_dir()


def test_drop_managed_table_removes_only_registered_owned_directory(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    outside = tmp_path / "outside-data"
    outside.mkdir()
    outside_file = outside / "keep.txt"
    outside_file.write_text("outside", encoding="utf-8")

    with WorkspaceContext.local(workspace.path) as context:
        registration = context.tables.create_managed("main", "default", "orders")
        managed_directory = registration.canonical_location
        managed_directory.mkdir()
        (managed_directory / "_delta_log").mkdir()
        (managed_directory / "part-000.parquet").write_bytes(b"table bytes")
        service = DeltaRsTableService(context)
        service.drop_table(TableHandle.from_registration(registration))

        assert not managed_directory.exists()
        with pytest.raises(TableNotFoundError):
            context.tables.get_by_id(registration.table_id)

    assert outside_file.read_text(encoding="utf-8") == "outside"


def test_managed_drop_rejects_a_table_path_replaced_by_symlink(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    outside = tmp_path / "outside-data"
    outside.mkdir()
    sentinel = outside / "keep.txt"
    sentinel.write_text("outside", encoding="utf-8")

    with WorkspaceContext.local(workspace.path) as context:
        registration = context.tables.create_managed("main", "default", "orders")
        registration.canonical_location.symlink_to(outside, target_is_directory=True)
        service = DeltaRsTableService(context)
        with pytest.raises(InvalidTableLocationError, match="symlink"):
            service.drop_table(TableHandle.from_registration(registration))
        assert context.tables.get_by_id(registration.table_id) == registration

    assert sentinel.read_text(encoding="utf-8") == "outside"


def test_drop_waits_for_unfinished_table_operation(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        registration = context.tables.create_managed("main", "default", "orders")
        handle = TableHandle.from_registration(registration)
        registration.canonical_location.mkdir()
        context.table_operations.prepare(
            table_id=registration.table_id,
            operation_id="unfinished-write",
            fingerprint="a" * 64,
            mode="append",
            schema_fingerprint="b" * 64,
            input_sha256="c" * 64,
            base_version=0,
            row_count=1,
        )
        service = DeltaRsTableService(context)

        with pytest.raises(PendingTableOperationError, match="unfinished-write"):
            service.drop_table(handle)

        assert registration.canonical_location.is_dir()
        assert context.tables.get_by_id(registration.table_id) == registration


@pytest.mark.skipif(os.name != "posix", reason="the workspace owner lock is POSIX-only")
def test_drop_recovers_after_process_dies_before_metadata_removal(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        registration = context.tables.create_managed("main", "default", "orders")
        registration.canonical_location.mkdir()
        (registration.canonical_location / "part.parquet").write_bytes(b"table bytes")

    deleted_marker = tmp_path / "physical-cleanup-complete"
    child_code = r"""
import sys
import time
from pathlib import Path
from brickhouse.tables import DeltaRsTableService, TableHandle
from brickhouse.workspace.context import WorkspaceContext

with WorkspaceContext.local(sys.argv[1]) as context:
    registration = context.tables.get_by_id(sys.argv[2])
    service = DeltaRsTableService(context)
    def stop_before_metadata_remove(table_id):
        Path(sys.argv[3]).write_text("removed", encoding="utf-8")
        while True:
            time.sleep(60)
    context.tables._remove_registration = stop_before_metadata_remove
    service.drop_table(TableHandle.from_registration(registration))
"""
    environment = {
        key: value for key, value in os.environ.items() if not key.startswith("DATABRICKS_")
    }
    environment["PYTHONPATH"] = str(ROOT / "src")
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            child_code,
            str(workspace.path),
            registration.table_id,
            str(deleted_marker),
        ],
        cwd=ROOT,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 15
        while not deleted_marker.exists() and child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.02)
        assert deleted_marker.exists(), "child did not reach the interrupted cleanup point"
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=10)
    assert child.returncode == -signal.SIGKILL
    assert not registration.canonical_location.exists()

    with WorkspaceContext.local(workspace.path) as recovered_context:
        recovered_registration = recovered_context.tables.get_by_id(registration.table_id)
        service = DeltaRsTableService(recovered_context)
        service.drop_table(TableHandle.from_registration(recovered_registration))
        with pytest.raises(TableNotFoundError):
            recovered_context.tables.get_by_id(registration.table_id)
