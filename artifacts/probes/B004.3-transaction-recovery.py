"""Probe delta-rs transaction identity and process-crash recovery locally.

Run only in the isolated development environment that pins the versions named
in the conformance report. This script creates temporary local files only.
"""

from __future__ import annotations

import hashlib
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import pyarrow as pa
from deltalake import CommitProperties, DeltaTable, Transaction, write_deltalake


def _properties(
    app_id: str,
    transaction_version: int,
    operation_id: str,
    payload_hash: str,
) -> CommitProperties:
    return CommitProperties(
        custom_metadata={
            "brickhouse_operation_id": operation_id,
            "brickhouse_payload_sha256": payload_hash,
        },
        app_transactions=[Transaction(app_id, transaction_version)],
    )


def _child_append(
    table_path: Path,
    app_id: str,
    transaction_version: int,
    operation_id: str,
    payload_hash: str,
    marker: Path,
) -> None:
    write_deltalake(
        str(table_path),
        pa.table({"id": [101]}),
        mode="append",
        commit_properties=_properties(app_id, transaction_version, operation_id, payload_hash),
    )
    marker.write_text("commit returned", encoding="utf-8")
    # The parent kills this process after seeing the marker, before it can
    # finalize the SQLite operation record.
    while True:
        time.sleep(1)


def _wait_for_marker(process: subprocess.Popen[bytes], marker: Path) -> None:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if marker.exists():
            return
        if process.poll() is not None:
            raise RuntimeError(
                f"writer process exited before the commit marker: {process.returncode}"
            )
        time.sleep(0.02)
    process.kill()
    process.wait(timeout=5)
    raise TimeoutError("writer process did not finish its Delta commit")


def _read_ids_with_sparkless(table_path: Path) -> list[int]:
    from sparkless.sql import SparkSession

    session = SparkSession.builder.appName("b0043-transaction-recovery").getOrCreate()
    try:
        rows = session.read.format("delta").load(str(table_path)).collect()
        return sorted(int(row[0]) for row in rows)
    finally:
        session.stop()


def _probe_unsafe_raw_retry(root: Path) -> None:
    table_path = root / "unsafe-retry-table"
    app_id = f"brickhouse-probe-unsafe-{uuid.uuid4().hex}"
    operation_id = uuid.uuid4().hex
    payload_hash = hashlib.sha256(b"[101]").hexdigest()
    write_deltalake(str(table_path), pa.table({"id": [0]}))
    properties = _properties(app_id, 0, operation_id, payload_hash)

    write_deltalake(
        str(table_path), pa.table({"id": [101]}), mode="append", commit_properties=properties
    )
    first = DeltaTable(str(table_path))
    assert first.transaction_version(app_id) == 0
    first.create_checkpoint()
    checkpointed = DeltaTable(str(table_path))
    assert checkpointed.transaction_version(app_id) == 0
    version_before_retry = checkpointed.version()

    # delta-rs records the SetTransaction action but does not suppress an
    # application replay on its own. A caller must reconcile before retrying.
    write_deltalake(
        str(table_path), pa.table({"id": [101]}), mode="append", commit_properties=properties
    )
    after_retry = DeltaTable(str(table_path))
    ids = after_retry.to_pyarrow_table().column("id").to_pylist()
    assert after_retry.version() == version_before_retry + 1
    assert ids.count(101) == 2
    print(
        "raw duplicate app transaction replay created a new commit and duplicate rows; "
        "the provider call is not an idempotency guard"
    )


def _probe_crash_reconciliation(root: Path) -> None:
    table_path = root / "recovery-table"
    database_path = root / "metadata.sqlite"
    marker = root / "child-commit-returned"
    workspace_id = uuid.uuid4().hex
    table_id = uuid.uuid4().hex
    app_id = f"brickhouse:{workspace_id}:{table_id}"
    operation_id = uuid.uuid4().hex
    transaction_version = 0
    payload_hash = hashlib.sha256(b"[101]").hexdigest()

    write_deltalake(str(table_path), pa.table({"id": [0]}))
    connection = sqlite3.connect(database_path)
    try:
        connection.execute(
            "CREATE TABLE operation_journal ("
            "operation_id TEXT PRIMARY KEY, payload_sha256 TEXT NOT NULL, "
            "delta_app_id TEXT NOT NULL, delta_txn_version INTEGER NOT NULL, "
            "state TEXT NOT NULL, delta_version INTEGER)"
        )
        connection.execute(
            "INSERT INTO operation_journal VALUES (?, ?, ?, ?, 'COMMITTING', NULL)",
            (operation_id, payload_hash, app_id, transaction_version),
        )
        connection.commit()
    finally:
        connection.close()

    child = subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--child",
            str(table_path),
            app_id,
            str(transaction_version),
            operation_id,
            payload_hash,
            str(marker),
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_for_marker(child, marker)
        child.send_signal(signal.SIGKILL)
        child.wait(timeout=5)
        assert child.returncode == -signal.SIGKILL
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=5)
    if child.stderr is not None:
        child.stderr.close()

    # Simulate startup reconciliation in a fresh provider object and SQLite
    # connection. The Delta commit is visible while the journal is still pending.
    table = DeltaTable(str(table_path))
    observed_transaction = table.transaction_version(app_id)
    assert observed_transaction == transaction_version
    assert table.version() == 1
    assert table.to_pyarrow_table().column("id").to_pylist().count(101) == 1
    matching_history = [
        entry for entry in table.history() if entry.get("brickhouse_operation_id") == operation_id
    ]
    assert len(matching_history) == 1
    assert matching_history[0]["version"] == 1

    connection = sqlite3.connect(database_path)
    try:
        row = connection.execute(
            "SELECT payload_sha256, delta_txn_version, state "
            "FROM operation_journal WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
        assert row == (payload_hash, transaction_version, "COMMITTING")
        if observed_transaction == transaction_version:
            connection.execute(
                "UPDATE operation_journal SET state = 'COMMITTED', delta_version = ? "
                "WHERE operation_id = ? AND payload_sha256 = ?",
                (table.version(), operation_id, payload_hash),
            )
        connection.commit()
        finalized = connection.execute(
            "SELECT state, delta_version FROM operation_journal WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()
        assert finalized == ("COMMITTED", 1)
    finally:
        connection.close()

    # A retry consults the durable transaction version first and returns the
    # already-committed result without issuing a second append.
    before_retry = DeltaTable(str(table_path))
    if before_retry.transaction_version(app_id) == transaction_version:
        retry_result = "already_committed"
    else:
        retry_result = "uncertain"
    assert retry_result == "already_committed"
    after_retry = DeltaTable(str(table_path))
    assert after_retry.version() == 1
    assert after_retry.to_pyarrow_table().column("id").to_pylist().count(101) == 1

    after_retry.create_checkpoint()
    checkpointed = DeltaTable(str(table_path))
    assert checkpointed.transaction_version(app_id) == transaction_version
    assert _read_ids_with_sparkless(table_path) == [0, 101]
    protocol = checkpointed.protocol()
    print(
        "SIGKILL after Delta commit: startup reconciliation finalized the journal "
        "from transaction version 0 and commit history version 1; guarded replay added "
        "no row; checkpoint preserved the transaction identity; Sparkless read [0, 101]"
    )
    print(f"selected Delta protocol versions: {protocol!r}")


def main() -> None:
    import deltalake
    import sparkless

    print(f"Python Delta provider: deltalake {deltalake.__version__}")
    print(f"Sparkless reader: sparkless {sparkless.__version__}")
    with tempfile.TemporaryDirectory(prefix="brickhouse-b0043-") as directory:
        root = Path(directory)
        _probe_unsafe_raw_retry(root)
        _probe_crash_reconciliation(root)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--child":
        _, _, table, app_id, version, operation_id, payload_hash, marker = sys.argv
        _child_append(Path(table), app_id, int(version), operation_id, payload_hash, Path(marker))
    else:
        main()
