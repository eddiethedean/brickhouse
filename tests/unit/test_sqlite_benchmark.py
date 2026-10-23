from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from scripts.benchmark_sqlite import MetadataExperiment
from scripts.summarize_sqlite_benchmark import analyze


def seed_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE sql_statements(state TEXT NOT NULL)")
        connection.execute("CREATE TABLE seed_values(value TEXT NOT NULL)")
        connection.execute("INSERT INTO seed_values VALUES ('seeded value')")
        connection.commit()
    finally:
        connection.close()


@pytest.mark.parametrize("storage", ["disk", "disk-shared", "memory"])
def test_real_sqlite_handles_share_seed_and_have_declared_lifetime(
    tmp_path: Path, storage: str
) -> None:
    path = tmp_path / "metadata.sqlite"
    seed_database(path)
    original = sqlite3.connect
    experiment = MetadataExperiment(path, storage)
    with experiment:
        first = sqlite3.connect(path)
        try:
            assert first.execute("SELECT value FROM seed_values").fetchone() == ("seeded value",)
            first.execute("BEGIN IMMEDIATE")
            first.execute("INSERT INTO sql_statements VALUES ('SUCCEEDED')")
            first.commit()
        finally:
            first.close()
        # A newly opened handle must observe data committed through the closed one.
        second = sqlite3.connect(path)
        try:
            assert second.execute("SELECT state FROM sql_statements").fetchall() == [("SUCCEEDED",)]
        finally:
            second.close()
    assert sqlite3.connect == original
    evidence = experiment.evidence
    assert evidence["fresh_thread_observations"][0] == evidence["fresh_thread_observations"][1]
    assert evidence["disk_statement_count"] == (0 if storage == "memory" else 1)
    assert (evidence["seed_sha256"] == evidence["seed_after_sha256"]) == (storage == "memory")
    if storage == "memory":
        assert evidence["objects_after_last_close"] == 0
        assert evidence["database_list"] == [(0, "main", "")]


@pytest.mark.parametrize("storage", ["disk-shared", "memory"])
def test_redirected_read_only_connections_still_reject_writes(tmp_path: Path, storage: str) -> None:
    path = tmp_path / "metadata.sqlite"
    seed_database(path)
    with MetadataExperiment(path, storage):
        connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
        try:
            assert connection.execute("SELECT value FROM seed_values").fetchone() == (
                "seeded value",
            )
            with pytest.raises(sqlite3.OperationalError, match="readonly"):
                connection.execute("INSERT INTO sql_statements VALUES ('SUCCEEDED')")
        finally:
            connection.close()


def test_memory_redirect_is_scoped_to_the_exact_metadata_file_and_preserves_factory(
    tmp_path: Path,
) -> None:
    path = tmp_path / "metadata.sqlite"
    other = tmp_path / "unrelated.sqlite"
    seed_database(path)

    class FactoryConnection(sqlite3.Connection):
        pass

    with MetadataExperiment(path, "memory"):
        connection = sqlite3.connect(path, factory=FactoryConnection)
        try:
            assert isinstance(connection, FactoryConnection)
            assert connection.execute("PRAGMA read_uncommitted").fetchone()[0] == 0
        finally:
            connection.close()
        connection = sqlite3.connect(other)
        try:
            connection.execute("CREATE TABLE unrelated(value INTEGER)")
            connection.commit()
        finally:
            connection.close()
    assert other.is_file()
    connection = sqlite3.connect(other)
    try:
        assert connection.execute("PRAGMA database_list").fetchone()[2] == str(other)
    finally:
        connection.close()


def test_retained_memory_experiment_recomputes_every_sample_and_lifetime_proof(
    assert_report_matches: Callable[[Any, Any], None],
) -> None:
    root = Path(__file__).resolve().parents[2] / "artifacts/benchmarks/2026-10-22-sqlite-memory"
    actual = analyze(root)
    published = json.loads((root / "analysis.json").read_text())
    published.pop("analysis_tool_sha256")
    assert_report_matches(actual, published)
    assert actual["trial_count"] == 18
    assert actual["verified_queries"] == 990
    assert actual["timed_queries"] == 900
    for row in actual["comparisons"]:
        assert row["sqlite_calls_per_request"]["sqlite_connect"] == 34
        assert row["sqlite_calls_per_request"]["sqlite_execute"] == 114
        assert row["sqlite_calls_per_request"]["sqlite_commit"] == 10


@pytest.mark.parametrize("flag", ["fixture_unchanged", "temporary_state_removed"])
def test_storage_summary_rejects_failed_fixture_or_cleanup(tmp_path: Path, flag: str) -> None:
    report = {"configuration": {}, "fixture_unchanged": True, "temporary_state_removed": True}
    report[flag] = False
    (tmp_path / "results.json").write_text(json.dumps(report))
    with pytest.raises(ValueError, match="Fixture or cleanup"):
        analyze(tmp_path)
