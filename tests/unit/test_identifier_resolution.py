from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path

import pytest

from brickhouse import Workspace
from brickhouse.catalog import (
    AmbiguousIdentifierError,
    InvalidIdentifierError,
    NamespaceNotFoundError,
    TableNotFoundError,
    parse_identifier,
)
from brickhouse.workspace.config import WorkspaceConfig
from brickhouse.workspace.lifecycle import initialize_workspace


def test_parser_preserves_quoted_parts_dots_and_escaped_backticks() -> None:
    parsed = parse_identifier("  `Main` . `schema.with.dot` . `a``tick`  ")

    assert parsed.values == ("Main", "schema.with.dot", "a`tick")
    assert all(part.quoted for part in parsed.parts)
    assert parsed.parts[0].comparison_key == "main"


@pytest.mark.parametrize(
    "identifier",
    ("", ".orders", "orders.", "main..orders", "`unterminated", "a`b", "a-b", "a.b.c.d"),
)
def test_parser_rejects_malformed_or_unsupported_identifiers(identifier: str) -> None:
    with pytest.raises(InvalidIdentifierError):
        parse_identifier(identifier)


def test_session_resolves_one_two_and_three_part_names_without_prefix_stripping(
    tmp_path: Path,
) -> None:
    descriptor = initialize_workspace(tmp_path / "workspace")
    with Workspace.local(descriptor.path) as context:
        table = context.tables.create_managed("main", "default", "orders")
        namespace = context.new_namespace_state()

        assert namespace.resolve_schema("default") == context.schemas.get("main", "default")
        assert namespace.resolve_schema("main.default") == context.schemas.get("main", "default")
        assert namespace.resolve_table("orders") == table
        assert namespace.resolve_table("default.orders") == table
        assert namespace.resolve_table("`MAIN`.`DEFAULT`.`Orders`") == table
        with pytest.raises(NamespaceNotFoundError, match="catalog does not exist: missing"):
            namespace.resolve_table("missing.default.orders")


def test_namespace_state_starts_from_workspace_configured_defaults(tmp_path: Path) -> None:
    descriptor = initialize_workspace(
        tmp_path / "workspace",
        config=WorkspaceConfig(default_catalog="demo", default_schema="analytics"),
    )

    with Workspace.local(descriptor.path) as context:
        namespace = context.new_namespace_state()

    assert (namespace.current_catalog, namespace.current_schema) == ("demo", "analytics")


def test_namespace_states_are_independent_and_failed_changes_are_atomic(
    tmp_path: Path,
) -> None:
    descriptor = initialize_workspace(tmp_path / "workspace")
    with sqlite3.connect(descriptor.metadata_path) as connection:
        connection.execute(
            "INSERT INTO schemas(schema_id, catalog_name, name, is_default) VALUES (?, ?, ?, 0)",
            (str(uuid.uuid4()), "main", "analytics"),
        )
        connection.execute(
            "INSERT INTO catalogs(catalog_id, name, is_default) VALUES (?, ?, 0)",
            (str(uuid.uuid4()), "warehouse"),
        )
        connection.execute(
            "INSERT INTO schemas(schema_id, catalog_name, name, is_default) VALUES (?, ?, ?, 0)",
            (str(uuid.uuid4()), "warehouse", "default"),
        )

    with Workspace.local(descriptor.path) as context:
        first = context.new_namespace_state()
        second = context.new_namespace_state()
        first.use_schema("analytics")
        assert (first.current_catalog, first.current_schema) == ("main", "analytics")
        assert (second.current_catalog, second.current_schema) == ("main", "default")

        first.use_catalog("WAREHOUSE")
        assert (first.current_catalog, first.current_schema) == ("warehouse", "default")
        assert (second.current_catalog, second.current_schema) == ("main", "default")
        first.use_schema("main.analytics")
        assert (first.current_catalog, first.current_schema) == ("main", "analytics")

        with pytest.raises(NamespaceNotFoundError, match="schema does not exist"):
            first.use_schema("main.missing")
        assert (first.current_catalog, first.current_schema) == ("main", "analytics")


def test_missing_and_case_ambiguous_table_targets_are_explicit(tmp_path: Path) -> None:
    descriptor = initialize_workspace(tmp_path / "workspace")
    with Workspace.local(descriptor.path) as context:
        context.tables.create_managed("main", "default", "orders")
        namespace = context.new_namespace_state()
        with pytest.raises(TableNotFoundError, match="main.default.missing"):
            namespace.resolve_table("missing")

        with sqlite3.connect(descriptor.metadata_path) as connection:
            connection.execute(
                "INSERT INTO table_registrations "
                "(table_id, catalog_name, schema_name, name, table_format, ownership, "
                "canonical_location, created_at) VALUES (?, ?, ?, ?, 'delta', 'external', ?, ?)",
                (
                    str(uuid.uuid4()),
                    "main",
                    "default",
                    "ORDERS",
                    str((tmp_path / "external-orders").resolve()),
                    "2026-10-19T00:00:00+00:00",
                ),
            )

        with pytest.raises(AmbiguousIdentifierError, match="matches multiple names"):
            namespace.resolve_table("orders")
