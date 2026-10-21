"""Instance-scoped SQLite repositories for catalog metadata."""

from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from brickhouse.workspace.context import WorkspaceContext

_IDENTIFIER_CHARS = frozenset("abcdefghijklmnopqrstuvwxyz0123456789_")


class CatalogRepositoryError(RuntimeError):
    """Base error for local catalog metadata operations."""


class NamespaceNotFoundError(CatalogRepositoryError):
    """A catalog or schema does not exist in this workspace."""


class TableNotFoundError(CatalogRepositoryError):
    """A table registration does not exist."""


class TableHasPendingOperationError(CatalogRepositoryError):
    """A table with an unfinished mutation cannot be removed."""

    def __init__(self, operation_id: str) -> None:
        self.operation_id = operation_id
        super().__init__(f"table has unfinished operation {operation_id!r}")


class TableAlreadyExistsError(CatalogRepositoryError):
    """A table name is already registered in the namespace."""


class TableLocationConflictError(CatalogRepositoryError):
    """A physical table location is already registered."""


class NamespaceAlreadyExistsError(CatalogRepositoryError):
    """A catalog or schema name is already present."""


class NamespaceInUseError(CatalogRepositoryError):
    """A namespace still owns registered table metadata."""


class ProtectedNamespaceError(CatalogRepositoryError):
    """A workspace or system namespace cannot be deleted."""


class InvalidTableLocationError(CatalogRepositoryError):
    """A table location is unsupported or violates workspace ownership."""


class TableOwnership(StrEnum):
    MANAGED = "managed"
    EXTERNAL = "external"


@dataclass(frozen=True, slots=True)
class CatalogRecord:
    catalog_id: str
    name: str
    is_default: bool
    comment: str | None = None


@dataclass(frozen=True, slots=True)
class SchemaRecord:
    schema_id: str
    catalog_name: str
    name: str
    is_default: bool
    comment: str | None = None


@dataclass(frozen=True, slots=True)
class TableRegistration:
    table_id: str
    catalog_id: str
    schema_id: str
    catalog_name: str
    schema_name: str
    name: str
    table_format: str
    ownership: TableOwnership
    canonical_location: Path
    created_at: str

    @property
    def full_name(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.name}"


class _Repository:
    def __init__(self, context: WorkspaceContext) -> None:
        self._context = context

    @contextmanager
    def _connection(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        workspace = self._context.workspace
        connection = sqlite3.connect(workspace.metadata_path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            if write:
                connection.execute("BEGIN IMMEDIATE")
            yield connection
            if write:
                connection.commit()
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()


class CatalogRepository(_Repository):
    """Persist and read catalogs for one active workspace context."""

    def list(self) -> list[CatalogRecord]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT catalog_id, name, is_default, comment FROM catalogs ORDER BY name"
            ).fetchall()
        return [
            CatalogRecord(row["catalog_id"], row["name"], bool(row["is_default"]), row["comment"])
            for row in rows
        ]

    def get(self, name: str) -> CatalogRecord:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT catalog_id, name, is_default, comment FROM catalogs WHERE name = ?",
                (name,),
            ).fetchone()
        if row is None:
            raise NamespaceNotFoundError(f"catalog does not exist: {name}")
        return CatalogRecord(
            row["catalog_id"], row["name"], bool(row["is_default"]), row["comment"]
        )

    def create(self, name: str, *, comment: str | None = None) -> CatalogRecord:
        _validate_identifier("catalog", name)
        catalog_id = str(uuid.uuid4())
        with self._connection(write=True) as connection:
            if connection.execute("SELECT 1 FROM catalogs WHERE name = ?", (name,)).fetchone():
                raise NamespaceAlreadyExistsError(f"catalog already exists: {name}")
            connection.execute(
                "INSERT INTO catalogs(catalog_id, name, is_default, comment) VALUES (?, ?, 0, ?)",
                (catalog_id, name, comment),
            )
            connection.executemany(
                "INSERT INTO schemas(schema_id, catalog_name, name, is_default, comment) "
                "VALUES (?, ?, ?, 0, NULL)",
                (
                    (str(uuid.uuid4()), name, "default"),
                    (str(uuid.uuid4()), name, "information_schema"),
                ),
            )
        return CatalogRecord(catalog_id, name, False, comment)

    def delete(self, name: str) -> None:
        with self._connection(write=True) as connection:
            row = connection.execute(
                "SELECT is_default FROM catalogs WHERE name = ?", (name,)
            ).fetchone()
            if row is None:
                raise NamespaceNotFoundError(f"catalog does not exist: {name}")
            if row["is_default"]:
                raise ProtectedNamespaceError(
                    f"the workspace default catalog cannot be deleted: {name}"
                )
            table_count = connection.execute(
                "SELECT count(*) FROM table_registrations WHERE catalog_name = ?", (name,)
            ).fetchone()[0]
            if table_count:
                raise NamespaceInUseError(
                    f"catalog contains {table_count} registered table(s): {name}"
                )
            connection.execute("DELETE FROM schemas WHERE catalog_name = ?", (name,))
            connection.execute("DELETE FROM catalogs WHERE name = ?", (name,))


class SchemaRepository(_Repository):
    """Persist and read schemas for one active workspace context."""

    def list(self, catalog_name: str) -> list[SchemaRecord]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT schema_id, catalog_name, name, is_default, comment FROM schemas "
                "WHERE catalog_name = ? ORDER BY name",
                (catalog_name,),
            ).fetchall()
        if not rows:
            with self._connection() as connection:
                catalog = connection.execute(
                    "SELECT 1 FROM catalogs WHERE name = ?", (catalog_name,)
                ).fetchone()
            if catalog is None:
                raise NamespaceNotFoundError(f"catalog does not exist: {catalog_name}")
        return [
            SchemaRecord(
                row["schema_id"],
                row["catalog_name"],
                row["name"],
                bool(row["is_default"]),
                row["comment"],
            )
            for row in rows
        ]

    def get(self, catalog_name: str, name: str) -> SchemaRecord:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT schema_id, catalog_name, name, is_default, comment FROM schemas "
                "WHERE catalog_name = ? AND name = ?",
                (catalog_name, name),
            ).fetchone()
        if row is None:
            raise NamespaceNotFoundError(f"schema does not exist: {catalog_name}.{name}")
        return SchemaRecord(
            row["schema_id"],
            row["catalog_name"],
            row["name"],
            bool(row["is_default"]),
            row["comment"],
        )

    def create(
        self,
        catalog_name: str,
        name: str,
        *,
        comment: str | None = None,
    ) -> SchemaRecord:
        _validate_identifier("catalog", catalog_name)
        _validate_identifier("schema", name)
        schema_id = str(uuid.uuid4())
        with self._connection(write=True) as connection:
            if (
                connection.execute(
                    "SELECT 1 FROM catalogs WHERE name = ?", (catalog_name,)
                ).fetchone()
                is None
            ):
                raise NamespaceNotFoundError(f"catalog does not exist: {catalog_name}")
            if connection.execute(
                "SELECT 1 FROM schemas WHERE catalog_name = ? AND name = ?",
                (catalog_name, name),
            ).fetchone():
                raise NamespaceAlreadyExistsError(f"schema already exists: {catalog_name}.{name}")
            connection.execute(
                "INSERT INTO schemas(schema_id, catalog_name, name, is_default, comment) "
                "VALUES (?, ?, ?, 0, ?)",
                (schema_id, catalog_name, name, comment),
            )
        return SchemaRecord(schema_id, catalog_name, name, False, comment)

    def delete(self, catalog_name: str, name: str) -> None:
        with self._connection(write=True) as connection:
            row = connection.execute(
                "SELECT is_default FROM schemas WHERE catalog_name = ? AND name = ?",
                (catalog_name, name),
            ).fetchone()
            if row is None:
                catalog = connection.execute(
                    "SELECT 1 FROM catalogs WHERE name = ?", (catalog_name,)
                ).fetchone()
                if catalog is None:
                    raise NamespaceNotFoundError(f"catalog does not exist: {catalog_name}")
                raise NamespaceNotFoundError(f"schema does not exist: {catalog_name}.{name}")
            if row["is_default"] or name == "information_schema":
                raise ProtectedNamespaceError(
                    f"system or workspace default schema cannot be deleted: {catalog_name}.{name}"
                )
            table_count = connection.execute(
                "SELECT count(*) FROM table_registrations "
                "WHERE catalog_name = ? AND schema_name = ?",
                (catalog_name, name),
            ).fetchone()[0]
            if table_count:
                raise NamespaceInUseError(
                    f"schema contains {table_count} registered table(s): {catalog_name}.{name}"
                )
            connection.execute(
                "DELETE FROM schemas WHERE catalog_name = ? AND name = ?",
                (catalog_name, name),
            )


class TableRepository(_Repository):
    """Persist table IDs, ownership, and canonical local locations."""

    def create_managed(
        self,
        catalog_name: str,
        schema_name: str,
        name: str,
    ) -> TableRegistration:
        table_id = str(uuid.uuid4())
        managed_root = self._managed_root()
        location = (managed_root / table_id).resolve(strict=False)
        if not location.is_relative_to(managed_root):
            raise InvalidTableLocationError("managed table location escapes the workspace root")
        return self._register(
            table_id,
            catalog_name,
            schema_name,
            name,
            TableOwnership.MANAGED,
            location,
        )

    def register_external(
        self,
        catalog_name: str,
        schema_name: str,
        name: str,
        location: str | Path,
    ) -> TableRegistration:
        canonical_location = self._canonicalize_location(location)
        if canonical_location.is_relative_to(self._managed_root()):
            raise InvalidTableLocationError(
                "external tables cannot be registered inside the workspace managed-table root"
            )
        return self._register(
            str(uuid.uuid4()),
            catalog_name,
            schema_name,
            name,
            TableOwnership.EXTERNAL,
            canonical_location,
        )

    def get(self, catalog_name: str, schema_name: str, name: str) -> TableRegistration:
        with self._connection() as connection:
            row = connection.execute(
                f"{_TABLE_SELECT} WHERE table_registrations.catalog_name = ? "
                "AND table_registrations.schema_name = ? AND table_registrations.name = ?",
                (catalog_name, schema_name, name),
            ).fetchone()
        if row is None:
            raise TableNotFoundError(f"table does not exist: {catalog_name}.{schema_name}.{name}")
        return self._table_record(row)

    def get_by_id(self, table_id: str) -> TableRegistration:
        with self._connection() as connection:
            row = connection.execute(
                f"{_TABLE_SELECT} WHERE table_registrations.table_id = ?", (table_id,)
            ).fetchone()
        if row is None:
            raise TableNotFoundError(f"table ID does not exist: {table_id}")
        return self._table_record(row)

    def get_by_location(self, location: str | Path) -> TableRegistration:
        canonical_location = self._canonicalize_location(location)
        with self._connection() as connection:
            row = connection.execute(
                f"{_TABLE_SELECT} WHERE table_registrations.canonical_location = ?",
                (str(canonical_location),),
            ).fetchone()
        if row is None:
            raise TableNotFoundError(f"table location is not registered: {canonical_location}")
        return self._table_record(row)

    def list(self, catalog_name: str, schema_name: str) -> list[TableRegistration]:
        with self._connection() as connection:
            rows = connection.execute(
                f"{_TABLE_SELECT} WHERE table_registrations.catalog_name = ? "
                "AND table_registrations.schema_name = ? ORDER BY table_registrations.name",
                (catalog_name, schema_name),
            ).fetchall()
        return [self._table_record(row) for row in rows]

    def _remove_registration(self, table_id: str) -> None:
        """Remove one registration and its scoped mutation history atomically."""
        with self._connection(write=True) as connection:
            exists = connection.execute(
                "SELECT 1 FROM table_registrations WHERE table_id = ?", (table_id,)
            ).fetchone()
            if exists is None:
                raise TableNotFoundError(f"table ID does not exist: {table_id}")
            pending = connection.execute(
                "SELECT operation_id FROM table_operation_journal "
                "WHERE table_id = ? AND state != 'committed' LIMIT 1",
                (table_id,),
            ).fetchone()
            if pending is not None:
                raise TableHasPendingOperationError(pending["operation_id"])
            # The journal has a RESTRICT foreign key. Its history is scoped to this
            # table identity and is removed in the same transaction as the registration.
            connection.execute(
                "DELETE FROM table_operation_journal WHERE table_id = ?", (table_id,)
            )
            connection.execute("DELETE FROM table_registrations WHERE table_id = ?", (table_id,))

    def _register(
        self,
        table_id: str,
        catalog_name: str,
        schema_name: str,
        name: str,
        ownership: TableOwnership,
        canonical_location: Path,
    ) -> TableRegistration:
        _validate_identifier("catalog", catalog_name)
        _validate_identifier("schema", schema_name)
        _validate_identifier("table", name)
        created_at = datetime.now(UTC).isoformat()
        with self._connection(write=True) as connection:
            namespace = connection.execute(
                "SELECT catalogs.catalog_id, schemas.schema_id FROM schemas "
                "JOIN catalogs ON catalogs.name = schemas.catalog_name "
                "WHERE schemas.catalog_name = ? AND schemas.name = ?",
                (catalog_name, schema_name),
            ).fetchone()
            if namespace is None:
                raise NamespaceNotFoundError(f"schema does not exist: {catalog_name}.{schema_name}")
            same_name = connection.execute(
                "SELECT table_id FROM table_registrations "
                "WHERE catalog_name = ? AND schema_name = ? AND name = ?",
                (catalog_name, schema_name, name),
            ).fetchone()
            if same_name is not None:
                raise TableAlreadyExistsError(
                    f"table already exists: {catalog_name}.{schema_name}.{name}"
                )
            same_location = connection.execute(
                "SELECT catalog_name, schema_name, name FROM table_registrations "
                "WHERE canonical_location = ?",
                (str(canonical_location),),
            ).fetchone()
            if same_location is not None:
                existing_name = (
                    f"{same_location['catalog_name']}.{same_location['schema_name']}"
                    f".{same_location['name']}"
                )
                raise TableLocationConflictError(
                    f"location is already registered to table {existing_name}"
                )
            connection.execute(
                "INSERT INTO table_registrations "
                "(table_id, catalog_name, schema_name, name, table_format, ownership, "
                "canonical_location, created_at) VALUES (?, ?, ?, ?, 'delta', ?, ?, ?)",
                (
                    table_id,
                    catalog_name,
                    schema_name,
                    name,
                    ownership.value,
                    str(canonical_location),
                    created_at,
                ),
            )
        return TableRegistration(
            table_id=table_id,
            catalog_id=namespace["catalog_id"],
            schema_id=namespace["schema_id"],
            catalog_name=catalog_name,
            schema_name=schema_name,
            name=name,
            table_format="delta",
            ownership=ownership,
            canonical_location=canonical_location,
            created_at=created_at,
        )

    def _managed_root(self) -> Path:
        workspace_path = self._context.workspace.path
        path = workspace_path / "tables"
        if path.is_symlink():
            raise InvalidTableLocationError("workspace managed-table directory cannot be a symlink")
        managed_root = path.resolve(strict=True)
        if managed_root.parent != workspace_path:
            raise InvalidTableLocationError(
                "workspace managed-table directory must not resolve outside its root"
            )
        return managed_root

    def _canonicalize_location(self, location: str | Path) -> Path:
        raw_location = str(location)
        if "://" in raw_location or raw_location.startswith("dbfs:"):
            raise InvalidTableLocationError("only local filesystem table locations are supported")
        try:
            return Path(location).expanduser().resolve(strict=False)
        except (OSError, RuntimeError) as error:
            raise InvalidTableLocationError("table location cannot be resolved") from error

    @staticmethod
    def _table_record(row: sqlite3.Row) -> TableRegistration:
        return TableRegistration(
            table_id=row["table_id"],
            catalog_id=row["catalog_id"],
            schema_id=row["schema_id"],
            catalog_name=row["catalog_name"],
            schema_name=row["schema_name"],
            name=row["name"],
            table_format=row["table_format"],
            ownership=TableOwnership(row["ownership"]),
            canonical_location=Path(row["canonical_location"]),
            created_at=row["created_at"],
        )


_TABLE_SELECT = (
    "SELECT table_registrations.*, catalogs.catalog_id AS catalog_id, "
    "schemas.schema_id AS schema_id FROM table_registrations "
    "JOIN catalogs ON catalogs.name = table_registrations.catalog_name "
    "JOIN schemas ON schemas.catalog_name = table_registrations.catalog_name "
    "AND schemas.name = table_registrations.schema_name"
)


def _validate_identifier(kind: str, value: str) -> None:
    if (
        not value
        or value[0] not in "abcdefghijklmnopqrstuvwxyz_"
        or any(character not in _IDENTIFIER_CHARS for character in value)
    ):
        raise CatalogRepositoryError(f"{kind} name must be a lowercase unquoted identifier")
