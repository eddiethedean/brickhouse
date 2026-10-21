"""Spark/Databricks-style multipart identifier parsing and namespace resolution."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from brickhouse.catalog.repository import (
    CatalogRecord,
    CatalogRepositoryError,
    NamespaceNotFoundError,
    SchemaRecord,
    TableNotFoundError,
    TableRegistration,
)

if TYPE_CHECKING:
    from brickhouse.workspace.context import WorkspaceContext

_REGULAR_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class InvalidIdentifierError(CatalogRepositoryError, ValueError):
    """An identifier is malformed or has an unsupported number of parts."""


class AmbiguousIdentifierError(CatalogRepositoryError):
    """An identifier matches more than one stored object under profile rules."""


@dataclass(frozen=True, slots=True)
class IdentifierPart:
    """One parsed name component, retaining whether it was backtick-delimited."""

    value: str
    quoted: bool

    @property
    def comparison_key(self) -> str:
        """Return the case-insensitive lookup key used by the initial profile."""
        return self.value.casefold()


@dataclass(frozen=True, slots=True)
class ParsedIdentifier:
    """A one-to-three-part identifier in the initial local catalog profile."""

    parts: tuple[IdentifierPart, ...]

    @property
    def values(self) -> tuple[str, ...]:
        return tuple(part.value for part in self.parts)


def parse_identifier(identifier: str) -> ParsedIdentifier:
    """Parse regular and backtick-delimited identifier parts.

    A dot separates parts only outside backticks. Two adjacent backticks inside
    a quoted part represent one literal backtick. The parser preserves spelling;
    resolution applies the profile's case-insensitive name matching separately.
    """
    if not isinstance(identifier, str) or not identifier.strip():
        raise InvalidIdentifierError("identifier must contain one to three nonempty parts")

    text = identifier.strip()
    parts: list[IdentifierPart] = []
    position = 0
    while position < len(text):
        while position < len(text) and text[position].isspace():
            position += 1
        if position == len(text):
            raise InvalidIdentifierError("identifier cannot end after a separator")

        if text[position] == "`":
            position += 1
            value: list[str] = []
            closed = False
            while position < len(text):
                character = text[position]
                if character == "`":
                    if position + 1 < len(text) and text[position + 1] == "`":
                        value.append("`")
                        position += 2
                        continue
                    position += 1
                    closed = True
                    break
                value.append(character)
                position += 1
            if not closed:
                raise InvalidIdentifierError(
                    "quoted identifier part is missing its closing backtick"
                )
            part_value = "".join(value)
            if not part_value:
                raise InvalidIdentifierError("identifier parts cannot be empty")
            if any(ord(character) < 32 or ord(character) == 127 for character in part_value):
                raise InvalidIdentifierError(
                    "identifier parts cannot contain ASCII control characters"
                )
            parts.append(IdentifierPart(part_value, quoted=True))
            while position < len(text) and text[position].isspace():
                position += 1
            if position < len(text) and text[position] != ".":
                raise InvalidIdentifierError("unexpected text after quoted identifier part")
        else:
            start = position
            while position < len(text) and text[position] != ".":
                if text[position] == "`":
                    raise InvalidIdentifierError(
                        "backticks must delimit a complete identifier part"
                    )
                position += 1
            part_value = text[start:position].strip()
            if _REGULAR_IDENTIFIER.fullmatch(part_value) is None:
                raise InvalidIdentifierError(
                    "unquoted identifier parts must use letters, digits, and underscores"
                )
            parts.append(IdentifierPart(part_value, quoted=False))

        if len(parts) > 3:
            raise InvalidIdentifierError("identifiers may contain at most three parts")
        if position == len(text):
            break
        if text[position] != ".":
            raise InvalidIdentifierError("identifier parts must be separated by dots")
        position += 1
        if position == len(text):
            raise InvalidIdentifierError("identifier cannot end after a separator")

    if not 1 <= len(parts) <= 3:
        raise InvalidIdentifierError("identifier must contain one to three parts")
    return ParsedIdentifier(tuple(parts))


class IdentifierResolver:
    """Create independent namespace states over one active workspace context."""

    def __init__(self, context: WorkspaceContext) -> None:
        self._context = context

    def new_state(self) -> NamespaceState:
        """Return a fresh namespace state initialized from workspace defaults."""
        config = self._context.workspace.config
        catalog = self._catalog(config.default_catalog)
        schema = self._schema(catalog.name, config.default_schema)
        return NamespaceState(self, catalog.name, schema.name)

    def _catalog(self, name: str) -> CatalogRecord:
        key = name.casefold()
        matches = [
            record for record in self._context.catalogs.list() if record.name.casefold() == key
        ]
        if not matches:
            raise NamespaceNotFoundError(f"catalog does not exist: {name}")
        if len(matches) > 1:
            raise AmbiguousIdentifierError(f"catalog identifier matches multiple names: {name}")
        return matches[0]

    def _schema(self, catalog_name: str, name: str) -> SchemaRecord:
        key = name.casefold()
        matches = [
            record
            for record in self._context.schemas.list(catalog_name)
            if record.name.casefold() == key
        ]
        if not matches:
            raise NamespaceNotFoundError(f"schema does not exist: {catalog_name}.{name}")
        if len(matches) > 1:
            raise AmbiguousIdentifierError(
                f"schema identifier matches multiple names: {catalog_name}.{name}"
            )
        return matches[0]

    def _table(
        self,
        catalog_name: str,
        schema_name: str,
        name: str,
    ) -> TableRegistration:
        key = name.casefold()
        matches = [
            table
            for table in self._context.tables.list(catalog_name, schema_name)
            if table.name.casefold() == key
        ]
        if not matches:
            raise TableNotFoundError(f"table does not exist: {catalog_name}.{schema_name}.{name}")
        if len(matches) > 1:
            full_names = ", ".join(sorted(table.full_name for table in matches))
            raise AmbiguousIdentifierError(f"table identifier matches multiple names: {full_names}")
        return matches[0]


class NamespaceState:
    """Mutable current catalog/schema values scoped to one caller-created session."""

    def __init__(self, resolver: IdentifierResolver, catalog_name: str, schema_name: str) -> None:
        self._resolver = resolver
        self._current_catalog = catalog_name
        self._current_schema = schema_name

    @property
    def current_catalog(self) -> str:
        return self._current_catalog

    @property
    def current_schema(self) -> str:
        return self._current_schema

    def use_catalog(self, identifier: str) -> None:
        """Set the catalog and reset the schema to Databricks' ``default``."""
        parsed = parse_identifier(identifier)
        if len(parsed.parts) != 1:
            raise InvalidIdentifierError("catalog identifiers must have exactly one part")
        catalog = self._resolver._catalog(parsed.parts[0].value)
        self._current_catalog = catalog.name
        self._current_schema = "default"

    def use_schema(self, identifier: str) -> None:
        """Set the schema, optionally with an explicit catalog, atomically."""
        parsed = parse_identifier(identifier)
        if len(parsed.parts) == 1:
            catalog_name = self._current_catalog
            schema_part = parsed.parts[0]
        elif len(parsed.parts) == 2:
            catalog_name = self._resolver._catalog(parsed.parts[0].value).name
            schema_part = parsed.parts[1]
        else:
            raise InvalidIdentifierError("schema identifiers must have one or two parts")
        schema = self._resolver._schema(catalog_name, schema_part.value)
        self._current_catalog = catalog_name
        self._current_schema = schema.name

    def resolve_schema(self, identifier: str) -> SchemaRecord:
        """Resolve ``schema`` or ``catalog.schema`` against this state."""
        parsed = parse_identifier(identifier)
        if len(parsed.parts) == 1:
            catalog_name = self._current_catalog
            schema_part = parsed.parts[0]
        elif len(parsed.parts) == 2:
            catalog_name = self._resolver._catalog(parsed.parts[0].value).name
            schema_part = parsed.parts[1]
        else:
            raise InvalidIdentifierError("schema identifiers must have one or two parts")
        return self._resolver._schema(catalog_name, schema_part.value)

    def resolve_table(self, identifier: str) -> TableRegistration:
        """Resolve ``table``, ``schema.table``, or ``catalog.schema.table``."""
        parsed = parse_identifier(identifier)
        if len(parsed.parts) == 1:
            catalog_name = self._resolver._catalog(self._current_catalog).name
            schema_name = self._resolver._schema(
                catalog_name,
                self._current_schema,
            ).name
            table_part = parsed.parts[0]
        elif len(parsed.parts) == 2:
            catalog_name = self._current_catalog
            schema_name = self._resolver._schema(
                catalog_name,
                parsed.parts[0].value,
            ).name
            table_part = parsed.parts[1]
        else:
            catalog_name = self._resolver._catalog(parsed.parts[0].value).name
            schema_name = self._resolver._schema(
                catalog_name,
                parsed.parts[1].value,
            ).name
            table_part = parsed.parts[2]
        return self._resolver._table(catalog_name, schema_name, table_part.value)
