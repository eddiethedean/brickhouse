"""Domain operations for the local catalog and schema namespace."""

from __future__ import annotations

from brickhouse.catalog.repository import CatalogRecord, SchemaRecord
from brickhouse.workspace.context import WorkspaceContext


class CatalogService:
    """Apply catalog and schema operations to one active workspace."""

    def __init__(self, context: WorkspaceContext) -> None:
        self._context = context

    def list_catalogs(self) -> list[CatalogRecord]:
        return self._context.catalogs.list()

    def get_catalog(self, name: str) -> CatalogRecord:
        return self._context.catalogs.get(name)

    def create_catalog(self, name: str, *, comment: str | None = None) -> CatalogRecord:
        return self._context.catalogs.create(name, comment=comment)

    def delete_catalog(self, name: str) -> None:
        self._context.catalogs.delete(name)

    def list_schemas(self, catalog_name: str) -> list[SchemaRecord]:
        return self._context.schemas.list(catalog_name)

    def get_schema(self, catalog_name: str, name: str) -> SchemaRecord:
        return self._context.schemas.get(catalog_name, name)

    def create_schema(
        self,
        catalog_name: str,
        name: str,
        *,
        comment: str | None = None,
    ) -> SchemaRecord:
        return self._context.schemas.create(catalog_name, name, comment=comment)

    def delete_schema(self, catalog_name: str, name: str) -> None:
        self._context.schemas.delete(catalog_name, name)
