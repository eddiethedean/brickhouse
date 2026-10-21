# ADR 0009 — Persisted catalog identities and table location ownership

- Status: accepted for phase 0.1 metadata implementation
- Date: 2026-10-19
- Related task: B007.1

## Context

Workspace format 1 initially stored catalog and schema names but no stable namespace IDs. B007.1 needs persisted catalog/schema/table identities and must prevent two names or ownership modes from silently claiming one physical table location. The implementation is metadata only: the table provider and catalog API are not yet integrated.

## Decision

Keep the workspace format at version 1 and advance its SQLite metadata schema to version 2. Migration 2 assigns UUIDs to existing catalog and schema rows, makes those IDs unique and immutable, and adds persistent table registrations with UUID table IDs, a Delta format marker, managed/external ownership, a canonical local path, creation time, and namespace/name uniqueness. Existing metadata schema 1 is upgraded transactionally while the workspace owner lock is held.

Expose read-only catalog and schema repositories plus table registration operations from an active `WorkspaceContext`. Managed registration reserves `tables/<table-id>` beneath the resolved workspace table root. External registration accepts local filesystem paths only and rejects canonical paths inside that managed root. Resolve paths before checking uniqueness so existing symlink aliases share one registration. A location is globally unique within the workspace, regardless of table name or ownership mode. Return catalog and schema IDs with each table record by resolving its persisted namespace.

Treat registration as metadata reservation only. It does not create a table directory or Delta log, inspect external table contents, write data, or provide catalog/SQL HTTP endpoints. B008/B010 own native table operations and must revalidate storage ownership at use time.

## Consequences

- Catalog, schema, and table identities survive workspace reopen and metadata schema migration.
- The namespace IDs are immutable; a later rename service must preserve IDs and handle table references deliberately.
- Canonical local paths prevent duplicate aliases in the repository, but do not establish safe concurrent filesystem operations by themselves.
- Cloud URIs, DBFS aliases, namespace CRUD, quoted identifier semantics, and data access remain unsupported until their own tasks provide evidence.
- `capabilities` and `doctor` report catalog/table metadata repositories as partial local persistence while leaving the catalog API and table-provider integration planned.
