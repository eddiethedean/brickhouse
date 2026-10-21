# ADR 0011: Bounded local SDK catalog and schema profile

- Status: accepted
- Date: 2026-10-19
- Tasks: B007.3

## Decision

Implement the pinned `databricks-sdk==0.153.0` catalog/schema methods as a local HTTP transport over BrickHouse's catalog service and SQLite repositories. The API supports catalog and schema create, list, get, and delete; list pagination; lowercase unquoted identifiers; and optional comments. It does not import the SDK at runtime. The pinned SDK is a development dependency used for protocol conformance tests.

Catalog creation creates `default` and `information_schema`. Metadata schema version 3 persists catalog/schema comments and inserts `information_schema` for existing catalogs without changing workspace format version 1. Catalog and schema UUIDs remain stable. Registered table metadata prevents deleting its parent schema or catalog. The workspace default catalog/schema and every `information_schema` are protected.

The local error profile uses Databricks-style `error_code` and `message` bodies for duplicate names, missing targets, invalid input, and namespaces in use. Unsupported create fields fail explicitly. The SDK's `force` delete flag does not bypass registered-table protection.

## Boundaries

- `serve` binds only to IPv4 loopback. The SDK bearer token is a compatibility value, not authentication or authorization.
- Permissions, workspace/catalog binding, storage roots, catalog properties, browse filtering, SQL, table data access, and broad Unity Catalog parity are not implemented.
- Identifiers must satisfy BrickHouse's initial lowercase unquoted profile. Quoted names and broader Databricks naming rules are not accepted by this REST slice.
- Local create/list/get/delete and persistence are tested with the exact SDK version. No Databricks workspace CRUD probe was run or is needed to run ordinary tests.
- Deletion removes empty namespace metadata only. It never removes table files or `_delta_log` data.

## Evidence

The pinned SDK local-server acceptance test and focused suite are recorded in [B007.3 conformance](../../artifacts/conformance/B007.3-sdk-catalog-schema.md).
