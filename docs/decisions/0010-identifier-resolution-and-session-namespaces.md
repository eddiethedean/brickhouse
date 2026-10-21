# ADR 0010 — Identifier resolution and isolated namespace state

- Status: accepted for the initial local catalog profile
- Date: 2026-10-19
- Related task: B007.2

## Context

B007.2 needs a deterministic local contract for delimited/multipart object names and current catalog/schema values. Workspace metadata is authoritative, and the same name can be expressed with different qualification. Namespace defaults must not become mutable workspace-global state or leak between future sessions.

The Databricks SQL reference capture accepted backtick-delimited aliases containing a dot and a doubled backtick, and its current catalog/schema functions returned non-null values. The capture did not create objects or test table lookup. Official Databricks docs define one-part table names against current catalog/schema, two-part names against current catalog plus explicit schema, and three-part names against explicit catalog/schema. Spark 3.5.8 documents backtick-delimited parts, doubled backticks, and case-insensitive identifier lookup. See [the capture](../../artifacts/conformance/B007.2-identifiers-reference.md).

## Decision

Provide a small parser for one-to-three-part Spark-style identifiers. Dots separate parts only outside backticks; doubled backticks represent one literal backtick. Preserve spelling and quoting in parsed values. Match catalog, schema, and table names case-insensitively; report ambiguity if persisted metadata contains multiple matches for one comparison key. The parser does not implement SQL expressions, ANSI double-quoted identifiers, temporary-view lookup, Spark SQL Path behavior, or all Unity Catalog naming restrictions.

`WorkspaceContext.new_namespace_state()` returns a fresh mutable `NamespaceState` initialized from the workspace's configured catalog/schema. It is a namespace-state primitive for a future session, not an execution session. A one-part table name resolves in the state's current catalog/schema; two parts use current catalog plus schema/table; three parts use explicit catalog/schema/table. Fully qualified names are never shortened by stripping a prefix. A schema name accepts one part in current catalog or two explicit parts.

`use_catalog` validates the catalog and sets the current schema to `default`. `use_schema` accepts `schema` or `catalog.schema`; it validates all requested parts before mutating state. Every state object is isolated, including multiple states from one workspace context. Missing namespaces/tables and malformed/ambiguous identifiers raise specific errors.

## Consequences

- The first profile supports deterministic local catalog binding without importing Sparkless, SQLGlot, or a remote client.
- Workspace defaults remain immutable configuration; callers create separate state for each session and pass that state to name resolution.
- The resolver returns persisted table registration records. It does not read Delta, search temporary views, implement SQL DDL/DML, or provide a session engine.
- Databricks SQL Path, system namespaces, temporary views, notebook `USE` behavior, and broader Unity Catalog naming rules need separate evidence before implementation.
- The local test suite uses synthetic metadata and never contacts Databricks. The separate one-statement reference capture is sanitized and outside test discovery.
