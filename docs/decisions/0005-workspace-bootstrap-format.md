# ADR 0005 — Workspace bootstrap defaults and format

- Status: accepted for the 0.1.0 implementation
- Date: 2026-10-16
- Related task: B006.1

## Context

BrickHouse needs a repeatable local workspace before adding catalog and table services. The account-specific catalog visible in the Databricks Free Edition workspace is not a suitable local default: it would make local initialization depend on a remote account and blur the boundary between reference observations and BrickHouse policy.

## Decision

Use the explicit local default namespace `main.default`. A new workspace has this layout:

```text
workspace/
  brickhouse.toml
  metadata.sqlite
  tables/
  files/
  staging/
  results/
  runs/
  checkpoints/
  integrations/
```

The version 1 TOML file has exactly one `[workspace]` table with `config_version = 1`, `format_version = 1`, `default_catalog`, and `default_schema`. Unknown or missing settings, unsupported versions, and invalid unquoted lowercase identifiers are rejected. Custom version 1 defaults can be supplied when creating a workspace.

`metadata.sqlite` starts at SQLite `user_version = 1` and contains the schema migration marker, workspace identity/config metadata, and initial catalog/schema rows. `initialize_workspace()` creates a new path and refuses an existing target, including one that appears during initialization. `open_workspace()` validates the config, format, initial metadata, and required directories through a read-only database connection.

This establishes a bootstrap schema, not a released upgrade path. B006.2 now provides an ordered transactional migration runner and POSIX process-owner lock with crash recovery; ADR 0006 records those boundaries. Only migration version 1 exists. CLI commands, service startup/shutdown, and automatic lock acquisition remain B006.3 work. A reopened descriptor does not hold a live database connection.

## Consequences

- Local defaults are deterministic and independent of personal Databricks account state.
- Config and persisted metadata must agree on format and default namespace.
- Repeated initialization is an error; reopening an existing workspace is explicit.
- Current evidence supports only the Python initialization/reopen primitives and v1 format. It does not establish catalog API, runtime, SQL, or client compatibility.
