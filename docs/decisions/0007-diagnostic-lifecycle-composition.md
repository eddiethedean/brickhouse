# ADR 0007 — Embedded and CLI lifecycle composition

- Status: accepted for the 0.1.0 implementation
- Date: 2026-10-19
- Related task: B006.3

## Context

B006.2 supplies workspace validation, an exclusive process-owner lock, and deterministic release. The project needs one embedded and CLI composition point without implying that catalog, SQL, or job services already exist. A diagnostics endpoint is useful to inspect the lifecycle while keeping unsupported APIs explicit.

## Decision

Expose `brickhouse.Workspace.local(directory)` as a one-shot context manager backed by `WorkspaceContext`. Entering validates the workspace and acquires its owner lock; leaving releases it. The context exposes the validated descriptor but no session or data service.

The CLI's `serve` command enters that same context and serves only `/health`, `/version`, and `/capabilities` on IPv4 loopback. It shuts down and releases ownership on Ctrl-C or SIGTERM. Provider versions are read from installed distribution metadata, without importing provider modules or loading workspace credentials. Each provider is reported as `not_integrated` until a BrickHouse adapter and its conformance evidence exist.

The `run` command is present as an explicit unavailable operation. It exits nonzero and does not execute a script until B013 implements the job supervisor. It must later use the same execution engine as API job submission. `init` remains a separate explicit operation that creates a workspace and refuses existing paths.

## Consequences

- CLI and embedded workspace ownership share one instance-scoped lifecycle implementation.
- The diagnostics server is not a Databricks REST/SDK endpoint and does not establish client compatibility.
- Binding is restricted to IPv4 loopback because the diagnostics routes have no authentication.
- Catalog, SQL, sessions, table providers, and Python jobs remain planned and must not be inferred from successful health checks.
- `doctor` and capability output may report installed packages but do not import them or use `DATABRICKS_TOKEN`.
