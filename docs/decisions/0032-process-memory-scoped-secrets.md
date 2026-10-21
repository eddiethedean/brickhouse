# ADR 0032 — Process-memory scoped secrets

- **Status:** Accepted
- **Date:** 2026-10-20
- **Task:** B016.2

## Context

Managed local jobs need a way to resolve `dbutils.secrets` values without putting them in submitted job settings, durable run context, child environment variables, process arguments, or returned SDK values. The local server has no authenticated secret-management HTTP route, no configured key-management service, and must not discover the owner's `.env` or Databricks reference token.

## Decision

`WorkspaceContext.secret_provider` is an explicit in-process provider. An embedding caller supplies string/byte values with `put(scope, key, value)` and grants scopes to a job ID with `grant_job(job_id, scope)`. Grants and values remain only in process memory and are cleared when the workspace context closes. They are not part of SQLite or filesystem state. Scope and key metadata are distinct from secret value resolution.

Before each worker attempt, the job service builds a bounded snapshot containing only the job's granted scopes. The snapshot is sent to the fresh child over its stdin pipe; the entry module consumes it before user code runs and constructs an attempt-local `dbutils.secrets`. No secret value is added to `BRICKHOUSE_RUN_CONTEXT_JSON`, environment variables, command-line arguments, worker outcome records, or job definitions. The selected `get`, `getBytes`, `list`, and `listScopes` methods return actual configured values/metadata. Access to ungranted scopes raises `PermissionError`; missing keys in granted scopes raise `KeyError`.

The worker supervisor redacts exact authorized secret bytes from captured stdout and stderr before building a result or updating the durable outcome journal. Redaction preserves byte length. This protects exact raw output occurrences only; arbitrary transformed or encoded values cannot be generally recognized.

## Limits

- No value or grant survives process restart. Reconfigure them through the embedding API before submitting subsequent jobs.
- The local HTTP API exposes no route for adding secrets or grants; the SDK token is not treated as an authorization credential.
- Values are held in ordinary Python process memory and cross a local anonymous pipe. This is not encryption, hardware isolation, or a malicious-code sandbox.
- A worker can use ordinary Python to inspect its own memory or transform a secret. Exact output redaction is not a substitute for granting only the scopes that code needs.
- Provider values are bounded to 4 KiB each and 1 MiB total; each per-attempt payload is capped at 48 KiB. Databricks Free Edition, `.env`, and GitHub secrets are never read by this provider.

See [B016.2 conformance](../../artifacts/conformance/B016.2-process-memory-scoped-secrets.md).
