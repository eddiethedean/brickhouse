# ADR 0024: Pre-dispatch execution-context resolution

- Status: accepted and implemented for the bounded resolver slice
- Date: 2026-10-20
- Task: B014.5
- Depends on: ADR 0016, ADR 0022, ADR 0023

## Context

B014.4 persists simulated classic resource identities, but the scheduler must validate each task's selected mode and environment before starting a child process. A selector accepted by the SDK does not imply that BrickHouse can provide its runtime, dependencies, access rules, installation behavior, or compute effects. The resolver therefore needs a versioned local mapping and a durable immutable value that later dispatch code can consume.

## Decision

1. `ExecutionContextResolver` accepts the pinned task wire models as JSON-like mappings and resolves exactly one Python script or notebook task per entry. It returns frozen `ResolvedTaskContext` values with the submitted `run_id`, task arguments, mode, runtime/environment profile, settings, resource identity/generation, timeout, retry policy, and simulated-autostart decision.
2. Serverless tasks must select a declared `environment_key`, omit classic selectors, use environment version `1`, and set `disable_auto_optimization=true`. The profile maps to the current local Python minor version and only accepts exact verified installed dependency pins. Non-standard performance targets, any explicit `Environment.client`, environment installation fields, unknown dependencies, serverless compute objects, and unmapped task fields fail before dispatch.
3. Serverless defaults are `spark.sql.ansi.enabled=true` and `spark.sql.session.timeZone=UTC`. Classic tasks must select one of inline `new_cluster`, a declared classic `job_cluster_key`, or an existing non-deleted all-purpose ID. Runtime and access mappings are delegated to the explicit `spark35-local-v1` compute service contract. Classic `spark_conf` values become the effective local settings; cluster allocation remains simulated.
4. Script and notebook paths must be normalized absolute descendants of `/Workspace`. Git paths, URI paths, parent traversal, non-canonical separators, SQL warehouse notebooks, and credential-shaped parameter values are rejected. Resolving a path does not prove that a file exists; B014.2 owns workspace-file lookup and SDK transport.
5. Explicit timeout `0` and omitted timeout mean no task timeout, matching the Jobs API contract. Positive timeouts are preserved up to the local worker's 24-hour bound; greater values fail before dispatch. Standard `ALL_SUCCESS` run conditions are accepted, while alternate conditions remain unsupported until the scheduler implements their semantics. Retry fields are validated and retained for B014.3.
6. This resolver does not create environments, start a worker, provision infrastructure, persist attempts, or prove Databricks compatibility. It does not promote C053–C056. C058 records only this internal, partial resolution capability; B014.2/B014.3 compose file transport and lifecycle, and B014.6 verifies actual worker effects.

## Verification boundary

Tests construct requests using `databricks-sdk==0.153.0` model `as_dict()` results and synthetic local compute records. Local filesystems, SQLite, package metadata, and the existing compute service are used. No `.env`, token, Databricks account, cloud resource, Spark process, or JVM is used. The SDK's documented zero-timeout behavior is described in the [Jobs API reference](https://docs.databricks.com/api/jobs/v2/job).

See [B014.5 conformance evidence](../../artifacts/conformance/B014.5-execution-context-resolution.md).
