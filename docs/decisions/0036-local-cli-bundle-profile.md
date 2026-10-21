# ADR 0036: Bounded local Databricks CLI bundle profile

**Status:** Accepted for the Databricks CLI v1.21.0 loopback profile\
**Date:** 2026-10-20\
**Decision owners:** BrickHouse maintainers

## Context

The Databricks CLI bundle workflow combines workspace identity lookup, directory creation, source/JSON synchronization, job creation, run submission/output, and resource destruction. BrickHouse has no Databricks workspace identity, authentication service, or allocated cloud compute. A CLI response or a metadata-only deploy cannot establish that a real workload ran.

## Decision

- Support one explicitly bounded Databricks CLI v1.21.0 profile: `bundle validate`, `bundle deploy`, `bundle run`, and `bundle destroy` for one classic single-task Python job with a `new_cluster` declaration and Python files under the configured `sync.paths`.
- Return a stable synthetic SCIM current-user identity and label it `simulated_local_identity`. Keep the CLI pointed at loopback with a test-only token; this profile does not implement authentication.
- Store uploaded `.py` sources and valid bounded `.json` deployment state under the workspace-owned files tree. Support the required workspace directory, status, raw export, and recursive delete routes while checking path containment and refusing symbolic-link traversal.
- Preserve the bundle's explicit deployment metadata on the immutable job definition. Run the source in BrickHouse's real local Python worker. Record new-cluster lifecycle metadata as simulated.
- Implement Jobs API delete as an immutable tombstone. A successful bundle destroy hides the job from future reads/runs, removes the bundle-managed file tree, and retains job definitions and submitted run history.
- Reject unsupported task/resource effects explicitly. Do not silently downgrade a notebook/pipeline or imply serverless or remote bundle compatibility.
- Keep the real CLI workflow opt-in. Ordinary tests and CI use no account token; the integration subprocess receives a synthetic token and temporary config/home.

## Alternatives considered

1. Treat a successful bundle deploy as sufficient evidence. Rejected because deployment metadata does not prove the Python task can run or return its output.
2. Delete the job definition and run history on destroy. Rejected because BrickHouse's job definitions, submissions, and attempts are immutable audit history.
3. Claim serverless or notebook bundle compatibility based on the same CLI transport. Rejected because these variants require separate execution and output evidence.
4. Authenticate the local API with the CLI's synthetic token. Deferred; the current API is a trusted local service and does not validate caller credentials.

## Consequences

- Metadata schema 12 stores immutable SDK job-deletion tombstones; schemas 1–11 upgrade transactionally.
- The local workspace API now has the bounded file status/export/mkdirs/delete behavior needed by this CLI workflow. Recursive cleanup is contained to the workspace-owned files root.
- The selected workflow is verified with a real CLI process against an ephemeral loopback server, including actual local worker output and destroy cleanup. This is local client behavior, not Databricks authentication, remote workspace, or cloud execution conformance.
- Only one classic single-task Python bundle is covered. Notebook tasks, pipelines, apps, dashboards, serverless jobs, SQL resources, multi-task output, broad bundle syntax, and remote behavior remain outside this capability.

## Evidence

See [B020.3 conformance](../../artifacts/conformance/B020.3-databricks-cli-bundle-v1.21.0.md), the [opt-in integration fixture](../../tests/integration/test_databricks_cli_bundle_v1_21_0.py), and the current schema description in [document 12](../plan/12-delivery-and-operations.md).
