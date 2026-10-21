# ADR 0023: Workspace-owned local classic compute lifecycle

- Status: accepted and implemented for the resource-metadata slice
- Date: 2026-10-20
- Task: B014.4
- Depends on: ADR 0016, ADR 0022

## Context

The phase 0.1 contract needs persistent classic all-purpose identity and new/shared job-compute lifetimes before job dispatch can resolve compute selectors. The pinned Databricks SDK exposes cluster lifecycle endpoints, but BrickHouse must not provision cloud VMs or start Spark/JVM processes. Resource state also needs to survive process restart and provide a durable fence for sessions that later work may invalidate.

## Decision

1. Store all-purpose clusters, per-run new/shared job resources, planned consumers, state generations, and append-only lifecycle events in the workspace SQLite metadata database (schema 7). The workspace remains the sole metadata authority.
2. Keep persistence in `ComputeRepository`, lifecycle rules in `ComputeService`, and SDK transport in `ComputeHttpApi`. Compose the API with the existing loopback server and expose the service through `WorkspaceContext.compute`.
3. Implement the pinned `databricks-sdk==0.153.0` classic cluster operations: create, get, list, edit, start, restart, terminate (`clusters.delete`), and permanent delete. Permanent deletion hides the resource from reads while retaining its row and event history.
4. Resolve only the explicit local runtime alias `spark35-local-v1`. Map standard/legacy user-isolation access to `classic-standard-local-v1`; map dedicated/single-user access to `classic-dedicated-local-v1` and require a user name. Reject automatic or legacy access modes without a deterministic local mapping, unknown runtime strings, Photon, JVM settings, policy effects, cloud-specific attributes, and unsupported request fields.
5. Treat compute allocation, node size/count, lifecycle timing, and access labels as simulated metadata. Never represent a metadata response as successful user work or a security boundary.
6. Register the full planned set of consumers before job-compute dispatch. A job resource starts on the first consumer and terminates when all planned consumers release it. All-purpose identity persists; a registered consumer can request autostart after termination. An injected timezone-aware clock drives idle termination without sleeping.
7. Increment a monotonic generation on identity-invalidating start, edit, restart, or termination, and append events that identify the transition and whether old sessions must be invalidated. B014.6 will connect those events to actual workers.

## Verification boundary

The exact SDK client is exercised against the loopback API with synthetic credentials and local SQLite files. This validates the selected wire shape, SDK model parsing, lifecycle transitions, pagination, error mapping, persistence and controlled-clock behavior. It is not a Databricks workspace probe and does not verify a Databricks Runtime or access mode. The local alias is intentionally not an actual Databricks Runtime version.

## Consequences

The resource identity/lifecycle contract is available for B014.5 context resolution and B014.6 worker integration. Workers, notebooks, permissions, security isolation, cluster networking, cloud allocation, and actual serverless or classic workload semantics remain separate gates. The service rejects conflicting edits/restarts/termination while job consumers are registered so it cannot claim to cancel work before worker invalidation is integrated.

See [B014.4 conformance evidence](../../artifacts/conformance/B014.4-classic-compute-lifecycle.md).
