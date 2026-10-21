# ADR 0035: Local Terraform resource identity and immutable job revisions

**Status:** Accepted for the bounded Terraform v1.137.0 profile\
**Date:** 2026-10-20\
**Decision owners:** BrickHouse maintainers

## Context

The pinned Databricks Terraform provider v1.137.0 discovers a numeric workspace ID for workspace-scoped resources. BrickHouse stores its local workspace identity as a UUID and does not have a Databricks account or workspace ID. The provider also updates jobs through `reset` and deletes them through the Jobs API, while BrickHouse job definitions and run snapshots are immutable by design.

## Decision

- Advertise `host_type: workspace` and a deterministic numeric identity derived from the local workspace UUID at `/.well-known/databricks-config`. Report this only as `terraform_provider_workspace_identity: simulated_local_identity`; it is not a Databricks workspace/account identifier and does not authenticate callers.
- For the selected provider subset, accept `databricks_workspace_file` and a single-task Python `databricks_job` that uses an existing local classic cluster. Reject other resource/job settings with a specific API error.
- Keep Terraform's stable job ID in schema 11 metadata. Each reset appends an immutable BrickHouse job definition revision and advances the resource's current-revision pointer. Delete tombstones the Terraform identity while retaining all definitions and run history.
- Route Terraform's single-task create/read/update calls through Jobs API 2.0. Support Jobs API 2.2 `delete` because the pinned provider uses that route for its resource destroy callback. Standard Python run submissions resolve the stable Terraform ID to its current immutable definition.
- Resolve the stable Terraform ID to its current immutable definition for SDK Jobs API 2.2 `get` and `run-now`. A delete through either API version checks active runs across every revision and tombstones both the Terraform identity and stable SDK-facing job record, while preserving all immutable definitions and run history.
- Run the provider conformance fixture against an ephemeral loopback workspace with a synthetic token, isolated Terraform home, and no `.env` or Databricks credential discovery.

## Alternatives considered

1. Mutate the existing `jobs` rows on Terraform reset. Rejected because database triggers and run snapshots intentionally protect job definitions as immutable.
2. Claim a local UUID is the provider's numeric Databricks ID. Rejected because the provider validates numeric IDs and because it would conflate BrickHouse and account identities.
3. Implement the full Terraform provider surface or silently accept unsupported settings. Rejected because phase 0.1 selects a small workflow and unsupported effects must remain visible.

## Consequences

- Workspace metadata schemas 1–10 migrate transactionally to schema 11, which stores the stable resource pointer, tombstone and append-only revision records.
- The exact local profile is verified with Terraform Core 1.16.6 and provider v1.137.0 for workspace-file and existing-cluster Python-job apply/read/update/refresh/destroy. Cluster allocation is simulated; the API has no authentication; no remote workspace behavior follows from this result.
- A failed Terraform job update does not advance the persisted backend revision. A subsequent provider refresh can reconcile local Terraform state to the active revision.
- Terraform cluster/warehouse resources, multi-task jobs, new-cluster provisioning and arbitrary provider settings remain separate unverified work. Bundle support is recorded separately under [ADR 0036](0036-local-cli-bundle-profile.md).

## Evidence

See [B020.2 conformance](../../artifacts/conformance/B020.2-terraform-v1.16.6-provider-v1.137.0.md), [the opt-in integration test](../../tests/integration/test_terraform_v1_16_6_provider_1_137_0.py), and the immutable job revision contract in [schema 11](../plan/12-delivery-and-operations.md). Schema 12 adds SDK job-deletion tombstones used by the local bundle destroy flow.
