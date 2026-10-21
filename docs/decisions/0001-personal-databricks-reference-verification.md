# ADR 0001 — Personal Databricks account as a development reference

Date: 2026-10-16
Status: accepted; B005 reference capture, local isolation checks, and optional workflow implementation are complete. The GitHub workflow has not been dispatched.

## Context

The owner authorized use of their personal Databricks account to verify representative code and design tests, and reported providing `DATABRICKS_TOKEN` locally and in GitHub Actions secrets. Remote execution latency makes running the ordinary test suite there unsuitable. The signed-in UI identified a Databricks Free Edition workspace; B005.3 recorded an authenticated read-only identity request, one existing serverless SQL warehouse, and one synthetic SQL reference capture.

## Options

- Documentation and local engine oracles alone leave Databricks-specific behavior unobserved.
- Running routine tests remotely adds latency and couples development to account availability.
- Occasional bounded reference probes produce real observations that deterministic local tests can consume.

## Decision

Adopt occasional reference probes, following [the capture plan](../plan/15-personal-databricks-verification.md). Use synthetic inputs and permitted existing resources, sanitize observations, and use them to design local behavioral tests and compare representative BrickHouse operations. Keep ordinary CI, milestone gates, and runtime execution local and independent of personal credentials. Optional GitHub captures are manually dispatched from trusted reviewed code. The initial probe used the existing Free Edition warehouse for one bounded literal-only statement and restored its stopped state.

The authorization covers bounded development reference use against existing resources. It supplies no budget for paid provisioning. Credentials and raw account responses remain outside tracked files. Remote observations establish expected behavior; passing local tests establish BrickHouse support.

## Consequences and evidence

B005 gains capture-tooling, provenance, manual-workflow, and isolation acceptance criteria and must be decomposed before implementation. Other tasks consume reviewed fixtures when useful. The owner instruction is evidence for the planning decision; the B005.3 capture supplies one observed behavior case, not a BrickHouse compatibility claim. Missing account features constrain reference scope without blocking independent local work.

## Migration impact

No workspace-format or runtime API migration. Existing planned task IDs and capability states are preserved. Future fixture revisions retain the source environment and normalization history.
