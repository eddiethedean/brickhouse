# ADR 0001 — Personal Databricks account as a development reference

Date: 2026-10-16
Status: accepted planning decision; implementation pending under B005.

## Context

The owner authorized use of their personal Databricks account to verify representative code and design tests, and reported providing `DATABRICKS_TOKEN` locally and in GitHub Actions secrets. Remote execution latency makes running the ordinary test suite there unsuitable. Workspace access, account features, host, and permitted compute have not been verified.

## Options

- Documentation and local engine oracles alone leave Databricks-specific behavior unobserved.
- Running routine tests remotely adds latency and couples development to account availability.
- Occasional bounded reference probes produce real observations that deterministic local tests can consume.

## Decision

Adopt occasional reference probes, following [the capture plan](../plan/15-personal-databricks-verification.md). Use synthetic inputs and permitted existing resources, sanitize observations, and use them to design local behavioral tests and compare representative BrickHouse operations. Keep ordinary CI, milestone gates, and runtime execution local and independent of personal credentials. Optional GitHub captures are manually dispatched from trusted reviewed code.

The authorization covers this development reference use. It supplies no budget for paid provisioning. Credentials and raw account responses remain outside tracked files. Remote observations establish expected behavior; passing local tests establish BrickHouse support.

## Consequences and evidence

B005 gains capture-tooling, provenance, manual-workflow, and isolation acceptance criteria and must be decomposed before implementation. Other tasks consume reviewed fixtures when useful. The owner instruction is evidence for the planning decision only; no live capture or compatibility claim has been established. Missing account features constrain reference scope without blocking independent local work.

## Migration impact

No workspace-format or runtime API migration. Existing planned task IDs and capability states are preserved. Future fixture revisions retain the source environment and normalization history.
