# ADR 0038: Reuse qualified native components with separate client profiles

- Status: accepted for planning; new candidates remain unqualified
- Date: 2026-10-20
- Research/planning task: B063
- Refines: dependency plan 03 and extension plan 11; preserves ADR 0034/0037 priority

## Context

The owner requested further research into Databricks' open-source ecosystem and an upgrade of the plans. Static public metadata/archive/source inspection identifies substantial reusable Python/native infrastructure, but also client-version conflicts, native wheel/toolchain gaps, Java reference implementations and interfaces that still need BrickHouse-owned behavior.

## Decision

1. Retain tested Sparkless/delta-rs/Arrow/client pins. [Document 20](../plan/20-open-source-reuse.md) and [the source inventory](../upstream/oss-reuse-inventory.md) distinguish those from exact research-only candidates. Track machine-readable provenance and task ownership in [planning/oss-reuse.json](../../planning/oss-reuse.json); it is not a runtime lock.
2. Prefer direct Jupyter execution/format/kernel components and a real optional MLflow service with provider-owned DB/artifacts. Papermill is conditional. Databricks notebook/registry/serving adapters and policy remain BrickHouse work.
3. Keep dbt-duckdb first. Qualify any dbt-databricks profile in its own lock because the inspected candidate rejects current SDK/SQL-connector pins. Keep Connect/sharing clients independently versioned and prove their native server paths; native wrapper dependencies and platform wheels are explicit gates.
4. Reuse pinned UC API/client contracts, not its Java server or a second catalog. Evaluate optional Iceberg/Kernel under B040/B041 before selecting any native provider extension; preserve the existing mutation authority and reject unproved features.
5. Require SC.0 to evaluate native Ballista/DataFusion infrastructure against extending Sparkless/Polars, accounting for toolchain/version, semantics, security and commit integration. No backend or transport is selected by this research and no fallback policy changes.
6. Strengthen still-planned owning epics with concrete qualification criteria and source references; preserve original criteria/evidence, completed task states, the 85 alpha children counted in the 2026-10-20 planning snapshot and all emulator/SH/SC behavior gates. Later implementation decomposition brought the ledger to 122 children; B064 initially added ten review-fix children, for 132 required phase children. B064.11 was subsequently added for interrupted SQL table creation, bringing the current phase total to 133. This does not change B063's decision. No new feature or dependency is added to 0.1 by B063.

## Consequences

Package installation, full transitive resolution, supported-platform wheels, native execution, interoperability, restart and deployed security remain implementation evidence gates. Static metadata compatibility is not behavioral parity. New candidate dependencies are absent from runtime requirements until their owner qualifies a bounded slice.

This slice changes plans, research inventory and offline planning validation only. It installs/executes no upstream candidate, modifies no source checkout or workspace, and authorizes no publication or cloud workload. See [B063 evidence](../../artifacts/conformance/B063-open-source-reuse.md).
