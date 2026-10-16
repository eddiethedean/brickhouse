# ADR 0002 — Map engineering milestones to 0.x release phases

Date: 2026-10-16
Status: accepted planning decision; all releases remain planned.

## Context

The owner requested a root ROADMAP.md with release phases numbered 0.x and specified that phase 0.1 initially ships as version 0.1.0. Existing milestones M0–M6 define engineering outcomes and task dependencies rather than release versions.

## Options

- Release each existing milestone separately, which would make the foundation-only M0 the initial release and require revising the existing alpha workflow scope.
- Retain the initial alpha requirements and group later work into bounded 0.x release phases while preserving engineering milestones and task dependencies.

## Decision

Adopt [the root release roadmap](../../ROADMAP.md). Phase 0.1/version 0.1.0 contains B001–B023 (M0–M3). Phases 0.2–0.8 group the later workspace, ML, vector, streaming, pipeline, protocol, and platform work; phase 0.9 contains B042's consolidated conformance release. The roadmap assigns every existing work package to one phase.

The task ledger remains authoritative for task definitions, dependencies, and state. Milestone completion and release phase completion are distinct. Versions after 0.1.0 are planning targets; dates depend on evidence. A future 1.0.0 commitment is undecided.

## Consequences and evidence

The initial release preserves the existing alpha acceptance criteria, including notebook and client dependencies. Later epics still require decomposition before implementation. The owner's request establishes the release naming decision; it supplies no implementation or publication evidence. All task and capability states remain unchanged.

## Migration impact

No runtime API, workspace format, or dependency changes. The task manifest adds a release_roadmap pointer to the root document without changing task IDs or prerequisite edges.
