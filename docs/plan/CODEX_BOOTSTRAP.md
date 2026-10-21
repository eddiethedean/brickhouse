# Codex implementation handoff

## First session prompt

Paste the following into Codex after copying this planning pack into the new repository:

> Build BrickHouse using this repository's planning documents. Read AGENTS.md, README.md, STATUS.md, and the active task's referenced plans. Begin with B001 in planning/tasks.json. Implement one coherent slice at a time, run its focused acceptance checks, and update task state, capability evidence, and STATUS.md. Continue with ready dependencies in order. Keep the runtime JVM-free and the catalog/table service authoritative. Record evidence before changing compatibility claims. Do not publish packages, create paid cloud resources, or send upstream messages without my authorization. When Sparkless changes are needed, identify the exact generic hook and coordinate it in the Sparkless checkout under that repository's instructions; return to BrickHouse with a tested artifact reference.

This prompt authorizes implementation and the relevant tests when the owner submits it. The current repository starter is planning only.

Phase 0.1 is now decomposed: begin with ready leaf **B001.1** under B001, then B001.2 and B001.3. Read the child's parent criteria and [the phase 0.1 contract](16-phase-0.1-initial-release.md) as well as its plan references. A parent is done only after all required children and the original acceptance criteria pass.

## First work slice: B001

Create only the package skeleton, build metadata, CLI help/version entry point, test structure, and development configuration needed to run subsequent probes. Do not scaffold hundreds of placeholder API routes. Preserve these plans and replace the README's planning-only banner only when actual behavior is described accurately.

B001 finishes with a built wheel, clean-environment import/help checks, and a short dependency/platform baseline. Follow B002–B005 to prove integration contracts before committing to broad implementations. Read actual Sparkless source before assuming proposed hooks exist.

## Session loop

1. Read STATUS.md and inspect Git status; preserve unrelated user changes.
2. Select a ready task. Inspect its dependencies and evidence, not just its label.
3. Decompose an epic or oversized task into bounded leaf tasks before implementation; record dependency edges and acceptance criteria.
4. Mark the chosen leaf task `in_progress` and write a short execution outline.
5. Implement the smallest real vertical behavior with focused tests.
6. Record results, limitations, and any design decisions. Mark `done` only when acceptance criteria pass.
7. Update capability evidence and STATUS.md; identify the next ready task.

If an integration is blocked, record the exact missing artifact/API and pursue independent ready work. Do not create fake results to advance the roadmap. Never mark parent epics complete while their child requirements remain open.

## Repository handoff format

STATUS.md should answer: what currently works; the active compatibility profile; last completed task and evidence; current task; blockers; next ready tasks; relevant native/client versions; workspace-format changes; and any uncommitted work. Keep detailed logs under task evidence paths, with no credentials.

Recommended future locations:

```text
docs/decisions/             # evidence-backed ADRs
docs/upstream/              # copied-source inventory and upstream contracts
docs/compatibility/        # generated supported behavior and mismatches
tests/fixtures/oracles/     # sanitized, versioned behavior captures
artifacts/conformance/     # generated reports; retention/git policy in B001
```

## Scope discipline

Use the owner's personal Databricks account for occasional bounded reference probes when designing tests or verifying representative code, under [document 15](15-personal-databricks-verification.md). The owner has supplied the token through local `.env` and GitHub Actions secrets; verify configuration before capture without displaying credentials. Required tests and CI run locally from sanitized fixtures. Token presence does not authorize paid provisioning, and this planning update does not initiate a remote run.

A matching Python method name, a successful import, or an HTTP 200 does not complete emulation. Prefer the golden workflow and shared-data invariants over broad empty surfaces. Use the long-term frontier to keep gaps visible. A narrower release must state its boundary and retain deferred tasks; it must not rename unimplemented behavior as supported.

## Long-term direction

Prioritize the JVM-free Databricks emulator through phases 0.1–0.9. [Document 18](18-self-hosted-platform.md) maps lightweight architecture preparation to each phase; B048 is the bounded local review before B023. Actual self-hosting tasks B050–B055 remain unavailable until B049 records emulator stability after B042. Do not select deployment epics merely because local policy or worker infrastructure exists. Decompose future epics before implementation, retain one catalog/commit authority, and keep deployment dependencies out of the base wheel.
