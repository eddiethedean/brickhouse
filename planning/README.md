# Planning manifests

These JSON files are machine-readable planning and implementation ledgers. New entries start `planned`; task states, capability evidence, and tested versions are updated as work is completed. The baseline date records when the original requirements inventory was created.

## tasks.json

Top-level fields: schema_version, project, baseline_date, release_roadmap, status_values, tasks. The release_roadmap field points to [the root release roadmap](../ROADMAP.md), which maps existing work packages to 0.x release phases. Each task has a unique id, milestone, kind (`task` or `epic`), title, status, depends_on, plan_refs, deliverable, acceptance_criteria, owner, evidence, blockers, and children.

Task states: `planned`, `in_progress`, `blocked`, `done`, `deferred`. Dependencies must exist and be acyclic. All required dependencies must be `done` before implementation. A deferred dependency is not complete. Epics require decomposition before coding; child tasks inherit prerequisite edges and must not depend on their own parent. Parents complete only after required children and parent acceptance criteria pass.

Phase 0.1 has 24 decomposed implementation work packages (B001–B023 and B046) with 85 required child tasks, plus standalone research/planning/review tasks B043–B045 and B047–B048. The original 68 children, five compute-expansion children, seven refinement children, and five bounded B014.3 scheduler/retry/concurrency/recovery slices retain separate acceptance criteria. The original emulator inventory has 46 top-level packages; the subsequent deployment planning extension is described below. [Document 16](../docs/plan/16-phase-0.1-initial-release.md) defines work lanes, local fixture limits and 27 acceptance families; [ADR 0017](../docs/decisions/0017-phase-0.1-delivery-refinement.md) records the provider/consumer split.

A child uses the same fields plus `parent_id` and `parent_criteria`, the one-based indices of parent acceptance criteria it covers. Reciprocal child/parent references, inherited parent prerequisites and complete criterion coverage are required. Every child inherits its parent's emulator release phase or post-stability stage; sibling dependencies order bounded work without depending on the parent. Research and provider checks cannot substitute for parent integration evidence. All prior parent acceptance criteria, task states and evidence are preserved by this refinement.

The JSON ledger is canonical for task definitions and state; keep docs/plan/09-task-backlog.md synchronized for readable acceptance criteria. Preserve IDs when changing titles. Add bounded child IDs such as B008.1. A task may be split without weakening its original acceptance criteria.

## Emulator and post-stability tracks

JVM-free Databricks emulation remains primary through release phases 0.1–0.9. B047 records [the self-hosted plan](../docs/plan/18-self-hosted-platform.md); B048 is a bounded architecture review before B023. B049/SH.0 assesses emulator stability after B042; every future deployment implementation B050–B055 depends transitively on that gate. SH.0–SH.6 have no assigned package versions. B042 has no deployment dependency.

The inventory now has **55 top-level packages**, **85 required phase 0.1 children** and **64 capabilities**. Existing emulator acceptance criteria, states and evidence are retained. Only unfinished B023 and its children gain the B048 prerequisite. Existing later emulator epics gain plan references, not deployment acceptance requirements. Deployment epics require bounded decomposition before implementation, with child release-stage inheritance and prerequisite checks just like emulator work.

## capabilities.json

Top-level fields: schema_version, project, baseline_date, inventory_status, status_values, capabilities. Each entry has id, domain, name, status, target_scope, profiles, task_ids, tested_versions, test_ids, evidence, and limitations.

Capability states: `planned`, `unsupported`, `partial`, `supported`, `simulated`, `experimental`. Initial profiles are planning targets, not tested claims. Split broad entries into per-operation/type/protocol cases as implementation proceeds. Version metadata must distinguish BrickHouse, Sparkless, native providers, client packages, dialect/runtime reference and workspace format as relevant.

`test_ids` identify actual runnable checks. `evidence` is an array of objects such as:

```json
{
  "date": "YYYY-MM-DD",
  "kind": "conformance",
  "command": "exact command used",
  "result": "passed/failed with concrete summary",
  "artifact": "repository-relative report path or durable CI URL",
  "commit": "actual commit if available"
}
```

Evidence paths must exist or resolve to a retained CI report. Do not use template values as real evidence. A task decision report may establish a design; only behavior tests establish implementation support. `supported` requires tested versions, test IDs and passing evidence. `partial` and `simulated` require explicit limitations. Do not mark the full initial broad entry supported because one happy path passed.

Compute-profile names in C053–C056 are planning targets for serverless/classic workloads, warehouse behavior and environments. Claims must identify the exact compute/access/environment/runtime/semantic/protocol combination; listing several targets does not establish every combination. C034 remains the separate infrastructure metadata requirement with intended eventual `simulated` status. Source research completing B043 does not promote any runtime capability.

Personal Databricks reference captures follow [document 15](../docs/plan/15-personal-databricks-verification.md). Record sanitized reference provenance separately from local implementation evidence, using evidence kinds such as `reference_capture` and `conformance`. A remote success supplies expected behavior; it does not establish BrickHouse support. Reference versions belong in fixture provenance and must not be presented as tested BrickHouse integration versions before local checks pass. Routine tests consume stored fixtures without personal credentials or remote execution.

## Validation to add during B001

Check schema fields and allowed values, unique IDs, dependency references/cycles, plan links, capability task references, and nonempty evidence for done/supported claims. Validate parent/child references, inherited prerequisites, complete/in-range parent-criterion coverage, release/stage inheritance, the B049 stability barrier, and completion of all required children before a parent can be done. Reject accidental unsupported version guesses. The planning pack was checked for JSON syntax, references, cycles, and local document links before handoff; this is not an implementation test result.

C059–C064 are planned post-stability deployment capabilities. Their deployment profile names are planning targets, not current settings or tested combinations. They start with no tested versions, test IDs or evidence. Existing C001–C058 claims are unchanged by B047.
