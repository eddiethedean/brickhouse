# Planning manifests

These JSON files are machine-readable starting points for implementation. All entries start `planned`, with no tested versions or implementation evidence.

## tasks.json

Top-level fields: schema_version, project, baseline_date, status_values, tasks. Each task has a unique id, milestone, kind (`task` or `epic`), title, status, depends_on, plan_refs, deliverable, acceptance_criteria, owner, evidence, blockers, and children.

Task states: `planned`, `in_progress`, `blocked`, `done`, `deferred`. Dependencies must exist and be acyclic. All required dependencies must be `done` before implementation. A deferred dependency is not complete. Epics require decomposition before coding; child tasks inherit prerequisite edges and must not depend on their own parent. Parents complete only after required children and parent acceptance criteria pass.

The JSON ledger is canonical for task definitions and state; keep docs/plan/09-task-backlog.md synchronized for readable acceptance criteria. Preserve IDs when changing titles. Add bounded child IDs such as B008.1. A task may be split without weakening its original acceptance criteria.

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

Personal Databricks reference captures follow [document 15](../docs/plan/15-personal-databricks-verification.md). Record sanitized reference provenance separately from local implementation evidence, using evidence kinds such as `reference_capture` and `conformance`. A remote success supplies expected behavior; it does not establish BrickHouse support. Reference versions belong in fixture provenance and must not be presented as tested BrickHouse integration versions before local checks pass. Routine tests consume stored fixtures without personal credentials or remote execution.

## Validation to add during B001

Check schema fields and allowed values, unique IDs, dependency references/cycles, plan links, capability task references, and nonempty evidence for done/supported claims. Add child/parent completion validation. Reject accidental unsupported version guesses. The planning pack was checked for JSON syntax, references, cycles, and local document links before handoff; this is not an implementation test result.
