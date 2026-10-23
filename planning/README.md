# Planning manifests

These JSON files are machine-readable planning and implementation ledgers. New entries start `planned`; task states, capability evidence, and tested versions are updated as work is completed. The baseline date records when the original requirements inventory was created.

## tasks.json

Top-level fields: schema_version, project, baseline_date, release_roadmap, status_values, tasks. The release_roadmap field points to [the root release roadmap](../ROADMAP.md), which maps existing work packages to 0.x release phases. Each task has a unique id, milestone, kind (`task` or `epic`), title, status, depends_on, plan_refs, deliverable, acceptance_criteria, owner, evidence, blockers, and children.

Task states: `planned`, `in_progress`, `blocked`, `done`, `deferred`. Dependencies must exist and be acyclic. All required dependencies must be `done` before implementation. A deferred dependency is not complete. Epics require decomposition before coding; child tasks inherit prerequisite edges and must not depend on their own parent. Parents complete only after required children and parent acceptance criteria pass.

Phase 0.1 has 24 original implementation packages (B001–B023 and B046) with 120 required implementation children, plus two B048 review children, eleven B064 fixes, and nine B066 final-release children/descendants: **142 phase children**. B064 is complete; B065 adds release automation. B066.1 passed 444 full-source cases per Python version with all required clients enabled. B066.2 passed the complete portable offline matrix; B066.3.1 completed metadata/diagnostics, B066.3.2 completed owner-verified PyPI configuration, and B066.4 completed the final artifact/evidence freeze. All 142 phase children and B066 are done. Version 0.1.0 uses Sparkless 4.15.2 and Apache-2.0. The ledger preserves earlier bounded slices and exact historical evidence. [Document 16](../docs/plan/16-phase-0.1-initial-release.md) defines the 27 acceptance families; [document 21](../docs/plan/21-release-readiness.md) defines the final readiness gates.

A child uses the same fields plus `parent_id` and `parent_criteria`, the one-based indices of parent acceptance criteria it covers. Reciprocal child/parent references, inherited parent prerequisites and complete criterion coverage are required. Every child inherits its parent's emulator release phase or post-stability stage; sibling dependencies order bounded work without depending on the parent. Research and provider checks cannot substitute for parent integration evidence. All prior parent acceptance criteria, task states and evidence are preserved by this refinement.

The JSON ledger is canonical for task definitions and state; keep docs/plan/09-task-backlog.md synchronized for readable acceptance criteria. Preserve IDs when changing titles. Add bounded child IDs such as B008.1. A task may be split without weakening its original acceptance criteria.

## Emulator and post-stability tracks

JVM-free Databricks emulation remains primary through release phases 0.1–0.9. B047 records [the self-hosted plan](../docs/plan/18-self-hosted-platform.md); B048 is a bounded architecture review before B023. B049/SH.0 assesses emulator stability after B042; every future deployment implementation B050–B055 depends transitively on that gate. SH.0–SH.6 have no assigned package versions. B042 has no deployment dependency.

The inventory now has **67 top-level packages**, **142 required phase 0.1 children** and **67 capabilities**. Existing emulator acceptance criteria, states and evidence are retained. B023 and its children carry the B048 prerequisite and are complete against the frozen local release-candidate scope; see the [owner-review bundle](../artifacts/releases/0.1.0/README.md). Existing later emulator epics gain plan references, not deployment acceptance requirements. Deployment epics require bounded decomposition before implementation, with child release-stage inheritance and prerequisite checks just like emulator work.

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

Compute-profile names in C053–C056 are planning targets for serverless/classic workloads, warehouse behavior and environments. Claims must identify the exact compute/access/environment/runtime/semantic/protocol combination; listing several targets does not establish every combination. C034 is the separate `simulated` infrastructure metadata capability; it records resource identity/lifecycle only and does not promote workload or SQL execution capabilities. Source research completing B043 does not promote any runtime capability.

Personal Databricks reference captures follow [document 15](../docs/plan/15-personal-databricks-verification.md). Record sanitized reference provenance separately from local implementation evidence, using evidence kinds such as `reference_capture` and `conformance`. A remote success supplies expected behavior; it does not establish BrickHouse support. Reference versions belong in fixture provenance and must not be presented as tested BrickHouse integration versions before local checks pass. Routine tests consume stored fixtures without personal credentials or remote execution.

## Implemented validation

Check schema fields and allowed values, unique IDs, dependency references/cycles, plan links, capability task references, and nonempty evidence for done/supported claims. Validate parent/child references, inherited prerequisites, complete/in-range parent-criterion coverage, release/stage inheritance, the B049 stability barrier, and completion of all required children before a parent can be done. Reject accidental unsupported version guesses. The planning pack was checked for JSON syntax, references, cycles, and local document links before handoff; this is not an implementation test result.

C059–C064 are planned post-stability deployment capabilities. Their deployment profile names are planning targets, not current settings or tested combinations. They start with no tested versions, test IDs or evidence. Existing C001–C058 claims are unchanged by B047.

## Optional native cluster track

B056 records source-grounded planning only, assigned to 0.1 with no new implementation child, workload gate or emulator dependency. B057–B062 / SC.0–SC.5 are optional future epics after B049 stability. [Document 19](../docs/plan/19-sparkless-cluster-execution.md) and [the upstream specification](../docs/upstream/sparkless-cluster-contracts.md) define K0–K5; B062 requires both B061 and B053. Existing emulator and self-hosted stages retain their original dependency graph. C065–C067 are planned only, with no tested versions, test IDs or evidence. Plan validation assigns SC stages separately and checks the stability and integration barriers.

## Open-source research inventory

[oss-reuse.json](oss-reuse.json) is a **research snapshot, not a runtime lock**. B063 records 22 exact Python package candidates (including existing retained pins and three transitive candidates), two checksum-verified native crate snapshots and one pinned UC source tree. Entries retain metadata/source/archive/member hashes, declared licenses, dependency/wheel observations, owning task IDs and reuse decisions. Static constraint comparisons record both conflicts and compatible bounds without claiming complete resolution.

Offline planning validation checks provenance and ownership and prevents research candidates from acquiring runtime-qualified status in this inventory. Ordinary tests never refresh metadata or run the public verifier. B017.1 qualifies the pinned optional notebook dependencies with real local kernels; B017.2 qualifies the marked Python source-export subset and handlers; B017.3 qualifies local child run/exit isolation, timeout propagation and process cleanup; B017.4 verifies the selected profile contexts; B017.5 verifies selected SDK notebook-task profiles and compute interruption against real local kernels. Other candidate adoption still requires owning-task install/native/behavior/recovery evidence and separate capability promotion. The inventory now has **67 top-level packages**, **142 required phase 0.1 children** and **67 capabilities**; B063 itself remains planning-only.

## Release qualification manifests

`release-requirements.json` maps all 27 document-16 case families to 63 runnable selectors and declares every required OS/Python/lane/profile gate. The planning validator checks these references against the plan and actual Python definitions. `client-artifacts.json` pins official CLI, Terraform, and provider archives with SHA-256 checksums for both platforms. The connector environment has a separate universal hash lock in `requirements/sql-connector.txt`. B066 records final-release qualification; current results must be tied to the exact built artifact.

The `python_provisioning` client manifest entry pins uv 0.13.5 and exact CPython 3.11.17/3.12.15 qualification runtimes. The UV wheel has its own hash lock in `requirements/qualification-tools.txt`. Provisioning is a CI preparation step, not a job-time installer or BrickHouse runtime dependency; the lane retains native interpreter/version/hash evidence before OS-enforced offline tests.

The final release validator also checks every capability test ID and synchronizes readable backlog statuses with the canonical ledger. The 0.1.0 case map has 27 families and 63 unique selectors; capability states remain unchanged by evidence/version corrections. Exact final source/artifact counts belong to the excluded release bundle.
