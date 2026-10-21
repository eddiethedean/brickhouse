# B044 — Phase 0.1 delivery refinement

Date: **2026-10-20**. Kind: planning and dependency review. This is not implementation or Databricks conformance evidence.

## Inputs reviewed

- README, STATUS, the architecture/dependency plans, documents 16–17, the canonical task/capability manifests, and the actual package, workspace config and worker source.
- Existing evidence that B009's tested provider hooks have no Python binding, and that B021's released local REST/kernel transport is not yet proven.
- Parent/child prerequisites and acceptance ownership for statement/warehouse, job/compute, notebook interruption and deployment-client work.

## Refinement

[ADR 0017](../../docs/decisions/0017-phase-0.1-delivery-refinement.md) records new early tasks B045/B046, seven bounded implementation children and the provider/consumer completion boundaries. [Document 16](../../docs/plan/16-phase-0.1-initial-release.md) now enumerates release variants, local test limits, case owners, implementation work lanes and final release stop conditions.

All prior parent acceptance criteria, task states and evidence are preserved. Checks moved out of resource/context children are explicitly required in their new integration children. No compute mode, notebook workflow, SQL mutation, client profile, recovery check or release artifact requirement is removed. Capability statuses remain unchanged.

## Checks and limitations

Run `python scripts/validate_planning.py` and `git diff --check`. The validator checks the dependency graph including parent completion edges, inherited prerequisites, full parent-criterion coverage, release assignment, backlog agreement and local document links. A comparison with the pre-edit snapshot additionally checks that prior task states/evidence and parent criteria/prerequisites remain intact. Final results are recorded in [planning/validation.json](../../planning/validation.json).

No runtime test, live Databricks request, dependency installation, Sparkless rebuild, or upstream modification is needed for this planning refinement. B044's completion establishes an actionable plan only; B045, B046 and all other implementation work require their own evidence.

## Completed planning checks

- `python scripts/validate_planning.py`: passed for 126 task entries, 46 work packages, 80 required children and 56 capabilities, including acyclic dependencies, inherited prerequisites, full parent-criterion coverage and release assignments.
- Backlog definitions agree with the ledger; all local Markdown links resolve. The exact final link count is in planning/validation.json.
- Snapshot comparison retained all 116 previous task states/evidence, every existing prerequisite edge, all previous parent criteria and all 56 capability states/versions/test evidence.
- Reviewed 27 unique acceptance families with valid task owners and seven explicitly enumerated Python variants. The notebook corpus is the same seven ipynb selections plus three source-export profiles.
- `git diff --check`: passed. No runtime source or runtime compatibility claim is changed by B044.
