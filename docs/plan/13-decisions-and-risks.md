# 13 — Decisions and risks

## Adopted planning defaults

These decisions guide implementation until evidence justifies a recorded change. They are not claims of completed software.

| ID | Decision | Rationale / revisit condition |
|---|---|---|
| D001 | One `brickhouse` distribution with internal subpackages and optional extras | Reduce version skew; split only at an independently useful stable boundary |
| D002 | Sparkless is the default semantic engine | Owner can supply generic missing hooks; requires actual upstream artifact and conformance |
| D003 | Selectively adapt pinned Minilake code/contracts | Avoid its competing catalog and Spark launch paths; confirm inventory in B002 |
| D004 | SQLite metadata plus real Delta managed/external tables | Shared table identity and data across interfaces; no duplicate DuckDB physical catalog |
| D005 | One mutation provider per workspace/profile | Consistent commits and recovery; B004 selects Python deltalake or exposed Rust provider |
| D006 | Child process per job, explicit scoped worker context | Isolate state/import facades and enable cancellation; not a hostile-code sandbox |
| D007 | Explicit capabilities and unsupported errors | Compatibility must be observable, testable, and versioned |
| D008 | Start with a Spark 3.5-based local profile | Matches reviewed default; add 4.x separately after evidence |
| D009 | Base installation has no Java/Docker/cloud requirement | Optional services cannot enter a claimed base execution path |
| D010 | Local trusted-user workspace first | Multi-user governance and sandboxed code need additional engineering |
| D011 | Personal Databricks account supplies occasional behavioral reference captures; routine tests stay local | Owner authorized reference verification and test design; remote latency favors sanitized reusable fixtures. See [ADR 0001](../decisions/0001-personal-databricks-reference-verification.md) and [capture plan](15-personal-databricks-verification.md) |

## Open decisions with owners and gates

| Question | Task / owner | Required decision artifact |
|---|---|---|
| Which Minilake files, if any, are worth adapting? | B002 / BrickHouse | Module inventory, coupling analysis, notices and selected endpoint contracts |
| Which Sparkless extension shape is viable? | B003+B009 / coordinated Sparkless | Small proven provider interface, concurrency/GIL/error contract, artifact reference |
| Python deltalake or Rust binding for mutations? | B004 / BrickHouse+Sparkless | Interoperability, protocol limits, uncertain-commit behavior, wheel/memory costs |
| Which SDK/CLI/Terraform/SQL connector versions? | B005 / BrickHouse | Exact pins and request fixtures; REST connector transport probe |
| Which personal-workspace host, features, and existing compute are permitted for captures? | B005 / BrickHouse+owner configuration | Verified non-secret capture profile, bounded resource policy, and recorded account limitations; token source already owner-supplied |
| How broad is initial SQL? | B019 / BrickHouse | Explicit operation/type corpus with mismatches, no fallback ambiguity |
| How will optional services share identities and data? | M4 epics / BrickHouse | Per-provider authority and lifecycle design before implementation |
| Which streaming provider and delivery guarantees? | B030–B035 / BrickHouse | Replay/state proof for chosen source/sink/operator combinations |
| Project license and distribution ownership? | Owner before public release | Confirmed license, notices, package/repository ownership |

Proceed with reversible work using these defaults. Escalate a decision only when evidence changes scope, creates an external commitment, or requires owner-specific information. Missing final publication details do not block implementation planning.

## Risk register

| Risk | Consequence | Mitigation / evidence gate |
|---|---|---|
| Semantic differences between engines | Locally passing tests give incorrect results | Sparkless default, deterministic routing, differential fixtures, partial capability flags |
| Personal reference account is slow, changes, or lacks a feature | Feedback stalls or one account's behavior is overgeneralized | Occasional small probes, versioned sanitized fixtures, explicit feature/profile limits; routine tests run locally |
| Personal credentials or account data reach routine CI/artifacts | Credential exposure or accidental remote execution | Ignore .env/raw captures, isolate capture-job secrets, bind local clients explicitly, redact before upload |
| Catalog/data divergence after crash | Missing or duplicated table results | Operation journal, versioned handles, recovery and uncertain-commit tests |
| Delta feature mismatch | Silent data corruption or wrong reads | Feature detection/rejection before execution; cross-provider fixtures |
| Upstream APIs too narrow | Integration depends on brittle internals | Generic owned Sparkless hooks; contained external adapters; avoid required external PRs |
| Minilake adaptation expands into an unmanaged fork | Upgrade cost and hidden Spark paths | Small provenance inventory, delete unused execution code, contract tests |
| Arrow conversions materialize large inputs | Memory blowups and slow jobs | Batch bounds, spill/staging, measured type fidelity and memory |
| Process-global state leaks | Different jobs or sessions interfere | Instance-scoped services, child workers, concurrent isolation tests |
| Import facade masks unsupported behavior | Incorrect claims of PySpark compatibility | Worker-only activation, explicit namespaces and error cases |
| Client transport assumptions are wrong | SDK works while SQL connector/dbt fails | Pin real clients; REST/Thrift/Connect tracked independently |
| Retries repeat side effects | Duplicate data or external actions | Idempotent table operations; no blanket job exactly-once claim |
| External wheels/services change | Installation or JVM-free guarantee breaks | Version matrix, clean install CI, dependency/process inspection |
| Scope grows faster than evidence | Large facade with little useful execution | Vertical gates, task decomposition, keep deferred frontier visible |
| Local access mistaken for production security | Service policy bypass via Python/filesystem | Trusted-local profile documented; separate governance/security work |

## Decision change protocol

Write an ADR under `docs/decisions/` with context, options, evidence, chosen contract, consequences, and migration impact. Link it from this document, affected task entries, and capability records. Preserve the original planning rationale in Git history. A failed experiment can close a research task when its deliverable is a decision; it cannot close an implementation task that promises working behavior.
