# BrickHouse implementation instructions

## Context and reading order

BrickHouse is a new Python project targeting broad Databricks behavioral emulation without a JVM. The owner maintains Sparkless. Minilake and other libraries are third-party projects. This file governs work in the new BrickHouse repository; it grants no permission to publish releases or spend money. The owner's existing authorization for bounded personal Databricks reference probes is recorded below and in document 15; other cloud workloads require separate authorization.

Before implementation, read README.md, STATUS.md, docs/plan/00-vision-and-scope.md, docs/plan/01-architecture.md, docs/plan/03-dependencies-and-minilake.md, and the active task's referenced documents. Treat all proposed interfaces as contracts to refine through tests, not existing APIs.

## Work selection

- Use planning/tasks.json as the task-state ledger and docs/plan/09-task-backlog.md for detailed acceptance criteria.
- Decompose epics and oversized work packages into bounded child tasks before implementation; preserve the parent acceptance criteria.
- Start at B001. Claim one ready task by marking it `in_progress`; dependencies must be `done` before dependent implementation starts.
- A task is `done` only when its listed acceptance criteria and relevant tests pass. Record concrete evidence and commit references when commits exist.
- If blocked by Sparkless work, document the missing contract, continue independent ready tasks, and do not simulate successful execution.
- Update STATUS.md, planning/tasks.json, relevant capability entries, and any changed design decisions in each completed slice.
- Re-read actual code and repository instructions before changing Sparkless or another checkout. Ownership permits coordination, not inventing an unverified API or silently publishing it.

## Architectural rules

- No JVM, Java process, Spark driver, `spark-submit`, or hidden remote Spark fallback in BrickHouse execution. Pure Python protocol clients are permitted only with verified native execution behind them.
- One authoritative catalog and one table-storage contract. SQL and DataFrames must see the same table IDs, locations, schemas, and committed versions.
- Keep API transport, domain services, persistence, engine adapters, and job execution separate. Request models must not own business logic.
- Sparkless is the default DataFrame and Spark-semantics engine. DuckDB is an explicit execution option behind capability checks, not a silent retry when Sparkless fails.
- Mutations go through the table service and declared native Delta provider. Never edit `_delta_log` manually or implement mutations as unprotected read/overwrite cycles.
- Session configuration, UDFs, current namespaces, temporary views, and task context must not leak between sessions or jobs.
- Jobs run in fresh child processes. Import compatibility is scoped to the worker. Never globally replace installed `pyspark`, `databricks`, or `delta` packages in a user's environment.
- New third-party source must have pinned provenance and retained notices. Do not copy a running Minilake server with its Spark launch paths into BrickHouse and call it JVM-free.
- Do not depend on private third-party APIs without an explicit, tested, contained adaptation decision.
- Start with one `brickhouse` distribution and optional extras. Internal modules are Python subpackages; splitting a wheel requires the package-boundary criteria in document 02.

## Compatibility and tests

- Unsupported operations must raise a specific Python error or a version-appropriate API error. Never return fabricated success, empty data, a constant, or a skipped job as an implementation of requested behavior.
- Intentional simulated infrastructure, such as cluster lifecycle metadata, must be labeled `simulated` in the capability registry and exposed diagnostics.
- Test behavior using real local files, Delta tables, SDK clients, child processes, and failure/restart scenarios. Mock only external failures or boundaries where a real dependency is impractical.
- Every compatibility claim requires exact client/profile versions and evidence. An endpoint responding 200 is not proof of behavioral parity.
- Use the owner's personal Databricks account for occasional bounded reference probes to verify representative code and design local tests, as authorized on 2026-10-16 and specified in docs/plan/15-personal-databricks-verification.md. Routine tests and required CI consume sanitized fixtures locally; they never run against that account.
- Keep Spark/Databricks reference capture separate from JVM-free runtime CI and execution. The existing authorization covers the documented probes using permitted existing resources; paid provisioning or expanded spend requires separate authorization. Token availability alone does not establish a compute budget.
- Use focused tests for each change, then the milestone gates. Do not invoke unrelated full Sparkless rebuilds for documentation-only BrickHouse work.
- Store credentials outside tracked files. DATABRICKS_TOKEN is owner-supplied through ignored local .env or the GitHub Actions secret and is available only to explicit reference capture. Ordinary tests must not load .env or discover real-workspace credentials. Redact job secrets and account identifiers from logs and diagnostic artifacts.

## Completing work

Report user-visible behavior, relevant test results, remaining limitations, and changed capability claims. Update implementation documentation to describe current behavior; retain a clearly marked future roadmap. Do not claim a milestone is complete based on scaffolding, mock responses, or planned dependencies.
