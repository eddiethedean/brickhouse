# 03 — Dependencies and Minilake strategy

## Ownership and adoption policy

Sparkless is a coordinated upstream: the owner can add reusable engine hooks and release them. BrickHouse must still pin a real tested version; it cannot assume that ownership makes an unpublished interface available to users.

Minilake and all other dependencies are independently maintained. Build on public APIs where sufficient. Do not copy Minilake implementation by default; if a later, specific adaptation is proposed, pin the file and revision, preserve copyright and provenance, and pass the contract tests before accepting it. Do not make an unmerged upstream PR a blocking runtime dependency.

## Proposed Minilake decision

**Decision: implement BrickHouse-owned catalog, jobs, and files services; do not adapt Minilake implementation code for phase 0.1.** B002 inspected the exact pinned source and recorded the reuse boundary in [ADR 0004](../decisions/0004-minilake-reuse-boundary.md).

Why: the reviewed Minilake app has module-global service state, its own persistence lifecycle, managed DuckDB storage, Spark-oriented job execution, and generated Spark Delta-write scripts. Mounting that app wholesale would create competing state authorities and preserve execution paths incompatible with BrickHouse's requirements. This is a design inference from inspected code, not a claim that Minilake should adopt BrickHouse's scope.

Candidates for research only: request/response shapes, endpoint conventions, SDK workflow examples, workspace/DBFS operations, job DAG/lifecycle ideas, and selected validation/error behavior. Re-express relevant behavior in BrickHouse-owned types and routes. Avoid carrying a second catalog or job scheduler just to reuse a route.

Alternatives to evaluate in B002:

| Choice | Appropriate condition |
|---|---|
| Use Minilake publicly as a separate optional reference server | Conformance comparison and development experiments |
| Depend on a future Minilake plugin API | Released extension points actually cover catalog, execution, and persistence without patches |
| Maintain a complete fork | Only if selective adaptation costs more and the team accepts ongoing fork maintenance |
| Reimplement a small endpoint from the public contract | Simpler than adapting tightly coupled source |

B002 outputs `docs/upstream/minilake-inventory.md` and ADR 0004: pinned commit, files/contracts, license, state dependencies, required removals, and owned replacement. Remove dead Spark paths rather than merely setting an environment variable that could be overridden.

## Source and license hygiene

Minilake reference: commit `0597285d1ba33daf76f553b82a32024ded7b78f6`, MIT, copyright Rafael Sales (2026). No code from this revision is copied. If a future change includes source, verify exact files and terms first; retain original notices and add `THIRD_PARTY_NOTICES.md` plus a machine-readable inventory before shipping. The current package contains no Minilake implementation.

Choose BrickHouse's project license with the owner before publishing; MIT is the proposed default, not a license declaration by this plan. Confirm licenses and dependency terms at the pinned release, including native binaries. Do not label all optional services as permissively licensed without checking the actual artifact.

If a later decision accepts source adaptation, track divergence with a small patch ledger and contract tests. Re-evaluate upgrades periodically, but never automatically overwrite copied files. Outreach, issues, and PRs require the user's authorization to send messages; no upstream submission is required to execute this plan.

## Candidate disposition

| Dependency | Proposed role | Adoption boundary |
|---|---|---|
| Sparkless | Default DataFrame and Spark SQL semantics; developer candidate `4.14.0` | Published public Python provider API verified; 56 release tests and 14 selected local integration checks pass, but nullable-struct bugs #1694/#1695 keep B046.3/B010 blocked. [Evidence](../../artifacts/conformance/B046.3-sparkless-4.14.0-provider.md); production adapter pending |
| Python `deltalake` | Integrated optional local table service; Sparkless routing pending | B004.1 exercised create/read/append/update/delete/merge at 1.2.1 against local files. B004.2 passed a typed cross-read using Sparkless 4.13.4 and Python `deltalake` 1.2.1, including exact decimal(12,2), null/date/timestamp/list/struct values; prior 4.13.3 regressions #1683/#1684/#1686 passed their local repros on 4.13.4. B004.3 verified local protocol reader 1/writer 2, transaction identity through checkpoint/restart, and Sparkless cross-read. Raw repeated provider writes duplicate rows, so the BrickHouse table service must reconcile transaction versions before retry. Cloud, concurrent external writers, advanced features, and configured transaction expiration are unverified; see [ADR 0008](../decisions/0008-delta-mutation-and-recovery.md). |
| SQLGlot | Databricks/Spark AST parsing and optional translation | Strict capability checks; no string-based mutation rewrites |
| DuckDB | Optional tested query backend | Shared committed files; no separate physical managed-table catalog |
| Databricks SDK/CLI/Terraform provider | Client conformance targets | Pin exact versions and test real requests |
| nbclient/nbformat/ipykernel | Notebook execution | Worker lifecycle and explicit magic handlers |
| Papermill | Optional parameter injection | Add only if its execution conventions fit the runner |
| fsspec | Python filesystem adapters | Not an automatic bridge to Rust IO |
| MLflow | Real tracking and registry service | Keep its authoritative stores; adapter for Databricks-facing routes |
| dbt-duckdb | Optional dbt execution | Separate local adapter profile; do not claim dbt-databricks parity |
| LanceDB | Optional embedded vector engine | Implement Databricks index lifecycle/query contract separately |
| dltHub `dlt` | Possible ingestion provider | Distinct from Databricks DLT/Lakeflow APIs |
| Quix Streams | Evaluate later for event streaming | Broker, windows, and checkpoint semantics require matching |
| PyCasbin | Optional permission evaluator | Model identities/inheritance and invoke checks in all access paths |
| Moto/Azurite | Optional storage conformance services | Pin API coverage; Azure Blob emulation is not assumed ADLS Gen2 parity |

## Dependency selection gates

M0 records exact versions, wheels, Python bounds, licenses, and a reproducible lockfile. Verify Linux/macOS native compatibility; do not infer it from an import working on one machine. Avoid installing the latest unconstrained release in a compatibility claim.

Base runtime must not depend on `pyspark`, `delta-spark`, Java drivers, Spark containers, or remote Databricks compute. Optional clients named similarly need inspection: a pure Python protocol client is acceptable, a bundled Spark runtime is not.

DuckDB extensions, model weights, and optional runtimes may download artifacts. Distinguish **JVM-free**, **cloud-independent**, and **offline-ready**. An offline mode must use pre-provisioned artifacts and fail with an actionable message instead of attempting hidden network access.
