# 03 — Dependencies and Minilake strategy

## Ownership and adoption policy

Sparkless is a coordinated upstream: the owner can add reusable engine hooks and release them. BrickHouse must still pin a real tested version; it cannot assume that ownership makes an unpublished interface available to users.

Minilake and all other dependencies are independently maintained. Build on public APIs where sufficient. Where Minilake source is needed, adapt a deliberately selected, pinned subset under its license, keeping copyright and provenance. Do not make an unmerged upstream PR a blocking runtime dependency.

## Proposed Minilake decision

**Default: targeted source adaptation into BrickHouse-owned services, not a runtime dependency on the unmodified Minilake app.** B002 confirms the initial module inventory before copying anything.

Why: the reviewed Minilake app has module-global service state, its own persistence lifecycle, managed DuckDB storage, Spark-oriented job execution, and generated Spark Delta-write scripts. Mounting that app wholesale would create competing state authorities and preserve execution paths incompatible with BrickHouse's requirements. This is a design inference from inspected code, not a claim that Minilake should adopt BrickHouse's scope.

Candidates for reuse: request/response shapes, endpoint conventions, SDK workflow examples, workspace/DBFS operations, job DAG/lifecycle ideas, and selected validation/error behavior. Each copied route must call BrickHouse's domain services and instance-scoped repositories. Avoid carrying a second catalog or job scheduler just to reuse a route.

Alternatives to evaluate in B002:

| Choice | Appropriate condition |
|---|---|
| Use Minilake publicly as a separate optional reference server | Conformance comparison and development experiments |
| Depend on a future Minilake plugin API | Released extension points actually cover catalog, execution, and persistence without patches |
| Maintain a complete fork | Only if selective adaptation costs more and the team accepts ongoing fork maintenance |
| Reimplement a small endpoint from the public contract | Simpler than adapting tightly coupled source |

B002 outputs `docs/upstream/minilake-inventory.md`: commit, file, license, intended reuse, tests, state dependencies, required changes, and owned replacement. Remove dead Spark paths rather than merely setting an environment variable that could be overridden.

## Source and license hygiene

Minilake reference: commit `0597285d1ba33daf76f553b82a32024ded7b78f6`, MIT, copyright Rafael Sales (2026). Verify the exact files before reuse. Keep original notices in adapted files and include `THIRD_PARTY_NOTICES.md` plus a machine-readable inventory when code enters the repo. This pack does not itself include copied Minilake implementation.

Choose BrickHouse's project license with the owner before publishing; MIT is the proposed default, not a license declaration by this plan. Confirm licenses and dependency terms at the pinned release, including native binaries. Do not label all optional services as permissively licensed without checking the actual artifact.

Track upstream divergence with a small patch ledger and contract tests. Re-evaluate upgrades periodically, but never automatically overwrite adapted files. Outreach, issues, and PRs require the user's authorization to send messages; no upstream submission is required to execute this plan.

## Candidate disposition

| Dependency | Proposed role | Adoption boundary |
|---|---|---|
| Sparkless | Default DataFrame and Spark SQL semantics | Public session/Arrow/catalog/writer hooks; coordinated releases |
| Python `deltalake` | Initial candidate for authoritative mutations | Verify feature support and compatibility with Sparkless's Rust delta-rs reader |
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
