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

The owner authorized choosing the project license on 2026-10-22; BrickHouse 0.1.0 uses Apache-2.0, with the full root LICENSE and SPDX distribution metadata. Dependencies retain their own terms and notices. Confirm licenses and dependency terms at the pinned release, including native binaries. Do not label all optional services as permissively licensed without checking the actual artifact.

If a later decision accepts source adaptation, track divergence with a small patch ledger and contract tests. Re-evaluate upgrades periodically, but never automatically overwrite copied files. Outreach, issues, and PRs require the user's authorization to send messages; no upstream submission is required to execute this plan.

## Candidate disposition

| Dependency | Proposed role | Adoption boundary |
|---|---|---|
| Sparkless | Default DataFrame and Spark SQL semantics; runtime/developer pin `4.15.2` | The B046 provider report records the earlier 4.14.1 Delta/null-struct qualification. The 4.15.0 release added public cancellation, bounded Arrow batches, and structured provider errors; B011.3.3 remains version-specific evidence for SQL cancellation. On 2026-10-22 BrickHouse advanced to the hash-locked 4.15.2 artifact, corrected the notebook worker's exact environment pin, and requalified the current wheel in the recorded base and notebook-extra G1–G4 macOS arm64 matrix. The selected UTC-aware Delta timestamp predicate passes with 4.15.2. This remains local behavior evidence; broader Spark/Databricks parity is incomplete. [Provider evidence](../../artifacts/conformance/B046.3-sparkless-4.14.1-provider.md), [4.15.0 cancellation evidence](../../artifacts/conformance/B011.3.3-sparkless-4.15.0-cancellation.md), [current candidate matrix](../../artifacts/releases/0.1.0/README.md) |
| Python `deltalake` | Core local table service and selected Sparkless/worker writer path | B004.1 exercised create/read/append/update/delete/merge at 1.2.1 against local files. B004.2 passed a typed cross-read using Sparkless 4.13.4 and Python `deltalake` 1.2.1, including exact decimal(12,2), null/date/timestamp/list/struct values; prior 4.13.3 regressions #1683/#1684/#1686 passed their local repros on 4.13.4. B004.3 verified local protocol reader 1/writer 2, transaction identity through checkpoint/restart, and Sparkless cross-read. B010.2 routes selected registered writes through the journaled service. Raw repeated provider writes duplicate rows, so the BrickHouse table service must reconcile transaction versions before retry. Cloud, concurrent external writers, advanced features, and configured transaction expiration are unverified; see [ADR 0008](../decisions/0008-delta-mutation-and-recovery.md). |
| SQLGlot `30.24.0` | Core dependency for the selected Databricks/Spark AST parser and binder | B011.1 hash-verifies the universal wheel and tests the parser-only role; it is not the query engine or proof of Spark semantics. Strict capability checks; no string-based mutation rewrites |
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

## B063 research-backed adoption refinement

[Document 20](20-open-source-reuse.md), [the pinned source report](../upstream/oss-reuse-inventory.md) and [the machine-readable snapshot](../../planning/oss-reuse.json) replace broad candidate assumptions with exact research versions, artifact/license/source hashes, runtime boundaries and owning qualification tasks. The B063 snapshot is historical research, not a runtime lock. The developer Sparkless pin advanced to 4.15.0 on 2026-10-21 and to 4.15.2 on 2026-10-22; deltalake 1.2.1, PyArrow 23.0.1 and tested client pins remain unchanged. The 4.15.0 cancellation report remains version-specific; current candidate qualification is recorded in the [0.1.0 bundle](../../artifacts/releases/0.1.0/README.md).

B011.1 qualifies `sqlglot==30.24.0` as a core parser/binder dependency after verifying the recorded universal wheel hash and the selected Databricks-dialect AST behavior. The service executes approved reads through Sparkless and applies DDL through BrickHouse catalog/table services; SQLGlot transpilation is not an execution route or a semantic fallback. The package's MIT notice and artifact hashes are retained in [THIRD_PARTY_NOTICES.md](../../THIRD_PARTY_NOTICES.md) and the development lock.

Prefer direct nbclient/nbformat/ipykernel execution and a real optional MLflow server/registry rather than duplicating those subsystems. Papermill is conditional. Keep dbt-duckdb first; the inspected dbt-databricks 1.12.7 rejects current SDK 0.153.0 and connector 4.6.0 and needs a separate locked client environment for any selected conformance profile. Delta Sharing 1.4.2 brings a native Kernel wrapper and an Arrow upper bound, so wheel/feature/client matrices are explicit gates. UC OpenAPI/client contracts are reusable inputs; its Java server remains excluded.

Treat PyIceberg/Delta Kernel as optional native-provider feasibility in B040/B041, and Ballista/DataFusion as concrete SC.0 comparisons behind B049. No default format, semantic engine, mutation provider or fallback policy changes. All still-planned owning epics retain their original criteria and gain concrete qualification requirements; no new feature is required by the alpha corpus.

## Dependency selection gates

M0 records exact versions, wheels, Python bounds, licenses, and a reproducible lockfile. Verify Linux/macOS native compatibility; do not infer it from an import working on one machine. Avoid installing the latest unconstrained release in a compatibility claim.

Base runtime must not depend on `pyspark`, `delta-spark`, Java drivers, Spark containers, or remote Databricks compute. Optional clients named similarly need inspection: a pure Python protocol client is acceptable, a bundled Spark runtime is not.

DuckDB extensions, model weights, and optional runtimes may download artifacts. Distinguish **JVM-free**, **cloud-independent**, and **offline-ready**. An offline mode must use pre-provisioned artifacts and fail with an actionable message instead of attempting hidden network access.
