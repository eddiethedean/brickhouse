# 20 — Native open-source reuse and qualification

Research snapshot date: **2026-10-21**. Planning/research task: B063. Decision: [ADR 0038](../decisions/0038-native-open-source-reuse.md). Exact metadata, artifact hashes, license observations, source paths and owner IDs: [research inventory](../../planning/oss-reuse.json). Readable evidence: [source report](../upstream/oss-reuse-inventory.md).

## Priority and decision boundary

BrickHouse remains a **JVM-free Databricks behavioral emulator first**. Reuse working native/Python components rather than reimplementing formats, notebook machinery or model registries. BrickHouse still owns the shared catalog/table contract, Databricks-facing behavior, authorization semantics, session/job lifetimes and truthful unsupported errors. An upstream project's feature list is not a compatibility claim.

Phases 0.1–0.9, the B049 stability gate, SH deployment stages and optional SC cluster stages retain their existing behavior requirements. This research installs no packages, changes no project dependency pins and modifies no upstream source. A version listed below is an **exact research candidate**, not an adopted runtime lock. Existing tested pins remain until an owning implementation slice qualifies a deliberate change.

The inventory covers both Databricks-origin projects and independently maintained ecosystem/native alternatives. It is not a claim that every package is a Databricks runtime dependency. The 22 Python package snapshots include three transitive components; two native crates and one pinned Unity Catalog source tree are inspected separately. Only sampled archives were downloaded, hash-verified and read statically. Wheel names/metadata do not establish installation or platform support, and no upstream package code was executed.

Historical implementation inventory snapshot, 2026-10-21: BrickHouse then pinned `sparkless==4.15.0`; its macOS arm64 wheel was installed and exercised for B011.3.3 cancellation. That snapshot establishes only the selected historical artifact result. The current pin is Sparkless 4.15.2, and the 0.1.0 owner-review candidate passed its selected local G1–G4 matrix. See the current qualification update below and the [candidate inventory](../../artifacts/releases/0.1.0/README.md).

## Adoption decisions

| Component / research snapshot | Decision and boundary | Owner / timing |
|---|---|---|
| Sparkless **4.14.1** (B063 research snapshot) | The B063 sampled release supplied public Python provider and nullable-struct evidence. BrickHouse's current runtime pin is 4.15.0; neither release establishes SQL/job/cluster completeness. | B010/B046; SC work stays behind B049 |
| `deltalake` **1.2.1** | Retain the native mutation provider and recovery journal. Qualify features individually; never choose a newer engine silently after failure. | B008/B019/B022 |
| PyArrow **23.0.1** | Retain typed IPC/staging/Parquet. Validate decimal/nested validity/timezone boundaries and bounded materialization. | B008/B010; B036/B039 for clients |
| SQLGlot **30.24.0** | B011.1 qualifies the core parser/AST/binder role. Transpilation is not a query engine or semantic proof. | B011.1/B019; existing 0.1 corpus |
| DuckDB **1.5.6** | Qualify only explicitly selected query/dbt routes over committed data. No independent managed-table authority or error-driven fallback. | B011/B027 |
| `nbclient` **0.11.0**, `nbformat` **5.11.1**, `ipykernel` **7.4.0**, `jupyter-client` **8.10.0** | Direct pinned optional notebook runtime. B017.1 verifies real fresh-kernel parameter injection, scoped Spark/dbutils, output artifacts and failures with Sparkless 4.14.1. BrickHouse still owns Databricks source conversion, magics, widget precedence, notebook run/exit and cancellation. | B017.1 evidence; remaining behavior in B017.2–B017.5 |
| Papermill **2.7.0** | Conditional only: adopt if direct nbclient leaves a concrete parameterization gap. Do not add a second notebook runner by default. | B017 decision during decomposition/implementation |
| MLflow **3.17.1**, matching skinny/tracing **3.17.1** | Use real optional local tracking, database-backed registry and selected Python-model execution. Preserve MLflow's store/artifact authority. Skinny alone does not supply the full selected service. | B025/B026 in 0.3; hosted UI/providers B051 later |
| Databricks SDK **0.153.0**, SQL connector **4.6.0** | Retain tested/researched client profiles. Keep REST/kernel and Thrift evidence separate. Client models are not domain services or server implementations. | B005/B020/B021/B037 |
| `pyspark-client` **4.2.0** | Future pure-Python Connect client candidate in a separate conformance environment. Full `pyspark`, its connect extra and a Spark server remain excluded from the native execution path. | B036 in 0.7; no new Connect requirement for 0.1 |
| Delta Sharing **1.4.2**, native wrapper **0.3.3** | Use the open protocol and independent non-Spark client for a native BrickHouse server. Reject/avoid Spark-specific reader methods. Qualify native wrapper artifacts and feature/credential/lifetime behavior explicitly. | B039 in 0.7 |
| Unity Catalog client **0.7.0** and pinned OSS OpenAPI source | Reuse as versioned contract/client inputs. Keep one BrickHouse catalog and explicit mapping to selected Databricks APIs. Do not mount the Java server or assume OSS/managed UC parity. | B028/B040; no replacement of existing catalog |
| PyIceberg **0.12.0** | Deferred optional-format feasibility under the platform inventory. Keep Iceberg and Delta provider identities distinct; no automatic dual write or extra catalog authority. | B040/B041 in 0.8, only a selected tested slice |
| Delta Kernel Rust **0.30.0** | Research native scan/feature/provider integration, not a wholesale replacement for delta-rs or an assumed distributed sink. Its API/version/features need exact probes. | B040/B041 and SC.0/SC.3 when applicable |
| DataFusion Python **55.0.0** / Ballista Rust **55.0.0** | Concrete SC.0 alternative to building equivalent native scheduling/exchange infrastructure. Preserve Sparkless's semantic frontend/default and prove every selected operator/commit path. No backend is selected by this research. | B057 after B049; integration still requires B061/B053 |

Exact metadata and archive license declarations in the snapshot are MIT, Apache-2.0 or BSD-3-Clause for these candidates. That is an artifact-specific observation, not a license statement about every transitive component, optional integration or future release. Before shipping, retain all required source/binary notices and audit the resolved graph. Research has copied no third-party implementation into BrickHouse.

## Concrete compatibility findings

### Keep client environments separate when their constraints conflict

The inspected `dbt-databricks==1.12.7` metadata requires **`databricks-sdk>=0.68.0,<0.118.0`** and **`databricks-sql-connector[pyarrow]>=4.1.1,<4.4.1`**. Both reject BrickHouse's current SDK **0.153.0** and connector **4.6.0** pins. These are observed metadata conflicts, not a failed runtime test. Its source uses the SQL connector and dbt-spark adapter abstractions; the dependency name alone neither proves a JVM launch nor proves a native path.

Retain dbt-duckdb as the first planned dbt profile. If B027 selects a dbt-databricks client workflow, create a separately locked environment with its actual compatible SDK/connector/dbt packages, point it explicitly to BrickHouse, and qualify that transport/macro/SQL subset independently. Do not downgrade the core client pins, strip dependency bounds or infer the B021 connector result covers a different version. Python model, Spark/session and remote job paths require separate rejection or real native execution evidence.

### Arrow and native wrapper versions need a joint matrix

Delta Sharing **1.4.2** caps PyArrow at **23.0.1** and requires wrapper **>=0.3.2,<1.0**. The inspected wrapper **0.3.3** embeds Rust Delta Kernel **0.22.0**, Arrow **57** and object_store **0.12.4**. These are not the separately inspected latest Kernel **0.30.0**; Rust Arrow and PyArrow version numbers are not expected to match numerically.

The wrapper snapshot publishes four wheel tags: macOS x86_64/arm64, manylinux_2_28 x86_64, and Windows amd64 with CPython abi3 >=3.10. It does not establish Linux arm64 support; do not force users to install Rust to fill an unadvertised wheel gap. Qualify the exact client/wrapper/PyArrow/native-feature combination, including release-specific TLS/credential behavior, before making a platform claim.

MLflow **3.17.1** declares PyArrow **>=4,<26**; its skinny dependency accepts SDK **>=0.20,<1**. Current Arrow/SDK pins fit those two bounds, but this is not a resolved dependency environment or runtime result. Preserve MLflow's full same-version dependency graph, pin cloud/telemetry behavior explicitly, and verify no `.env` or real workspace credentials can redirect local operations. The presence of a Docker Python dependency is not itself a Docker-daemon requirement; prohibit Docker execution backends in the native base workflow and test actual processes.

### Local notebook and Connect packaging boundaries

The selected notebook metadata supports Python >=3.11; `ipykernel` **7.4.0** requires Python >=3.11. B017.1's real-kernel evidence currently covers Python 3.11.14 on macOS arm64; Python 3.12 and other OS/CPU wheels remain release qualification. Install/provision explicit kernel environments before job execution and choose their interpreter paths directly. No `%pip`, dynamic kernel dependency resolution or shared host-global state is introduced.

`pyspark-client==4.2.0` is a source-distribution-only snapshot on PyPI. Its archive has no JAR/class files and contains the Connect/protobuf modules; build/installation and full transitive runtime are still unqualified. A normal install must not fetch full PySpark or start a Spark server. B036 must test exact protobuf/client behavior, Arrow results, error timing and native server execution. A 4.2 client does not automatically establish Spark 3.5 or any Databricks Runtime semantics. Client namespace collisions with real `pyspark` are handled by isolated environments, not global import replacement.

### Existing native infrastructure deserves evaluation

Ballista supplies native scheduler/executor concepts backed by DataFusion. The inspected **55.0.0** Rust crate declares minimum Rust **1.94.0**, newer than Sparkless's inspected 1.93 baseline. SC.0 must compare an optional separate executable/service boundary versus in-process adaptation and account for toolchain, Arrow/DataFusion version skew, distribution semantics, security, cancellation, spill and sink integration. Do not force a Sparkless toolchain upgrade or Polars replacement just to reuse a scheduler.

Delta Kernel may help scan advanced Delta features, but protocol support, CDF/deletion vectors and writer transactions remain operation/version-specific. Retain delta-rs as the current mutation authority. An extension needs real cross-read/write and uncertain-commit recovery evidence; accepting a feature descriptor or readable file is not sufficient.

## Required interfaces and state authority

| Boundary | Contract to implement/test |
|---|---|
| Notebook provider | Explicit interpreter/environment; kernel-local session/helpers; cell outputs/errors; distinct `%run` and child-run semantics; cancellation/reaping/cleanup |
| MLflow provider | Explicit local tracking/registry endpoints and stores; stable IDs/name mapping; model/artifact scope; lifecycle/health; offline dependency policy; backup/restore hooks |
| dbt adapter | Same catalog/table IDs, committed snapshots and mutation journal; tested SQL/materializations; versioned artifacts; no hidden DuckDB managed copies |
| UC contract adapter | Versioned identifier/schema/ownership/error mapping through existing domain services; policy enforcement once per common operation; no duplicate catalog or Java sidecar |
| Sharing provider | Authorized pinned snapshots, supported feature envelope, file/result lifetime and revocation, bounded delivery and explicit denied/expired outcomes |
| Optional native table provider | Declared format/protocol/version/capabilities, native IO/credentials, stable identity and ownership; independent migration decision before changing defaults |
| Native cluster candidate | Typed plan/operator capabilities, verified local/remote batching, snapshot/commit authority, attempt fencing and qualified artifacts; no semantic fallback |

These are responsibilities, not new public method signatures. Use public dependency APIs; private API access needs its own explicit tested adaptation decision. B028 service policy and later SH authentication/isolation apply to all exposed provider paths. A provider's UI/API may not bypass the shared identity or expose another principal's artifacts just because the BrickHouse entry point authenticates callers.

## Phase and task changes

No new dependency or third-party feature is required to finish 0.1. B011/B017/B021/B023 use this research to select and qualify the dependencies for their already-required workflows. At the B063 planning snapshot on 2026-10-20, 85 required child tasks were recorded; the current phase ledger has **133 required children**: 120 original implementation children, B048.1/.2, and eleven B064 review-fix children. These subsequent task decompositions preserve the original requirements. B064.11 tracks an additional code-review follow-up. Candidate qualification is part of the owning implementation slice; B063's research evidence does not satisfy it. B011.1 has since hash-verified SQLGlot 30.24.0 and qualified its parser/binder use only; all query execution remains through BrickHouse's declared Sparkless path and SQLGlot syntax acceptance alone carries no compatibility claim.

Append concrete qualification criteria to the still-planned extension epics: B025/B026 for MLflow state/environment and native flavor evidence; B027 for separate dbt client locks and shared table effects; B028 for UC contract/policy reuse; B036 for the pure Connect artifact; B039 for native wrapper/sharing lifetime; B040/B041 for bounded optional Iceberg/Kernel decisions. SC.0/B057 must include Ballista/DataFusion in its comparison, with explicit semantic/toolchain/provider gaps before choosing the architecture. These epics still require bounded decomposition before coding.

SH.1/SH.2 continue to qualify actual deployed credentials, authenticated endpoints, provider stores/artifact restore and lifecycle. A working local MLflow/Jupyter service cannot establish production security. All actual deployment/cluster implementation remains gated on B049, and SC.5 still requires both B061 and B053.

## Qualification and promotion gates

1. **Resolve the exact environment.** Pin selected package/source/native artifacts, extras and hashes. Test supported Python/OS/CPU wheels and record any source-build prerequisite. Keep incompatible clients in isolated locks. `latest` is not a runtime dependency or a version claim.
2. **Prove the real local behavior.** Use actual files/providers/kernels/SDK clients for the finite selected workflow, including exact schema/types, missing/unsupported/denied input and restart. Preserve upstream authority instead of duplicating its mutable state.
3. **Prove native and credential boundaries.** Observe process/dependency/network behavior; no Java, Spark driver, unselected container backend, remote Databricks execution or unexpected artifact downloads. Ordinary tests never discover `.env`/profiles or inherit personal credentials. Provision model weights/dependencies separately before offline testing.
4. **Exercise resource and recovery behavior.** Bound memory/results/logs/files, cancel in-flight work, reap descendants, reconcile writes and restore actual provider/data history. MLflow stores, table logs and notebook/job artifacts have explicit coordinated ownership/recovery policies.
5. **Promote only the tested slice.** Record exact versions, task/test IDs and evidence in the capability registry; retain other operations/profiles as planned/unsupported. Source metadata, a reachable endpoint and an import succeeding cannot promote behavior.

The [inventory validator](../../scripts/validate_planning.py) runs offline and checks provenance, owner references and research/qualification boundaries. [The public artifact verifier](../../artifacts/probes/B063-verify-oss-artifacts.py) is explicit opt-in research tooling; it hashes exact archives and reads metadata/source as data, never installs or executes upstream code. Publication, cloud provisioning and account-reference probes retain their separate existing authorization boundaries.


## Implementation requalification update (2026-10-22)

The adopted base dependency is now `sparkless==4.15.2`. The exact current BrickHouse candidate was qualified in four fresh offline installation lanes on macOS arm64 CPython 3.11.15 and 3.12.13: 137 selected base G1–G4 cases and 175 notebook-extra cases per interpreter, 624 total. The selected UTC-aware Delta timestamp predicate passes on the published Sparkless 4.15.2 wheel under upstream PR [#1721](https://github.com/eddiethedean/robin-sparkless/pull/1721), closing [issue #1720](https://github.com/eddiethedean/robin-sparkless/issues/1720). This is local candidate qualification only; it does not establish broader Sparkless semantics, Linux/Windows support, or remote Databricks compatibility. See the exact hashes and machine reports in the [0.1.0 candidate bundle](../../artifacts/releases/0.1.0/README.md).
