# 11 — ML, data tooling, and optional extensions

## Integration rule

Use an existing native/Python implementation where it can supply real behavior. BrickHouse owns the Databricks-facing contract and translation. Provider feature lists are research inputs, not evidence of compatibility. Optional providers must be lazy-loaded, version-pinned for conformance, independently stoppable, and absent from base installations.

## MLflow and model serving

Use a real local MLflow tracking server, its supported backend store, and local artifact storage. Preserve MLflow as the authority for experiments, runs, artifacts, and its model registry rather than duplicating those records in BrickHouse SQLite. Store only identifiers/mappings needed by the workspace facade.

Start with Python model flavors that execute without Java. Test create experiment, log parameters/metrics/artifacts, resume a run, register/promote a version, restart, and load a model for prediction. Spark model flavors remain unsupported unless a verified native implementation exists.

Databricks registry URIs, three-part UC model names, aliases, permissions, lineage, and serving endpoint routes need adapters and separate evidence. An MLflow registry does not implement these automatically. Serving must manage model version, environment, request schema, output encoding, load errors, lifecycle, and resource limits. Begin with a local Python prediction worker; record throughput and cancellation limitations. Remote foundation-model endpoints require explicit external configuration and are not part of the cloud-independent profile.

### Concrete MLflow qualification

B063 statically inspected MLflow **3.17.1** and matching skinny/tracing candidates. B025 qualifies an actual optional local service with explicitly configured DB-backed registry/artifact/endpoints and restart/restore; B026 qualifies selected provisioned native Python model environments. These versions are research candidates, not added dependencies. Do not substitute skinny for a full server, expose provider paths outside shared policy, allow implicit cloud SDK credential discovery, or launch Spark/Docker backends from the native profile. See [document 20](20-open-source-reuse.md) and [source hashes/constraints](../../planning/oss-reuse.json).

## dbt

Offer a separate local `dbt-duckdb` profile first. Ensure its models resolve the same catalog and storage contract: default dbt materializations must not create a second authoritative DuckDB managed-table world. Prototype views/read-only queries first; native Delta materializations must call the table service or a reviewed equivalent transaction path.

Validate sources, refs, incremental models, tests, schema changes, and artifacts on an actual dbt project. `dbt-databricks` support is a separate protocol/macro/materialization target. Do not advertise that existing Databricks dbt projects run unchanged because dbt-duckdb is installed.

### Independent dbt profiles

The inspected **dbt-databricks 1.12.7** requires SDK `<0.118.0` and connector `<4.4.1`, conflicting with current pins **0.153.0/4.6.0**. B027 must use an independent exact lock if it selects that future client workflow, and prove its actual SQL/macros/transport instead of inferring compatibility from B021 or dbt-duckdb. Keep dbt-duckdb as the first profile and route registered materializations through the table service. Reject unselected Python-model/Spark/session routes rather than provisioning remote execution or bypassing commits.

## Vector Search

Evaluate LanceDB for the index engine. BrickHouse must own index metadata, endpoint/index lifecycle, vector dimension/schema checks, filtering, result scores, update/delete behavior, and permissions. Delta-sync indexes need a documented source-version cursor and recovery model. Direct-access and Delta-sync indexes are separate capabilities.

Begin with caller-supplied embeddings and deterministic fixtures. Embedding model downloads or remote APIs are opt-in dependencies. Index persistence and table changes must be visible after restart; approximate-neighbor ranking tolerances must be explicit in conformance tests.

## Feature engineering and MLlib

Treat feature tables, point-in-time joins, training-set lineage, online publication, and model feature lookup as distinct tasks. Reuse the shared Delta/catalog model. A regular table is not a feature store until its temporal and retrieval contracts are implemented.

Native libraries such as scikit-learn can implement selected algorithms later, but estimator names alone do not establish Spark MLlib numerical, parameter, serialization, or pipeline compatibility. Keep this a research epic until fixtures and supported estimators are specified.

## Filesystem and cloud-storage tests

Use fsspec for Python-facing abstraction where needed. Test native Sparkless/Delta readers independently, including credentials, URI normalization, and staging. Moto server mode and Azurite are optional integration-test services so native clients can make real HTTP requests. Python-only mocks cannot intercept arbitrary Rust network clients. Do not infer ADLS Gen2 coverage from Azure Blob support.

Cloud emulators are not required for local files. Keep service versions, ports, cleanup, and startup in explicit test tooling; never make Docker or Node a hidden base requirement.

## Governance

Introduce local identities, groups, grants, ownership, inheritance, and audit events before claiming authorization. PyCasbin can evaluate policies but does not define Databricks permission semantics or authenticate users. Enforce checks in HTTP routes, SQL binding, worker actions, catalog resolution, file access, and model/vector operations.

Row filters, column masks, lineage, audit/system tables, and workspace isolation are separate contracts. Python code with direct host filesystem access can bypass service policy; the initial trusted-local execution model must not be advertised as a hostile multi-tenant security boundary.

## Deferred services

OSS Unity Catalog's Java server is unsuitable for the base native-only runtime. Its public contracts can still inform fixtures. Bytewax remains a candidate to revisit, but its reported maintenance transition warrants caution before making it foundational. Dashboard/Genie, account administration, marketplace, Lakebase, and managed infrastructure need explicit native feasibility work; no fabricated responses should conceal their absence.

## Later deployment reuse

Emulator phases 0.2–0.4 retain local policy, provider and feature behavior as their primary work. Record principal provenance, provider-owned authoritative state, artifact references, credentials and backup/lifecycle responsibilities while decomposing the existing epics. Direct Python access on the trusted host remains a documented limit.

Actual authenticated service deployment, browser workflows and provider state restore belong to B050–B051/SH.1–SH.2 after emulator stability; hostile-code boundaries for serving and notebook workers belong to B052/SH.3. Optional provider endpoints cannot bypass workspace policy merely because the BrickHouse HTTP API authenticates users. [Document 18](18-self-hosted-platform.md) defines this later track; none of these deployment guarantees follows from passing a local G6 workflow.

## Selective catalog, sharing and native-format reuse

[Document 20](20-open-source-reuse.md) records explicit contract/client reuse for UC and Delta Sharing. B028 maps selected pinned UC APIs through the existing policy/catalog; no OSS Java server or second catalog is adopted. B039 qualifies a real native sharing service against an independently locked non-Spark client, including its Rust wrapper wheel/features, pinned snapshots, authorization, expiry and revocation. Source/model availability never substitutes for governance or managed Databricks parity.

B040/B041 may select bounded PyIceberg/Delta Kernel feasibility slices with distinct format identity, native IO and operation-level feature/recovery evidence. No automatic Delta/Iceberg dual-write, source-built fallback for missing user wheels or mutation-provider replacement is implied. Artifact-specific licensing/notices and ordinary local credential/offline boundaries remain required.
