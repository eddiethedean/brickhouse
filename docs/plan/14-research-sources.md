# 14 — Research baseline and primary sources

Baseline date: **2026-10-16**. This file separates source observations from BrickHouse design choices. Follow links and inspect exact releases again before implementation; unversioned documentation can change. Source availability and feature descriptions do not establish local interoperability or BrickHouse support.

## Reviewed repositories

### R01 — Sparkless

Reviewed local checkout: `eddiethedean/robin-sparkless`, main at `b7306725`, package version 4.13.3 at inspection. Source structure includes engine-neutral contracts, the Polars implementation, Rust facades, and Python/PyO3 bindings. The reviewed default compatibility profile is Spark 3.5, with separate 4.x work. Existing Delta support does not establish that external catalog/writer hooks or a public Arrow bridge are ready for BrickHouse.

Sources: [repository at the reviewed revision](https://github.com/eddiethedean/robin-sparkless/tree/b7306725), [engine architecture](https://github.com/eddiethedean/robin-sparkless/blob/b7306725/docs/ENGINE_ARCHITECTURE.md), [compatibility profiles](https://github.com/eddiethedean/robin-sparkless/blob/b7306725/docs/PYSPARK_COMPAT_PROFILES.md).

**Action:** B003 verifies actual public interfaces and current behavior. S001–S008 in document 04 are proposed integration work, not existing Sparkless API names. Source and runnable fixtures take precedence over stale feature summaries.

### R02 — Minilake

Reviewed commit: `0597285d1ba33daf76f553b82a32024ded7b78f6`; declared version 1.8.0; Python >=3.11. The repository declares MIT licensing; retain the original copyright and license notice for reused code.

Sources: [pinned repository](https://github.com/dmux/minilake/tree/0597285d1ba33daf76f553b82a32024ded7b78f6), [package metadata](https://github.com/dmux/minilake/blob/0597285d1ba33daf76f553b82a32024ded7b78f6/pyproject.toml), [license](https://github.com/dmux/minilake/blob/0597285d1ba33daf76f553b82a32024ded7b78f6/LICENSE).

The executor has Spark-oriented launch paths. SQL reads use DuckDB while some Delta mutations generate Spark code. Managed DuckDB tables and external Delta tables have different storage paths. This supports BrickHouse's decision to own execution and table services rather than mount the original app unchanged; that architectural choice is an inference, not an upstream recommendation.

Sources: [executor](https://github.com/dmux/minilake/blob/0597285d1ba33daf76f553b82a32024ded7b78f6/src/minilake/docker_executor.py), [SQL statements service](https://github.com/dmux/minilake/blob/0597285d1ba33daf76f553b82a32024ded7b78f6/src/minilake/services/sql_statements.py), [catalog service](https://github.com/dmux/minilake/blob/0597285d1ba33daf76f553b82a32024ded7b78f6/src/minilake/services/unity_catalog.py), [Spark and Delta documentation](https://github.com/dmux/minilake/blob/0597285d1ba33daf76f553b82a32024ded7b78f6/docs/spark-and-delta.md).

The reviewed secrets service rejects direct secret retrieval; supported internal job substitution is a different path. This is a concrete reason to test SDK/dbutils behavior rather than assume endpoint names imply compatibility. [Secrets service](https://github.com/dmux/minilake/blob/0597285d1ba33daf76f553b82a32024ded7b78f6/src/minilake/services/secrets.py).

**Action:** B002 inventories exact source before reuse. Minilake remains an independent project; no extension, ownership, or release commitment from its maintainer is assumed.

## Native data and runtime building blocks

| ID | Primary source | Relevant observation | BrickHouse qualification |
|---|---|---|---|
| R03 | [delta-rs 1.2.1 DeltaTable API](https://delta-io.github.io/delta-rs/1.2.1/api/delta_table/), [transaction API](https://delta-io.github.io/delta-rs/1.2.1/api/transaction/), and [Delta transaction protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md) | B004.3 observed `transaction_version`, `CommitProperties.app_transactions`, checkpoint persistence, and an unsafe duplicate when raw same-version appends are replayed | Python `deltalake==1.2.1` selected for phase 0.1; exact protocol/features and uncertain recovery are recorded in [ADR 0008](../decisions/0008-delta-mutation-and-recovery.md) and [the probe report](../../artifacts/conformance/B004.3-transaction-recovery.md) |
| R04 | [SQLGlot repository](https://github.com/tobymao/sqlglot) | SQL AST parsing and dialect translation | Translation success is not Spark/Databricks semantic equivalence |
| R05 | [DuckDB Delta extension documentation](https://duckdb.org/docs/stable/core_extensions/delta.html) | Delta query access is available through an extension | Verify extension installation/offline artifacts and supported features; shared storage only |
| R06 | [fsspec features](https://filesystem-spec.readthedocs.io/en/latest/features.html) | Python filesystem interfaces and implementations | Rust readers need their own URI support or explicit staging |
| R07 | [nbclient execution documentation](https://nbclient.readthedocs.io/en/latest/client.html), [Papermill documentation](https://papermill.readthedocs.io/en/latest/) | Jupyter notebook execution and optional parameterization | Databricks source formats, magics, context and dbutils remain adapter work |

## Clients and protocol boundaries

| ID | Primary source | Research finding | Required follow-up |
|---|---|---|---|
| R08 | [Databricks SDK dbutils source](https://github.com/databricks/databricks-sdk-py/blob/main/databricks/sdk/dbutils.py) | Utilities combine direct HTTP behavior, local helpers and cluster-backed paths; taskValues behavior cannot be assumed to implement local orchestration | Inspect exact SDK version; implement selected utility contracts in BrickHouse |
| R09 | [Databricks SQL Connector documentation](https://docs.databricks.com/aws/en/dev-tools/python-sql-connector) | Documentation includes optional REST Statement Execution mode using the kernel extra and `use_kernel=True`; default transport remains a separate compatibility target | B005/B021 must prove a released client can target the local service, including auth/TLS/results; do not assume Minilake already supports it |
| R10 | [Statement Execution API guide](https://docs.databricks.com/aws/en/dev-tools/sql-execution-tutorial) | SQL statement submission/results have an explicit public contract | Record exact supported request fields, parameter types, polling, result chunks and errors |
| R11 | [PySpark installation documentation](https://spark.apache.org/docs/latest/api/python/getting_started/install.html), [Spark Connect overview](https://spark.apache.org/docs/latest/spark-connect-overview.html) | `pyspark-client` offers a pure Python Connect client; Connect uses a remote protocol | A native server is substantial new work; `pyspark-connect` and full PySpark dependencies are different packages |
| R12 | [Databricks CLI repository](https://github.com/databricks/cli), [Terraform provider repository](https://github.com/databricks/terraform-provider-databricks) | Real clients provide concrete API/deployment compatibility targets | Pin versions, resources and one small workflow before making broad claims |
| R13 | [Databricks dbutils reference](https://docs.databricks.com/aws/en/dev-tools/databricks-utils), [task values guide](https://docs.databricks.com/aws/en/jobs/task-values) | `taskValues` accepts JSON values up to 48 KiB and 250 values per run; `get` distinguishes non-`None` defaults from notebook-only `debugValue`; the utility is documented for Python notebooks | B016.3 implements the bounded set/get contract as an explicit Python script-worker extension and must not claim notebook or cloud parity |

A pure Python client can still call a JVM-backed remote service. BrickHouse must prove both a native local execution path and client compatibility; one does not imply the other.

### 2026-10-16 released-client snapshot (B005.1)

The inspected candidate artifacts are `databricks-sdk==0.153.0`, Databricks CLI `v1.21.0`, Terraform Core `1.16.6`, Databricks Terraform provider `1.137.0`, SQL connector `4.6.0`, and optional SQL kernel `1.1.0`. Exact release links, artifact hashes, minimal workflows, and the REST/kernel versus Thrift distinction are in the [B005.1 client matrix](../../artifacts/conformance/B005.1-client-profile-matrix.md). The SDK is marked Beta by its publisher. The SQL connector 4.6.0 release notes and its same-tag connection-parameter page disagree on whether the kernel is stable or early access; the matrix records the discrepancy and requires explicit backend selection.

The current local semantics candidate is published Sparkless 4.14.1 under `spark35-local-v1`, release commit `ad1b9445bad609ac4bf74d2cf3ad2f68832ff49b`; [B046.3 evidence](../../artifacts/conformance/B046.3-sparkless-4.14.1-provider.md) records the wheel hash, 14 provider checks and six passing nullable-struct cases. The previous 4.14.0 report retains its exact failures. Compatibility-profile behavior is not inferred from the version bump. Historical B004.2 used 4.13.4 at `34049bde128931e21bf34b761cfa10007d62af5e`. B004.2 tested the arm64 CPython 3.11 wheel with PyArrow 23.0.1 and Python `deltalake` 1.2.1. This does not identify a Databricks Runtime or warehouse. The owner described the account as new; the signed-in UI showed an available Databricks Free Edition workspace. A one-request identity probe and a one-row synthetic SQL statement succeeded against the explicit workspace host. The inventory contained one stopped serverless `PRO`, `2X-Small` SQL warehouse and three visible catalogs (`workspace`, `samples`, `system`); the warehouse was restored to stopped state after the query. The host, account/org ID, resource IDs, and token are omitted from tracked evidence. The exact SQL warehouse engine version was not exposed. The query used no scratch namespace. Credential source is recorded by variable name only in [the B005.3 report](../../artifacts/conformance/B005.3-databricks-reference-sql.md).

## Optional ecosystem services

| ID | Primary source | Candidate role | Boundary |
|---|---|---|---|
| R13 | [MLflow tracking server](https://mlflow.org/docs/latest/self-hosting/architecture/tracking-server/), [backend stores](https://mlflow.org/docs/latest/self-hosting/architecture/backend-store/), [registry workflows](https://mlflow.org/docs/latest/ml/model-registry/workflow/) | Real local tracking, artifacts, registry and Python model ecosystem | Databricks UC/serving APIs and JVM-dependent flavors need independent treatment |
| R14 | [dbt-duckdb](https://github.com/duckdb/dbt-duckdb) | Local dbt execution and extension points | Delta/materialization behavior must preserve authoritative tables; not dbt-databricks parity |
| R15 | [LanceDB](https://github.com/lancedb/lancedb) | Native embedded vector storage/query | Databricks endpoint/index lifecycle, Delta sync and auth remain BrickHouse work |
| R16 | [dlt filesystem destination](https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem) | Python ingestion with local/filesystem destinations and table-format integrations | Not Databricks DLT or Lakeflow; verify exact destination/format behavior |
| R17 | [Quix Streams](https://github.com/quixio/quix-streams) | Candidate Python/native event processing runtime | Kafka/state/window/checkpoint semantics require conformance; explicit optional broker |
| R18 | [PyCasbin](https://github.com/pycasbin/casbin) | Permission evaluation building block | Does not supply identities, authentication or UC policy semantics |
| R19 | [Moto server mode](https://moto.readthedocs.io/en/latest/docs/server_mode.html), [Azurite](https://github.com/Azure/Azurite) | Optional HTTP storage emulators usable by native clients | Verify exact service coverage; do not assume full cloud or ADLS Gen2 parity |

## Feasibility cautions and frontier sources

- **R20 — Unity Catalog:** the [OSS quickstart](https://github.com/unitycatalog/unitycatalog/blob/main/docs/quickstart.md) requires Java for its server. Its contracts may be informative, but that server is not a base JVM-free dependency.
- **R21 — Bytewax:** the [repository](https://github.com/bytewax/bytewax) describes a community maintenance transition. Reassess current maintenance and releases before making it foundational.
- **R22 — Lakeflow:** [local development](https://docs.databricks.com/aws/en/ldp/develop-locally) and [expectations](https://docs.databricks.com/aws/en/ldp/expectations) define useful target behavior. Local development tooling is not proof of a native Spark-free runtime.
- **R23 — Import-name collision:** Databricks documents [a dlt import collision](https://kb.databricks.com/en_US/libraries/dltimportexception-error-when-importing-the-dlt-module). Treat dltHub and Databricks DLT as separate APIs/packages.

## R24 — Serverless and classic compute (2026-10-20 supplement)

[Document 17](17-serverless-and-classic-compute.md#research-and-source-limits) records the official AWS documentation/API review for serverless Python jobs/notebooks, classic all-purpose/jobs and access modes, SQL warehouse types, environments and Connect semantics. [The B043 report](../../artifacts/conformance/B043-serverless-classic-plan.md) pins inspected `databricks-sdk-py` v0.153.0 source hashes and model fields; it does not upgrade the existing client pin.

The review establishes a planning distinction between compute mode, access mode, environment/runtime and protocol, and identifies the SQL serverless API type/flag combination. Documentation is version-dependent, includes preview features and inconsistent JAR task listings, and must be revalidated for selected fixtures. The owner's serverless SQL observations cannot establish classic or serverless Python behavior. [ADR 0016](../decisions/0016-serverless-and-classic-compute-profiles.md) adopts the resulting release and capability contracts without claiming implementation support.

## Owner-provided reference environment

On 2026-10-16 the owner authorized occasional use of their personal Databricks account to verify representative code and design tests, and reported supplying `DATABRICKS_TOKEN` in local `.env` and GitHub Actions secrets. One authenticated connection and one sanitized SQL capture are recorded in [artifact B005.3](../../artifacts/conformance/B005.3-databricks-reference-sql.md). [Document 15](15-personal-databricks-verification.md) defines capture provenance and conversion into local fixtures. Routine tests never run on the account.

## What has not been proven

No BrickHouse integration, benchmark, production security claim, streaming guarantee, or package installation is established by this research. The B005.1 client matrix is an inspected version snapshot, not tested client compatibility. The proposed combination is an engineering plan requiring adapters, upstream hooks and conformance tests, not evidence that the dependencies already fully emulate Databricks together.

PyPI/repository names were not reserved. Recheck ownership/availability before publication. Maintain a dependency record with source, exact version/commit, license, wheels, supported Python/OS/architecture, native dependencies, API assumptions and last validation date.

## R25 — Open-source reuse supplement (B063, 2026-10-20)

[Document 20](20-open-source-reuse.md) and [the source inventory report](../upstream/oss-reuse-inventory.md) record public metadata and static artifact/source inspection of 22 Python distribution snapshots, two native crates and a pinned UC source tree. [planning/oss-reuse.json](../../planning/oss-reuse.json) retains exact URLs/hashes, license observations, source/member paths, wheels, dependency bounds and qualification owners. These are research candidates and observed constraints, not installed or tested versions.

Key primary-source findings are incompatible dbt-databricks versus core SDK/connector bounds, Delta Sharing's native wrapper/Arrow limit, full MLflow same-version service components, a pure Connect client packaging boundary, UC's Java runtime versus reusable OpenAPI, and Ballista/DataFusion as a concrete native cluster option with a newer Rust requirement. The report links the exact registry metadata and immutable source. Normal tests consume this saved snapshot offline; refresh/verification is explicit public research, never a Databricks account probe or hidden runtime dependency update.
