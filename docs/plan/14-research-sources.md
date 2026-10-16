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
| R03 | [delta-rs Python DeltaTable API](https://delta-io.github.io/delta-rs/latest/api/delta_table/) and [table management](https://delta-io.github.io/delta-rs/latest/usage/managing-tables/) | Native Delta operations provide mutation and maintenance building blocks | Pin exact release; prove feature envelope, transaction reconciliation, and Sparkless interoperability |
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

A pure Python client can still call a JVM-backed remote service. BrickHouse must prove both a native local execution path and client compatibility; one does not imply the other.

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

## Owner-provided reference environment

On 2026-10-16 the owner authorized occasional use of their personal Databricks account to verify representative code and design tests, and reported supplying `DATABRICKS_TOKEN` in local `.env` and GitHub Actions secrets. This is configuration/authorization context, not a verified connection or captured behavior. Record the actual host, account feature scope, permitted compute, client versions, and relevant runtime/warehouse version during B005. [Document 15](15-personal-databricks-verification.md) defines capture provenance and conversion into local fixtures. Routine tests never run on the account.

## What has not been proven

No BrickHouse integration, benchmark, production security claim, client matrix, streaming guarantee, or package installation is established by this research. Exact versions beyond the reviewed repositories are intentionally left to M0. The proposed combination is an engineering plan requiring adapters, upstream hooks and conformance tests, not evidence that the dependencies already fully emulate Databricks together.

PyPI/repository names were not reserved. Recheck ownership/availability before publication. Maintain a dependency record with source, exact version/commit, license, wheels, supported Python/OS/architecture, native dependencies, API assumptions and last validation date.
