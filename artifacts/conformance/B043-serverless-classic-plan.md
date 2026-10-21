# B043 — Serverless and classic compute research

Date: **2026-10-20**. Kind: research and planning validation. This report establishes a design decision; it is not runtime conformance evidence.

## Reviewed source

Reviewed the official Databricks AWS documentation and APIs linked in [document 17](../../docs/plan/17-serverless-and-classic-compute.md). Documentation dates vary and the pages are not immutable; observations must be revalidated against the chosen reference version before implementation claims.

Fetched the official `databricks/databricks-sdk-py` **v0.153.0** source modules with Python `urllib.request`, parsed them with `ast`, and inspected model fields without executing upstream code or loading credentials:

| Module | SHA-256 | Relevant fields |
|---|---|---|
| [jobs.py](https://github.com/databricks/databricks-sdk-py/blob/v0.153.0/databricks/sdk/service/jobs.py) | `98365a83fb5f5984e729435c2bb602b036faeaad342c8866d6a4f702651a9859` | `Task`: `new_cluster`, `existing_cluster_id`, `job_cluster_key`, `environment_key`, `compute`, `disable_auto_optimization`. `JobSettings`: `environments`, `performance_target`. `JobCluster`: `new_cluster`, `serverless_compute_id`. |
| [compute.py](https://github.com/databricks/databricks-sdk-py/blob/v0.153.0/databricks/sdk/service/compute.py) | `02a1c1e5cdc0cc825312d2697c31dd8133e674f5ec25d5ce6cb652a602e561ff` | `Environment`: `base_environment`, `client`, `environment_version`, `dependencies`, `java_dependencies`, `project_environment`. `ClusterSpec`: `spark_version`, `data_security_mode`, `dependency_mode`, `spark_conf`, `spark_env_vars`, `autotermination_minutes`. |
| [sql.py](https://github.com/databricks/databricks-sdk-py/blob/v0.153.0/databricks/sdk/service/sql.py) | `5028f0837cb46f4c269b75229eb6fb54f1df6c1dadbf805084dd0ac683269cf3` | `GetWarehouseResponse`: `warehouse_type`, `enable_serverless_compute`, `auto_stop_mins`, `state`, `cluster_size`, `min_num_clusters`, `max_num_clusters`. |

`PerformanceTarget` contains `PERFORMANCE_OPTIMIZED` and `STANDARD`. These are inspected client definitions, not evidence that BrickHouse accepts them or that every documented feature is available to the owner's account.

## Result and limits

- Added the behavior matrix, release allocation, case designs and authority boundaries in document 17 and accepted [ADR 0016](../../docs/decisions/0016-serverless-and-classic-compute-profiles.md).
- Expanded existing implementation criteria for initial serverless/classic Python/notebook subsets and all three SQL warehouse types, with later environment/access/retry, streaming/pipeline and Connect gates.
- Added separate planned workload/environment capabilities. Infrastructure metadata remains planned, with its intended eventual simulated status explicit.
- Recorded runtime/version/preview dependence, inconsistent JAR task documentation, unavailable classic observations, and the intentional lack of shared driver state.
- Used public source research only. No Databricks account request, credential read, cloud resource change, dependency installation, Sparkless rebuild or runtime implementation test was needed.

Planning validation and the final consistency review are recorded in [planning/validation.json](../../planning/validation.json). Only B043's research deliverable can be completed by these checks; all affected runtime capabilities still require local implementation evidence.
