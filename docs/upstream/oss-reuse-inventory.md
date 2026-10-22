# B063 — Open-source reuse research inventory

Date: **2026-10-20**. Decision/phase mapping: [document 20](../plan/20-open-source-reuse.md). Canonical snapshot: [planning/oss-reuse.json](../../planning/oss-reuse.json). This report records **metadata and static source/archive inspection**, not candidate execution, a dependency lock or new BrickHouse support.

## Method and provenance

Queried public PyPI JSON for retained project pins and finite stable research candidates published before 2026-10-21 UTC. The snapshot retains exact version metadata, Python/dependency constraints, release/sample artifact URLs and SHA-256, all advertised wheel filenames, declared licenses and the sampled archive's metadata/license/source-file hashes. The sample may be for a different platform than the running host; archives were read as data, not installed.

Downloaded and SHA-256 verified one sample artifact for each of **22 Python distributions**. Read selected public source modules in MLflow, nbclient, ipykernel, Connect, sharing, Unity Catalog client, dbt adapters and PyIceberg; other packages received static archive/metadata/license inventory only. No JAR/class member was found in those sampled archives. This does not prove all artifacts, transitive dependencies or optional execution paths are JVM-free.

The native/source supplement records checksum-verified crates `delta_kernel==0.30.0` and `ballista==55.0.0`, plus Unity Catalog source commit **`ccd5ca4cf7026abdc43b42556224f13b78235ed0`**. Read their actual Cargo metadata, licenses/README and the UC OpenAPI/build/launcher files. The UC source commit is a separate contract/server inspection, not a claim that it exactly matches Python client 0.7.0. No local upstream checkout changed.

## Findings anchored to exact artifacts

| Finding | Primary evidence | Plan consequence |
|---|---|---|
| Current native local core has bounded existing evidence | [Sparkless 4.14.1](https://pypi.org/pypi/sparkless/4.14.1/json), [delta-rs 1.2.1](https://pypi.org/pypi/deltalake/1.2.1/json), [PyArrow 23.0.1](https://pypi.org/pypi/pyarrow/23.0.1/json), existing B008/B010/B046 reports | Retain pins and single mutation authority; upstream features do not extend tested scope |
| Full MLflow 3.17.1 requires skinny/tracing at exactly the same version; declares Arrow <26; skinny accepts SDK <1 | [MLflow metadata](https://pypi.org/pypi/mlflow/3.17.1/json), [skinny metadata](https://pypi.org/pypi/mlflow-skinny/3.17.1/json), inspected server source | Qualify a complete optional local service with explicit DB/artifact/endpoints and selected native Python models; do not substitute skinny for server functionality |
| Notebook tooling can provide kernel execution/format management, not Databricks behavior | [nbclient 0.11.0](https://pypi.org/pypi/nbclient/0.11.0/json), [nbformat 5.11.1](https://pypi.org/pypi/nbformat/5.11.1/json), [ipykernel 7.4.0](https://pypi.org/pypi/ipykernel/7.4.0/json), [execution API](https://nbclient.readthedocs.io/en/latest/client.html) | Prefer direct components; retain magic/context/child/cancel contracts in B017 and make Papermill conditional |
| dbt-databricks 1.12.7 rejects both current SDK and connector pins | [Exact metadata](https://pypi.org/pypi/dbt-databricks/1.12.7/json), wheel connection module and packaging requirement comparison | Separate locked client environment for any selected future profile; do not downgrade tested core clients |
| Delta Sharing 1.4.2 pins Arrow <=23.0.1 and depends on a native wrapper; Spark and non-Spark entry points differ | [Sharing metadata](https://pypi.org/pypi/delta-sharing/1.4.2/json), [wrapper 0.3.3 metadata](https://pypi.org/pypi/delta-kernel-rust-sharing-wrapper/0.3.3/json), inspected reader/source and wrapper Cargo.toml | Qualify exact native wheel/platform/feature matrix; use independent non-Spark path, native server and explicit revocation/lifetime tests |
| Wrapper 0.3.3 uses Kernel 0.22.0/Arrow 57, independently of latest Kernel 0.30.0 | Hash-verified wrapper sdist Cargo.toml and [Kernel crate metadata](https://crates.io/api/v1/crates/delta_kernel/0.30.0) | Keep Rust/Python package versions separate; no assumed advanced-feature or write support |
| UC OSS server invokes Java and uses a separate OpenAPI source | [Pinned launcher](https://github.com/unitycatalog/unitycatalog/blob/ccd5ca4cf7026abdc43b42556224f13b78235ed0/bin/start-uc-server), [pinned API](https://github.com/unitycatalog/unitycatalog/blob/ccd5ca4cf7026abdc43b42556224f13b78235ed0/api/all.yaml), [client metadata](https://pypi.org/pypi/unitycatalog-client/0.7.0/json) | Reuse contracts/client fixtures selectively; no UC Java sidecar or duplicate catalog; no managed-UC parity assumption |
| Connect client 4.2.0 is sdist-only and has no JAR/class archive members | [Exact metadata](https://pypi.org/pypi/pyspark-client/4.2.0/json), inspected setup/Connect modules, [official install boundary](https://spark.apache.org/docs/latest/api/python/getting_started/install.html) | Candidate for B036 only; install/native server/protobuf/error behavior remain unqualified; full PySpark is a different package |
| Native Iceberg access is available as a candidate library | [PyIceberg 0.12.0 metadata](https://pypi.org/pypi/pyiceberg/0.12.0/json), inspected REST catalog source | Evaluate under B040/B041 only; no default format switch or dual-write promise |
| Ballista is native distributed infrastructure, but the crate toolchain exceeds inspected Sparkless baseline | [Ballista 55.0.0 crate](https://crates.io/api/v1/crates/ballista/55.0.0), [native architecture](https://datafusion.apache.org/ballista/user-guide/introduction.html), [DataFusion 55.0.0 metadata](https://pypi.org/pypi/datafusion/55.0.0/json) | Concrete SC.0 comparison with Rust 1.94, Arrow/Polars, Spark semantics and provider-commit gaps; no automatic replacement |

## License observations and retained-source policy

Candidate metadata/license members declare Apache-2.0 for Delta/Arrow/MLflow/Databricks clients/Connect/sharing/UC/Iceberg/dbt/DataFusion/Ballista/Kernel, MIT for Sparkless/SQLGlot/DuckDB and BSD-3-Clause for selected Jupyter/Papermill packages. Full dependency graphs, native vendored libraries and optional extras still need artifact-specific notices/audit during qualification. A metadata classifier is recorded as such, not mistaken for a complete legal inventory.

No third-party implementation was copied into tracked source. File content read for research remains outside the repository; the tracked inventory stores paths, URLs, hashes and decision summaries. If future work copies or generates upstream-derived code, pin the exact source/spec/generator and retain its notices under the repository's existing provenance policy. Using a dependency does not transfer its feature or security claims to BrickHouse.

## Reproduction and gaps

`python artifacts/probes/B063-verify-oss-artifacts.py --verify-public-artifacts` re-downloads the exact recorded sample archives and pinned native/source files, verifies SHA-256 and inspected archive-member hashes, and reports results. It requires explicit public network access and executes no upstream package/setup code. Normal planning validation/tests read the saved inventory offline. Public index JSON can change; immutable artifact/source hashes are the reproduction boundary, while captured metadata hashes identify the original observation.

## Post-snapshot runtime update — 2026-10-21

The B063 inventory remains a historical research snapshot dated 2026-10-20. BrickHouse's developer dependency was subsequently advanced to published Sparkless 4.15.0 after its release on 2026-10-21. The tested macOS arm64 wheel hash, public cancellation probe, and SQL integration evidence are recorded in [B011.3.3](../../artifacts/conformance/B011.3.3-sparkless-4.15.0-cancellation.md). This runtime update does not revise the earlier sampled-artifact inventory or qualify untested platforms and integrations.

No candidate environment was resolved, installed or run. No full Sparkless rebuild, `.env`/profile read, personal Databricks request, cloud provisioning, external message or publication occurred. Runtime conformance remains with the owning task IDs in document 20 and the ledger. [The B063 completion report](../../artifacts/conformance/B063-open-source-reuse.md) records local planning checks separately.
