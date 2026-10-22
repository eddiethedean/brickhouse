# B063 — Native open-source reuse research and plan upgrade

Date: **2026-10-20**. Outcome: **research/planning complete; new candidates remain unqualified**.

## Delivered decisions

[Document 20](../../docs/plan/20-open-source-reuse.md), [the exact source report](../../docs/upstream/oss-reuse-inventory.md), [ADR 0038](../../docs/decisions/0038-native-open-source-reuse.md) and [planning/oss-reuse.json](../../planning/oss-reuse.json) record component-specific choices, observed version/license/native boundaries, implementation owners and finite qualification gates.

Retain proven Sparkless/delta-rs/Arrow/client pins. Prefer direct nbclient/nbformat/ipykernel components and a full optional MLflow service/registry, making Papermill conditional. Keep dbt-duckdb first and isolate any dbt-databricks client profile whose observed SDK/connector bounds conflict with core pins. Use UC contracts/clients without its Java server; qualify Delta Sharing's non-Spark client/native wrapper/Arrow/wheel matrix. Optional Iceberg/Kernel and Ballista/DataFusion remain future selected feasibility work, preserving the default engine and one mutation authority.

No phase 0.1 behavior, implementation child or runtime requirement was added. Existing emulator phases, SH/SC outcomes and stability/integration barriers remain. Eleven still-planned epics gain concrete qualification criteria; current implementation slices gain guidance references only.

## Research performed

Read public primary documentation, exact PyPI metadata and sampled archives for **22 Python distributions**, checksum-verified crates **delta_kernel 0.30.0 / ballista 55.0.0**, and Unity Catalog source **ccd5ca4cf7026abdc43b42556224f13b78235ed0**. Exact metadata/artifact/source/member hashes, URLs, license observations, advertised wheel filenames and source inspection scope are retained in the inventory. Source content read for research was not copied into tracked implementation.

Key observed constraints include dbt-databricks **1.12.7** excluding current SDK **0.153.0** and connector **4.6.0**; sharing **1.4.2** capping PyArrow **23.0.1** and requiring a native wrapper; wrapper **0.3.3** embedding Kernel **0.22.0**/Arrow **57** with four advertised wheel tags; MLflow **3.17.1** requiring same-version skinny/tracing; pure Connect **4.2.0** being sdist-only; UC invoking Java; and Ballista **55.0.0** declaring Rust **1.94.0** versus the inspected Sparkless 1.93 baseline.

These are static artifact/source observations, not full environment resolution, platform installation, runtime tests or behavioral compatibility. No candidate was installed, imported, executed or built. Presence/absence of a sampled archive member does not qualify all transitive execution paths.

## Reproducibility and local validation

[The explicit public verifier](../../artifacts/probes/B063-verify-oss-artifacts.py) was run with `--verify-public-artifacts`; **26 checks passed** across exact archives and pinned source files. [The retained result](B063-oss-artifact-verification.json) sets `qualified_for_runtime` to false. It verifies hashes and archive members without running upstream code. This is separate from ordinary offline tests/CI and does not discover personal credentials or Databricks hosts.

The inventory records **eight static dependency comparisons**. Focused tests parse the actual requirement strings and confirm both conflicting dbt bounds and six compatible bounds, while explicitly withholding full resolver/runtime claims.

```text
env -u DATABRICKS_TOKEN -u DATABRICKS_HOST PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 /tmp/brickhouse-sparkless-4140/bin/python -m pytest -q tests/unit/test_planning_validation.py tests/unit/test_oss_research_inventory.py
```

**28 passed**. New negative cases reject research/runtime promotion, missing or mismatched hashes, unknown task ownership, shared conflicting-client environments, altered requirements and unpinned source. Existing stage/stability/integration checks remain. `python scripts/validate_planning.py`, Ruff check/format, strict mypy for validator/verifier and `git diff --check` passed. Final counts/links are recorded in planning/validation.json.

## Preservation audit

Compared pre-edit manifests saved outside the repository. All **147** original tasks retain their criteria in original order, exact child lists/prerequisite edges, plan references and prior evidence. Only **B063** was added. Qualification criteria were appended to planned B025/B026/B027/B028/B036/B039/B040/B041/B050/B051/B057; existing 85 alpha implementation children are unchanged. All **67** capability states, tested versions, test IDs and evidence remain identical; only planned entries gain research limitations. No capability is promoted by this task.

Pre-edit SHA-256 values:

- tasks.json: `538ffbdd3d2d5872b2099bc95f9c84f302684b364aca771559b9f479e80641ed`
- capabilities.json: `99acd347c564b9ee72ed4ab3a7ef25dadce694ea1265b3826f65b8b0b087a129`

## Limits

The snapshot is not a runtime lock or complete transitive license/security inventory. Owners must resolve/test supported artifacts, native process/network behavior, data semantics, bounded resources, restart/cancellation and deployed policy before adoption. Existing tests/candidate versions are not generalized to different profiles. No runtime pins, workspace/source checkout, personal credentials, Databricks account/workspace request, cloud provisioning, external message or publication changed in B063.
