# B018 — SDK-driven job workflow

**Status:** passed locally on 2026-10-21  
**Scope:** G2 script and notebook workflows against a built BrickHouse wheel and local loopback services. Sparkless 4.15.0 is the base runtime. No Databricks account, cloud compute, Java, or remote fallback was used.

## Acceptance evidence

1. **SDK job data flow:** The pinned SDK submits a parameterized Python producer that resolves a granted secret, appends typed rows to a registered Delta table through the authenticated parent service, and publishes task values. A dependent task, a fresh Sparkless session, and SQL Statement Execution observe the same commit and schema. See [B018.1.1](B018.1.1-job-to-delta-data-flow.md).
2. **Failure and isolation:** Dependency failure, distinct retry attempts, compute-interruption cancellation, descendant reaping, committed-data preservation, and isolation of parameters, widgets, secrets, task values, and output were exercised. See [B018.1.2](B018.1.2-job-lifecycle-isolation.md).
3. **Installation lanes:** The built base wheel ran with notebook packages, Java, and Databricks credentials unavailable. A separate `[notebooks]` install ran the kernel, source-export, cancellation, and cleanup suites. The test driver used local loopback. See [B018.2](B018.2-installation-lanes.md).
4. **Compute, warehouse, and notebook variants:** The synthetic orders script workflow ran through all seven selected serverless/classic-standard/classic-dedicated compute selectors, and a job-produced exact Delta version was read through serverless, pro, and classic local SQL warehouse profiles. The notebook-extra lane separately ran SDK NotebookTask variants across the seven compute selectors and three source-export profiles, reading real committed table data and recording actual notebook output. See [B018.1.3](B018.1.3-g2-compute-profiles.md), [B018.1.4](B018.1.4-g2-warehouse-profiles.md), and [B018.2](B018.2-installation-lanes.md).

## Verification

The B018.1 focused SDK/worker/Sparkless suite passed **45 tests**. The built-wheel base lane passed **10 selected G2 cases**; the notebook-extra lane passed **36 cases**. Both installation environments passed `uv pip check`; `uv lock --check` and `uv build` passed. Exact environment and commands are retained in the child reports.

The tested candidate used Python 3.11.15, Sparkless 4.15.0, delta-rs 1.2.1, PyArrow 23.0.1, SQLGlot 30.24.0, and Databricks SDK 0.153.0 for loopback only. Notebook pins were ipykernel 7.4.0, jupyter-client 8.10.0, nbclient 0.11.0, and nbformat 5.11.1.

## Limits

This evidence qualifies selected local SDK/client shapes and wheel installation paths; it does not establish Databricks service, runtime, access-control, or performance parity. Classic compute and SQL warehouse resources are simulated. Worker Delta writes remain bounded to authenticated append of registered tables. Notebook and SQL coverage is selected, and the base install does not include notebook dependencies. The phase 0.1 release gate remains open.
