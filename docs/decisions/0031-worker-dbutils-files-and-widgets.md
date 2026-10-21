# ADR 0031 — Worker-scoped dbutils files and widgets

- **Status:** Accepted
- **Date:** 2026-10-20
- **Task:** B016.1

## Context

SDK Python jobs already execute in fresh child processes with an immutable JSON run context. The selected 0.1.0 worker contract calls for real file effects and parameterized widgets, while keeping imports and mutable state out of the BrickHouse server process. A full Databricks DBFS, mounts, volumes, or notebook utility implementation is outside this bounded slice.

## Decision

Launch SDK Python scripts through `brickhouse.runtime.worker_entry` only when the immutable context contains the B016 dbutils mapping. The entry point creates a new `dbutils` object and passes it into the script globals using `runpy`; the host interpreter and its `builtins`, `pyspark`, and `databricks` modules are unchanged. The uploaded script remains `__main__`, retains its original `sys.argv`, and can import sibling files from its own directory.

The supported filesystem maps both `dbfs:/...` and `/dbfs/...` into `workspace/files/dbfs`. It implements `ls`, `head`, `put`, `cp`, `mv`, `rm`, and `mkdirs` with actual local filesystem operations. Unmapped paths, traversal components, symbolic links, special files, the root removal, and oversized operations fail explicitly. File previews are limited to 1 MiB; writes and recursive copies/removals are limited to 64 MiB and 10,000 files. Recursive directory copy requires `recurse=True`, refuses an existing destination tree, and keeps its effects below the selected root.

The widget subset is `text`, `dropdown`, `combobox`, `multiselect`, `get`, `getAll`, `remove`, and `removeAll`. Values are strings. Declared defaults are overridden by `name=value` arguments in task `parameters`, then by matching run-now `python_params`; other argv values still reach the script unchanged. `get` before declaration raises `KeyError`. Each worker gets a new widget store.

## Consequences and limits

- Files under the DBFS mapping are real and persist across runs in the local workspace. `/Workspace/...` uploaded source remains in its existing workspace source location; DBFS does not alias workspace paths.
- This is a supported local subset, not a claim of broad Databricks DBFS or widget parity. Mounts, volumes, cloud URIs, `dbutils.secrets`, notebook utilities, and unknown methods are outside this decision. Task values were not included in B016.1; B016.3 later adds their bounded worker subset under [ADR 0033](0033-attempt-scoped-task-values.md).
- Python job code is trusted local code. The `dbutils.fs` resolver rejects traversal and symlink paths, but arbitrary Python code can use ordinary file APIs and is not sandboxed.
- Widget parameter resolution does not rewrite the task's argv. Run-now argv continues to replace static task argv under the existing SDK contract, while widget bindings apply the documented precedence independently.
- The worker entry module adds only the resolved BrickHouse package directory to the child's `PYTHONPATH`; it never inherits the caller's import path or cloud credentials.

## Evidence

See [B016.1 conformance](../../artifacts/conformance/B016.1-worker-dbutils-files-widgets.md) for the local filesystem, path-boundary, widget, SDK loopback, and regression checks.
