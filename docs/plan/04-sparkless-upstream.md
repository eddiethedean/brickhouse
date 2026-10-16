# 04 — Sparkless upstream work

## Boundary

BrickHouse owns Databricks APIs, catalog persistence, orchestration, protocol adapters, notebook context, and capability reporting. Sparkless owns DataFrame semantics, expressions, SQL execution, types, and reusable execution/storage extension hooks.

The reviewed Sparkless workspace has engine-neutral traits under `crates/robin-sparkless-core/src/engine/`, the Polars backend under `crates/robin-sparkless-polars/`, root Rust facades under `src/`, and Python/PyO3 interfaces under `python/`. The current session's name lookup is local; the existing traits are not evidence of a ready-made remote catalog plugin. No public Arrow bridge was confirmed in the narrow source inspection. B003 must verify actual interfaces before proposing edits.

## Proposed coordinated changes

| ID | Change | Required contract and evidence |
|---|---|---|
| S001 | External catalog provider | Session-scoped resolution of multipart identifiers to a typed table handle; stable ID, canonical location, schema, version, provider, permissions context; no global resolver |
| S002 | Table write delegation | `saveAsTable`, `insertInto`, supported path saves, and SQL mutation entry points delegate when a provider owns the target; one commit service; preserve schema/mode/options |
| S003 | Arrow import/export | Typed Arrow batches/streams between Sparkless and Python/native providers; preserve nullable nested types, decimals, timezone metadata, and bounded materialization behavior |
| S004 | Session isolation | Context-managed configuration, namespaces, temporary views, UDF registries, and runtime state; concurrent sessions must not inherit each other's settings |
| S005 | Stable errors and capabilities | Structured engine error category and context; supported operation inspection; no string matching of Polars diagnostics for control flow |
| S006 | External relation binding | SQL/DataFrame reads share catalog resolution and table snapshots; provider tables can enter expressions/joins without lossy JSON conversion |
| S007 | Worker import compatibility | Prefer ordinary Sparkless exports; add reusable missing names/semantics upstream while BrickHouse owns worker-only import routing |
| S008 | Future incremental execution | Bounded batch interface, expression reuse, and cancellation points for streaming integration; no claim of full streaming from a no-op API |

S001–S006 are designs to prove, not names of existing APIs. B003 determines the smallest compatible additions. B009 implements the needed upstream slice and B010 consumes a tested release or an explicit development artifact.

## Provider design considerations

- Return engine-neutral descriptors from catalog hooks. Keep FastAPI, SQLite, Databricks SDK, and BrickHouse types out of Sparkless.
- A missing external table is distinct from an unavailable provider. Define shadowing: session temp views first where the target profile requires it, then bound namespace/catalog resolution. Never silently strip a catalog prefix and return an unrelated local table.
- Resolve names to stable table identities. Do not interpolate SQL identifiers or filesystem paths from unvalidated user input.
- Python callbacks invoked by Rust must specify GIL handling, reentrancy, cancellation, exception conversion, and thread ownership. Test provider callbacks under concurrent sessions; avoid invoking network callbacks while holding an engine-wide mutex.
- Writer delegation must cover methods users actually call. Reject bypasses until supported. Passing a JVM catalog class name in `spark.conf` must never report that a Java plugin was loaded.
- An external mutation result must include committed version and observed schema. Invalidate cached plans/views by version rather than assuming all session copies remain valid.

## Delta ownership

Initial preference: BrickHouse's Python `deltalake` provider owns mutations while Sparkless reads supported Delta snapshots. B004 evaluates duplicate native dependency cost and exact protocol interoperability. An alternative is exposing equivalent reusable Rust delta-rs operations from Sparkless and using that as the sole provider. Pick one mutation provider per workspace/profile and record the choice before B008.

Do not require Sparkless and Python deltalake to use numerically identical package versions; their dependency versions differ. Require agreement on Delta protocol and features, proven by bidirectional data tests. Column mapping, deletion vectors, partitions, timestamp variants, generated columns, and CDF each require explicit support evidence.

## Coordinated delivery

1. Write a small failing behavior case demonstrating the missing hook.
2. Implement reusable Sparkless changes under its repository instructions.
3. Run targeted Rust/Python tests and the required upstream checks.
4. Record the commit and artifact used by BrickHouse development.
5. Integrate through a narrow adapter and test both the new and oldest supported Sparkless releases.
6. Require a published compatible Sparkless wheel before a normal public BrickHouse release. Local source overrides are acceptable for development; they are not user installation instructions.

No automatic publication is authorized by this document. If a required upstream artifact is unavailable, keep dependent tasks blocked with a precise contract reference and continue independent work.
