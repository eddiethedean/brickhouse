# ADR 0004: Minilake reuse boundary

- Status: Accepted for phase 0.1
- Date: 2026-10-16
- Context: B002.1 audit of Minilake commit `0597285d1ba33daf76f553b82a32024ded7b78f6`; see [the file-level inventory](../upstream/minilake-inventory.md).

## Decision

Implement the initial BrickHouse catalog, jobs, and files behavior as BrickHouse-owned services. Use the pinned Minilake source and its tests only as research references for endpoint shapes, request validation, and workflow ordering. Do not copy Minilake route, persistence, configuration, executor, or app implementation code into phase 0.1.

The source is MIT licensed (copyright Rafael Sales, 2026), but permissive licensing alone does not make its architecture suitable. The inspected routes own module-global state, use a process-global settings singleton, and mix HTTP handling with filesystem or DuckDB access. The app factory also imports global DuckDB, optional notebook services, persistence callbacks, and Spark image prewarming. Jobs start a Spark container by default; the Unity Catalog service keeps its own table registry and DuckDB-managed data. Adapting these modules would duplicate the BrickHouse catalog, table provider, job lifecycle, and persistence authorities.

## Consequences and removal requirements

- Build transport models and routes against BrickHouse domain services and instance-scoped repositories.
- Keep one catalog/table identity and one native Delta mutation provider. REST, SQL, and Sparkless must share those committed table IDs, locations, schemas, and versions.
- Jobs execute only in fresh Python child processes with explicit context. Do not carry Docker, Spark, Java, `spark-submit`, synthetic skipped-success, or Spark environment secret-resolution paths into BrickHouse.
- DBFS and Files, if implemented, share a BrickHouse-owned storage service and root with traversal protection. The Minilake module-global upload handles are not adopted.
- Translate relevant Minilake tests into BrickHouse-owned contract tests. The tests listed in the inventory remain requirements, not evidence until BrickHouse tests actually pass.
- No Minilake code is copied, so no third-party notice is added for Minilake in this slice. If later source adaptation is proposed, add exact file/revision provenance, retain the MIT copyright and license text in `THIRD_PARTY_NOTICES.md`, update a machine-readable dependency/source inventory, and pass the contract tests before accepting the copy.

## Alternatives considered

1. **Mount or depend on the Minilake app:** rejected because it introduces a second catalog/persistence authority and Spark/Docker execution paths.
2. **Selectively adapt routes now:** rejected because each route's dependencies cross ownership boundaries; replacing those dependencies would leave mostly the public request/response contract, which is simpler and safer to implement as BrickHouse-owned code.
3. **Copy isolated algorithms:** deferred. Path handling, identifier checks, and route behavior are small enough to implement under BrickHouse's own service contracts and directly test. Revisit only when a concrete, nontrivial algorithm has a measured benefit and its license/provenance can be preserved.

This decision covers the phase 0.1 initial catalog/jobs/files behavior. It does not prohibit future targeted reuse after a new source-level review.
