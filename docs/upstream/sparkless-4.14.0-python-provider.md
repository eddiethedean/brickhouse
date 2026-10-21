# Sparkless 4.14.0 — public Python provider contract

Inspected: **2026-10-20**. Task: B046.1. This is source/API research; local execution evidence is recorded separately by B046.2–B046.3. The earlier Rust-only findings remain historical evidence for their exact revisions.

## Pinned artifacts and source

- Release: [Sparkless 4.14.0](https://pypi.org/project/sparkless/4.14.0/).
- Tag: [v4.14.0](https://github.com/eddiethedean/robin-sparkless/tree/v4.14.0), peeled commit `e46ddc6b9aa8903b0d98c0401791fffb858c9770`.
- Inspected sdist SHA-256: `ac19471972f9a5729b122cbe5fa0dae33690793b7d53efa9cbab54b986ce262a`.
- Tested wheel: `sparkless-4.14.0-cp38-abi3-macosx_11_0_arm64.whl`; SHA-256 `ee694552b0de19d300a19644d8948e5a7270a5452abe5634ffa62c1909179585`.
- Primary code: [Python contract](https://github.com/eddiethedean/robin-sparkless/blob/e46ddc6b9aa8903b0d98c0401791fffb858c9770/python/sparkless/sparkless/external_catalog.py), [PyO3 adapter](https://github.com/eddiethedean/robin-sparkless/blob/e46ddc6b9aa8903b0d98c0401791fffb858c9770/python/src/external_catalog.rs), [session binding](https://github.com/eddiethedean/robin-sparkless/blob/e46ddc6b9aa8903b0d98c0401791fffb858c9770/python/src/lib.rs), [public documentation](https://github.com/eddiethedean/robin-sparkless/blob/e46ddc6b9aa8903b0d98c0401791fffb858c9770/docs/EXTERNAL_CATALOGS.md).

The existing local Sparkless checkout was inspected read-only at `7c8bfe203b95fbe4598aa76daf392128fb8540cc`, including CONTRIBUTING and the Python testing guidance; it had no root AGENTS.md. The pinned release source, not the newer checkout, defines this report. No upstream code was changed or rebuilt.

## Public API

`SparkSession.set_external_catalog_provider(provider_or_none)` and its camelCase alias attach/detach a provider. A provider implements `resolve_table(identifier)` returning `ExternalTableDescriptor` or a genuine miss (`None`). Writable objects also implement `write_table(request)` returning `ExternalTableWriteResult`. These are public APIs in the released wheel.

The descriptor carries stable table ID, canonical location, Sparkless StructType, provider name, Delta format, optional snapshot version and opaque permissions context. Public validators check descriptors and commit receipts. Permissions context must contain no credentials; it is not native authorization enforcement.

The write request preserves identifier, descriptor, mode, format, ordered options (including duplicates), partition names and Arrow IPC stream bytes. A successful callback must commit before returning the exact version and matching identity/schema descriptor. Invalid metadata, receipts and callback errors fail; a provider miss has a distinct local-name path. The schema bridge has typed integer/decimal/nested representations rather than JSON row conversion.

## Ownership, errors and execution limits

- The adapter owns a strong Python reference while attached; detach/stop release it. `newSession()` starts with an independent empty provider slot. Existing readers share their session's slot.
- Python callbacks synchronously acquire the GIL. The Python SparkSession remains declared unsendable: construct, use and stop it on its owning thread/process. Separate owner threads can use their own sessions; do not transfer native session objects between threads.
- Writes materialize a DataFrame and buffer the entire IPC stream before callback dispatch. Callback input bounds do not bound this earlier allocation. B010 must keep the selected input envelope explicit; this is not a streaming or constant-memory interface.
- Callback exceptions become native engine/Python errors. The binding does not preserve arbitrary original Python exception subclasses as structured service errors. BrickHouse must validate/map its own domain failures without parsing native diagnostic strings for routing.
- There is no asynchronous callback or in-process cancellation token. Worker process termination is the containment boundary; a committed Delta write remains committed if its callback is interrupted before returning a receipt.
- Native descriptor/schema checks do not establish every Delta feature or preserve Delta nullability checks. The BrickHouse provider must validate protocol/features/schema and canonical ownership through its own table service before returning a descriptor.

## Supported integration slice and remaining work

The released hook covers named-table reads, append/overwrite delegation and selected SQL inserts. Read-only providers, invalid receipts, descriptor failures, provider lifetime and session isolation have focused upstream cases. Logical-map writes, async transport, path-save delegation, `insertInto`, and SQL UPDATE/DELETE delegation are outside the hook. They require explicit BrickHouse handling/rejection or separate provider routes; their existence must not be inferred from this release.

For B010, use the public descriptor/IPC/receipt boundary with the authoritative Delta table service. Reserve metadata and handle table creation explicitly; block unprotected registered-table write paths. Keep process/session ownership, schema conversion and provider failure mapping contained in the engine adapter. The released Python hook removes the API-absence diagnosis, but [B046.3](../../artifacts/conformance/B046.3-sparkless-4.14.0-provider.md) remains blocked on nullable-struct bugs #1694/#1695; it does not implement the production adapter, SQL/job/notebook APIs or Databricks compute behavior.
