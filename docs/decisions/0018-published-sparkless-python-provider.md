# ADR 0018: Verify the published Sparkless Python provider

- Status: adopted dependency/API decision; production integration remains B010
- Date: 2026-10-20
- Tasks: B046.1–B046.3; refines ADR 0017's artifact prerequisite

## Context and decision

The owner reported publication of Sparkless 4.14.0. The release source and verified macOS arm64 wheel expose a public Python catalog/read/write callback API. The earlier Rust-only absence finding applies to the historical revision and published 4.13.4, not this release. [The API inventory](../upstream/sparkless-4.14.0-python-provider.md) records signatures, source/wheel hashes and limits; [focused callback verification](../../artifacts/conformance/B046.2-sparkless-4.14.0-python-binding.md) records 56 passing release tests.

Adopt **4.14.0** as the exact engine candidate. Verify the already-published implementation and wheel instead of rebuilding or reimplementing that API. B046's artifact criteria now explicitly accept a hash-verified published source/wheel; a source-built candidate remains the alternative when no compatible release is available. Required callback, real-provider and provenance checks are retained. This supplies a publicly obtainable candidate for B023 to qualify on its own advertised platform matrix; known type defects prevent treating it as a compatible release yet.

## Integration constraints

Use only `sparkless.external_catalog` public descriptors/requests/receipts and the public session registration API. Keep native sessions on their owner thread/process. Preserve one catalog/table/commit authority; callbacks write through the journaled Delta service. Validate protocol/features, schema and canonical table ownership before returning a native descriptor. Do not classify native diagnostic strings to route operations or silently retry writes.

The writer materializes data before callback dispatch. Small probe/callback bounds are not a bounded-memory engine guarantee. Callback exception wrapping and synchronous execution are explicit limits. In-process async cancellation is unavailable; fresh worker process containment must preserve actual committed outcomes. Path saves, insertInto, SQL UPDATE/DELETE and other undelegated paths need explicit BrickHouse adaptation/rejection and cannot be claimed from the hook's existence.

B046.3 verified 14 selected real Delta commit/lifecycle checks but reproduced parent-null ingestion and all-null-field collection failures, filed as [#1694](https://github.com/eddiethedean/robin-sparkless/issues/1694) and [#1695](https://github.com/eddiethedean/robin-sparkless/issues/1695). [The report](../../artifacts/conformance/B046.3-sparkless-4.14.0-provider.md) replaces the missing-API diagnosis with exact typed-contract blockers; B046.3 and B010.1 remain blocked until they pass. B010 remains responsible for production sessions/writers and subsequent SQL/job/notebook release gates. This decision does not establish Databricks behavior or clear G1–G4 by itself.

This decision authorizes no release publication or cloud workload. No upstream implementation edit or credential change was performed; the owner separately authorized reporting blocking bugs as upstream issues.
