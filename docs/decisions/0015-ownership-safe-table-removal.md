# ADR 0015: Ownership-safe table removal

- Status: accepted
- Date: 2026-10-19
- Related: ADR 0009, ADR 0012, ADR 0013, ADR 0014
- Implemented by: B008.4

## Context

External table locations belong to the caller and must survive a table drop. Managed table data is owned by the workspace, but filesystem paths can be replaced by symlinks and filesystem deletion cannot share one transaction with SQLite metadata.

## Decision

`DeltaRsTableService.drop_table(handle)` first verifies that the handle still matches its registration and that no operation is unfinished. For an external table, it removes only the SQLite registration and associated committed operation journal rows; it never traverses or changes the external location.

For a managed table, the service validates that the registered location is the canonical UUID-named direct child of the workspace `tables` directory. It rejects a symlink at the managed root or table path. On POSIX systems with symlink-resistant `shutil.rmtree` support and `O_DIRECTORY`/`O_NOFOLLOW`, it opens the managed root by file descriptor and removes only that child. Only after filesystem cleanup succeeds does one SQLite transaction remove the table's committed operation records and registration. A missing child counts as completed physical cleanup. An unfinished operation prevents removal.

## Interrupted cleanup

Filesystem removal precedes metadata removal. If deletion fails partway through, or the process dies after the directory is removed but before SQLite commits, the registration remains. The caller repeats `drop_table()` using the still-registered handle. Recursive deletion is safe to repeat, including when the directory is already absent. If cleanup detects a changed symlink/path or lacks secure platform support, it stops and leaves the registration intact for inspection.

## Consequences

- External bytes are preserved; managed files are removed only under the validated workspace-owned root.
- Metadata visibility lasts until managed cleanup completes; after interruption the table may have a partially removed physical directory until drop is retried.
- Removing a table also removes its scoped operation journal history because the journal references registrations with `ON DELETE RESTRICT`.
- This implementation is tested on local macOS POSIX filesystems and does not claim Windows, network filesystems, cross-workspace writers, or adversarial mutation of the workspace by another process.
- Table removal is implemented in the local optional Delta service; SQL and the HTTP API do not route `DROP TABLE` through it yet.

## Evidence

See [B008.4 ownership-safe removal evidence](../../artifacts/conformance/B008.4-ownership-safe-removal.md).
