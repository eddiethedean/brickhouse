# 21 — Final 0.1.0 release readiness

B066 preserves every phase 0.1 acceptance criterion and closes the remaining final-release qualification, packaging, and publication-configuration gaps. Its four required children are tracked in [the task ledger](../../planning/tasks.json) and [detailed backlog](09-task-backlog.md). B066.1 passed 444 full-source tests on each Python version without skips; B066.2 is active for the portable offline platform matrix.

The final candidate must pass the complete current source suite with required clients enabled on Python 3.11 and 3.12. Each of the 27 release case families in [document 16](16-phase-0.1-initial-release.md) maps to actual runnable cases, exact dependency/client versions, and retained positive, negative, and recovery results.

Linux x86_64 and macOS arm64 each require clean base and notebook-extra installs on Python 3.11/3.12. Dependencies and pinned CLI/Terraform/connector tooling are provisioned before an OS-enforced offline phase. Required G1–G4 cases may use loopback and local Unix sockets only. Databricks credentials and .env/profile discovery are excluded from ordinary checks. A skipped required case fails qualification.

Version 0.1.0 metadata, the owner-selected project license, retained third-party notices, readable current user documentation, and release notes are part of the final source. The tag workflow must validate the exact source, artifact metadata, complete readiness checklist and qualification records before its separate OIDC publishing job. Preparing this release does not create a release tag or publish a distribution.

The final owner-review bundle records exact wheel/source hashes and the tested source revision; its own hash inventory excludes its manifest. Runtime code and test inputs must match the qualified candidate. Historical development-artifact reports remain explicitly dated and hash-bound.
