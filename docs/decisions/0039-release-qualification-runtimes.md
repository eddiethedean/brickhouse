# ADR 0039 — Exact native release-qualification runtimes

- Status: accepted for 0.1.0 qualification
- Date: 2026-10-22
- Task: B066.4

## Decision

Qualify Ubuntu 22.04 x86_64 and macOS 26 arm64, each on native CPython 3.11.17
and 3.12.15, in separate base and notebook lanes. Every original G1–G4 selector,
compute/warehouse variant, recovery case and complete source-suite gate remains
required. No required failure is retried or skipped into a pass.

Use hash-locked uv 0.13.5 solely to provision exact python-build-standalone
interpreters before tests. Retain the interpreter build and executable hash.
This does not add a BrickHouse runtime dependency or job-time installer.

## Evidence and limits

The official setup-python macOS arm64 manifest stops at CPython 3.11.9/3.12.10;
`check-latest` cannot supply the newer security builds there. The managed native
3.11.17 and 3.12.15 installers were exercised locally, including venv creation.

macOS 14.8.9 qualification intermittently denied the pinned native CLI's numeric
loopback connections, even after native jobs completed successfully. Both older
framework and current standalone interpreter runs showed the issue. A numeric
IP policy experiment failed compilation and was rejected. Localhost TCP/UDP
and local Unix sockets are permitted; a strict EPERM/EACCES preflight proves
that other IP connections remain denied. Linux uses a fresh loopback-only
network namespace and restores the non-root user before candidate execution.

The final profile uses GitHub's standard macOS 26 arm64 runner, the same OS
family as the successful local native-client probes. macOS 14 remains explicitly
unqualified for the final artifact. Choosing this version preserves the required
macOS architecture, Python minors and behaviors; it does not establish support
for other macOS releases. The final candidate must still pass every gate on the
selected profiles before its artifact bundle is accepted.

See [release readiness](../plan/21-release-readiness.md), the
[case map](../../planning/release-requirements.json), and the
[review bundle](../../artifacts/releases/0.1.0/README.md). Runtime provisioner
metadata is pinned in [client artifacts](../../planning/client-artifacts.json)
and [the tool lock](../../requirements/qualification-tools.txt).
