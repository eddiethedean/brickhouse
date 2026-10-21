# ADR 0019 — Private worker authentication

- Status: accepted for the phase 0.1 worker protocol
- Date: 2026-10-20
- Tasks: B013.2; follows B013.1

## Context

B013.1 starts a fresh Python child with bounded JSON context and does not inherit the caller's environment. The worker needs a private way to call local BrickHouse services. Public Databricks compatibility tokens do not authenticate those calls. Credentials must remain separate from request payloads, URLs, logs, and ordinary run context, and one attempt must not authorize another.

## Decision

Use protocol version 1. A worker request is a bounded (64 KiB), strict UTF-8 JSON envelope containing version, run ID, attempt ID, request ID, service, operation, and JSON payload. Reject duplicate/unknown envelope fields, invalid IDs, unsupported versions, and non-finite/non-JSON payloads.

Transmit a random 256-bit URL-safe bearer credential separately in the `Authorization: Bearer` header for each worker service call. The caller may supply it to its child as `BRICKHOUSE_WORKER_TOKEN`; it is not placed in `BRICKHOUSE_RUN_CONTEXT_JSON` or a request URL. The parent retains only a SHA-256 digest keyed to the run and attempt. Default lifetime is five minutes; callers can choose a positive TTL up to 24 hours and one minute, covering the worker's maximum execution time and termination grace. Expiry and explicit attempt revocation reject subsequent calls. The authenticator is thread-safe and attempt identities are validated in constant time only after digest lookup. Credential `repr` omits the bearer value; worker results contain no credential field. The task-value socket use is described in [ADR 0033](0033-attempt-scoped-task-values.md).

The worker launcher validates that the credential's run/attempt match the immutable context before starting the child. The attempt owner is responsible for calling `revoke()` or `revoke_attempt()` on completion; B013.3 composes revocation with cancellation/reaping. The wire parser/authenticator are internal primitives. No public job route or HTTP dispatch layer is added in this slice; the later job service must call the authenticator before dispatching the envelope's named operation and must keep its own route allowlist.

## Consequences and limits

- Invalid, expired, revoked, malformed, or cross-attempt calls are rejected; there is no anonymous/local fallback.
- A leaked bearer token grants its single attempt's local service scope until revoked or expired. This is not a hostile-code sandbox. A child and its descendants can read the environment variable.
- Protocol version 1 bounds and parses messages but does not promise idempotency or authorize arbitrary service/operation pairs. The service router owns operation permissions and replay behavior.
- Token values are ephemeral and never persisted. Restarting the parent invalidates every token. This is intentional because B013 workers are local children.
- This decision does not implement job persistence, process cancellation, descendant cleanup, or a real private listener. Those remain B013.3 and B014.
