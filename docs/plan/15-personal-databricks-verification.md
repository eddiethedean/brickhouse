# 15 — Personal Databricks reference verification

Status: adopted development plan; capture tooling and remote verification are not implemented or verified.

## Purpose and authorization

Use the owner's personal Databricks account as a real-world behavioral reference when designing BrickHouse and its tests. The owner authorized this use on 2026-10-16 and reported supplying `DATABRICKS_TOKEN` in the local `.env` file and GitHub Actions secrets. Credential availability and workspace access have not been checked. This authorization persists for occasional, bounded reference probes during implementation.

Remote startup and execution are too slow for routine testing. Every normal development test, pull-request check, milestone gate, and release test runs locally against BrickHouse and retained reference fixtures. Real Databricks runs answer selected behavior questions and verify representative code; they are outside the test suite and never complete a BrickHouse runtime operation.

This authorization covers small probes with synthetic data in a dedicated, run-scoped namespace using explicitly selected existing resources. It does not authorize purchasing services, provisioning paid compute, or increasing spend limits. Record the permitted compute/resource policy before the first execution; an available token alone supplies no budget or compute selection. Use the account's available features as observed rather than assuming an account tier, runtime, or Unity Catalog entitlement.

## Reference-to-test workflow

1. Identify a concrete behavior question or mismatch, the relevant task/capability, and the equivalent Databricks operation. Prefer one small script or request covering representative success and failure cases over a remote suite.
2. Specify synthetic inputs, expected schema/types, session settings, ordering rules, limits, and cleanup before capture. Useful cases include nulls, decimal precision, timezones, namespace resolution, job outputs, SDK errors, and table mutations.
3. Explicitly select the real workspace and permitted existing compute if needed. Execute only the chosen probe, with bounded rows, requests, duration, retries, and created resources. Reuse a captured result unless a new question or relevant version/implementation change warrants a fresh run.
4. Capture actual results, schemas, errors, and lifecycle observations. Record the script/request and settings needed to reproduce them. If representative BrickHouse code already exists, run the equivalent small operation and retain the comparison.
5. Sanitize and review the capture. Replace documented nondeterministic or identifying values with consistent placeholders while preserving relationships, types, error categories, and meaningful behavior. Keep raw responses outside tracked files and CI uploads.
6. Turn the reviewed observation into deterministic tests that exercise BrickHouse locally with real local dependencies. Tests must assert the observed behavior, including failures, without contacting Databricks or requiring a token.
7. Record mismatches and profile limits. A remote success establishes reference behavior; only a passing local behavioral test supports a BrickHouse capability claim. Close probe-owned resources and record cleanup failures without touching unrelated account data.

Denied permissions, unavailable features, or compute restrictions are evidence about the observed account and capture scope. Record those limits; do not substitute invented successful fixtures or generalize them into all Databricks versions.

## Credentials and configuration

| Setting | Local capture | Manual GitHub capture |
|---|---|---|
| `DATABRICKS_TOKEN` | Owner-supplied, ignored `.env` or explicit process environment | Owner-supplied Actions secret, injected into the capture job only |
| `DATABRICKS_HOST` | Explicit workspace URL, supplied separately | Explicit repository/environment configuration |
| Compute and scratch namespace | Explicit non-secret configuration for the selected probe | Explicit configuration/dispatch inputs constrained to permitted resources |

The host and compute/resource policy remain to be configured and verified. Do not infer a workspace from the token. The capture tool may explicitly load `.env`; importing BrickHouse or running normal tests must not load it or activate remote SDK authentication. Local SDK tests use an explicit loopback BrickHouse host and local compatibility token, with inherited Databricks credentials/profile discovery excluded from their client configuration and worker environments.

Never print the token, authorization headers, `.env` contents, secret values, or unredacted HTTP traces. Sanitize workspace URLs, user/account identifiers, object paths, signed URLs, and response bodies before retaining evidence. Fixture manifests describe the credential source by variable name only. `.env` and raw captures are ignored; only reviewed synthetic fixtures enter source control.

## Capture tooling and GitHub Actions plan

B005 owns the initial client/profile inventory, bounded capture tooling, fixture format, and capture-isolation checks. Decompose that work before implementation while preserving B005's existing acceptance criteria. The tool executes a named, reviewed probe independently of pytest or other test discovery. Missing credentials/configuration fail an explicitly requested capture with an actionable message; local tests run normally without them.

Normal CI jobs receive no personal Databricks token and run without access to a real workspace. Prove fixture consumption with credentials absent and real-host traffic blocked; also prove inherited credentials cannot redirect local clients. Keep optional local PySpark reference generation in its separate oracle environment.

An optional `workflow_dispatch` workflow refreshes selected reference captures from reviewed code on the trusted default branch. No push, pull-request, scheduled, or `pull_request_target` event runs personal-account probes. Inject `DATABRICKS_TOKEN` only into the capture job, restrict permissions, allow only curated probes and permitted resources, serialize captures against the scratch namespace, and enforce timeout/request/data limits. Reject untrusted refs and arbitrary submitted code. This workflow is a development tool, outside required CI and release gates.

Redact before any artifact upload; retain only sanitized outputs with bounded retention. Review resulting fixtures before committing them. Neither workflow dispatch nor token presence grants permission to provision paid infrastructure. Cleanup is limited to resources recorded as owned by that capture.

## Fixture and evidence contract

Store reviewed fixtures under `tests/fixtures/oracles/databricks/`, with capture scripts and versioned manifests alongside them. Use `artifacts/oracles/raw/` for ignored local raw material. These paths are future tooling targets, not existing fixtures.

Each manifest records:

- Capture ID/date, task and capability IDs, source product, exact SDK/CLI/connector versions, and the relevant runtime or warehouse version when observable. Mark unavailable version fields explicitly; a SQL warehouse observation is not automatically a Spark runtime observation.
- Observed account feature scope, a sanitized workspace alias, compute kind, semantic profile mapping and exclusions, timezone/ANSI/session settings, and capture-tool/source revision when available.
- Exact operation/script, synthetic input data/schema, returned schema/types/values or error class/code/message, and lifecycle observations where relevant.
- Ordering requirements, normalization/redaction rules, sanitized artifact paths, limits used, and cleanup outcome.
- The corresponding local regression test IDs and comparison/mismatch report once implemented. Reference evidence and passing local implementation evidence remain separately identifiable.

Re-capture only affected cases when a reference version, client transport, or unresolved behavior changes. Preserve earlier observations and their profile mappings rather than overwriting historical evidence. The personal account is one observed environment; claims remain bounded to the tested clients, features, and profiles.
