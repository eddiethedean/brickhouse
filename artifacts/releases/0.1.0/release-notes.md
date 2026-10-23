# BrickHouse 0.1.0 release notes

Prepared initial alpha; this candidate has not been published.

BrickHouse runs selected Databricks workflows locally without a JVM. Sparkless
4.15.2 and native delta-rs share one persistent catalog and table authority across
SQL, DataFrames, supervised Python jobs, and optional Jupyter notebooks.

The release includes bounded SQL query/DDL/parameter/mutation/history/snapshot
behavior; fresh worker processes; job DAGs, retries, cancellation and timeouts;
widgets, scoped secrets and Python task values; simulated serverless/classic
compute and SQL warehouse lifecycles; selected pinned SDK/CLI/bundle/Terraform
and SQL connector integrations; and durable restart and local snapshot/restore.

The release orders workflow computes and writes native BIGINT/DECIMAL aggregates
across seven Python, seven ipynb, and three source notebook variants, verifies
three warehouse profiles, and checks reopened table identity and data. No casts
are used to force the expected aggregate output types.

Apache-2.0 is the project license. Third-party dependencies retain their own
licenses and notices. Base installs exclude the optional notebook runtime.

Qualification passed **1368 installed-wheel offline cases** across Ubuntu 22.04 x86_64/macOS 26 arm64, Python 3.11/3.12, base/notebooks, plus **1832 full-source passes** across the four OS/Python combinations. Required skips: **0**. See the [CI run](https://github.com/eddiethedean/brickhouse/actions/runs/39875187679), [exact versions](versions.json), and [scope](scope.json).

Compatibility remains partial: 24 capabilities are partial, 2 simulated, and
41 planned; none is fully supported. SQL COUNT's local int32 output differs
from DataFrame int64 (Sparkless issue #1723). Thrift, kernel Arrow-stream/LZ4,
broad SQL/metadata/remote authentication and Windows remain unqualified.
Workers are not sandboxed and the loopback API token is not authentication.
The owner configured and verified the saved PyPI pending publisher. Actual OIDC
upload and publication require separate authorization; see the [checklist](release-checklist.md).
