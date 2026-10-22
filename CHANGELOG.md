# Changelog

## 0.1.0

Initial alpha for local, JVM-free Databricks workflow emulation.

- One persistent catalog and native Delta table service shared by Sparkless,
  SQL statements, Python workers, and optional Jupyter notebooks.
- Selected SQL queries, DDL, parameters, mutations, history, and snapshots.
- Bounded Python job DAGs, retries, cancellation, timeout, context isolation,
  scoped secrets, widgets, task values, and retained output.
- Serverless and classic local compute profiles and simulated cluster/warehouse
  lifecycle metadata. No cloud compute is allocated.
- Selected Databricks SDK 0.153.0, CLI 1.21.0, Terraform 1.16.6/provider 1.137.0,
  and SQL connector 4.6.0/kernel 1.1.0 integrations against loopback services.
- Operation-journal recovery, workspace snapshot/restore, and ownership checks.
- Sparkless 4.15.2 with the selected nested-field and UTC Delta timestamp fixes.
- Separate base and notebook installations; Apache-2.0 project license with
  retained dependency notices.

Compatibility is partial and bounded by the [capability registry](planning/capabilities.json).
See the [release review bundle](artifacts/releases/0.1.0/README.md) for exact
platform/Python qualification evidence and remaining publication prerequisites.
