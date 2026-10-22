# Phase 0.1 orders workflow

This example creates a managed Delta table through SQL, submits a real Python
worker with the pinned Databricks SDK, appends typed order rows through the
shared local table service, and reads the committed aggregate back through SQL.
The SDK token is a local compatibility value; the service has no inbound
authentication. Classic compute and SQL warehouse resources are simulated.

Use a throwaway workspace. The run leaves its table, job history, and warehouse
in the workspace so you can inspect them after the command finishes.

```sh
python -m pip install .
python -m pip install databricks-sdk==0.153.0
brickhouse init --workspace ./workspace
brickhouse serve --workspace ./workspace --host 127.0.0.1 --port 8000
```

In another terminal, from the repository root, run:

```sh
python examples/phase_0_1/run_orders.py --host http://127.0.0.1:8000
```

The program prints one JSON record containing the table, warehouse, job, run,
and query result. The expected result is `[["alice", "2", "15.75"]]`.
Only this classic-standard Python job and local serverless-enabled Pro warehouse
combination is shown. It does not establish Databricks cloud/runtime parity,
remote authentication, or sandboxing. Remove the throwaway workspace directory
when finished.
