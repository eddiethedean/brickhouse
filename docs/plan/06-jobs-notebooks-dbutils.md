# 06 — Jobs, notebooks, and dbutils

## Runtime strategy

Replace Spark-oriented job launching with a BrickHouse supervisor and fresh Python worker. Reuse selected Minilake task validation/lifecycle ideas only after adapting them to persistent instance-scoped state. The worker gets a run ID, attempt ID, profile, parameters, namespace, environment reference, workspace mapping, cancellation channel, and run-scoped internal credential.

Use the resolved environment's interpreter, not an unqualified `python3` from PATH. Initially require the current supported BrickHouse environment. Later environment specifications can create cached virtual environments from locked dependencies. Do not automatically run arbitrary library installation requests from a cluster configuration.

## Job lifecycle

Persist job definitions, immutable submitted settings, run/task IDs, attempt numbers, timestamps, parameters, state transitions, logs, outputs, and error details. Validate task types and dependencies before execution. Map internal states to the pinned Databricks client's enums at the API boundary.

Required behavior: idempotent submission where the API provides a token, dependency validation, bounded concurrency, timeout, cancellation of the process group, process reaping, retries, and restart reconciliation. A terminal state is immutable except for an explicitly modeled repair/new attempt. Skipping a task because its dependency condition failed is valid; skipping an unsupported requested task as if execution succeeded is not.

Cancellation is an observable lifecycle: request stop, signal worker, escalate after grace, reap, persist terminal state. A write that already committed remains committed; cancellation must not fabricate rollback. Retry policy consults mutation idempotency records.

## Python compatibility

Explicit Sparkless imports are the first supported mode. Managed-worker compatibility can later expose selected `pyspark.sql` and `delta.tables` names through a launcher-scoped import finder/bootstrap. Preserve type identity within the worker, nested imports, aliases, and exception mappings. Track each covered namespace in capabilities.

Do not ship a top-level `pyspark` wheel that overwrites the genuine package. Do not rewrite user source with regex. Unsupported `pyspark.ml`, `rdd`, Java gateways, arbitrary jars, Scala tasks, and JVM-specific configuration must produce clear capability errors until a native equivalent is deliberately implemented.

The `delta.tables.DeltaTable` facade is a distinct compatibility task from installing `deltalake`. Map supported methods and builders into the table service, including path validation and commit IDs. Never let it create an independent unmanaged writer path for registered tables.

## dbutils implementation plan

| Surface | Proposed behavior |
|---|---|
| `fs.ls/head/put/cp/mv/rm/mkdirs` | File service and namespace resolver; reuse SDK behavior where endpoints match |
| `widgets.text/dropdown/combobox/multiselect/get/getAll/remove/removeAll` | Per-run parameter store with validation and defaults; optional notebook UI layer |
| `secrets.get/getBytes/list/listScopes` | Job-scoped secret provider; metadata APIs separate from value resolution |
| `jobs.taskValues.set/get` | Durable values keyed by run, task, attempt, and key; enforce serialized limits/default rules |
| `notebook.run` | Child run using parameter scope, timeout, output contract, and parent cancellation |
| `notebook.exit` | Controlled worker signal converted to notebook output, not accidental process failure |
| Context getters | Versioned documented subset of run/job/path context |
| Mounts and credential utilities | Explicit unsupported state until meaningful native semantics exist |

The SDK's native secrets getter calls an endpoint rejected by the reviewed Minilake server. Installing the SDK does not close that gap. Inject a scoped provider into the managed runtime; never place the entire secret store in environment variables or public responses. Capture/redact secret-bearing logs and document limitations of redaction for transformed values.

## Notebook execution

Accept a documented format subset: `.ipynb` via nbformat/nbclient and Databricks Python source exports via a tested converter. Treat cell separators and magic markers structurally. Handle `%python`, `%sql`, `%md`, and `%run` through explicit handlers; reject Scala/R and unknown magics. `%run` shares notebook scope where required; `dbutils.notebook.run` creates a child invocation. Do not conflate them.

Inject `spark`, `dbutils`, and supported `display` helpers into a fresh kernel. Capture cell outputs, execution counts, errors, and notebook result artifacts. Parameterization may use Papermill, but widgets and job parameter precedence remain BrickHouse contracts. Timeout and cancellation must reach the kernel and descendants.

`%pip` and interpreter restart require environment lifecycle design; initially reject them with a clear explanation. Do not permit a notebook dependency installer to introduce Spark/JVM runtime silently.

## Minimum end-to-end job test

Use the real SDK to upload a Python script, create a job, submit it, poll it, and retrieve output. The script reads parameters and one scoped secret, builds/transforms a DataFrame, writes a managed Delta table, and returns a count. A second task reads that table by name. Validate output and data through the SQL API, then restart the server and repeat the read.

Negative cases: invalid namespace, unsupported task type, missing workspace file, bad parameter type, failing dependency, worker crash, timeout, cancellation during a write, secret access outside job scope, repeated submission token, and restart with an active run. No Java process or external compute request may occur.
