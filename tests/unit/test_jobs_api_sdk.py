from __future__ import annotations

import importlib.metadata
import io
import json
import os
import select
import signal
import sqlite3
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from threading import Thread

import pyarrow as pa
import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InvalidParameterValue, ResourceConflict, ResourceDoesNotExist
from databricks.sdk.service.compute import ClusterSpec, DataSecurityMode, Environment
from databricks.sdk.service.jobs import (
    JobCluster,
    JobEnvironment,
    RunLifeCycleState,
    RunResultState,
    SparkPythonTask,
    Task,
    TaskDependency,
)
from databricks.sdk.service.workspace import ImportFormat, Language

from brickhouse.catalog import parse_identifier
from brickhouse.diagnostic_server import create_diagnostic_server
from brickhouse.jobs.models import JobAttemptState
from brickhouse.jobs.repository import JobNotFoundError
from brickhouse.runtime.outcomes import worker_process_exists
from brickhouse.tables import DeltaRsTableService, WriteMode
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace


@contextmanager
def local_jobs_sdk_client(path: Path) -> Iterator[tuple[WorkspaceClient, Path, WorkspaceContext]]:
    descriptor = initialize_workspace(path)
    context = WorkspaceContext.local(descriptor.path)
    context.__enter__()
    server = create_diagnostic_server(context, "127.0.0.1", 0)
    thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    client = WorkspaceClient(
        host=f"http://127.0.0.1:{server.server_port}",
        token="brickhouse-local-test-token",
        auth_type="pat",
    )
    try:
        yield client, descriptor.path, context
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
        context.close()


def test_pinned_sdk_uploads_runs_and_retrieves_bounded_python_output(tmp_path: Path) -> None:
    assert importlib.metadata.version("databricks-sdk") == "0.153.0"
    source = (
        b"import json, os, sys, time\n"
        b"if '--large' in sys.argv: print('X' * 1100000)\n"
        b"if '--slow' in sys.argv: time.sleep(2)\n"
        b"print('ARGUMENTS=' + json.dumps(sys.argv[1:]))\n"
        b"print('RUN_CONTEXT=' + os.environ['BRICKHOUSE_RUN_CONTEXT_JSON'])\n"
    )

    with local_jobs_sdk_client(tmp_path / "workspace") as (client, workspace_path, _context):
        client.workspace.upload(
            "/Workspace/Shared/echo.py",
            io.BytesIO(source),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
        with pytest.raises(ResourceDoesNotExist):
            client.jobs.create(
                name="missing-input",
                tasks=[
                    Task(
                        task_key="missing",
                        spark_python_task=SparkPythonTask(
                            python_file="/Workspace/Shared/missing.py"
                        ),
                        new_cluster=ClusterSpec(
                            spark_version="spark35-local-v1",
                            data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
                            num_workers=0,
                        ),
                    )
                ],
            )

        job = client.jobs.create(
            name="echo-arguments",
            description="Run one local Python script and capture its output.",
            max_concurrent_runs=1,
            tasks=[
                Task(
                    task_key="echo",
                    description="Echo the bounded argv values.",
                    spark_python_task=SparkPythonTask(
                        python_file="/Workspace/Shared/echo.py",
                        parameters=["--name", "Alice Example"],
                    ),
                    new_cluster=ClusterSpec(
                        spark_version="spark35-local-v1",
                        data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
                        num_workers=0,
                    ),
                )
            ],
        )
        persisted_definition = client.jobs.get(job.job_id)
        assert persisted_definition.settings is not None
        assert persisted_definition.settings.name == "echo-arguments"
        assert persisted_definition.settings.tasks is not None
        assert persisted_definition.settings.tasks[0].spark_python_task is not None
        assert (
            persisted_definition.settings.tasks[0].spark_python_task.python_file
            == "/Workspace/Shared/echo.py"
        )
        assert persisted_definition.settings.tasks[0].new_cluster is not None
        assert (
            persisted_definition.settings.tasks[0].new_cluster.spark_version == "spark35-local-v1"
        )
        accepted = client.jobs.run_now(job.job_id, idempotency_token="echo-run-001")
        run = accepted.result(timeout=timedelta(seconds=10))
        assert run.state is not None
        assert run.state.life_cycle_state is RunLifeCycleState.TERMINATED
        assert run.state.result_state is RunResultState.SUCCESS
        assert run.number_in_job == 1
        assert run.tasks is not None and run.tasks[0].task_key == "echo"
        assert run.description == "Run one local Python script and capture its output."
        assert run.tasks[0].description == "Echo the bounded argv values."

        output = client.jobs.get_run_output(run.run_id)
        assert output.metadata is not None
        assert output.metadata.state is not None
        assert output.metadata.state.result_state is RunResultState.SUCCESS
        assert output.logs is not None
        assert 'ARGUMENTS=["--name", "Alice Example"]' in output.logs
        assert '"run_id":"1"' in output.logs
        assert output.logs_truncated is False

        replay = client.jobs.run_now(job.job_id, idempotency_token="echo-run-001").result(
            timeout=timedelta(seconds=10)
        )
        assert replay.run_id == run.run_id
        with pytest.raises(InvalidParameterValue):
            client.jobs.run_now(job.job_id, python_params=["api_key=must-not-be-stored"])

        first_slow_submission = client.jobs.run_now(
            job.job_id,
            python_params=["--slow"],
            idempotency_token="echo-run-slow-001",
        )
        replayed_slow_submission = client.jobs.run_now(
            job.job_id,
            python_params=["--slow"],
            idempotency_token="echo-run-slow-001",
        )
        assert first_slow_submission.run_id == replayed_slow_submission.run_id
        slow_status = client.jobs.get_run(first_slow_submission.run_id)
        assert slow_status.state is not None
        assert slow_status.state.life_cycle_state is RunLifeCycleState.RUNNING
        assert first_slow_submission.result(timeout=timedelta(seconds=10)).state is not None

        large_run = client.jobs.run_now(job.job_id, python_params=["--large"]).result(
            timeout=timedelta(seconds=10)
        )
        large_output = client.jobs.get_run_output(large_run.run_id)
        assert large_output.logs is not None
        assert len(large_output.logs.encode("utf-8")) <= 1024 * 1024
        assert large_output.logs_truncated is True

        client.workspace.upload(
            "/Workspace/Shared/fail.py",
            io.BytesIO(
                b"import sys\n"
                b"print('before failure')\n"
                b"print('observed error', file=sys.stderr)\n"
                b"sys.exit(4)\n"
            ),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
        failing_job = client.jobs.create(
            name="observed-failure",
            tasks=[
                Task(
                    task_key="fail",
                    spark_python_task=SparkPythonTask(python_file="/Workspace/Shared/fail.py"),
                    new_cluster=ClusterSpec(
                        spark_version="spark35-local-v1",
                        data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
                        num_workers=0,
                    ),
                )
            ],
        )
        failed_run = client.jobs.run_now(failing_job.job_id).result(timeout=timedelta(seconds=10))
        assert failed_run.state is not None
        assert failed_run.state.life_cycle_state is RunLifeCycleState.TERMINATED
        assert failed_run.state.result_state is RunResultState.FAILED
        failed_output = client.jobs.get_run_output(failed_run.run_id)
        assert failed_output.logs == "before failure\n"
        assert failed_output.error == "observed error\n"

        with pytest.raises(InvalidParameterValue):
            client.jobs.create(
                name="unsupported-retries",
                tasks=[
                    Task(
                        task_key="retry",
                        max_retries=11,
                        spark_python_task=SparkPythonTask(python_file="/Workspace/Shared/echo.py"),
                        new_cluster=ClusterSpec(
                            spark_version="spark35-local-v1",
                            data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
                            num_workers=0,
                        ),
                    )
                ],
            )
        with pytest.raises(InvalidParameterValue):
            client.jobs.create(
                name="unsupported-concurrency",
                max_concurrent_runs=5,
                tasks=[
                    Task(
                        task_key="echo",
                        spark_python_task=SparkPythonTask(python_file="/Workspace/Shared/echo.py"),
                        new_cluster=ClusterSpec(
                            spark_version="spark35-local-v1",
                            data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
                            num_workers=0,
                        ),
                    )
                ],
            )

        client.jobs.delete(job.job_id)
        with pytest.raises(ResourceDoesNotExist):
            client.jobs.get(job.job_id)
        with pytest.raises(ResourceDoesNotExist):
            client.jobs.run_now(job.job_id)

    with WorkspaceContext.local(workspace_path) as reopened:
        assert reopened.workspace.path.joinpath("files", "Shared", "echo.py").read_bytes() == source
        saved_job = reopened.jobs.get_job(job.job_id)
        saved_run = reopened.jobs.get_run(run.run_id)
        assert saved_job.name == "echo-arguments"
        assert saved_run.state.value == "succeeded"
        with pytest.raises(JobNotFoundError):
            reopened.jobs.get_active_job(job.job_id)
        assert reopened.jobs.get_run(large_run.run_id).state.value == "succeeded"
        assert reopened.jobs.get_run(failed_run.run_id).state.value == "failed"
        attempts = reopened.jobs.list_attempts(run.run_id, "echo")
        assert len(attempts) == 1
        persisted_output = reopened.jobs.get_attempt_output(run.run_id, "echo", 1)
        assert persisted_output is not None
        assert 'ARGUMENTS=["--name", "Alice Example"]' in persisted_output.stdout
        persisted_large_output = reopened.jobs.get_attempt_output(large_run.run_id, "echo", 1)
        assert persisted_large_output is not None
        assert persisted_large_output.stdout_truncated is True
        assert len(persisted_large_output.stdout.encode("utf-8")) <= 1024 * 1024


def test_sdk_admits_declared_concurrency_and_replays_tokens_without_duplicates(
    tmp_path: Path,
) -> None:
    source = b"import time\ntime.sleep(4)\nprint('finished')\n"

    with local_jobs_sdk_client(tmp_path / "workspace") as (client, _workspace_path, context):
        client.workspace.upload(
            "/Workspace/Shared/slow.py",
            io.BytesIO(source),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
        job = client.jobs.create(
            name="two-active-runs",
            max_concurrent_runs=2,
            tasks=[
                Task(
                    task_key="slow",
                    spark_python_task=SparkPythonTask(python_file="/Workspace/Shared/slow.py"),
                    new_cluster=ClusterSpec(
                        spark_version="spark35-local-v1",
                        data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
                        num_workers=0,
                    ),
                )
            ],
        )

        first = client.jobs.run_now(job.job_id, idempotency_token="capacity-token-001")
        second = client.jobs.run_now(job.job_id)
        replay = client.jobs.run_now(job.job_id, idempotency_token="capacity-token-001")
        assert replay.run_id == first.run_id
        with pytest.raises(ResourceConflict):
            client.jobs.run_now(job.job_id)
        with pytest.raises(ResourceConflict):
            client.jobs.run_now(
                job.job_id,
                python_params=["changed"],
                idempotency_token="capacity-token-001",
            )
        assert [run.run_id for run in context.jobs.list_runs(job.job_id)] == [
            first.run_id,
            second.run_id,
        ]

        first_result = first.result(timeout=timedelta(seconds=10))
        second_result = second.result(timeout=timedelta(seconds=10))
        assert first_result.run_id == first.run_id
        assert second_result.run_id == second.run_id
        terminal_replay = client.jobs.run_now(
            job.job_id, idempotency_token="capacity-token-001"
        ).result(timeout=timedelta(seconds=10))
        assert terminal_replay.run_id == first.run_id
        assert len(context.jobs.list_runs(job.job_id)) == 2


def test_sdk_enforces_the_local_process_limit_across_jobs(tmp_path: Path) -> None:
    source = b"import time\ntime.sleep(10)\n"

    with local_jobs_sdk_client(tmp_path / "workspace") as (client, _workspace_path, context):
        client.workspace.upload(
            "/Workspace/Shared/slow.py",
            io.BytesIO(source),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
        jobs = [
            client.jobs.create(
                name=f"global-limit-{index}",
                max_concurrent_runs=4,
                tasks=[
                    Task(
                        task_key="slow",
                        spark_python_task=SparkPythonTask(python_file="/Workspace/Shared/slow.py"),
                        new_cluster=ClusterSpec(
                            spark_version="spark35-local-v1",
                            data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
                            num_workers=0,
                        ),
                    )
                ],
            )
            for index in range(5)
        ]

        admitted_runs = [client.jobs.run_now(job.job_id) for job in jobs[:4]]
        with pytest.raises(ResourceConflict):
            client.jobs.run_now(jobs[4].job_id)
        assert len(context.jobs.list_runs(jobs[4].job_id)) == 0
        assert len(admitted_runs) == 4


@pytest.mark.skipif(os.name != "posix", reason="hard process restart check is POSIX-scoped")
def test_sdk_restart_marks_abandoned_retry_interrupted_and_keeps_prior_output(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    script = (
        b"from pathlib import Path\n"
        b"import sys, time\n"
        b"counter = Path('attempt-count.txt')\n"
        b"count = int(counter.read_text()) + 1 if counter.exists() else 1\n"
        b"counter.write_text(str(count))\n"
        b"if count == 1:\n"
        b"    print('first-attempt-output')\n"
        b"    sys.exit(7)\n"
        b"Path('second-attempt.started').write_text('ready')\n"
        b"print('second-attempt-before-interrupt', flush=True)\n"
        b"time.sleep(60)\n"
    )
    server_code = (
        "import sys\n"
        "from brickhouse.diagnostic_server import create_diagnostic_server\n"
        "from brickhouse.workspace.context import WorkspaceContext\n"
        "with WorkspaceContext.local(sys.argv[1]) as context:\n"
        "    server = create_diagnostic_server(context, '127.0.0.1', 0)\n"
        "    print(server.server_port, flush=True)\n"
        "    server.serve_forever(poll_interval=0.01)\n"
    )
    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in {"DATABRICKS_TOKEN", "DATABRICKS_HOST"}
    }
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[2] / "src")
    server_process: subprocess.Popen[str] | None = None
    worker_pid: int | None = None
    compute_id: str | None = None
    try:
        server_process = subprocess.Popen(
            [sys.executable, "-c", server_code, str(workspace.path)],
            cwd=Path(__file__).resolve().parents[2],
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert server_process.stdout is not None
        ready, _, _ = select.select([server_process.stdout], [], [], 10)
        assert ready, "job server did not report its loopback port"
        port_line = server_process.stdout.readline().strip()
        if not port_line:
            _, server_error = server_process.communicate(timeout=5)
            raise AssertionError(f"job server exited before startup: {server_error}")
        port = int(port_line)
        client = WorkspaceClient(
            host=f"http://127.0.0.1:{port}",
            token="brickhouse-local-test-token",
            auth_type="pat",
        )
        client.workspace.upload(
            "/Workspace/Shared/retry.py",
            io.BytesIO(script),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
        job = client.jobs.create(
            name="restart-during-retry",
            tasks=[
                Task(
                    task_key="retry",
                    max_retries=1,
                    spark_python_task=SparkPythonTask(python_file="/Workspace/Shared/retry.py"),
                    new_cluster=ClusterSpec(
                        spark_version="spark35-local-v1",
                        data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
                        num_workers=0,
                    ),
                )
            ],
        )
        submission = client.jobs.run_now(job.job_id, idempotency_token="restart-token-001")
        active_marker = workspace.path / "files" / "second-attempt.started"
        deadline = time.monotonic() + 10
        while not active_marker.exists() and time.monotonic() < deadline:
            if server_process.poll() is not None:
                raise AssertionError("job server exited before the retry started")
            time.sleep(0.01)
        assert active_marker.exists(), "retry worker did not reach its active state"

        with sqlite3.connect(workspace.metadata_path, timeout=5) as connection:
            row = connection.execute(
                "SELECT worker_pid FROM worker_attempt_outcomes "
                "WHERE run_id = ? AND state = 'running'",
                (str(submission.run_id),),
            ).fetchone()
            resource_row = connection.execute(
                "SELECT compute_id FROM compute_resources WHERE run_id = ?",
                (submission.run_id,),
            ).fetchone()
        assert row is not None and row[0] is not None
        worker_pid = row[0]
        assert resource_row is not None
        compute_id = resource_row[0]

        server_process.kill()
        server_process.wait(timeout=5)
        try:
            os.kill(worker_pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        worker_deadline = time.monotonic() + 5
        while worker_process_exists(worker_pid) and time.monotonic() < worker_deadline:
            time.sleep(0.01)
        assert not worker_process_exists(worker_pid), "worker process did not exit"

        with WorkspaceContext.local(workspace.path) as context:
            server = create_diagnostic_server(context, "127.0.0.1", 0)
            thread = Thread(
                target=server.serve_forever,
                kwargs={"poll_interval": 0.01},
                daemon=True,
            )
            thread.start()
            restarted_client = WorkspaceClient(
                host=f"http://127.0.0.1:{server.server_port}",
                token="brickhouse-local-test-token",
                auth_type="pat",
            )
            try:
                recovered = restarted_client.jobs.get_run(submission.run_id)
                assert recovered.state is not None
                assert recovered.state.life_cycle_state is RunLifeCycleState.INTERNAL_ERROR
                assert recovered.state.state_message == "WORKSPACE_RESTARTED"
                assert recovered.tasks is not None
                assert recovered.tasks[0].state is not None
                assert recovered.tasks[0].state.life_cycle_state is RunLifeCycleState.INTERNAL_ERROR

                attempts = context.jobs.list_attempts(submission.run_id, "retry")
                assert len(attempts) == 2
                assert attempts[0].state is JobAttemptState.FAILED
                assert attempts[0].exit_code == 7
                assert attempts[1].state is JobAttemptState.INTERRUPTED
                assert attempts[1].exit_code is None
                worker_outcome = context.worker_outcomes.get(
                    str(submission.run_id), attempts[1].attempt_id
                )
                assert worker_outcome is not None
                assert worker_outcome.state == "interrupted"
                assert worker_outcome.exit_code is None
                recovered_consumer = context.compute.get_run_consumer(submission.run_id, "retry")
                assert recovered_consumer is not None
                assert recovered_consumer.compute_id == compute_id
                assert recovered_consumer.state == "released"
                assert context.compute.get_resource(compute_id).state.value == "TERMINATED"

                output = restarted_client.jobs.get_run_output(submission.run_id)
                assert output.logs == "first-attempt-output\n"
                replay = restarted_client.jobs.run_now(
                    job.job_id, idempotency_token="restart-token-001"
                )
                assert replay.run_id == submission.run_id
                assert len(context.jobs.list_runs(job.job_id)) == 1
                assert (workspace.path / "files" / "attempt-count.txt").read_text() == "2"
            finally:
                server.shutdown()
                thread.join(timeout=5)
                server.server_close()
    finally:
        if server_process is not None and server_process.poll() is None:
            server_process.kill()
            server_process.wait(timeout=5)
        if worker_pid is not None:
            try:
                os.kill(worker_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_sdk_executes_out_of_order_dag_only_after_dependencies_succeed(tmp_path: Path) -> None:
    with local_jobs_sdk_client(tmp_path / "workspace") as (client, workspace_path, context):
        for name, source in (
            (
                "producer.py",
                b"from pathlib import Path\nPath('producer.ready').write_text('ready')\n"
                b"print('producer completed')\n",
            ),
            (
                "consumer.py",
                b"from pathlib import Path\n"
                b"assert Path('producer.ready').read_text() == 'ready'\n"
                b"print('consumer observed producer')\n",
            ),
        ):
            client.workspace.upload(
                f"/Workspace/Shared/{name}",
                io.BytesIO(source),
                format=ImportFormat.SOURCE,
                language=Language.PYTHON,
                overwrite=True,
            )

        cluster = ClusterSpec(
            spark_version="spark35-local-v1",
            data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
            num_workers=0,
        )
        job = client.jobs.create(
            name="out-of-order-dag",
            tasks=[
                Task(
                    task_key="consumer",
                    depends_on=[TaskDependency(task_key="producer")],
                    spark_python_task=SparkPythonTask(python_file="/Workspace/Shared/consumer.py"),
                    new_cluster=cluster,
                ),
                Task(
                    task_key="producer",
                    spark_python_task=SparkPythonTask(python_file="/Workspace/Shared/producer.py"),
                    new_cluster=cluster,
                ),
            ],
        )
        completed = client.jobs.run_now(job.job_id).result(timeout=timedelta(seconds=10))
        assert completed.state is not None
        assert completed.state.life_cycle_state is RunLifeCycleState.TERMINATED
        assert completed.state.result_state is RunResultState.SUCCESS
        task_states = {
            task.task_key: task for task in context.jobs.list_task_runs(completed.run_id)
        }
        assert task_states["producer"].state.value == "succeeded"
        assert task_states["consumer"].state.value == "succeeded"
        producer_output = context.jobs.get_attempt_output(completed.run_id, "producer", 1)
        consumer_output = context.jobs.get_attempt_output(completed.run_id, "consumer", 1)
        assert producer_output is not None and producer_output.stdout == "producer completed\n"
        assert consumer_output is not None
        assert consumer_output.stdout == "consumer observed producer\n"

    assert (workspace_path / "files" / "producer.ready").read_text() == "ready"


def test_sdk_task_values_cross_only_successful_upstream_attempts_and_runs(
    tmp_path: Path,
) -> None:
    producer_with_retry = (
        b"import json, os, sys\n"
        b"from pathlib import Path\n"
        b"assert 'BRICKHOUSE_WORKER_TOKEN' not in os.environ\n"
        b"assert 'BRICKHOUSE_WORKER_SOCKET_FD' not in os.environ\n"
        b"run_id = json.loads(os.environ['BRICKHOUSE_RUN_CONTEXT_JSON'])['run_id']\n"
        b"marker = Path('task-value-attempt-' + run_id)\n"
        b"if not marker.exists():\n"
        b"    marker.write_text('failed once')\n"
        b"    assert dbutils.jobs.taskValues.set('state', 'stale') is True\n"
        b"    dbutils.jobs.taskValues.set('failed_only', 'not-visible')\n"
        b"    sys.exit(5)\n"
        b"assert dbutils.jobs.taskValues.set('state', "
        b"{'phase': 'retry-success', 'items': [1, 2]})\n"
        b"assert dbutils.jobs.taskValues.set('nullable', None) is True\n"
    )
    peer = b"dbutils.jobs.taskValues.set('peer_value', 'present')\n"
    consumer = (
        b"import json\n"
        b"from brickhouse.runtime.dbutils_local import DbutilsOperationNotSupportedError\n"
        b"values = dbutils.jobs.taskValues\n"
        b"state = values.get(taskKey='producer', key='state', "
        b"default='no-state', debugValue='debug')\n"
        b"if state == 'no-state':\n"
        b"    assert values.get('producer', 'nullable', default='wrong') == 'wrong'\n"
        b"else:\n"
        b"    assert state == {'phase': 'retry-success', 'items': [1, 2]}\n"
        b"    assert values.get('producer', 'nullable', default='wrong') is None\n"
        b"assert values.get('producer', 'failed_only', default='fallback') == 'fallback'\n"
        b"assert values.get('producer', 'missing', default={'fallback': True}) "
        b"== {'fallback': True}\n"
        b"try:\n"
        b"    values.get('missing_task', 'missing', default='fallback')\n"
        b"except ValueError:\n"
        b"    print('MISSING_TASK=ValueError')\n"
        b"else:\n"
        b"    raise AssertionError('default masked a missing task')\n"
        b"try:\n"
        b"    values.get('producer', 'missing', debugValue='debug')\n"
        b"except ValueError:\n"
        b"    print('DEBUG_VALUE_NOT_USED_IN_JOB=ValueError')\n"
        b"else:\n"
        b"    raise AssertionError('debugValue masked a missing job value')\n"
        b"try:\n"
        b"    values.get('producer', 'missing', default=None)\n"
        b"except ValueError:\n"
        b"    print('NONE_DEFAULT=ValueError')\n"
        b"else:\n"
        b"    raise AssertionError('None was accepted as a default')\n"
        b"try:\n"
        b"    dbutils.jobs.notebook.run('nested', 10)\n"
        b"except DbutilsOperationNotSupportedError:\n"
        b"    print('UNSUPPORTED_JOBS_METHOD=explicit')\n"
        b"else:\n"
        b"    raise AssertionError('unsupported jobs method was accepted')\n"
        b"try:\n"
        b"    values.get('peer', 'peer_value')\n"
        b"except PermissionError:\n"
        b"    print('UNRELATED_TASK=PermissionError')\n"
        b"else:\n"
        b"    raise AssertionError('unrelated task value was visible')\n"
        b"print('STATE=' + json.dumps(state, sort_keys=True))\n"
    )

    with local_jobs_sdk_client(tmp_path / "workspace") as (client, _workspace_path, context):
        for name, source in (
            ("task-value-producer.py", producer_with_retry),
            ("task-value-peer.py", peer),
            ("task-value-consumer.py", consumer),
        ):
            client.workspace.upload(
                f"/Workspace/Shared/{name}",
                io.BytesIO(source),
                format=ImportFormat.SOURCE,
                language=Language.PYTHON,
                overwrite=True,
            )
        cluster = ClusterSpec(
            spark_version="spark35-local-v1",
            data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
            num_workers=0,
        )
        job = client.jobs.create(
            name="task-value-dependencies",
            tasks=[
                Task(
                    task_key="consumer",
                    depends_on=[TaskDependency(task_key="producer")],
                    spark_python_task=SparkPythonTask(
                        python_file="/Workspace/Shared/task-value-consumer.py"
                    ),
                    new_cluster=cluster,
                ),
                Task(
                    task_key="peer",
                    spark_python_task=SparkPythonTask(
                        python_file="/Workspace/Shared/task-value-peer.py"
                    ),
                    new_cluster=cluster,
                ),
                Task(
                    task_key="producer",
                    max_retries=1,
                    spark_python_task=SparkPythonTask(
                        python_file="/Workspace/Shared/task-value-producer.py"
                    ),
                    new_cluster=cluster,
                ),
            ],
        )
        completed = client.jobs.run_now(job.job_id).result(timeout=timedelta(seconds=10))
        assert completed.state is not None
        assert completed.state.result_state is RunResultState.SUCCESS
        producer_attempts = context.jobs.list_attempts(completed.run_id, "producer")
        assert [attempt.state.value for attempt in producer_attempts] == ["failed", "succeeded"]
        assert producer_attempts[0].attempt_id != producer_attempts[1].attempt_id
        consumer_output = context.jobs.get_attempt_output(completed.run_id, "consumer", 1)
        assert consumer_output is not None
        assert "MISSING_TASK=ValueError" in consumer_output.stdout
        assert "DEBUG_VALUE_NOT_USED_IN_JOB=ValueError" in consumer_output.stdout
        assert "NONE_DEFAULT=ValueError" in consumer_output.stdout
        assert "UNSUPPORTED_JOBS_METHOD=explicit" in consumer_output.stdout
        assert "UNRELATED_TASK=PermissionError" in consumer_output.stdout
        assert 'STATE={"items": [1, 2], "phase": "retry-success"}' in consumer_output.stdout
        with sqlite3.connect(context.workspace.metadata_path) as connection:
            rows = connection.execute(
                "SELECT task_key, attempt_number, value_key, value_json FROM job_task_values "
                "WHERE run_id = ? ORDER BY task_key, attempt_number, value_key",
                (completed.run_id,),
            ).fetchall()
        assert ("producer", 1, "failed_only", '"not-visible"') in rows
        assert ("producer", 2, "state", '{"items":[1,2],"phase":"retry-success"}') in rows

        client.workspace.upload(
            "/Workspace/Shared/task-value-producer.py",
            io.BytesIO(b"print('producer deliberately set no values')\n"),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
        second_run = client.jobs.run_now(job.job_id).result(timeout=timedelta(seconds=10))
        assert second_run.state is not None
        assert second_run.state.result_state is RunResultState.SUCCESS
        second_consumer = context.jobs.get_attempt_output(second_run.run_id, "consumer", 1)
        assert second_consumer is not None
        assert "MISSING_TASK=ValueError" in second_consumer.stdout
        assert 'STATE="no-state"' in second_consumer.stdout


def test_sdk_propagates_failure_without_running_dependents_and_keeps_independent_tasks(
    tmp_path: Path,
) -> None:
    with local_jobs_sdk_client(tmp_path / "workspace") as (client, workspace_path, context):
        sources = {
            "fail.py": b"import sys\nprint('failed upstream')\nsys.exit(7)\n",
            "blocked.py": (b"from pathlib import Path\nPath('blocked.ran').write_text('bad')\n"),
            "independent.py": (
                b"from pathlib import Path\nPath('independent.ran').write_text('ok')\n"
                b"print('independent completed')\n"
            ),
        }
        for name, source in sources.items():
            client.workspace.upload(
                f"/Workspace/Shared/{name}",
                io.BytesIO(source),
                format=ImportFormat.SOURCE,
                language=Language.PYTHON,
                overwrite=True,
            )

        cluster = ClusterSpec(
            spark_version="spark35-local-v1",
            data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
            num_workers=0,
        )
        job = client.jobs.create(
            name="dependency-failure",
            tasks=[
                Task(
                    task_key="blocked",
                    depends_on=[TaskDependency(task_key="fail")],
                    spark_python_task=SparkPythonTask(python_file="/Workspace/Shared/blocked.py"),
                    new_cluster=cluster,
                ),
                Task(
                    task_key="fail",
                    spark_python_task=SparkPythonTask(python_file="/Workspace/Shared/fail.py"),
                    new_cluster=cluster,
                ),
                Task(
                    task_key="independent",
                    spark_python_task=SparkPythonTask(
                        python_file="/Workspace/Shared/independent.py"
                    ),
                    new_cluster=cluster,
                ),
            ],
        )
        failed = client.jobs.run_now(job.job_id).result(timeout=timedelta(seconds=10))
        assert failed.state is not None
        assert failed.state.life_cycle_state is RunLifeCycleState.TERMINATED
        assert failed.state.result_state is RunResultState.FAILED
        sdk_tasks = {task.task_key: task for task in failed.tasks or []}
        assert sdk_tasks["blocked"].state is not None
        assert sdk_tasks["blocked"].state.life_cycle_state is RunLifeCycleState.SKIPPED
        assert sdk_tasks["blocked"].state.result_state is RunResultState.UPSTREAM_FAILED
        task_states = {task.task_key: task for task in context.jobs.list_task_runs(failed.run_id)}
        assert task_states["fail"].state.value == "failed"
        assert task_states["blocked"].state.value == "skipped"
        assert task_states["blocked"].error_code == "DEPENDENCY_FAILED"
        assert task_states["blocked"].attempt_count == 0
        assert task_states["independent"].state.value == "succeeded"
        assert context.jobs.list_attempts(failed.run_id, "blocked") == []

    files = workspace_path / "files"
    assert not (files / "blocked.ran").exists()
    assert (files / "independent.ran").read_text() == "ok"


def test_sdk_retries_failed_task_with_distinct_persisted_attempts(tmp_path: Path) -> None:
    with local_jobs_sdk_client(tmp_path / "workspace") as (client, _workspace_path, context):
        source = (
            b"from pathlib import Path\nimport sys\n"
            b"marker = Path('retry.marker')\n"
            b"if not marker.exists():\n"
            b"    marker.write_text('first')\n"
            b"    print('first attempt failed')\n"
            b"    sys.exit(9)\n"
            b"print('second attempt succeeded')\n"
        )
        client.workspace.upload(
            "/Workspace/Shared/retry.py",
            io.BytesIO(source),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
        job = client.jobs.create(
            name="retry-on-failure",
            tasks=[
                Task(
                    task_key="retry",
                    max_retries=1,
                    min_retry_interval_millis=20,
                    spark_python_task=SparkPythonTask(python_file="/Workspace/Shared/retry.py"),
                    new_cluster=ClusterSpec(
                        spark_version="spark35-local-v1",
                        data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
                        num_workers=0,
                    ),
                )
            ],
        )

        run = client.jobs.run_now(job.job_id).result(timeout=timedelta(seconds=10))
        assert run.state is not None
        assert run.state.result_state is RunResultState.SUCCESS
        assert run.tasks is not None and run.tasks[0].attempt_number == 1
        attempts = context.jobs.list_attempts(run.run_id, "retry")
        assert [attempt.state.value for attempt in attempts] == ["failed", "succeeded"]
        assert attempts[0].attempt_number == 1
        assert attempts[1].attempt_number == 2
        assert attempts[0].attempt_id != attempts[1].attempt_id
        assert attempts[0].exit_code == 9 and attempts[1].exit_code == 0
        assert attempts[0].finished_at is not None and attempts[1].started_at is not None
        retry_delay = datetime.fromisoformat(attempts[1].started_at) - datetime.fromisoformat(
            attempts[0].finished_at
        )
        assert retry_delay >= timedelta(milliseconds=15)
        first_output = context.jobs.get_attempt_output(run.run_id, "retry", 1)
        second_output = context.jobs.get_attempt_output(run.run_id, "retry", 2)
        assert first_output is not None and first_output.stdout == "first attempt failed\n"
        assert second_output is not None and second_output.stdout == "second attempt succeeded\n"


def test_sdk_worker_injects_local_dbutils_filesystem_and_widget_context(tmp_path: Path) -> None:
    source = (
        b"import json, sys\n"
        b"dbutils.widgets.text('mode', 'default')\n"
        b"dbutils.widgets.text('optional', 'fallback')\n"
        b"mode = dbutils.widgets.get('mode')\n"
        b"if mode == 'task': dbutils.widgets.text('transient', 'first-run-only')\n"
        b"print('WIDGETS=' + json.dumps(dbutils.widgets.getAll(), sort_keys=True))\n"
        b"print('ARGUMENTS=' + json.dumps(sys.argv[1:]))\n"
        b"dbutils.fs.mkdirs('dbfs:/captures')\n"
        b"path = 'dbfs:/captures/' + mode + '.txt'\n"
        b"dbutils.fs.put(path, mode, overwrite=True)\n"
        b"print('HEAD=' + dbutils.fs.head(path))\n"
        b"print('FILES=' + json.dumps([item.name for item in dbutils.fs.ls('dbfs:/captures')]))\n"
    )

    with local_jobs_sdk_client(tmp_path / "workspace") as (client, workspace_path, _context):
        client.workspace.upload(
            "/Workspace/Shared/dbutils.py",
            io.BytesIO(source),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
        job = client.jobs.create(
            name="worker-dbutils-files-widgets",
            tasks=[
                Task(
                    task_key="dbutils",
                    spark_python_task=SparkPythonTask(
                        python_file="/Workspace/Shared/dbutils.py",
                        parameters=["mode=task", "plain-argument"],
                    ),
                    new_cluster=ClusterSpec(
                        spark_version="spark35-local-v1",
                        data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
                        num_workers=0,
                    ),
                )
            ],
        )
        first = client.jobs.run_now(job.job_id).result(timeout=timedelta(seconds=10))
        second = client.jobs.run_now(
            job.job_id,
            python_params=["mode=run-now", "replacement-argument"],
        ).result(timeout=timedelta(seconds=10))
        first_output = client.jobs.get_run_output(first.run_id)
        second_output = client.jobs.get_run_output(second.run_id)

        assert first_output.logs is not None
        assert (
            'WIDGETS={"mode": "task", "optional": "fallback", "transient": "first-run-only"}'
        ) in first_output.logs
        assert 'ARGUMENTS=["mode=task", "plain-argument"]' in first_output.logs
        assert "HEAD=task" in first_output.logs
        assert 'FILES=["task.txt"]' in first_output.logs
        assert second_output.logs is not None
        assert 'WIDGETS={"mode": "run-now", "optional": "fallback"}' in second_output.logs
        assert 'ARGUMENTS=["mode=run-now", "replacement-argument"]' in second_output.logs
        assert "HEAD=run-now" in second_output.logs
        assert 'FILES=["run-now.txt", "task.txt"]' in second_output.logs
        assert (workspace_path / "files" / "dbfs" / "captures" / "task.txt").read_text(
            encoding="utf-8"
        ) == "task"
        assert (workspace_path / "files" / "dbfs" / "captures" / "run-now.txt").read_text(
            encoding="utf-8"
        ) == "run-now"


def test_sdk_timeout_retry_requires_explicit_retry_on_timeout(tmp_path: Path) -> None:
    with local_jobs_sdk_client(tmp_path / "workspace") as (client, _workspace_path, context):
        source = b"import time\ntime.sleep(2)\nprint('completed')\n"
        client.workspace.upload(
            "/Workspace/Shared/timeout.py",
            io.BytesIO(source),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
        job = client.jobs.create(
            name="timeout-does-not-retry-by-default",
            tasks=[
                Task(
                    task_key="timeout",
                    max_retries=1,
                    timeout_seconds=1,
                    spark_python_task=SparkPythonTask(python_file="/Workspace/Shared/timeout.py"),
                    new_cluster=ClusterSpec(
                        spark_version="spark35-local-v1",
                        data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
                        num_workers=0,
                    ),
                )
            ],
        )
        run = client.jobs.run_now(job.job_id).result(timeout=timedelta(seconds=10))
        assert run.state is not None and run.state.result_state is RunResultState.FAILED
        attempts = context.jobs.list_attempts(run.run_id, "timeout")
        assert len(attempts) == 1
        assert attempts[0].state is JobAttemptState.TIMED_OUT

        retry_source = (
            b"from pathlib import Path\nimport time\n"
            b"marker = Path('timeout-retry.marker')\n"
            b"if not marker.exists():\n"
            b"    marker.write_text('first')\n"
            b"    time.sleep(2)\n"
            b"print('timeout retry succeeded')\n"
        )
        client.workspace.upload(
            "/Workspace/Shared/retry-timeout.py",
            io.BytesIO(retry_source),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
        retry_job = client.jobs.create(
            name="timeout-retry-enabled",
            tasks=[
                Task(
                    task_key="retry-timeout",
                    max_retries=1,
                    retry_on_timeout=True,
                    timeout_seconds=1,
                    spark_python_task=SparkPythonTask(
                        python_file="/Workspace/Shared/retry-timeout.py"
                    ),
                    new_cluster=ClusterSpec(
                        spark_version="spark35-local-v1",
                        data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
                        num_workers=0,
                    ),
                )
            ],
        )
        retry_run = client.jobs.run_now(retry_job.job_id).result(timeout=timedelta(seconds=10))
        assert retry_run.state is not None
        assert retry_run.state.result_state is RunResultState.SUCCESS
        retry_attempts = context.jobs.list_attempts(retry_run.run_id, "retry-timeout")
        assert [attempt.state for attempt in retry_attempts] == [
            JobAttemptState.TIMED_OUT,
            JobAttemptState.SUCCEEDED,
        ]


def test_sdk_stops_after_the_configured_number_of_retries(tmp_path: Path) -> None:
    with local_jobs_sdk_client(tmp_path / "workspace") as (client, _workspace_path, context):
        client.workspace.upload(
            "/Workspace/Shared/always-fails.py",
            io.BytesIO(b"import sys\nprint('still failing')\nsys.exit(5)\n"),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
        job = client.jobs.create(
            name="one-retry-then-stop",
            tasks=[
                Task(
                    task_key="always-fails",
                    max_retries=1,
                    spark_python_task=SparkPythonTask(
                        python_file="/Workspace/Shared/always-fails.py"
                    ),
                    new_cluster=ClusterSpec(
                        spark_version="spark35-local-v1",
                        data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
                        num_workers=0,
                    ),
                )
            ],
        )
        run = client.jobs.run_now(job.job_id).result(timeout=timedelta(seconds=10))
        assert run.state is not None and run.state.result_state is RunResultState.FAILED
        attempts = context.jobs.list_attempts(run.run_id, "always-fails")
        assert len(attempts) == 2
        assert all(attempt.state is JobAttemptState.FAILED for attempt in attempts)
        assert attempts[0].attempt_id != attempts[1].attempt_id


def test_sdk_binds_workers_to_all_seven_compute_variants(tmp_path: Path) -> None:
    source = (
        b"import json, os\n"
        b"print('MODULE_STATE=' + str(globals().get('_brickhouse_state', 'fresh')))\n"
        b"globals()['_brickhouse_state'] = 'used'\n"
        b"print('WORK_CONTEXT=' + os.environ['BRICKHOUSE_RUN_CONTEXT_JSON'])\n"
        b"print('WORKER_PID=' + str(os.getpid()))\n"
    )
    variants = (
        ("SL", "serverless", "serverless", 1),
        ("CSN", "new", "standard", 1),
        ("CSE", "existing", "standard", 1),
        ("CSH", "shared", "standard", 2),
        ("CDN", "new", "dedicated", 1),
        ("CDE", "existing", "dedicated", 1),
        ("CDH", "shared", "dedicated", 2),
    )

    with local_jobs_sdk_client(tmp_path / "workspace") as (client, _workspace, context):
        client.workspace.upload(
            "/Workspace/Shared/compute-profile.py",
            io.BytesIO(source),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )

        for variant, selection, access, task_count in variants:
            security_mode = (
                DataSecurityMode.DATA_SECURITY_MODE_STANDARD
                if access == "standard"
                else DataSecurityMode.DATA_SECURITY_MODE_DEDICATED
            )
            single_user_name = "brickhouse-test-owner" if access == "dedicated" else None
            cluster_spec = ClusterSpec(
                spark_version="spark35-local-v1",
                data_security_mode=security_mode,
                single_user_name=single_user_name,
                num_workers=0,
            )
            tasks: list[Task] = []
            job_clusters: list[JobCluster] = []
            environments: list[JobEnvironment] = []
            existing_id: str | None = None
            if selection == "serverless":
                environments.append(
                    JobEnvironment(
                        environment_key="local-python",
                        spec=Environment(environment_version="1", dependencies=[]),
                    )
                )
            elif selection == "existing":
                existing = client.clusters.create(
                    "spark35-local-v1",
                    cluster_name=f"{variant}-persistent",
                    data_security_mode=security_mode,
                    single_user_name=single_user_name,
                    num_workers=0,
                ).result()
                existing_id = existing.cluster_id
                client.clusters.delete(existing_id).result()
            elif selection == "shared":
                job_clusters.append(JobCluster(job_cluster_key="shared", new_cluster=cluster_spec))

            for index in range(task_count):
                task_key = f"task_{index + 1}"
                task = Task(
                    task_key=task_key,
                    depends_on=[TaskDependency(task_key="task_1")] if index == 1 else None,
                    spark_python_task=SparkPythonTask(
                        python_file="/Workspace/Shared/compute-profile.py",
                        parameters=["--variant", variant, "--task", task_key],
                    ),
                )
                if selection == "serverless":
                    task.environment_key = "local-python"
                    task.disable_auto_optimization = True
                elif selection == "existing":
                    task.existing_cluster_id = existing_id
                elif selection == "shared":
                    task.job_cluster_key = "shared"
                else:
                    task.new_cluster = cluster_spec
                tasks.append(task)

            job = client.jobs.create(
                name=f"compute-profile-{variant}",
                tasks=tasks,
                job_clusters=job_clusters,
                environments=environments,
            )
            run = client.jobs.run_now(job.job_id).result(timeout=timedelta(seconds=20))
            assert run.state is not None
            assert run.state.life_cycle_state is RunLifeCycleState.TERMINATED
            assert run.state.result_state is RunResultState.SUCCESS

            worker_contexts: list[dict[str, object]] = []
            worker_pids: list[int] = []
            for index in range(task_count):
                task_key = f"task_{index + 1}"
                task_run = context.jobs.get_task_run(run.run_id, task_key)
                assert task_run.state.value == "succeeded"
                output = context.jobs.get_attempt_output(run.run_id, task_key, 1)
                assert output is not None
                assert "MODULE_STATE=fresh" in output.stdout
                context_line = next(
                    line.removeprefix("WORK_CONTEXT=")
                    for line in output.stdout.splitlines()
                    if line.startswith("WORK_CONTEXT=")
                )
                worker_contexts.append(json.loads(context_line))
                pid_line = next(
                    line.removeprefix("WORKER_PID=")
                    for line in output.stdout.splitlines()
                    if line.startswith("WORKER_PID=")
                )
                worker_pids.append(int(pid_line))

            expected_profile = (
                "serverless-local-v1" if selection == "serverless" else f"classic-{access}-local-v1"
            )
            assert {item["profile"] for item in worker_contexts} == {expected_profile}
            assert len({item["attempt_id"] for item in worker_contexts}) == task_count
            assert len(set(worker_pids)) == task_count

            consumers = [
                context.compute.get_run_consumer(run.run_id, f"task_{index + 1}")
                for index in range(task_count)
            ]
            if selection == "serverless":
                assert all(item is None for item in consumers)
                assert all(item["compute_id"] is None for item in worker_contexts)
                continue

            assert all(item is not None and item.state == "released" for item in consumers)
            compute_ids = {item.compute_id for item in consumers if item is not None}
            assert len(compute_ids) == 1
            compute_id = next(iter(compute_ids))
            assert {item["compute_id"] for item in worker_contexts} == {compute_id}
            resource = context.compute.get_resource(compute_id)
            assert resource.is_simulated is True
            assert resource.profile == expected_profile
            if selection == "existing":
                assert compute_id == existing_id
                assert resource.state.value == "RUNNING"
                assert all(
                    item["compute_generation"] == resource.generation for item in worker_contexts
                )
            else:
                assert resource.resource_kind.value == (
                    "job_shared" if selection == "shared" else "job_new"
                )
                assert resource.state.value == "TERMINATED"
            if selection == "shared":
                events = context.compute.get_lifecycle_events(compute_id)
                assert sum(event.event_type == "consumer_started" for event in events) == 2
                assert sum(event.event_type == "consumer_released" for event in events) == 2


@pytest.mark.skipif(os.name != "posix", reason="process-group cleanup check is POSIX-scoped")
@pytest.mark.parametrize("action", ("restart", "terminate"))
def test_compute_interruption_reaps_workers_and_preserves_committed_delta(
    tmp_path: Path, action: str
) -> None:
    script = (
        b"import os, subprocess, sys, time\n"
        b"child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
        b"open('compute-child.pid', 'w').write(str(child.pid))\n"
        b"print('worker-ready', flush=True)\n"
        b"time.sleep(60)\n"
    )
    with local_jobs_sdk_client(tmp_path / "workspace") as (
        client,
        workspace_path,
        context,
    ):
        cluster = client.clusters.create(
            "spark35-local-v1",
            cluster_name="interruptible-worker",
            data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
            num_workers=0,
        ).result()
        initial_generation = context.compute.get_cluster(cluster.cluster_id).generation
        context.tables.create_managed("main", "default", "committed_before_compute_reset")
        table_service = DeltaRsTableService(context)
        handle = table_service.resolve(
            parse_identifier("committed_before_compute_reset"),
            context.new_namespace_state(),
        )
        committed = table_service.write_arrow(
            handle,
            pa.table({"id": pa.array([42], type=pa.int64())}),
            WriteMode.CREATE,
            operation_id="commit-before-compute-reset",
        )
        assert committed.version == 0

        client.workspace.upload(
            "/Workspace/Shared/interruptible.py",
            io.BytesIO(script),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
        job = client.jobs.create(
            name="restart-compute-during-worker",
            tasks=[
                Task(
                    task_key="interrupt",
                    spark_python_task=SparkPythonTask(
                        python_file="/Workspace/Shared/interruptible.py"
                    ),
                    existing_cluster_id=cluster.cluster_id,
                )
            ],
        )
        submission = client.jobs.run_now(job.job_id)
        child_pid_file = workspace_path / "files" / "compute-child.pid"
        deadline = time.monotonic() + 10
        worker_pid: int | None = None
        while time.monotonic() < deadline:
            if child_pid_file.exists():
                attempts = context.jobs.list_attempts(submission.run_id, "interrupt")
                if attempts:
                    outcome = context.worker_outcomes.get(
                        str(submission.run_id), attempts[0].attempt_id
                    )
                    if outcome is not None:
                        worker_pid = outcome.worker_pid
                if worker_pid is not None:
                    break
            time.sleep(0.01)
        assert child_pid_file.exists(), "worker did not launch its child process"
        child_pid = int(child_pid_file.read_text())
        assert worker_pid is not None

        if action == "restart":
            changed = client.clusters.restart(cluster.cluster_id).result()
            expected_compute_state = "RUNNING"
        else:
            changed = client.clusters.delete(cluster.cluster_id).result()
            expected_compute_state = "TERMINATED"
        assert changed.state is not None
        assert changed.state.value == expected_compute_state
        run = submission.result(timeout=timedelta(seconds=10))
        assert run.state is not None
        assert run.state.life_cycle_state is RunLifeCycleState.TERMINATED
        assert run.state.result_state is RunResultState.CANCELED

        attempts = context.jobs.list_attempts(run.run_id, "interrupt")
        assert len(attempts) == 1
        assert attempts[0].state is JobAttemptState.CANCELLED
        assert attempts[0].termination_reason == "cancelled"
        assert attempts[0].exit_code is not None
        outcome = context.worker_outcomes.get(str(run.run_id), attempts[0].attempt_id)
        assert outcome is not None and outcome.state == "cancelled"
        assert not worker_process_exists(worker_pid), "worker process was not reaped"
        assert not worker_process_exists(child_pid), "worker descendant was not reaped"

        current = context.compute.get_cluster(cluster.cluster_id)
        assert current.state.value == expected_compute_state
        assert current.generation == initial_generation + 1
        assert table_service.read_table(handle).data.to_pylist() == [{"id": 42}]

    with WorkspaceContext.local(workspace_path) as reopened:
        table_service = DeltaRsTableService(reopened)
        handle = table_service.resolve(
            parse_identifier("committed_before_compute_reset"),
            reopened.new_namespace_state(),
        )
        snapshot = table_service.read_table(handle)
        assert snapshot.snapshot.version == 0
        assert snapshot.data.to_pylist() == [{"id": 42}]


def test_sdk_worker_resolves_only_granted_secrets_and_redacts_retained_output(
    tmp_path: Path,
) -> None:
    secret = "synthetic-secret-for-dbutils"
    unrelated_secret = "unrelated-scope-secret"
    source = (
        b"import os\n"
        b"print('SCOPES=' + ','.join(scope.name for scope in dbutils.secrets.listScopes()))\n"
        b"print('KEYS=' + ','.join(item.key for item in dbutils.secrets.list('approved')))\n"
        b"print('SECRET=' + dbutils.secrets.get('approved', 'api-key'))\n"
        b"print('SECRET_BYTES=' + repr(dbutils.secrets.getBytes('approved', 'api-key')))\n"
        b"try:\n"
        b"    dbutils.secrets.get('unrelated', 'hidden')\n"
        b"except PermissionError:\n"
        b"    print('DENIED=1')\n"
        b"try:\n"
        b"    dbutils.secrets.get('approved', 'missing')\n"
        b"except KeyError:\n"
        b"    print('MISSING=1')\n"
        b"has_secret = any('synthetic-secret' in v for v in os.environ.values())\n"
        b"print('ENV_HAS_SECRET=' + str(has_secret))\n"
        b"print('CONTEXT=' + os.environ['BRICKHOUSE_RUN_CONTEXT_JSON'])\n"
    )

    with local_jobs_sdk_client(tmp_path / "workspace") as (
        client,
        workspace_path,
        context,
    ):
        client.workspace.upload(
            "/Workspace/Shared/secrets.py",
            io.BytesIO(source),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
        job = client.jobs.create(
            name="worker-scoped-secrets",
            tasks=[
                Task(
                    task_key="secrets",
                    spark_python_task=SparkPythonTask(python_file="/Workspace/Shared/secrets.py"),
                    new_cluster=ClusterSpec(
                        spark_version="spark35-local-v1",
                        data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
                        num_workers=0,
                    ),
                )
            ],
        )
        provider = context.secret_provider
        provider.put("approved", "api-key", secret)
        provider.put("unrelated", "hidden", unrelated_secret)
        provider.grant_job(job.job_id, "approved")

        run = client.jobs.run_now(job.job_id).result(timeout=timedelta(seconds=10))
        output = client.jobs.get_run_output(run.run_id)
        assert run.state is not None and run.state.result_state is RunResultState.SUCCESS
        assert output.logs is not None
        assert "SCOPES=approved" in output.logs
        assert "KEYS=api-key" in output.logs
        assert f"SECRET={'*' * len(secret)}" in output.logs
        assert "SECRET_BYTES=b'" + "*" * len(secret) + "'" in output.logs
        assert "DENIED=1" in output.logs
        assert "MISSING=1" in output.logs
        assert "ENV_HAS_SECRET=False" in output.logs
        assert secret not in output.logs + (output.error or "")
        assert unrelated_secret not in output.logs + (output.error or "")
        assert secret not in context.jobs.get_job(job.job_id).tasks[0].settings.__repr__()
        assert secret.encode("utf-8") not in (workspace_path / "metadata.sqlite").read_bytes()

    assert provider.snapshot_for_job(job.job_id).scope_names == ()
