from __future__ import annotations

import importlib.metadata
import sqlite3
import sys
from pathlib import Path

import pytest
from databricks.sdk.service.compute import ClusterSpec, DataSecurityMode, Environment
from databricks.sdk.service.jobs import (
    JobCluster,
    JobEnvironment,
    NotebookTask,
    PythonWheelTask,
    SparkPythonTask,
    Task,
)

from brickhouse.compute import ComputeResourceKind
from brickhouse.compute.models import ComputeState
from brickhouse.jobs import (
    ComputeMode,
    ExecutionContextNotFoundError,
    ExecutionContextResolver,
    InvalidExecutionContextError,
    UnsupportedExecutionContextError,
)
from brickhouse.workspace import WorkspaceContext, initialize_workspace


def _python_task(task_key: str, **options: object) -> Task:
    return Task(
        task_key=task_key,
        spark_python_task=SparkPythonTask(
            python_file="/Workspace/Shared/orders.py",
            parameters=["--customer", "alice"],
        ),
        **options,
    )


def _local_cluster(
    *,
    name: str = "job",
    mode: DataSecurityMode = DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
    spark_conf: dict[str, str] | None = None,
) -> ClusterSpec:
    return ClusterSpec(
        cluster_name=name,
        spark_version="spark35-local-v1",
        data_security_mode=mode,
        num_workers=1,
        spark_conf=spark_conf,
    )


def test_serverless_environment_and_defaults_resolve_from_pinned_sdk_models(
    tmp_path: Path,
) -> None:
    assert importlib.metadata.version("databricks-sdk") == "0.153.0"
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        resolver = ExecutionContextResolver(context)
        task = Task(
            task_key="serverless",
            spark_python_task=SparkPythonTask(
                python_file="/Workspace/Shared/orders.py",
                parameters=["--customer", "alice"],
            ),
            environment_key="python311",
            disable_auto_optimization=True,
            max_retries=1,
            min_retry_interval_millis=250,
            timeout_seconds=120,
        )
        environment = JobEnvironment(
            environment_key="python311",
            spec=Environment(
                environment_version="1",
                dependencies=["pyarrow==23.0.1", "deltalake==1.2.1"],
            ),
        )
        resolved = resolver.resolve_job(
            [task.as_dict()],
            run_id=302,
            environments=[environment.as_dict()],
        )[0]

        assert resolved.compute_mode is ComputeMode.SERVERLESS
        assert resolved.run_id == 302
        assert resolved.profile == "serverless-local-v1"
        assert resolved.runtime_profile is None
        assert resolved.environment_profile == (
            f"serverless-python{sys.version_info.major}{sys.version_info.minor}-local-v1"
        )
        assert resolved.environment_key == "python311"
        assert resolved.environment_version == "1"
        assert resolved.dependencies == ("deltalake==1.2.1", "pyarrow==23.0.1")
        assert resolved.performance_target == "STANDARD"
        assert resolved.effective_settings == {
            "spark.sql.ansi.enabled": "true",
            "spark.sql.session.timeZone": "UTC",
        }
        assert resolved.retry_settings == {
            "max_retries": 1,
            "min_retry_interval_millis": 250,
            "retry_on_timeout": None,
        }
        assert resolved.timeout_seconds == 120
        assert resolved.task_arguments == {
            "parameters": ["--customer", "alice"],
            "python_file": "/Workspace/Shared/orders.py",
        }
        settings = resolved.effective_settings
        settings["spark.sql.ansi.enabled"] = "false"
        assert resolved.effective_settings["spark.sql.ansi.enabled"] == "true"
        assert "alice" not in repr(resolved)
        assert "orders.py" not in repr(resolved)

        with sqlite3.connect(context.workspace.metadata_path) as connection:
            assert connection.execute("SELECT count(*) FROM compute_resources").fetchone() == (0,)


def test_classic_inline_shared_and_existing_compute_selectors_resolve_before_dispatch(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        resolver = ExecutionContextResolver(context)
        inline_task = _python_task(
            "inline",
            new_cluster=_local_cluster(spark_conf={"spark.sql.shuffle.partitions": "8"}),
        )
        shared_tasks = [
            _python_task("first", job_cluster_key="shared"),
            _python_task("second", job_cluster_key="shared"),
        ]
        shared_definition = JobCluster(
            job_cluster_key="shared",
            new_cluster=_local_cluster(name="shared"),
        )
        inline, first, second = resolver.resolve_job(
            [inline_task.as_dict(), *(item.as_dict() for item in shared_tasks)],
            job_clusters=[shared_definition.as_dict()],
        )

        assert inline.compute_mode is ComputeMode.CLASSIC
        assert inline.compute_resource_kind is ComputeResourceKind.JOB_NEW
        assert inline.compute_key == "inline_inline"
        assert inline.profile == "classic-standard-local-v1"
        assert inline.runtime_profile == "spark35-local-v1"
        assert inline.effective_settings == {"spark.sql.shuffle.partitions": "8"}
        assert first.compute_resource_kind is ComputeResourceKind.JOB_SHARED
        assert second.compute_resource_kind is ComputeResourceKind.JOB_SHARED
        assert first.compute_key == second.compute_key == "shared"

        cluster = context.compute.create_cluster(
            {
                "cluster_name": "existing",
                "spark_version": "spark35-local-v1",
                "data_security_mode": "DATA_SECURITY_MODE_DEDICATED",
                "single_user_name": "local-user",
                "num_workers": 1,
            }
        )
        task = _python_task("existing", existing_cluster_id=cluster.compute_id)
        existing = resolver.resolve_job([task.as_dict()])[0]
        assert existing.compute_id == cluster.compute_id
        assert existing.compute_generation == cluster.generation
        assert existing.profile == "classic-dedicated-local-v1"
        assert existing.autostart is False

        context.compute.terminate_cluster(cluster.compute_id)
        terminated = resolver.resolve_job([task.as_dict()])[0]
        assert terminated.compute_id == cluster.compute_id
        assert terminated.autostart is True
        assert context.compute.get_cluster(cluster.compute_id).state is ComputeState.TERMINATED


def test_context_resolver_rejects_conflicts_missing_resources_and_unmapped_effects(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        resolver = ExecutionContextResolver(context)
        serverless_task = Task(
            task_key="serverless",
            spark_python_task=SparkPythonTask(python_file="/Workspace/Shared/orders.py"),
            environment_key="missing",
            disable_auto_optimization=True,
        )
        with pytest.raises(ExecutionContextNotFoundError, match="environment_key"):
            resolver.resolve_job([serverless_task.as_dict()])

        with pytest.raises(UnsupportedExecutionContextError, match="disable_auto_optimization"):
            resolver.resolve_job(
                [
                    Task(
                        task_key="no-retry",
                        spark_python_task=SparkPythonTask(
                            python_file="/Workspace/Shared/orders.py"
                        ),
                        environment_key="python311",
                        disable_auto_optimization=False,
                    ).as_dict()
                ],
                environments=[
                    JobEnvironment(
                        environment_key="python311",
                        spec=Environment(environment_version="1"),
                    ).as_dict()
                ],
            )

        conflicting = _python_task(
            "conflict",
            existing_cluster_id="missing",
            new_cluster=_local_cluster(),
        )
        with pytest.raises(InvalidExecutionContextError, match="only one compute resource"):
            resolver.resolve_job([conflicting.as_dict()])

        with pytest.raises(ExecutionContextNotFoundError, match="existing_cluster_id"):
            resolver.resolve_job([_python_task("missing", existing_cluster_id="absent").as_dict()])

        with pytest.raises(UnsupportedExecutionContextError, match="serverless compute objects"):
            resolver.resolve_job(
                [_python_task("classic", job_cluster_key="serverless").as_dict()],
                job_clusters=[
                    {
                        "job_cluster_key": "serverless",
                        "serverless_compute_id": "compute-object",
                    }
                ],
            )

        with pytest.raises(UnsupportedExecutionContextError, match="environment_version"):
            resolver.resolve_job(
                [serverless_task.as_dict()],
                environments=[
                    JobEnvironment(
                        environment_key="missing",
                        spec=Environment(environment_version="2"),
                    ).as_dict()
                ],
            )

        with pytest.raises(UnsupportedExecutionContextError, match="dependency"):
            resolver.resolve_job(
                [serverless_task.as_dict()],
                environments=[
                    JobEnvironment(
                        environment_key="missing",
                        spec=Environment(environment_version="1", dependencies=["requests"]),
                    ).as_dict()
                ],
            )

        with pytest.raises(InvalidExecutionContextError, match="data_security_mode"):
            resolver.resolve_job(
                [
                    _python_task(
                        "unmapped-access",
                        new_cluster=ClusterSpec(
                            spark_version="spark35-local-v1",
                            num_workers=1,
                            data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_AUTO,
                        ),
                    ).as_dict()
                ]
            )

        with pytest.raises(UnsupportedExecutionContextError, match="task type"):
            resolver.resolve_job(
                [
                    Task(
                        task_key="wheel",
                        python_wheel_task=PythonWheelTask(
                            package_name="example", entry_point="main"
                        ),
                        existing_cluster_id="missing",
                    ).as_dict()
                ]
            )


def test_notebook_context_requires_python_compute_and_mapped_environment(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        resolver = ExecutionContextResolver(context)
        environment = JobEnvironment(
            environment_key="python311",
            spec=Environment(environment_version="1"),
        )
        notebook = Task(
            task_key="notebook",
            notebook_task=NotebookTask(
                notebook_path="/Workspace/Shared/orders",
                base_parameters={"customer": "alice"},
            ),
            environment_key="python311",
            disable_auto_optimization=True,
        )
        resolved = resolver.resolve_job([notebook.as_dict()], environments=[environment.as_dict()])[
            0
        ]
        assert resolved.task_type == "notebook"
        assert resolved.task_arguments["notebook_path"] == "/Workspace/Shared/orders"

        warehouse_notebook = Task(
            task_key="sql-notebook",
            notebook_task=NotebookTask(
                notebook_path="/Workspace/Shared/sql",
                warehouse_id="warehouse-id",
            ),
            environment_key="python311",
            disable_auto_optimization=True,
        )
        with pytest.raises(UnsupportedExecutionContextError, match="SQL warehouse notebooks"):
            resolver.resolve_job(
                [warehouse_notebook.as_dict()], environments=[environment.as_dict()]
            )


@pytest.mark.parametrize(
    "path",
    [
        "relative/orders.py",
        "/Users/me/orders.py",
        "/Workspace/Shared/../secret.py",
        "/Workspace//Shared/orders.py",
        "/Workspace/Shared\\orders.py",
        "file:/Workspace/Shared/orders.py",
    ],
)
def test_task_paths_must_be_normalized_workspace_local_paths(tmp_path: Path, path: str) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        task = Task(
            task_key="invalid-path",
            spark_python_task=SparkPythonTask(python_file=path),
            new_cluster=_local_cluster(),
        )
        with pytest.raises(InvalidExecutionContextError, match="python_file"):
            ExecutionContextResolver(context).resolve_job([task.as_dict()])


def test_unmapped_environment_client_and_timeout_values_fail_before_dispatch(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        resolver = ExecutionContextResolver(context)
        task = Task(
            task_key="serverless",
            spark_python_task=SparkPythonTask(python_file="/Workspace/Shared/orders.py"),
            environment_key="python311",
            disable_auto_optimization=True,
        )
        for timeout in (-1, True):
            with pytest.raises(InvalidExecutionContextError, match="timeout_seconds"):
                resolver.resolve_job(
                    [{**task.as_dict(), "timeout_seconds": timeout}],
                    environments=[
                        JobEnvironment(
                            environment_key="python311",
                            spec=Environment(environment_version="1"),
                        ).as_dict()
                    ],
                )
        for timeout in (86401, 2**31):
            with pytest.raises(UnsupportedExecutionContextError, match="worker limit"):
                resolver.resolve_job(
                    [{**task.as_dict(), "timeout_seconds": timeout}],
                    environments=[
                        JobEnvironment(
                            environment_key="python311",
                            spec=Environment(environment_version="1"),
                        ).as_dict()
                    ],
                )
        with pytest.raises(UnsupportedExecutionContextError, match="local Python environment"):
            resolver.resolve_job(
                [task.as_dict()],
                environments=[
                    JobEnvironment(
                        environment_key="python311",
                        spec=Environment(environment_version="1", client="1"),
                    ).as_dict()
                ],
            )
        no_timeout = resolver.resolve_job(
            [{**task.as_dict(), "timeout_seconds": 0}],
            environments=[
                JobEnvironment(
                    environment_key="python311",
                    spec=Environment(environment_version="1"),
                ).as_dict()
            ],
        )[0]
        assert no_timeout.timeout_seconds is None
