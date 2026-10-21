from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from brickhouse.compute import ComputeResourceKind, ComputeState
from brickhouse.compute.models import (
    ComputeConflictError,
    ComputeStateTransitionError,
    InvalidComputeRequestError,
    UnsupportedComputeEffectError,
)
from brickhouse.compute.service import ComputeService
from brickhouse.jobs import JobTaskDefinition
from brickhouse.workspace import WorkspaceContext, initialize_workspace


class ControlledClock:
    def __init__(self) -> None:
        self.now = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, *, minutes: int) -> None:
        self.now += timedelta(minutes=minutes)


def _cluster_settings(**overrides: object) -> dict[str, object]:
    return {
        "cluster_name": "local-classic",
        "spark_version": "spark35-local-v1",
        "num_workers": 1,
        **overrides,
    }


def test_all_purpose_identity_idle_expiry_and_autostart_are_durable(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    clock = ControlledClock()
    with WorkspaceContext.local(workspace.path) as context:
        compute = ComputeService(context, clock=clock)
        cluster = compute.create_cluster(_cluster_settings(autotermination_minutes=10))
        assert cluster.state is ComputeState.RUNNING
        assert cluster.resource_kind is ComputeResourceKind.ALL_PURPOSE
        assert cluster.is_simulated is True

        job = context.jobs.create_job(
            "first-run",
            [JobTaskDefinition("task")],
        )
        run = context.jobs.submit_run(job.job_id).run
        compute.register_all_purpose_consumer(cluster.compute_id, run.run_id, "task")
        compute.start_for_consumer(cluster.compute_id, run.run_id, "task")
        clock.advance(minutes=30)
        assert compute.expire_idle() == []

        compute.release_consumer(cluster.compute_id, run.run_id, "task")
        clock.advance(minutes=9)
        assert compute.expire_idle() == []
        clock.advance(minutes=1)
        expired = compute.expire_idle()
        assert [item.compute_id for item in expired] == [cluster.compute_id]
        assert expired[0].state is ComputeState.TERMINATED
        assert expired[0].generation == 2

        next_run = context.jobs.submit_run(job.job_id).run
        compute.register_all_purpose_consumer(cluster.compute_id, next_run.run_id, "task")
        with pytest.raises(ComputeStateTransitionError, match="explicit all-purpose autostart"):
            compute.start_for_consumer(cluster.compute_id, next_run.run_id, "task")
        restarted = compute.start_for_consumer(
            cluster.compute_id,
            next_run.run_id,
            "task",
            autostart=True,
        )
        assert restarted.state is ComputeState.RUNNING
        assert restarted.generation == 3
        assert compute.release_consumer(cluster.compute_id, next_run.run_id, "task").state is (
            ComputeState.RUNNING
        )
        events = compute.get_lifecycle_events(cluster.compute_id)
        assert any(
            event.event_type == "autoterminating" and event.invalidates_sessions for event in events
        )
        assert any(
            event.event_type == "started"
            and event.state is ComputeState.PENDING
            and event.invalidates_sessions
            for event in events
        )

    with WorkspaceContext.local(workspace.path) as reopened:
        durable = reopened.compute.get_cluster(cluster.compute_id)
        assert durable.state is ComputeState.RUNNING
        assert durable.generation == 3
        assert len(reopened.compute.get_lifecycle_events(cluster.compute_id)) >= 10


def test_shared_job_compute_uses_first_and_last_planned_consumers(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        compute = context.compute
        job = context.jobs.create_job(
            "shared-run",
            [JobTaskDefinition("first"), JobTaskDefinition("second")],
        )
        run = context.jobs.submit_run(job.job_id).run
        resource = compute.provision_job_compute(
            run.run_id,
            "shared",
            ComputeResourceKind.JOB_SHARED,
            _cluster_settings(),
            ["first", "second"],
        )
        assert resource.state is ComputeState.PENDING
        assert len(compute.get_consumers(resource.compute_id)) == 2

        started = compute.start_for_consumer(resource.compute_id, run.run_id, "first")
        assert started.state is ComputeState.RUNNING
        assert compute.release_consumer(resource.compute_id, run.run_id, "first").state is (
            ComputeState.RUNNING
        )
        compute.start_for_consumer(resource.compute_id, run.run_id, "second")
        terminated = compute.release_consumer(resource.compute_id, run.run_id, "second")
        assert terminated.state is ComputeState.TERMINATED
        assert terminated.generation == 2
        assert [item.state for item in compute.get_consumers(resource.compute_id)] == [
            "released",
            "released",
        ]
        assert compute.get_lifecycle_events(resource.compute_id)[-1].event_type == "terminated"

        with sqlite3.connect(context.workspace.metadata_path) as connection:
            with pytest.raises(sqlite3.IntegrityError, match="invalid compute resource state"):
                connection.execute(
                    "UPDATE compute_resources SET state = 'RUNNING' WHERE compute_id = ?",
                    (resource.compute_id,),
                )
            with pytest.raises(sqlite3.IntegrityError, match="lifecycle events are immutable"):
                connection.execute(
                    "DELETE FROM compute_lifecycle_events WHERE compute_id = ?",
                    (resource.compute_id,),
                )


def test_compute_requires_explicit_local_runtime_and_rejects_unmapped_effects(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        with pytest.raises(InvalidComputeRequestError, match="spark_version is not mapped"):
            context.compute.create_cluster(_cluster_settings(spark_version="15.4.x-scala2.12"))
        with pytest.raises(InvalidComputeRequestError, match="data_security_mode is not mapped"):
            context.compute.create_cluster(_cluster_settings(data_security_mode="LEGACY_TABLE_ACL"))
        with pytest.raises(InvalidComputeRequestError, match="data_security_mode is not mapped"):
            context.compute.create_cluster(
                _cluster_settings(data_security_mode="DATA_SECURITY_MODE_AUTO")
            )
        with pytest.raises(UnsupportedComputeEffectError, match="Photon"):
            context.compute.create_cluster(_cluster_settings(runtime_engine="PHOTON"))
        with pytest.raises(
            UnsupportedComputeEffectError,
            match="unsupported cluster request field",
        ):
            context.compute.create_cluster(_cluster_settings(aws_attributes={"zone_id": "x"}))
        with pytest.raises(UnsupportedComputeEffectError, match="JVM-specific"):
            context.compute.create_cluster(
                _cluster_settings(spark_conf={"spark.driver.extraJavaOptions": "-Xmx1g"})
            )
        with pytest.raises(InvalidComputeRequestError, match="credential-shaped"):
            context.compute.create_cluster(
                _cluster_settings(spark_conf={"spark.my.password": "hidden"})
            )


def test_job_new_requires_one_consumer_and_run_key_is_idempotent(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        job = context.jobs.create_job(
            "new-run",
            [JobTaskDefinition("first"), JobTaskDefinition("second")],
        )
        run = context.jobs.submit_run(job.job_id).run
        with pytest.raises(InvalidComputeRequestError, match="exactly one consumer"):
            context.compute.provision_job_compute(
                run.run_id,
                "new",
                ComputeResourceKind.JOB_NEW,
                _cluster_settings(),
                ["first", "second"],
            )

        shared = context.compute.provision_job_compute(
            run.run_id,
            "shared",
            ComputeResourceKind.JOB_SHARED,
            _cluster_settings(),
            ["first", "second"],
        )
        replay = context.compute.provision_job_compute(
            run.run_id,
            "shared",
            ComputeResourceKind.JOB_SHARED,
            _cluster_settings(),
            ["second", "first"],
        )
        assert replay.compute_id == shared.compute_id
        with pytest.raises(ComputeConflictError, match="different run resource"):
            context.compute.provision_job_compute(
                run.run_id,
                "shared",
                ComputeResourceKind.JOB_SHARED,
                _cluster_settings(cluster_name="different"),
                ["first", "second"],
            )
