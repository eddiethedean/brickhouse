from __future__ import annotations

import importlib.metadata
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Thread

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InvalidParameterValue, ResourceDoesNotExist
from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType, State

from brickhouse.compute import (
    MAX_ACTIVE_STATEMENTS_PER_WAREHOUSE,
    MAX_ACTIVE_STATEMENTS_PER_WORKSPACE,
    MAX_QUEUED_STATEMENTS_PER_WAREHOUSE,
    ComputeService,
    UnsupportedWarehouseEffectError,
    WarehouseAdmissionLimitError,
    WarehouseConflictError,
    WarehouseNotFoundError,
    WarehouseState,
)
from brickhouse.diagnostic_server import create_diagnostic_server
from brickhouse.workspace import WorkspaceContext, initialize_workspace


@contextmanager
def local_sql_sdk_client(
    path: Path,
) -> Iterator[tuple[WorkspaceClient, Path, WorkspaceContext]]:
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


def _create_warehouse(client: WorkspaceClient, name: str, warehouse_type: str, **kwargs: object):
    return client.warehouses.create(
        name=name,
        warehouse_type=CreateWarehouseRequestWarehouseType(warehouse_type),
        cluster_size="2X-Small",
        **kwargs,
    ).result()


def test_pinned_sdk_warehouse_types_lifecycle_pagination_and_persistence(tmp_path: Path) -> None:
    assert importlib.metadata.version("databricks-sdk") == "0.153.0"

    with local_sql_sdk_client(tmp_path / "workspace") as (client, workspace_path, context):
        serverless = _create_warehouse(
            client,
            "serverless",
            "PRO",
            enable_serverless_compute=True,
            auto_stop_mins=10,
            min_num_clusters=1,
            max_num_clusters=2,
        )
        pro = _create_warehouse(client, "pro", "PRO", enable_serverless_compute=False)
        classic = _create_warehouse(client, "classic", "CLASSIC")

        assert serverless.state is State.RUNNING
        assert serverless.warehouse_type.value == "PRO"
        assert serverless.enable_serverless_compute is True
        assert pro.enable_serverless_compute is False
        assert classic.warehouse_type.value == "CLASSIC"
        assert classic.enable_serverless_compute is False
        assert [item.name for item in client.warehouses.list(page_size=1)] == [
            "serverless",
            "pro",
            "classic",
        ]
        assert client.warehouses.get(serverless.id).id == serverless.id

        edited = client.warehouses.edit(
            serverless.id,
            cluster_size="Small",
            statement_timeout=180,
        ).result()
        assert edited.state is State.RUNNING
        assert edited.cluster_size == "Small"
        assert edited.statement_timeout == 180

        for record in (serverless, pro, classic):
            stopped = client.warehouses.stop(record.id).result()
            assert stopped.state is State.STOPPED
            started = client.warehouses.start(record.id).result()
            assert started.state is State.RUNNING

        client.warehouses.delete(classic.id)
        with pytest.raises(ResourceDoesNotExist):
            client.warehouses.get(classic.id)

        local_service = context.compute.warehouses
        local_records = local_service.list_warehouses()
        assert len(local_records) == 2
        assert all(record.is_simulated for record in local_records)
        assert {record.profile for record in local_records} == {
            "sql-serverless-local-v1",
            "sql-pro-local-v1",
        }

    with WorkspaceContext.local(workspace_path) as reopened:
        assert [item.name for item in reopened.compute.warehouses.list_warehouses()] == [
            "serverless",
            "pro",
        ]


def test_warehouse_sdk_routes_reject_invalid_profiles_and_unimplemented_effects(
    tmp_path: Path,
) -> None:
    with local_sql_sdk_client(tmp_path / "workspace") as (client, _workspace_path, context):
        with pytest.raises(InvalidParameterValue, match="requires warehouse_type=PRO"):
            _create_warehouse(
                client,
                "invalid-classic",
                "CLASSIC",
                enable_serverless_compute=True,
            )
        with pytest.raises(InvalidParameterValue, match="auto_stop_mins"):
            _create_warehouse(client, "invalid-idle", "PRO", auto_stop_mins=5)
        with pytest.raises(InvalidParameterValue, match="cluster_size"):
            client.warehouses.create(
                name="invalid-size",
                warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
                cluster_size="invented-size",
            ).result()
        with pytest.raises(InvalidParameterValue, match="unsupported warehouse request field"):
            client.warehouses.create(
                name="unsupported-option",
                warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
                cluster_size="2X-Small",
                instance_profile_arn="arn:aws:iam::123456789012:instance-profile/unused",
            ).result()
        with pytest.raises(UnsupportedWarehouseEffectError, match="fixed COST_OPTIMIZED"):
            context.compute.warehouses.create(
                {
                    "name": "unsupported-spot-policy",
                    "warehouse_type": "CLASSIC",
                    "cluster_size": "2X-Small",
                    "spot_instance_policy": "BALANCED",
                }
            )
        with pytest.raises(ResourceDoesNotExist):
            client.warehouses.get("missing-warehouse")


def test_admission_queue_enforces_limits_fifo_and_stop_invalidation(
    tmp_path: Path,
) -> None:
    descriptor = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(descriptor.path) as context:
        warehouses = ComputeService(context).warehouses

        def create(name: str):
            return warehouses.create(
                {
                    "name": name,
                    "warehouse_type": "PRO",
                    "enable_serverless_compute": True,
                    "cluster_size": "2X-Small",
                    "auto_stop_mins": 0,
                }
            )

        first = create("first")
        second = create("second")
        third = create("third")
        assert MAX_ACTIVE_STATEMENTS_PER_WAREHOUSE == 1
        assert MAX_ACTIVE_STATEMENTS_PER_WORKSPACE == 2
        assert MAX_QUEUED_STATEMENTS_PER_WAREHOUSE == 8

        first_active = warehouses.admit_statement(first.warehouse_id, "first-active")
        first_queued = warehouses.admit_statement(first.warehouse_id, "first-queued-0")
        second_active = warehouses.admit_statement(second.warehouse_id, "second-active")
        third_queued = warehouses.admit_statement(third.warehouse_id, "third-queued")
        assert first_active.state == "ACTIVE"
        assert first_queued.state == "QUEUED"
        assert second_active.state == "ACTIVE"
        assert third_queued.state == "QUEUED"
        assert warehouses.count_active_statements(first.warehouse_id) == 1
        assert not warehouses.validate_admission(first_queued.statement_id)
        queued = [first_queued]
        for index in range(1, MAX_QUEUED_STATEMENTS_PER_WAREHOUSE):
            queued.append(warehouses.admit_statement(first.warehouse_id, f"first-queued-{index}"))
        with pytest.raises(WarehouseAdmissionLimitError, match="eight queued per warehouse"):
            warehouses.admit_statement(first.warehouse_id, "first-queue-overflow")

        warehouses.release_statement(first_active.statement_id)
        assert warehouses.get_admission(first_queued.statement_id).state == "ACTIVE"
        assert warehouses.count_active_statements(first.warehouse_id) == 1
        assert warehouses.validate_admission(second_active.statement_id)

        stopped = warehouses.stop(first.warehouse_id)
        assert stopped.state is WarehouseState.STOPPED
        assert not warehouses.validate_admission(first_queued.statement_id)
        assert warehouses.get_admission(first_queued.statement_id).state == "ACTIVE"
        assert warehouses.count_active_statements(first.warehouse_id) == 1
        assert all(
            warehouses.get_admission(item.statement_id).state == "INVALIDATED"
            for item in queued[1:]
        )
        assert warehouses.get_admission(third_queued.statement_id).state == "QUEUED"
        with pytest.raises(WarehouseConflictError, match="not running"):
            warehouses.admit_statement(first.warehouse_id, "stmt-stopped")

        restarted = warehouses.start(first.warehouse_id)
        fresh = warehouses.admit_statement(restarted.warehouse_id, "stmt-fresh")
        assert fresh.generation == restarted.generation
        assert fresh.state == "QUEUED"
        warehouses.release_statement(first_queued.statement_id)
        assert warehouses.get_admission(third_queued.statement_id).state == "ACTIVE"
        warehouses.release_statement(second_active.statement_id)
        assert warehouses.get_admission(fresh.statement_id).state == "ACTIVE"
        delete_queued = warehouses.admit_statement(first.warehouse_id, "stmt-delete-queued")
        assert delete_queued.state == "QUEUED"
        warehouses.delete(first.warehouse_id)
        assert not warehouses.validate_admission(fresh.statement_id)
        assert warehouses.get_admission(fresh.statement_id).state == "ACTIVE"
        assert warehouses.get_admission(delete_queued.statement_id).state == "INVALIDATED"
        with pytest.raises(WarehouseNotFoundError, match="does not exist"):
            warehouses.get(first.warehouse_id)

    with WorkspaceContext.local(descriptor.path) as reopened:
        leases = reopened.compute.warehouses
        assert leases.get_admission("stmt-delete-queued").state == "INVALIDATED"
        with sqlite3.connect(descriptor.metadata_path) as connection:
            assert connection.execute(
                "SELECT state, invalidated_at IS NOT NULL "
                "FROM sql_statement_admission_leases WHERE statement_id = 'stmt-delete-queued'"
            ).fetchone() == ("INVALIDATED", 1)
        leases.release_statement("stmt-fresh")
        leases.release_statement(third_queued.statement_id)


def test_old_generation_active_lease_blocks_restarted_warehouse_admission(
    tmp_path: Path,
) -> None:
    descriptor = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(descriptor.path) as context:
        warehouses = ComputeService(context).warehouses
        record = warehouses.create(
            {
                "name": "generation-fence",
                "warehouse_type": "PRO",
                "enable_serverless_compute": True,
                "cluster_size": "2X-Small",
                "auto_stop_mins": 0,
            }
        )
        active = warehouses.admit_statement(record.warehouse_id, "old-generation")
        stopped = warehouses.stop(record.warehouse_id)
        assert stopped.state is WarehouseState.STOPPED
        assert warehouses.get_admission(active.statement_id).state == "ACTIVE"

        restarted = warehouses.start(record.warehouse_id)
        fresh = warehouses.admit_statement(restarted.warehouse_id, "new-generation")
        assert fresh.state == "QUEUED"
        assert warehouses.count_active_statements(record.warehouse_id) == 1

        warehouses.release_statement(active.statement_id)
        assert warehouses.get_admission(fresh.statement_id).state == "ACTIVE"
        assert warehouses.count_active_statements(record.warehouse_id) == 1


def test_warehouse_auto_stop_uses_controlled_clock_and_active_lease(tmp_path: Path) -> None:
    descriptor = initialize_workspace(tmp_path / "workspace")
    now = datetime(2026, 10, 20, 12, tzinfo=UTC)

    def clock() -> datetime:
        return now

    with WorkspaceContext.local(descriptor.path) as context:
        warehouses = ComputeService(context, clock=clock).warehouses
        record = warehouses.create(
            {
                "name": "idle",
                "warehouse_type": "CLASSIC",
                "cluster_size": "Small",
                "auto_stop_mins": 10,
            }
        )
        lease = warehouses.admit_statement(record.warehouse_id, "active")
        now += timedelta(minutes=10)
        assert warehouses.expire_idle() == []
        warehouses.release_statement(lease.statement_id)
        assert warehouses.expire_idle() == []
        now += timedelta(minutes=10)
        stopped = warehouses.expire_idle()
        assert len(stopped) == 1
        assert stopped[0].state is WarehouseState.STOPPED
        assert warehouses.get_admission(lease.statement_id).state == "RELEASED"
        assert [event.event_type for event in warehouses.list_events(record.warehouse_id)] == [
            "created_and_started",
            "auto_stopped",
        ]

    with WorkspaceContext.local(descriptor.path) as reopened:
        persisted = reopened.compute.warehouses.get(record.warehouse_id)
        assert persisted.state is WarehouseState.STOPPED
