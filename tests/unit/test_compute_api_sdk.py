from __future__ import annotations

import importlib.metadata
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from threading import Thread

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InvalidParameterValue, ResourceDoesNotExist
from databricks.sdk.service.compute import (
    DataSecurityMode,
    ListClustersFilterBy,
    RuntimeEngine,
    State,
)

from brickhouse.diagnostic_server import create_diagnostic_server
from brickhouse.workspace import WorkspaceContext, initialize_workspace


@contextmanager
def local_compute_sdk_client(
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


def test_pinned_sdk_classic_cluster_lifecycle_persists_and_paginates(
    tmp_path: Path,
) -> None:
    assert importlib.metadata.version("databricks-sdk") == "0.153.0"

    with local_compute_sdk_client(tmp_path / "workspace") as (
        client,
        workspace_path,
        _context,
    ):
        first = client.clusters.create(
            "spark35-local-v1",
            cluster_name="first",
            num_workers=1,
            autotermination_minutes=10,
        ).result()
        second = client.clusters.create(
            "spark35-local-v1",
            cluster_name="second",
            num_workers=0,
            data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_STANDARD,
            autotermination_minutes=0,
        ).result()
        assert first.state is State.RUNNING
        assert first.cluster_name == "first"
        assert "simulated" in first.state_message.casefold()
        assert client.clusters.get(first.cluster_id).cluster_id == first.cluster_id
        assert [cluster.cluster_id for cluster in client.clusters.list(page_size=1)] == [
            first.cluster_id,
            second.cluster_id,
        ]
        assert [
            cluster.cluster_id
            for cluster in client.clusters.list(filter_by=ListClustersFilterBy(is_pinned=False))
        ] == [first.cluster_id, second.cluster_id]
        assert list(client.clusters.list(filter_by=ListClustersFilterBy(is_pinned=True))) == []
        assert client.clusters.get(first.cluster_id).cluster_id == first.cluster_id

        edited = client.clusters.edit(
            first.cluster_id,
            "spark35-local-v1",
            cluster_name="renamed",
            num_workers=2,
            autotermination_minutes=0,
        ).result()
        assert edited.cluster_name == "renamed"
        assert edited.num_workers == 2

        restarted = client.clusters.restart(first.cluster_id).result()
        assert restarted.state is State.RUNNING
        terminated = client.clusters.delete(first.cluster_id).result()
        assert terminated.state is State.TERMINATED
        started = client.clusters.start(first.cluster_id).result()
        assert started.state is State.RUNNING
        client.clusters.delete(first.cluster_id).result()
        client.clusters.permanent_delete(first.cluster_id)

        with pytest.raises(ResourceDoesNotExist):
            client.clusters.get(first.cluster_id)
        with pytest.raises(ResourceDoesNotExist):
            client.clusters.permanent_delete(first.cluster_id)

        active = client.clusters.create(
            "spark35-local-v1",
            cluster_name="direct-permanent-delete",
            num_workers=0,
        ).result()
        client.clusters.permanent_delete(active.cluster_id)
        with pytest.raises(ResourceDoesNotExist):
            client.clusters.get(active.cluster_id)

    with WorkspaceContext.local(workspace_path) as reopened:
        assert [record.cluster_name for record in reopened.compute.list_clusters()] == ["second"]


def test_pinned_sdk_cluster_routes_reject_unmapped_runtime_and_compute_effects(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        server = create_diagnostic_server(context, "127.0.0.1", 0)
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()
        client = WorkspaceClient(
            host=f"http://127.0.0.1:{server.server_port}",
            token="brickhouse-local-test-token",
            auth_type="pat",
        )
        try:
            with pytest.raises(InvalidParameterValue):
                client.clusters.create(
                    "15.4.x-scala2.12",
                    cluster_name="unmapped",
                    num_workers=1,
                ).result()
            with pytest.raises(InvalidParameterValue):
                client.clusters.create(
                    "spark35-local-v1",
                    cluster_name="photon",
                    num_workers=1,
                    runtime_engine=RuntimeEngine.PHOTON,
                ).result()
            with pytest.raises(InvalidParameterValue):
                client.clusters.create(
                    "spark35-local-v1",
                    cluster_name="legacy-access",
                    num_workers=1,
                    data_security_mode=DataSecurityMode.LEGACY_TABLE_ACL,
                ).result()
            with pytest.raises(InvalidParameterValue):
                client.clusters.create(
                    "spark35-local-v1",
                    cluster_name="auto-access",
                    num_workers=1,
                    data_security_mode=DataSecurityMode.DATA_SECURITY_MODE_AUTO,
                ).result()
        finally:
            server.shutdown()
            thread.join(timeout=5)
            server.server_close()
