from __future__ import annotations

import importlib.metadata
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from threading import Thread

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    AlreadyExists,
    InvalidParameterValue,
    ResourceConflict,
    ResourceDoesNotExist,
)

from brickhouse.catalog import NamespaceNotFoundError
from brickhouse.diagnostic_server import create_diagnostic_server
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace


@contextmanager
def local_sdk_client(
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


def test_pinned_sdk_catalog_and_schema_lifecycle_persists_and_paginates(
    tmp_path: Path,
) -> None:
    assert importlib.metadata.version("databricks-sdk") == "0.153.0"

    with local_sdk_client(tmp_path / "workspace") as (client, workspace_path, context):
        alpha = client.catalogs.create("alpha", comment="analytics catalog")
        beta = client.catalogs.create("beta")
        created_schema = client.schemas.create("landing", "alpha", comment="incoming data")

        assert alpha.name == "alpha"
        assert alpha.comment == "analytics catalog"
        assert beta.name == "beta"
        assert created_schema.catalog_name == "alpha"
        assert created_schema.name == "landing"
        assert created_schema.comment == "incoming data"
        assert client.catalogs.get("alpha").comment == "analytics catalog"
        assert client.schemas.get("alpha.landing").schema_id == created_schema.schema_id

        catalog_names = [item.name for item in client.catalogs.list(max_results=1)]
        assert catalog_names == ["alpha", "beta", "main"]
        schema_names = [item.name for item in client.schemas.list("alpha", max_results=1)]
        assert schema_names == ["default", "information_schema", "landing"]

        with pytest.raises(AlreadyExists):
            client.catalogs.create("alpha")
        with pytest.raises(AlreadyExists):
            client.schemas.create("landing", "alpha")
        with pytest.raises(ResourceDoesNotExist):
            client.catalogs.get("missing")
        with pytest.raises(ResourceDoesNotExist):
            client.schemas.get("alpha.missing")
        with pytest.raises(InvalidParameterValue):
            client.catalogs.create("not a valid identifier")
        with pytest.raises(InvalidParameterValue):
            list(client.catalogs.list(max_results=-1))

        client.schemas.create("occupied", "alpha")
        context.tables.create_managed("alpha", "occupied", "events")
        with pytest.raises(ResourceConflict):
            client.schemas.delete("alpha.occupied", force=True)
        with pytest.raises(ResourceConflict):
            client.catalogs.delete("alpha", force=True)

        client.schemas.delete("alpha.landing")
        client.catalogs.delete("beta")
        with pytest.raises(ResourceDoesNotExist):
            client.schemas.get("alpha.landing")
        with pytest.raises(ResourceDoesNotExist):
            client.catalogs.get("beta")
        with pytest.raises(InvalidParameterValue):
            client.catalogs.delete("main")
        with pytest.raises(InvalidParameterValue):
            client.schemas.delete("main.default")

    with WorkspaceContext.local(workspace_path) as reopened:
        assert [item.name for item in reopened.catalogs.list()] == ["alpha", "main"]
        assert reopened.schemas.get("alpha", "default").name == "default"
        assert reopened.schemas.get("alpha", "information_schema").name == "information_schema"
        assert reopened.tables.get("alpha", "occupied", "events").table_format == "delta"
        with pytest.raises(NamespaceNotFoundError):
            reopened.schemas.get("alpha", "landing")
