"""Local host discovery metadata for pinned Databricks clients."""

from __future__ import annotations

import hashlib
from http import HTTPStatus
from urllib.parse import urlsplit

from brickhouse.api.catalog import ApiResponse
from brickhouse.workspace.context import WorkspaceContext

_HOST_METADATA_PATH = "/.well-known/databricks-config"
_CURRENT_USER_PATH = "/api/2.0/preview/scim/v2/Me"


class HostMetadataHttpApi:
    """Advertise the explicit local workspace profile to Databricks clients."""

    def __init__(self, context: WorkspaceContext) -> None:
        self._context = context

    def dispatch(self, method: str, target: str) -> ApiResponse | None:
        parsed = urlsplit(target)
        if parsed.path == _CURRENT_USER_PATH:
            if method != "GET":
                return ApiResponse(
                    HTTPStatus.METHOD_NOT_ALLOWED,
                    {"error_code": "METHOD_NOT_ALLOWED", "message": "only GET is supported"},
                )
            return ApiResponse(
                HTTPStatus.OK,
                {
                    "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
                    "id": "brickhouse-local-user",
                    "userName": "brickhouse-local@invalid",
                    "displayName": "BrickHouse Local User",
                    "active": True,
                },
            )
        if parsed.path != _HOST_METADATA_PATH:
            return None
        if method != "GET":
            return ApiResponse(
                HTTPStatus.METHOD_NOT_ALLOWED,
                {"error_code": "METHOD_NOT_ALLOWED", "message": "only GET is supported"},
            )
        workspace_id = self._simulated_workspace_id(self._context.workspace.workspace_id)
        return ApiResponse(
            HTTPStatus.OK,
            {"workspace_id": workspace_id, "host_type": "workspace"},
        )

    @staticmethod
    def _simulated_workspace_id(workspace_id: str) -> str:
        """Derive a stable numeric client ID without exposing it as a real Databricks ID."""
        digest = hashlib.sha256(f"brickhouse-local:{workspace_id}".encode()).digest()
        value = int.from_bytes(digest[:8], "big") & ((1 << 63) - 1)
        return str(value or 1)
