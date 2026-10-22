"""Truthful local capability and dependency diagnostics."""

from __future__ import annotations

import importlib.metadata
import platform
import sqlite3
from typing import TYPE_CHECKING

from brickhouse._version import __version__

if TYPE_CHECKING:
    from brickhouse.workspace.lifecycle import Workspace


def _distribution_version(distribution: str) -> str | None:
    try:
        return importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        return None


def provider_diagnostics() -> dict[str, dict[str, str | bool | None]]:
    """Report package installation separately from code integration."""
    providers: dict[str, dict[str, str | bool | None]] = {}
    for name, distribution, integrated in (
        ("sparkless", "sparkless", False),
        ("delta-rs", "deltalake", True),
    ):
        version = _distribution_version(distribution)
        providers[name] = {
            "installed": version is not None,
            "version": version,
            "integrated": integrated,
            "status": "integrated_optional" if integrated else "not_integrated",
        }
    return providers


def capability_report(workspace: Workspace | None = None) -> dict[str, object]:
    """Describe the implemented foundation and explicitly absent services."""
    workspace_report: dict[str, object] | None = None
    if workspace is not None:
        workspace_report = {
            "format_version": workspace.config.format_version,
            "metadata_schema_version": workspace.metadata_schema_version,
            "default_catalog": workspace.config.default_catalog,
            "default_schema": workspace.config.default_schema,
        }
    return {
        "brickhouse_version": __version__,
        "python_version": platform.python_version(),
        "sqlite_version": sqlite3.sqlite_version,
        "workspace": workspace_report,
        "capabilities": {
            "workspace_initialization": "supported",
            "workspace_ownership": "supported_on_local_posix_filesystems",
            "diagnostic_http": "supported_on_loopback",
            "terraform_provider_workspace_identity": "simulated_local_identity",
            "bundle_current_user_identity": "simulated_local_identity",
            "databricks_cli_bundle": "partial_local_classic_single_task_python",
            "catalog_metadata_repository": "partial_local_persistence",
            "table_registration_metadata": "partial_local_persistence",
            "catalog_api": "partial_local_rest_profile",
            "classic_cluster_lifecycle": "simulated_all_purpose_metadata",
            "job_compute_lifecycle": "simulated_run_scoped_metadata",
            "sql_warehouse_lifecycle": "simulated_persistent_metadata",
            "workspace_python_source_import": "partial_local_workspace_files",
            "sql_api": "planned",
            "table_provider_integration": "partial_local_delta",
            "python_job_execution": "partial_local_sdk_python_script_dag",
            "sdk_job_deletion": "soft_delete_with_history_retention",
            "dbutils_files_widgets_secrets": "partial_local_worker_subset",
            "dbutils_task_values": "partial_local_attempt_scoped_python_script_jobs",
        },
        "providers": provider_diagnostics(),
    }


def doctor_report(workspace: Workspace | None = None) -> dict[str, object]:
    """Return local runtime facts without loading credentials or optional APIs."""
    report = capability_report(workspace)
    report["doctor"] = {
        "python_supported": tuple(map(int, platform.python_version_tuple()[:2])) >= (3, 11),
        "workspace_validated": workspace is not None,
        "ownership_lock_platform": "posix" if platform.system() != "Windows" else "windows",
    }
    return report
