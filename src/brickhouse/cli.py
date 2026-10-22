"""Command-line entry point for local BrickHouse workspace operations."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from brickhouse._version import __version__
from brickhouse.diagnostic_server import require_ipv4_loopback_host, serve_diagnostics
from brickhouse.diagnostics import capability_report, doctor_report
from brickhouse.workspace.context import WorkspaceContext, WorkspaceContextError
from brickhouse.workspace.lifecycle import (
    WorkspaceError,
    initialize_workspace,
    open_workspace,
)
from brickhouse.workspace.ownership import WorkspaceOwnershipError


class FeatureUnavailableError(RuntimeError):
    """A requested BrickHouse feature has not been implemented yet."""


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="brickhouse",
        description="Local, JVM-free Databricks behavioral emulation.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
        help="show the installed version and exit",
    )
    commands = parser.add_subparsers(dest="command")

    initialize = commands.add_parser("init", help="initialize a local workspace")
    initialize.add_argument("--workspace", required=True, type=Path)

    serve = commands.add_parser("serve", help="serve local workspace APIs")
    serve.add_argument("--workspace", required=True, type=Path)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", default=8000, type=_port_number)

    run = commands.add_parser("run", help="reserved; submit jobs through the SDK")
    run.add_argument("script", type=Path)

    capabilities = commands.add_parser(
        "capabilities", help="show implemented and planned capabilities"
    )
    capabilities.add_argument("--format", choices=("json", "text"), default="text")

    doctor = commands.add_parser("doctor", help="show local runtime diagnostics")
    doctor.add_argument("--workspace", type=Path)
    return parser


def _port_number(value: str) -> int:
    try:
        port = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("port must be an integer from 0 to 65535") from error
    if not 0 <= port <= 65535:
        raise argparse.ArgumentTypeError("port must be an integer from 0 to 65535")
    return port


def _render_report(report: dict[str, object], output_format: str) -> str:
    if output_format == "json":
        return json.dumps(report, indent=2, sort_keys=True)
    lines = [
        f"BrickHouse {report['brickhouse_version']}",
        f"Python {report['python_version']} · SQLite {report['sqlite_version']}",
        "Capabilities:",
    ]
    capabilities = report["capabilities"]
    if isinstance(capabilities, dict):
        lines.extend(f"  {name}: {status}" for name, status in capabilities.items())
    lines.append("Providers (installed does not imply integrated):")
    providers = report["providers"]
    if isinstance(providers, dict):
        for name, details in providers.items():
            if isinstance(details, dict):
                version = details["version"] or "not installed"
                lines.append(f"  {name}: {details['status']} ({version})")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    """Run one explicit workspace or diagnostic command."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 0

    try:
        if args.command == "init":
            initialized_workspace = initialize_workspace(args.workspace)
            print(
                f"Initialized workspace {initialized_workspace.workspace_id} "
                f"at {initialized_workspace.path} "
                f"({initialized_workspace.config.default_catalog}."
                f"{initialized_workspace.config.default_schema})"
            )
            return 0

        if args.command == "serve":
            require_ipv4_loopback_host(args.host)
            with WorkspaceContext.local(args.workspace) as context:
                serve_diagnostics(context, args.host, args.port)
            return 0

        if args.command == "run":
            raise FeatureUnavailableError(
                "The run command is reserved; submit Python jobs through the local Jobs API. "
                "No script was started."
            )

        if args.command == "capabilities":
            print(_render_report(capability_report(), args.format))
            return 0

        if args.command == "doctor":
            doctor_workspace = (
                open_workspace(args.workspace) if args.workspace is not None else None
            )
            print(json.dumps(doctor_report(doctor_workspace), indent=2, sort_keys=True))
            return 0
    except (
        FeatureUnavailableError,
        ValueError,
        WorkspaceContextError,
        WorkspaceError,
        WorkspaceOwnershipError,
        OSError,
    ) as error:
        print(f"brickhouse: error: {error}", file=sys.stderr)
        return 2

    parser.error("unknown command")
    return 2
