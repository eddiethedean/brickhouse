"""Benchmark-only loopback launcher for a pinned external Spark statement agent.

The external implementation is not copied or shipped as a BrickHouse provider.
See the explicit adaptation decision in docs/benchmarks/local-emulators.md.
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agent-file", type=Path, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--spark-remote", required=True)
    args = parser.parse_args()
    remote = urlsplit(args.spark_remote)
    if remote.scheme != "sc" or remote.hostname != "127.0.0.1" or remote.port is None:
        parser.error("The benchmark engine must be an explicit IPv4 loopback Spark Connect URL")
    os.environ["SPARK_REMOTE"] = args.spark_remote
    sys.path.insert(0, str(args.agent_file.resolve().parent))
    spec = importlib.util.spec_from_file_location("benchmark_upstream_agent", args.agent_file)
    if spec is None or spec.loader is None:
        raise RuntimeError("Cannot load the pinned upstream agent")
    agent = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = agent
    spec.loader.exec_module(agent)
    vars(agent)["AGENT_URL"] = f"http://127.0.0.1:{args.port}"
    with ThreadingHTTPServer(("127.0.0.1", args.port), agent.Handler) as server:
        server.serve_forever()


if __name__ == "__main__":
    main()
