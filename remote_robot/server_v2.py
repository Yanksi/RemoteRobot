"""Run the Remote Robot v2 management/discovery endpoint."""

from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path

from websockets.asyncio.server import serve

from .dashboard import DashboardServer
from .journal import InMemoryRunJournal
from .lease import LeaseManager
from .network import MAX_MANAGEMENT_MESSAGE, PROTOCOL_V2, ManagementEndpoint
from .operations import OperationsProjection
from .registry import CapabilityRegistry


LOG = logging.getLogger("remote_robot.server_v2")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8766)
    parser.add_argument(
        "--dashboard-host",
        default="127.0.0.1",
        help="loopback IP literal for the read-only dashboard",
    )
    parser.add_argument(
        "--dashboard-port",
        type=int,
        default=8080,
        help="read-only dashboard port; use 0 to disable",
    )
    parser.add_argument(
        "--dashboard-refresh-ms",
        type=int,
        default=1000,
        help="operations projection polling interval (minimum 100 ms)",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser


async def run(args: argparse.Namespace) -> None:
    registry = CapabilityRegistry.load(args.registry)
    endpoint = ManagementEndpoint(registry)
    journal = InMemoryRunJournal()
    projection = OperationsProjection(
        registry,
        LeaseManager(),
        journal,
        execution_available=False,
    )
    dashboard: DashboardServer | None = None
    try:
        if args.dashboard_port != 0:
            dashboard = DashboardServer(
                projection,
                host=args.dashboard_host,
                port=args.dashboard_port,
                refresh_interval_s=args.dashboard_refresh_ms / 1000,
            )
            await dashboard.start()
            LOG.info("read-only operations dashboard listening on %s", dashboard.url)

        async with serve(
            endpoint.handle,
            args.host,
            args.port,
            subprotocols=[PROTOCOL_V2],
            origins=[None],
            max_size=MAX_MANAGEMENT_MESSAGE,
            ping_interval=20,
            ping_timeout=20,
        ) as server:
            LOG.info(
                "v2 management endpoint for %s listening on ws://%s:%d; execution is not enabled yet",
                registry.server_id,
                args.host,
                args.port,
            )
            await server.serve_forever()
    finally:
        if dashboard is not None:
            await dashboard.close()
        await projection.close()
        journal.close()


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
