from __future__ import annotations

import json
from pathlib import Path
import unittest

from aiohttp import ClientSession

from remote_robot.dashboard import DashboardServer
from remote_robot.journal import InMemoryRunJournal
from remote_robot.lease import LeaseManager
from remote_robot.operations import OperationsProjection
from remote_robot.registry import CapabilityRegistry
from remote_robot.server_v2 import build_parser
from remote_robot.worker import InMemoryWorker


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class DashboardServerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        registry = CapabilityRegistry.load(
            PROJECT_ROOT / "examples" / "server" / "registry"
        )
        self.journal = InMemoryRunJournal()
        self.worker = InMemoryWorker(adapter_name="demo_robot")
        await self.worker.prepare_disabled()
        self.projection = OperationsProjection(
            registry,
            LeaseManager(),
            self.journal,
            {"demo_robot": self.worker},
        )
        self.dashboard = DashboardServer(
            self.projection,
            host="127.0.0.1",
            port=0,
            refresh_interval_s=0.1,
        )
        await self.dashboard.start()

    async def asyncTearDown(self) -> None:
        await self.dashboard.close()
        await self.projection.close()
        await self.worker.close()
        self.journal.close()

    async def test_static_dashboard_snapshot_health_and_security_headers(self) -> None:
        async with ClientSession() as session:
            async with session.get(self.dashboard.url + "/") as response:
                self.assertEqual(response.status, 200)
                html = await response.text()
                self.assertIn("Operations", html)
                self.assertIn("READ", html)
                self.assertEqual(response.headers["X-Frame-Options"], "DENY")
                self.assertIn("default-src 'self'", response.headers["Content-Security-Policy"])

            async with session.get(
                self.dashboard.url + "/assets/dashboard.css"
            ) as response:
                self.assertEqual(response.status, 200)
                self.assertIn("--acid", await response.text())

            async with session.get(self.dashboard.url + "/api/snapshot") as response:
                snapshot = await response.json()
                self.assertEqual(snapshot["server"]["server_id"], "example-server")
                self.assertTrue(snapshot["server"]["dashboard_read_only"])
                self.assertEqual(snapshot["roots"][0]["node_id"], "demo_cell")

            async with session.get(self.dashboard.url + "/api/health") as response:
                health = await response.json()
                self.assertTrue(health["ok"])
                self.assertTrue(health["read_only"])

            async with session.get(
                self.dashboard.url + "/api/events?after=not-an-int"
            ) as response:
                self.assertEqual(response.status, 400)

    async def test_sse_replays_correlated_operations_snapshot(self) -> None:
        async with ClientSession() as session:
            response = await session.get(self.dashboard.url + "/api/events?after=0")
            try:
                lines: list[str] = []
                while True:
                    line = (await response.content.readline()).decode("utf-8").rstrip()
                    if not line:
                        break
                    lines.append(line)
                event_id = next(line.removeprefix("id: ") for line in lines if line.startswith("id: "))
                event_type = next(
                    line.removeprefix("event: ")
                    for line in lines
                    if line.startswith("event: ")
                )
                payload = json.loads(
                    next(
                        line.removeprefix("data: ")
                        for line in lines
                        if line.startswith("data: ")
                    )
                )
                self.assertEqual(event_type, "operations")
                self.assertEqual(int(event_id), payload["event_seq"])
                self.assertEqual(
                    payload["snapshot"]["server"]["server_id"], "example-server"
                )
            finally:
                response.close()

        # Wake the handler after the client closes so cleanup doesn't wait for
        # the keepalive interval.
        self.journal.append("wake-run", {"type": "run.opened"})
        await self.projection.refresh()

    def test_non_loopback_binding_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "loopback"):
            DashboardServer(self.projection, host="0.0.0.0", port=8080)


class DashboardCliTests(unittest.TestCase):
    def test_v2_server_enables_loopback_dashboard_by_default(self) -> None:
        args = build_parser().parse_args(["--registry", "registry.example"])
        self.assertEqual(args.dashboard_host, "127.0.0.1")
        self.assertEqual(args.dashboard_port, 8080)
        self.assertEqual(args.dashboard_refresh_ms, 1000)


if __name__ == "__main__":
    unittest.main()
