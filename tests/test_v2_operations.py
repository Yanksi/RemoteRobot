from __future__ import annotations

import asyncio
import json
from pathlib import Path
import time
import unittest

from remote_robot.journal import InMemoryRunJournal
from remote_robot.lease import LeaseManager
from remote_robot.operations import OperationsProjection
from remote_robot.registry import CapabilityRegistry
from remote_robot.worker import InMemoryWorker


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class OperationsProjectionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.registry = CapabilityRegistry.load(
            PROJECT_ROOT / "examples" / "server" / "registry"
        )
        self.leases = LeaseManager()
        self.journal = InMemoryRunJournal()
        self.worker = InMemoryWorker(adapter_name="demo_robot")
        await self.worker.prepare_disabled()
        self.projection = OperationsProjection(
            self.registry,
            self.leases,
            self.journal,
            {"demo_robot": self.worker},
        )

    async def asyncTearDown(self) -> None:
        await self.projection.close()
        await self.worker.close()
        self.journal.close()

    async def test_snapshot_reduces_registry_runtime_leases_and_journal(self) -> None:
        initial = await self.projection.snapshot()
        self.assertEqual(initial["server"]["server_id"], "example-server")
        self.assertTrue(initial["server"]["dashboard_read_only"])
        self.assertEqual(initial["summary"]["active_run_count"], 0)
        by_node = {node["node_id"]: node for node in initial["nodes"]}
        self.assertEqual(by_node["demo_robot"]["display_state"], "idle")
        self.assertEqual(by_node["demo_cell"]["display_state"], "idle")
        self.assertNotIn("private_seed", json.dumps(initial))

        first_seq = initial["operations_event_seq"]
        unchanged = await self.projection.snapshot()
        self.assertEqual(unchanged["operations_event_seq"], first_seq)

        self.leases.acquire(
            "run-dashboard",
            leaves={"demo_robot.arm", "demo_robot.gripper"},
            resources={"simulator:demo_robot", "camera:example_overhead"},
            safety_domains={"example_table", "example_cell"},
        )
        self.journal.append(
            "run-dashboard",
            {
                "type": "run.opened",
                "identity_id": "example_operator",
                "root_id": "demo_cell",
                "program_sha256": "a" * 64,
            },
        )
        self.journal.append(
            "run-dashboard", {"type": "phase.started", "phase_id": "move"}
        )
        await self.worker.prepare_phase(
            {
                "run_id": "run-dashboard",
                "phase_id": "move",
                "phase_execution_id": "run-dashboard:0:demo",
                "duration_us": 1_000_000,
            }
        )
        await self.worker.start_phase(time.monotonic_ns() // 1_000)
        await asyncio.sleep(0)

        running = await self.projection.snapshot()
        self.assertGreater(running["operations_event_seq"], first_seq)
        self.assertEqual(running["summary"]["active_run_count"], 1)
        self.assertEqual(running["summary"]["occupied_leaf_count"], 2)
        by_node = {node["node_id"]: node for node in running["nodes"]}
        self.assertEqual(by_node["demo_robot"]["display_state"], "running")
        self.assertEqual(by_node["demo_cell"]["display_state"], "running")
        self.assertEqual(by_node["demo_cell"]["occupied_by"], ["run-dashboard"])
        self.assertEqual(running["runs"][0]["state"], "running")
        self.assertEqual(running["runs"][0]["identity_id"], "example_operator")
        self.assertEqual(running["recent_events"][0]["type"], "phase.started")

    async def test_subscription_emits_only_meaningful_changes(self) -> None:
        initial = await self.projection.snapshot()
        stream = self.projection.subscribe(initial["operations_event_seq"])
        waiting = asyncio.create_task(anext(stream))

        self.journal.append(
            "faulted-run",
            {
                "type": "run.opened",
                "identity_id": "example_operator",
                "root_id": "demo_cell",
                "program_sha256": "b" * 64,
            },
        )
        self.journal.append(
            "faulted-run",
            {
                "type": "run.faulted",
                "phase_id": "move",
                "error_type": "TrackingError",
            },
        )
        await self.projection.refresh()
        event = await asyncio.wait_for(waiting, timeout=1.0)

        self.assertEqual(event.event_type, "operations.snapshot")
        self.assertGreater(event.event_seq, initial["operations_event_seq"])
        self.assertEqual(event.snapshot["summary"]["faulted_run_count"], 1)
        await stream.aclose()


if __name__ == "__main__":
    unittest.main()
