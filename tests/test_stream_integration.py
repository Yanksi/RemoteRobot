from __future__ import annotations

import asyncio
import unittest

from websockets.asyncio.server import serve

from robot_client import RobotStreamClient
from robot_protocol import ProgramHeader, SafetyPolicy, TrajectoryPoint, program_digest
from robot_server import ProgramController, SimulatedBackend, connection_handler


class WebSocketIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_bidirectional_stream_completes_on_simulator(self) -> None:
        token = "integration-test-token-000000000000"
        policy = SafetyPolicy(
            min_segment_us=10_000,
            max_velocity_deg_s=100.0,
            max_acceleration_deg_s2=1000.0,
        )
        controller = ProgramController(
            SimulatedBackend,
            policy,
            control_hz=100.0,
            telemetry_hz=20.0,
            heartbeat_timeout_s=2.0,
            hold_after_run_s=0.0,
        )
        async with serve(
            lambda ws: connection_handler(ws, controller, token),
            "127.0.0.1",
            0,
            subprotocols=["robot-stream.v1"],
        ) as server:
            port = server.sockets[0].getsockname()[1]
            client = await RobotStreamClient.open(f"ws://127.0.0.1:{port}", token)
            try:
                header = ProgramHeader("integration", coordinate_mode="relative_deg")
                points = [
                    TrajectoryPoint(0, (0.0,) * 6),
                    TrajectoryPoint(100_000, (1.0, 0, 0, 0, 0, 0)),
                ]
                await client.send(
                    {
                        "type": "open_run",
                        "run_id": "integration-run",
                        "header": header.record(),
                        "run_mode": "sealed",
                        "program_digest": program_digest(header, points),
                        "disconnect_policy": "stop_after_timeout",
                    }
                )
                opened = await client.wait_for("run_opened")
                self.assertEqual(opened["state"], "buffering")
                receipt = await client.send_chunk(0, points, True)
                self.assertTrue(receipt["final"])
                await client.send({"type": "start"})

                async with asyncio.timeout(2.0):
                    while True:
                        event = await client.events.get()
                        if event.get("type") == "run_finished":
                            self.assertEqual(event["outcome"], "completed")
                            break
            finally:
                await client.close()


if __name__ == "__main__":
    unittest.main()
