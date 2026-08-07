from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest.mock import patch

from websockets.asyncio.server import serve

from remote_robot.network import (
    PROTOCOL_V2,
    ManagementEndpoint,
    NetworkError,
    RegistryClient,
)
from remote_robot.registry import CapabilityRegistry


class V2DiscoveryStreamTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        root = Path(__file__).resolve().parents[1]
        self.registry = CapabilityRegistry.load(root / "registry.example")
        self.endpoint = ManagementEndpoint(self.registry)
        self.token = "workcell-test-token-0000000000000000"
        self.env = patch.dict(
            os.environ,
            {"REMOTE_ROBOT_WORKCELL_TOKEN": self.token},
        )
        self.env.start()
        self.server_context = serve(
            self.endpoint.handle,
            "127.0.0.1",
            0,
            subprotocols=[PROTOCOL_V2],
            origins=[None],
        )
        self.server = await self.server_context.__aenter__()
        port = self.server.sockets[0].getsockname()[1]
        self.url = f"ws://127.0.0.1:{port}"

    async def asyncTearDown(self) -> None:
        await self.server_context.__aexit__(None, None, None)
        self.env.stop()

    async def test_client_lists_authorized_roots_and_describes_composite(self) -> None:
        client = await RegistryClient.connect(self.url, "workcell_operator", self.token)
        try:
            robots = await client.list_robots()
            self.assertEqual([robot["node_id"] for robot in robots], ["demo_workcell"])
            manifest = await client.describe_robot("demo_workcell")
            self.assertEqual(manifest["kind"], "composite")
            self.assertEqual(
                [child["role"] for child in manifest["children"]],
                ["primary_arm", "assistant_arm"],
            )
            info = await client.server_info()
            self.assertEqual(info["protocol"], PROTOCOL_V2)
            self.assertFalse(info["execution_available"])
        finally:
            await client.close()

    async def test_identity_cannot_describe_hidden_root(self) -> None:
        client = await RegistryClient.connect(self.url, "workcell_operator", self.token)
        try:
            with self.assertRaises(NetworkError) as raised:
                await client.describe_robot("so101_canada")
            self.assertEqual(raised.exception.code, "NOT_FOUND")
        finally:
            await client.close()

    async def test_wrong_token_fails_without_revealing_identity(self) -> None:
        with self.assertRaises(NetworkError) as raised:
            await RegistryClient.connect(
                self.url,
                "workcell_operator",
                "wrong-token-000000000000000000000",
            )
        self.assertEqual(raised.exception.code, "AUTH_FAILED")
        self.assertNotIn("REMOTE_ROBOT_WORKCELL_TOKEN", raised.exception.message)


if __name__ == "__main__":
    unittest.main()
