from __future__ import annotations

from argparse import Namespace
from contextlib import redirect_stdout
from io import StringIO
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from websockets.asyncio.server import serve

from examples.client.discover_and_compile import run
from remote_robot.network import PROTOCOL_V2, ManagementEndpoint
from remote_robot.program import CompiledProgram
from remote_robot.registry import CapabilityRegistry


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_ROOT = PROJECT_ROOT / "examples"


class LoopbackExampleTests(unittest.IsolatedAsyncioTestCase):
    async def test_discover_and_compile_example_runs_over_loopback(self) -> None:
        registry = CapabilityRegistry.load(EXAMPLE_ROOT / "server" / "registry")
        endpoint = ManagementEndpoint(registry)
        token = "example-loopback-token-000000000000000"

        async with serve(
            endpoint.handle,
            "127.0.0.1",
            0,
            subprotocols=[PROTOCOL_V2],
            origins=[None],
        ) as server:
            port = server.sockets[0].getsockname()[1]
            with tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "program.rrp"
                args = Namespace(
                    url=f"ws://127.0.0.1:{port}",
                    identity="example_operator",
                    token_env="REMOTE_ROBOT_EXAMPLE_TOKEN",
                    root="demo_cell",
                    program=EXAMPLE_ROOT / "client" / "program.json",
                    output=output,
                )
                with patch.dict(
                    os.environ, {"REMOTE_ROBOT_EXAMPLE_TOKEN": token}
                ), redirect_stdout(StringIO()):
                    self.assertEqual(await run(args), 0)

                compiled = CompiledProgram.read(output)
                self.assertEqual(compiled.root_id, "demo_cell")
                self.assertEqual(
                    [
                        len(sequence["axes"])
                        for sequence in compiled.phases[0]["sequences"]
                    ],
                    [3, 1],
                )
                self.assertEqual(
                    compiled.manifest_hash, registry.manifest_hash("demo_cell")
                )


if __name__ == "__main__":
    unittest.main()
