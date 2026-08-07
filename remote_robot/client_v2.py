"""Query a Remote Robot v2 server's authorized registry."""

from __future__ import annotations

import argparse
import asyncio
import json
import os

from .network import RegistryClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="ws://127.0.0.1:8766")
    parser.add_argument("--identity", required=True)
    parser.add_argument("--token-env", required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list")
    describe = commands.add_parser("describe")
    describe.add_argument("node_id")
    commands.add_parser("server-info")
    return parser


async def run(args: argparse.Namespace) -> int:
    token = os.environ.get(args.token_env, "")
    if len(token) < 32:
        raise SystemExit(f"{args.token_env} must contain at least 32 characters")
    client = await RegistryClient.connect(args.url, args.identity, token)
    try:
        if args.command == "list":
            value = await client.list_robots()
        elif args.command == "describe":
            value = await client.describe_robot(args.node_id)
        else:
            value = await client.server_info()
        print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    finally:
        await client.close()


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(run(build_parser().parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
