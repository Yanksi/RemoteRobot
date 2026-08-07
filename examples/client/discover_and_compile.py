"""Discover one v2 root and compile a program against its remote manifest."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any, Mapping

from remote_robot.network import RegistryClient
from remote_robot.program import ProgramCompiler, ProgramError, load_template


HERE = Path(__file__).resolve().parent


class RemoteManifestView:
    """Read-only compiler view backed by one recursive server manifest."""

    def __init__(self, manifest: Mapping[str, Any]) -> None:
        self._manifest = dict(manifest)
        self.server_id = self._required_string(self._manifest, "server_id")
        self.root_id = self._required_string(self._manifest, "node_id")
        self._manifest_hash = self._required_string(
            self._manifest, "manifest_hash"
        )

    @staticmethod
    def _required_string(value: Mapping[str, Any], key: str) -> str:
        result = value.get(key)
        if not isinstance(result, str) or not result:
            raise ProgramError("INVALID_MANIFEST", f"manifest.{key} is invalid")
        return result

    def describe(self, node_id: str) -> Mapping[str, Any]:
        if node_id != self.root_id:
            raise ProgramError("GROUP_NOT_FOUND", f"unknown root {node_id!r}")
        return dict(self._manifest)

    def manifest_hash(self, node_id: str) -> str:
        self.describe(node_id)
        return self._manifest_hash

    def resolve_group(self, root_id: str, role_path: str) -> Mapping[str, Any]:
        self.describe(root_id)
        segments = role_path.split(".")
        if not segments or any(not segment for segment in segments):
            raise ProgramError("GROUP_NOT_FOUND", f"invalid role path {role_path!r}")

        current: Mapping[str, Any] = self._manifest
        for role in segments[:-1]:
            if current.get("kind") != "composite":
                raise ProgramError(
                    "GROUP_NOT_FOUND", f"{role_path!r} crosses a physical node"
                )
            children = current.get("children")
            if not isinstance(children, list):
                raise ProgramError("INVALID_MANIFEST", "composite children are invalid")
            matches = [
                child
                for child in children
                if isinstance(child, Mapping) and child.get("role") == role
            ]
            if len(matches) != 1 or not isinstance(matches[0].get("manifest"), Mapping):
                raise ProgramError(
                    "GROUP_NOT_FOUND", f"manifest has no unique role {role!r}"
                )
            current = matches[0]["manifest"]

        if current.get("kind") != "physical":
            raise ProgramError(
                "GROUP_NOT_FOUND", f"{role_path!r} does not end at a physical group"
            )
        groups = current.get("groups")
        if not isinstance(groups, list):
            raise ProgramError("INVALID_MANIFEST", "physical groups are invalid")
        group_id = segments[-1]
        matches = [
            group
            for group in groups
            if isinstance(group, Mapping) and group.get("group_id") == group_id
        ]
        if len(matches) != 1:
            raise ProgramError(
                "GROUP_NOT_FOUND", f"physical node has no unique group {group_id!r}"
            )
        return {
            "physical_node_id": self._required_string(current, "node_id"),
            "group_id": group_id,
            "group": dict(matches[0]),
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="ws://127.0.0.1:8766")
    parser.add_argument("--identity", default="example_operator")
    parser.add_argument("--token-env", default="REMOTE_ROBOT_EXAMPLE_TOKEN")
    parser.add_argument("--root", default="demo_cell")
    parser.add_argument("--program", type=Path, default=HERE / "program.json")
    parser.add_argument("--output", type=Path, default=HERE / "program.rrp")
    return parser


async def run(args: argparse.Namespace) -> int:
    token = os.environ.get(args.token_env, "")
    if len(token) < 32:
        raise SystemExit(f"{args.token_env} must contain at least 32 characters")

    client = await RegistryClient.connect(args.url, args.identity, token)
    try:
        server_info = await client.server_info()
        robots = await client.list_robots()
        visible_ids = [str(robot.get("node_id")) for robot in robots]
        if args.root not in visible_ids:
            raise SystemExit(
                f"root {args.root!r} is not visible; authorized roots: {visible_ids}"
            )
        manifest = await client.describe_robot(args.root)
    finally:
        await client.close()

    program = ProgramCompiler(RemoteManifestView(manifest)).compile(
        load_template(args.program)
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    program.write(args.output)

    summary = {
        "server": server_info,
        "authorized_roots": visible_ids,
        "selected_root": args.root,
        "manifest_hash": manifest["manifest_hash"],
        "compiled_program": program.inspect(),
        "written": str(args.output.resolve()),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    if server_info.get("execution_available") is not True:
        print(
            "\nExecution was not requested: this server currently exposes "
            "discovery only."
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(run(build_parser().parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
