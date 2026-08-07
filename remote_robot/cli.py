"""Local v2 registry and program tooling.

This CLI never connects hardware. It supports discovery against the local
registry, deterministic compilation/inspection, and explicit v1 migration.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .migrate import migrate_v1_so101_template
from .program import CompiledProgram, ProgramCompiler, ProgramError
from .registry import CapabilityRegistry, RegistryError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, default=Path("registry.example"))
    commands = parser.add_subparsers(dest="command", required=True)

    list_command = commands.add_parser("list", help="list authorized exposed roots")
    list_command.add_argument("--identity", default="administrator")

    describe = commands.add_parser("describe", help="print a recursive capability manifest")
    describe.add_argument("node_id")
    describe.add_argument("--identity")

    compile_command = commands.add_parser("compile", help="compile a JSON/TOML template to .rrp")
    compile_command.add_argument("template", type=Path)
    compile_command.add_argument("output", type=Path)

    inspect = commands.add_parser("inspect", help="inspect a compiled .rrp artifact")
    inspect.add_argument("program", type=Path)

    validate = commands.add_parser("validate", help="validate hashes and current manifest binding")
    validate.add_argument("program", type=Path)

    migrate = commands.add_parser("migrate-v1", help="convert a v1 SO-101 file to a v2 template")
    migrate.add_argument("source", type=Path)
    migrate.add_argument("output", type=Path)
    migrate.add_argument("--root", required=True)
    migrate.add_argument("--arm-group", default="arm")
    migrate.add_argument("--gripper-group", default="gripper")
    return parser


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "inspect":
            _print(CompiledProgram.read(args.program).inspect())
            return 0
        if args.command == "migrate-v1":
            template = migrate_v1_so101_template(
                args.source,
                root_id=args.root,
                arm_role_path=args.arm_group,
                gripper_role_path=args.gripper_group,
            )
            args.output.write_text(
                json.dumps(template, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            _print({"written": str(args.output), "root_id": args.root})
            return 0

        registry = CapabilityRegistry.load(args.registry)
        if args.command == "list":
            _print(registry.list_robots(args.identity))
            return 0
        if args.command == "describe":
            _print(registry.describe(args.node_id, args.identity))
            return 0
        compiler = ProgramCompiler(registry)
        if args.command == "compile":
            program = compiler.compile_file(args.template, args.output)
            _print(program.inspect() | {"written": str(args.output)})
            return 0
        if args.command == "validate":
            program = CompiledProgram.read(args.program)
            compiler.validate_for_current_registry(program)
            _print(program.inspect() | {"valid": True, "current_manifest": True})
            return 0
    except (OSError, RegistryError, ProgramError, ValueError) as exc:
        code = getattr(exc, "code", type(exc).__name__)
        print(f"{code}: {exc}", file=sys.stderr)
        return 2
    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
