from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

import cbor2

from remote_robot.program import (
    CompiledProgram,
    ProgramCompiler,
    ProgramError,
    digest_value,
    load_template,
)
from remote_robot.registry import CapabilityRegistry


def revolute_axis(index: int) -> dict[str, Any]:
    return {
        "name": f"joint_{index}",
        "kind": "revolute",
        "unit": "rad",
        "lower": -3.0,
        "upper": 3.0,
        "max_velocity": 10.0,
        "max_acceleration": 50.0,
    }


class FakeRegistry:
    server_id = "test-server"

    def __init__(self) -> None:
        self.current_hash = "a" * 64
        self.exposed_as_run_root = True
        self.groups = {
            "left_arm.arm": {
                "physical_node_id": "arm-017",
                "group_id": "arm",
                "group": {
                    "command_schemas": ["joint_position_trajectory/v1"],
                    "axes": [revolute_axis(index) for index in range(7)],
                },
            },
            "alias_arm.arm": {
                "physical_node_id": "arm-017",
                "group_id": "arm",
                "group": {
                    "command_schemas": ["joint_position_trajectory/v1"],
                    "axes": [revolute_axis(index) for index in range(7)],
                },
            },
            "left_arm.gripper": {
                "physical_node_id": "arm-017",
                "group_id": "gripper",
                "group": {
                    "command_schemas": ["normalized_position_trajectory/v1"],
                    "axes": [
                        {
                            "name": "opening",
                            "kind": "normalized",
                            "unit": "1",
                            "lower": 0.0,
                            "upper": 1.0,
                            "max_velocity": 10.0,
                            "max_acceleration": 50.0,
                        }
                    ],
                },
            },
        }

    def describe(self, node_id: str) -> dict[str, Any]:
        if node_id != "workcell":
            raise KeyError(node_id)
        return {
            "node_id": node_id,
            "manifest_hash": self.current_hash,
            "exposed_as_run_root": self.exposed_as_run_root,
        }

    def manifest_hash(self, node_id: str) -> str:
        self.describe(node_id)
        return self.current_hash

    def resolve_group(self, root_id: str, role_path: str) -> dict[str, Any]:
        self.describe(root_id)
        try:
            return self.groups[role_path]
        except KeyError as exc:
            raise ProgramError("GROUP_NOT_FOUND", role_path) from exc


def valid_template() -> dict[str, Any]:
    return {
        "format": "robot-program-template",
        "version": 2,
        "program_id": "seven-axis-demo",
        "root_id": "workcell",
        "phases": [
            {
                "phase_id": "approach",
                "sequences": [
                    {
                        "group": "left_arm.arm",
                        "command_schema": "joint_position_trajectory/v1",
                        "reference": "phase_start_measured",
                        "samples": [
                            {"t_us": 0, "values": [0] * 7},
                            {"t_us": 1_000_000, "values": [0.1] * 7},
                        ],
                    },
                    {
                        "group": "left_arm.gripper",
                        "command_schema": "normalized_position_trajectory/v1",
                        "reference": "absolute",
                        "samples": [
                            {"t_us": 0, "values": [0.8]},
                            {"t_us": 1_000_000, "values": [0.2]},
                        ],
                    },
                ],
            }
        ],
    }


class ProgramCompilerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = FakeRegistry()
        self.compiler = ProgramCompiler(self.registry)

    def test_compiles_arbitrary_dof_and_multiple_dense_groups(self) -> None:
        program = self.compiler.compile(valid_template())
        self.assertEqual(program.server_id, "test-server")
        self.assertEqual(program.root_id, "workcell")
        self.assertEqual(len(program.phases[0]["sequences"][0]["axes"]), 7)
        self.assertEqual(len(program.phases[0]["sequences"][1]["axes"]), 1)
        self.assertEqual(program.phases[0]["duration_us"], 1_000_000)
        decoded = CompiledProgram.from_bytes(program.to_bytes())
        self.assertEqual(decoded.sha256, program.sha256)
        self.assertEqual(decoded.inspect()["phase_count"], 1)

    def test_canonical_artifact_is_deterministic(self) -> None:
        first = self.compiler.compile(valid_template())
        second = self.compiler.compile(copy.deepcopy(valid_template()))
        self.assertEqual(first.to_bytes(), second.to_bytes())
        self.assertEqual(first.sha256, second.sha256)

    def test_artifact_rejects_trailing_or_noncanonical_cbor(self) -> None:
        program = self.compiler.compile(valid_template())
        with self.assertRaises(ProgramError) as trailing:
            CompiledProgram.from_bytes(program.to_bytes() + b"\x00")
        self.assertEqual(trailing.exception.code, "NONCANONICAL_ARTIFACT")

        envelope = cbor2.loads(program.to_bytes())
        reversed_envelope = {"sha256": envelope["sha256"], "body": envelope["body"]}
        noncanonical = cbor2.dumps(reversed_envelope, canonical=False)
        self.assertNotEqual(noncanonical, program.to_bytes())
        with self.assertRaises(ProgramError) as reordered:
            CompiledProgram.from_bytes(noncanonical)
        self.assertEqual(reordered.exception.code, "NONCANONICAL_ARTIFACT")

    def test_dense_sample_dimension_is_enforced(self) -> None:
        template = valid_template()
        template["phases"][0]["sequences"][0]["samples"][1]["values"] = [0.1] * 6
        with self.assertRaises(ProgramError) as raised:
            self.compiler.compile(template)
        self.assertEqual(raised.exception.code, "DENSE_AXIS_MISMATCH")

    def test_relative_sequence_must_start_at_zero(self) -> None:
        template = valid_template()
        template["phases"][0]["sequences"][0]["samples"][0]["values"][0] = 0.1
        with self.assertRaises(ProgramError) as raised:
            self.compiler.compile(template)
        self.assertEqual(raised.exception.code, "START_STATE_MISMATCH")

    def test_duplicate_alias_to_same_leaf_group_is_rejected(self) -> None:
        template = valid_template()
        duplicate = copy.deepcopy(template["phases"][0]["sequences"][0])
        duplicate["group"] = "alias_arm.arm"
        template["phases"][0]["sequences"].append(duplicate)
        with self.assertRaises(ProgramError) as raised:
            self.compiler.compile(template)
        self.assertEqual(raised.exception.code, "DUPLICATE_GROUP_SEQUENCE")

    def test_absolute_axis_limit_is_enforced(self) -> None:
        template = valid_template()
        sequence = template["phases"][0]["sequences"][0]
        sequence["reference"] = "absolute"
        sequence["samples"][0]["values"] = [0.0] * 7
        sequence["samples"][1]["values"][3] = 3.1
        with self.assertRaises(ProgramError) as raised:
            self.compiler.compile(template)
        self.assertEqual(raised.exception.code, "LIMIT_VIOLATION")

    def test_normalized_group_rejects_values_outside_zero_one(self) -> None:
        template = valid_template()
        template["phases"][0]["sequences"][1]["samples"][1]["values"] = [-0.01]
        with self.assertRaises(ProgramError) as raised:
            self.compiler.compile(template)
        self.assertEqual(raised.exception.code, "LIMIT_VIOLATION")

    def test_relative_normalized_group_accepts_negative_close_offset(self) -> None:
        template = valid_template()
        sequence = template["phases"][0]["sequences"][1]
        sequence["reference"] = "phase_start_measured"
        sequence["samples"][0]["values"] = [0.0]
        sequence["samples"][1]["values"] = [-0.2]
        program = self.compiler.compile(template)
        values = program.phases[0]["sequences"][1]["samples"][1]["values"]
        self.assertEqual(values, [-0.2])

    def test_manifest_change_invalidates_compiled_program(self) -> None:
        program = self.compiler.compile(valid_template())
        self.registry.current_hash = "b" * 64
        with self.assertRaises(ProgramError) as raised:
            self.compiler.validate_for_current_registry(program)
        self.assertEqual(raised.exception.code, "MANIFEST_MISMATCH")

    def test_hidden_node_cannot_be_compiled_as_a_run_root(self) -> None:
        self.registry.exposed_as_run_root = False
        with self.assertRaises(ProgramError) as raised:
            self.compiler.compile(valid_template())
        self.assertEqual(raised.exception.code, "ROOT_NOT_EXPOSED")

    def test_dense_axis_values_reject_booleans(self) -> None:
        template = valid_template()
        template["phases"][0]["sequences"][0]["samples"][1]["values"][0] = True
        with self.assertRaises(ProgramError) as raised:
            self.compiler.compile(template)
        self.assertEqual(raised.exception.code, "SCHEMA_INVALID")

    def test_nonempty_mode_transitions_are_capability_gated_at_compile_time(self) -> None:
        template = valid_template()
        template["phases"][0]["transitions"] = [
            {
                "group": "left_arm.arm",
                "from_schema": "joint_position_trajectory/v1",
                "to_schema": "joint_force_trajectory/v1",
            }
        ]
        with self.assertRaises(ProgramError) as raised:
            self.compiler.compile(template)
        self.assertEqual(raised.exception.code, "CAPABILITY_MISSING")

    def test_tampered_phase_is_rejected_even_with_rehashed_envelope(self) -> None:
        program = self.compiler.compile(valid_template())
        envelope = cbor2.loads(program.to_bytes())
        envelope["body"]["phases"][0]["sequences"][0]["samples"][1]["values"][0] = 0.2
        envelope["sha256"] = digest_value(envelope["body"])
        with self.assertRaises(ProgramError) as raised:
            CompiledProgram.from_bytes(cbor2.dumps(envelope, canonical=True))
        self.assertEqual(raised.exception.code, "HASH_MISMATCH")

    def test_recomputed_digests_do_not_bypass_registry_execution_validation(self) -> None:
        program = self.compiler.compile(valid_template())
        envelope = cbor2.loads(program.to_bytes())
        body = envelope["body"]
        phase = body["phases"][0]
        sequence = phase["sequences"][0]
        sequence["samples"][1]["values"][0] = 10.0
        sequence["sha256"] = digest_value(
            {key: value for key, value in sequence.items() if key != "sha256"}
        )
        phase["sha256"] = digest_value(
            {key: value for key, value in phase.items() if key != "sha256"}
        )
        outline_phase = body["outline"]["phases"][0]
        outline_phase["sequences"][0]["sha256"] = sequence["sha256"]
        outline_phase["payload_sha256"] = phase["sha256"]
        body["outline_sha256"] = digest_value(body["outline"])
        envelope["sha256"] = digest_value(body)

        attacker_rehashed = CompiledProgram.from_bytes(
            cbor2.dumps(envelope, canonical=True)
        )
        with self.assertRaises(ProgramError) as raised:
            self.compiler.validate_for_current_registry(attacker_rehashed)
        self.assertEqual(raised.exception.code, "VELOCITY_LIMIT")

    def test_json_and_toml_templates_are_loaded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            json_path = root / "program.json"
            json_path.write_text(json.dumps(valid_template()), encoding="utf-8")
            self.assertEqual(load_template(json_path)["version"], 2)

            toml_path = root / "program.toml"
            toml_path.write_text(
                'format = "robot-program-template"\nversion = 2\nroot_id = "workcell"\nphases = []\n',
                encoding="utf-8",
            )
            self.assertEqual(load_template(toml_path)["root_id"], "workcell")

    def test_real_composite_registry_compiles_arbitrary_group_dimensions(self) -> None:
        project_root = Path(__file__).resolve().parents[1]
        registry = CapabilityRegistry.load(project_root / "registry.example")
        template = load_template(project_root / "programs" / "v2_workcell_demo.json")
        program = ProgramCompiler(registry).compile(template)
        dimensions = [
            len(sequence["axes"])
            for sequence in program.phases[0]["sequences"]
        ]
        self.assertEqual(dimensions, [5, 1, 7])
        self.assertEqual(program.root_id, "demo_workcell")
        self.assertEqual(program.manifest_hash, registry.manifest_hash("demo_workcell"))


if __name__ == "__main__":
    unittest.main()
