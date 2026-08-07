from __future__ import annotations

import json
from pathlib import Path
import shutil
import tempfile
import unittest

from remote_robot.registry import CapabilityRegistry, RegistryError


EXAMPLE_REGISTRY = Path(__file__).parents[1] / "registry.example"


class CapabilityRegistryTests(unittest.TestCase):
    def test_loads_arbitrary_dimensions_and_split_so101_groups(self) -> None:
        registry = CapabilityRegistry.load(EXAMPLE_REGISTRY)

        so101_arm = registry.resolve_group("so101_canada", "arm")
        gripper = registry.resolve_group("so101_canada", "gripper")
        simulated_arm = registry.resolve_group("simulator_7dof", "arm")

        self.assertEqual(so101_arm.group.dimension, 5)
        self.assertEqual(gripper.group.dimension, 1)
        self.assertEqual(gripper.group.axes[0].unit, "1")
        self.assertEqual(simulated_arm.group.dimension, 7)
        self.assertEqual(
            [axis.axis_id for axis in simulated_arm.group.axes],
            [f"joint_{number}" for number in range(1, 8)],
        )

    def test_composite_role_paths_resolve_to_canonical_leaves(self) -> None:
        registry = CapabilityRegistry.load(EXAMPLE_REGISTRY)

        primary = registry.resolve_group("demo_workcell", "primary_arm.arm")
        assistant = registry.resolve_group("demo_workcell", "assistant_arm.arm")

        self.assertEqual(primary.physical_node_id, "so101_canada")
        self.assertEqual(primary.group_id, "arm")
        self.assertEqual(primary.canonical_id, "so101_canada.arm")
        self.assertEqual(assistant.physical_node_id, "simulator_7dof")

    def test_recursive_hash_is_deterministic_and_changes_with_child_revision(self) -> None:
        original = CapabilityRegistry.load(EXAMPLE_REGISTRY)
        self.assertEqual(
            original.manifest_hash("demo_workcell"),
            CapabilityRegistry.load(EXAMPLE_REGISTRY).manifest_hash("demo_workcell"),
        )

        with tempfile.TemporaryDirectory() as directory:
            copied = Path(directory) / "registry"
            shutil.copytree(EXAMPLE_REGISTRY, copied)
            child_path = copied / "robots" / "so101_canada.toml"
            contents = child_path.read_text(encoding="utf-8")
            child_path.write_text(
                contents.replace("revision = 1", "revision = 2", 1),
                encoding="utf-8",
            )
            changed = CapabilityRegistry.load(copied)

        self.assertNotEqual(
            original.manifest_hash("so101_canada"),
            changed.manifest_hash("so101_canada"),
        )
        self.assertNotEqual(
            original.manifest_hash("demo_workcell"),
            changed.manifest_hash("demo_workcell"),
        )

    def test_configuration_revision_invalidates_physical_and_composite_hashes(self) -> None:
        original = CapabilityRegistry.load(EXAMPLE_REGISTRY)
        with tempfile.TemporaryDirectory() as directory:
            copied = Path(directory) / "registry"
            shutil.copytree(EXAMPLE_REGISTRY, copied)
            physical_path = copied / "robots" / "so101_canada.toml"
            contents = physical_path.read_text(encoding="utf-8")
            physical_path.write_text(
                contents.replace(
                    "configuration_revision = 1",
                    "configuration_revision = 2",
                    1,
                ),
                encoding="utf-8",
            )
            changed = CapabilityRegistry.load(copied)

        self.assertEqual(
            changed.describe("so101_canada")["configuration_revision"], 2
        )
        self.assertNotEqual(
            original.manifest_hash("so101_canada"),
            changed.manifest_hash("so101_canada"),
        )
        self.assertNotEqual(
            original.manifest_hash("demo_workcell"),
            changed.manifest_hash("demo_workcell"),
        )

    def test_connection_changes_require_explicit_revision_and_never_enter_manifest(self) -> None:
        original = CapabilityRegistry.load(EXAMPLE_REGISTRY)
        with tempfile.TemporaryDirectory() as directory:
            copied = Path(directory) / "registry"
            shutil.copytree(EXAMPLE_REGISTRY, copied)
            physical_path = copied / "robots" / "so101_canada.toml"
            contents = physical_path.read_text(encoding="utf-8")
            contents = contents.replace('port = "COM3"', 'port = "SECRET-PORT"')
            contents = contents.replace(
                'calibration_file = "calibration/my_follower_arm.json"',
                'calibration_file = "private/new-secret-calibration.json"',
            )
            physical_path.write_text(contents, encoding="utf-8")
            changed_connection = CapabilityRegistry.load(copied)

        self.assertEqual(
            original.manifest_hash("so101_canada"),
            changed_connection.manifest_hash("so101_canada"),
        )
        self.assertEqual(
            original.manifest_hash("demo_workcell"),
            changed_connection.manifest_hash("demo_workcell"),
        )
        manifest_text = json.dumps(
            changed_connection.describe("demo_workcell"), sort_keys=True
        )
        self.assertNotIn("SECRET-PORT", manifest_text)
        self.assertNotIn("new-secret-calibration", manifest_text)

    def test_public_manifests_do_not_expose_connection_values(self) -> None:
        registry = CapabilityRegistry.load(EXAMPLE_REGISTRY)
        manifest_text = json.dumps(registry.describe("demo_workcell"), sort_keys=True)

        self.assertNotIn("COM3", manifest_text)
        self.assertNotIn("calibration/my_follower_arm.json", manifest_text)
        self.assertNotIn("not-published", manifest_text)
        self.assertNotIn("connection", manifest_text)

    def test_discovery_is_filtered_by_identity_and_exposed_flag(self) -> None:
        registry = CapabilityRegistry.load(EXAMPLE_REGISTRY)

        admin_nodes = {item["node_id"] for item in registry.list_robots("administrator")}
        operator_nodes = {
            item["node_id"] for item in registry.list_robots("workcell_operator")
        }

        self.assertEqual(
            admin_nodes,
            {"so101_canada", "simulator_7dof", "demo_workcell"},
        )
        self.assertEqual(operator_nodes, {"demo_workcell"})
        operator = registry.identity("workcell_operator")
        self.assertTrue(operator.can_control("demo_workcell", "primary_arm.arm"))
        self.assertFalse(operator.can_control("so101_canada", "arm"))
        with self.assertRaises(RegistryError):
            registry.describe("so101_canada", identity_id="workcell_operator")

    def test_cycle_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = self._empty_registry(Path(directory))
            self._write_composite(root, "a", 'role = "b"\nnode = "b"')
            self._write_composite(root, "b", 'role = "a"\nnode = "a"')

            with self.assertRaisesRegex(RegistryError, "cycle"):
                CapabilityRegistry.load(root)

    def test_missing_child_reference_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = self._empty_registry(Path(directory))
            self._write_composite(root, "a", 'role = "missing"\nnode = "missing"')

            with self.assertRaisesRegex(RegistryError, "missing node"):
                CapabilityRegistry.load(root)

    def test_duplicate_canonical_leaf_requires_an_explicit_alias(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "registry"
            shutil.copytree(EXAMPLE_REGISTRY, root)
            duplicate = root / "robots" / "ambiguous.toml"
            duplicate.write_text(
                self._composite_text(
                    "ambiguous",
                    """
[[node.children]]
role = "first"
node = "so101_canada"

[[node.children]]
role = "second"
node = "so101_canada"
""",
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RegistryError, "ambiguous paths"):
                CapabilityRegistry.load(root)

            duplicate.write_text(
                self._composite_text(
                    "ambiguous",
                    """
[[node.children]]
role = "first"
node = "so101_canada"

[[node.children]]
role = "second"
node = "so101_canada"
alias = true
""",
                ),
                encoding="utf-8",
            )
            registry = CapabilityRegistry.load(root)
            self.assertEqual(
                registry.resolve_group("ambiguous", "second.arm").canonical_id,
                "so101_canada.arm",
            )

    @staticmethod
    def _empty_registry(parent: Path) -> Path:
        root = parent / "registry"
        (root / "robots").mkdir(parents=True)
        (root / "server.toml").write_text(
            '[server]\nid = "test-server"\nregistry_version = 2\n',
            encoding="utf-8",
        )
        return root

    @classmethod
    def _write_composite(cls, root: Path, node_id: str, child_fields: str) -> None:
        (root / "robots" / f"{node_id}.toml").write_text(
            cls._composite_text(
                node_id,
                f"[[node.children]]\n{child_fields}\n",
            ),
            encoding="utf-8",
        )

    @staticmethod
    def _composite_text(node_id: str, children: str) -> str:
        return f"""
[node]
id = "{node_id}"
kind = "composite"
revision = 1
exposed_as_run_root = true
cross_collision_checked = false
{children}
"""


if __name__ == "__main__":
    unittest.main()
