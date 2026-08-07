from __future__ import annotations

import json
import math
import tempfile
import unittest
from pathlib import Path

from remote_robot.migrate import migrate_v1_so101_template


class V1MigrationTests(unittest.TestCase):
    def test_splits_arm_and_gripper_and_converts_canonical_units(self) -> None:
        header = {
            "type": "header",
            "format": "so101-joint-trajectory",
            "version": 1,
            "program_id": "legacy",
            "model": "so101-follower",
            "joints": [
                "shoulder_pan",
                "shoulder_lift",
                "elbow_flex",
                "wrist_flex",
                "wrist_roll",
                "gripper",
            ],
            "coordinate_mode": "relative_deg",
            "interpolation": "quintic_stop",
        }
        points = [
            {"type": "point", "t_us": 0, "q_deg": [0, 0, 0, 0, 0, 0]},
            {"type": "point", "t_us": 2_000_000, "q_deg": [10, 0, 0, 0, 0, -20]},
        ]
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "legacy.jsonl"
            source.write_text(
                "\n".join(json.dumps(record) for record in [header, *points]) + "\n",
                encoding="utf-8",
            )
            template = migrate_v1_so101_template(
                source,
                root_id="canada_so101",
                arm_role_path="arm",
                gripper_role_path="gripper",
            )

        sequences = template["phases"][0]["sequences"]
        self.assertEqual(template["root_id"], "canada_so101")
        self.assertEqual(sequences[0]["reference"], "phase_start_measured")
        self.assertEqual(len(sequences[0]["samples"][1]["values"]), 5)
        self.assertAlmostEqual(sequences[0]["samples"][1]["values"][0], math.radians(10))
        self.assertEqual(sequences[1]["samples"][1]["values"], [-0.2])


if __name__ == "__main__":
    unittest.main()
