"""Explicit v1 SO-101 trajectory to v2 template migration."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

from robot_protocol import load_program

from .program import PROGRAM_VERSION, TEMPLATE_FORMAT


def migrate_v1_so101_template(
    source: Path,
    *,
    root_id: str,
    arm_role_path: str = "arm",
    gripper_role_path: str = "gripper",
) -> dict[str, Any]:
    """Convert one six-value degree/percent v1 file to a v2 template.

    The first five values become a canonical-radian arm sequence. The sixth is
    the SO-101 gripper aperture percentage and becomes a normalized scalar.
    This conversion doesn't compile or authorize the result; callers must still
    bind it to the selected v2 manifest through :class:`ProgramCompiler`.
    """

    header, points = load_program(source)
    reference = (
        "phase_start_measured"
        if header.coordinate_mode == "relative_deg"
        else "absolute"
    )
    arm_samples = [
        {
            "t_us": point.t_us,
            "values": [math.radians(value) for value in point.q_deg[:5]],
        }
        for point in points
    ]
    gripper_samples = [
        {
            "t_us": point.t_us,
            "values": [point.q_deg[5] / 100.0],
        }
        for point in points
    ]
    return {
        "format": TEMPLATE_FORMAT,
        "version": PROGRAM_VERSION,
        "program_id": f"{header.program_id}-v2",
        "root_id": root_id,
        "phases": [
            {
                "phase_id": "migrated-v1-trajectory",
                "sequences": [
                    {
                        "group": arm_role_path,
                        "command_schema": "joint_position_trajectory/v1",
                        "reference": reference,
                        "samples": arm_samples,
                    },
                    {
                        "group": gripper_role_path,
                        "command_schema": "normalized_position_trajectory/v1",
                        "reference": reference,
                        "samples": gripper_samples,
                    },
                ],
            }
        ],
    }
