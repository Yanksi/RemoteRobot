from __future__ import annotations

import json
import time
import unittest
from types import SimpleNamespace

import numpy as np

from robot_protocol import (
    ProgramHeader,
    ProtocolError,
    SafetyPolicy,
    TrajectoryPoint,
    chunk_digest,
    interpolate_quintic,
    program_digest,
    validate_points,
)
from robot_server import ProgramRun, SimulatedBackend, configure_connected_robot_safely


class FakeBus:
    def __init__(self) -> None:
        self.motors = {
            "shoulder_pan": object(),
            "shoulder_lift": object(),
            "elbow_flex": object(),
            "wrist_flex": object(),
            "wrist_roll": object(),
            "gripper": object(),
        }
        self.events: list[tuple] = []

    def disable_torque(self) -> None:
        self.events.append(("disable_torque",))

    def configure_motors(self) -> None:
        self.events.append(("configure_motors",))

    def write(self, register: str, motor: str, value: int) -> None:
        self.events.append(("write", register, motor, value))

    def sync_read(self, register: str, num_retry: int) -> dict[str, float]:
        self.events.append(("sync_read", register, num_retry))
        return {name: float(index) for index, name in enumerate(self.motors)}

    def sync_write(self, register: str, values: dict[str, float]) -> None:
        self.events.append(("sync_write", register, values.copy()))

    def enable_torque(self) -> None:
        self.events.append(("enable_torque",))


class RecordingBackend(SimulatedBackend):
    def __init__(self, q: np.ndarray | None = None) -> None:
        super().__init__()
        if q is not None:
            self.q = q.copy()
        self.ever_enabled = False
        self.closed = False

    def enable_at_current(self, measured_q: np.ndarray) -> None:
        super().enable_at_current(measured_q)
        self.ever_enabled = True

    def close(self) -> None:
        self.closed = True
        super().close()


class SafeConnectTests(unittest.TestCase):
    def make_robot(self) -> SimpleNamespace:
        return SimpleNamespace(
            bus=FakeBus(),
            config=SimpleNamespace(
                position_p_coefficient=16,
                position_i_coefficient=0,
                position_d_coefficient=32,
                num_read_retries=2,
            ),
        )

    def test_goal_is_synchronized_before_torque_is_enabled(self) -> None:
        robot = self.make_robot()
        configure_connected_robot_safely(robot, lambda: True, position_mode=0)
        sync_write_index = next(
            index
            for index, event in enumerate(robot.bus.events)
            if event[:2] == ("sync_write", "Goal_Position")
        )
        enable_index = next(
            index for index, event in enumerate(robot.bus.events) if event[0] == "enable_torque"
        )
        self.assertLess(sync_write_index, enable_index)
        self.assertEqual(robot.bus.events[0][0], "disable_torque")

    def test_expired_lease_never_enables_torque(self) -> None:
        robot = self.make_robot()
        with self.assertRaises(RuntimeError):
            configure_connected_robot_safely(robot, lambda: False, position_mode=0)
        self.assertNotIn("enable_torque", [event[0] for event in robot.bus.events])


class ProtocolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = SafetyPolicy(min_segment_us=10_000)

    def test_relative_program_must_start_at_zero(self) -> None:
        header = ProgramHeader("bad", coordinate_mode="relative_deg")
        points = [TrajectoryPoint(0, (1.0, 0, 0, 0, 0, 0))]
        with self.assertRaisesRegex(ProtocolError, "all-zero"):
            validate_points(header, points, self.policy, require_complete=False)

    def test_velocity_limit_is_checked_before_execution(self) -> None:
        previous = TrajectoryPoint(0, (0.0,) * 6)
        current = TrajectoryPoint(10_000, (40.0, 0, 0, 0, 0, 0))
        with self.assertRaises(ProtocolError) as raised:
            self.policy.validate_segment(previous, current)
        self.assertEqual(raised.exception.code, "VELOCITY_LIMIT")

    def test_quintic_interpolation_stops_at_both_endpoints(self) -> None:
        p0 = TrajectoryPoint(0, (0.0,) * 6)
        p1 = TrajectoryPoint(1_000_000, (10.0,) * 6)
        self.assertEqual(interpolate_quintic(p0, p1, 0), p0.q_deg)
        self.assertEqual(interpolate_quintic(p0, p1, 1_000_000), p1.q_deg)
        self.assertAlmostEqual(interpolate_quintic(p0, p1, 500_000)[0], 5.0)

    def test_chunk_digest_survives_json_numeric_normalization(self) -> None:
        original = [TrajectoryPoint(0, (0.0, 0, 0, 0, 0, 0))]
        wire_value = json.loads(json.dumps(original[0].record()))
        parsed = [TrajectoryPoint.from_record(wire_value)]
        self.assertEqual(chunk_digest(0, original, True), chunk_digest(0, parsed, True))


class ProgramRunTests(unittest.TestCase):
    def make_run(
        self,
        points: list[TrajectoryPoint],
        backend: RecordingBackend,
        *,
        final: bool = True,
        run_mode: str = "sealed",
        coordinate_mode: str = "relative_deg",
    ) -> tuple[ProgramRun, list[dict]]:
        header = ProgramHeader("test", coordinate_mode=coordinate_mode)
        events: list[dict] = []
        policy = SafetyPolicy(
            min_segment_us=10_000,
            start_buffer_us=50_000,
            max_velocity_deg_s=100.0,
            max_acceleration_deg_s2=1000.0,
        )
        run = ProgramRun(
            "run-1",
            header,
            run_mode,
            program_digest(header, points),
            "stop_after_timeout",
            lambda: backend,
            events.append,
            policy,
            control_hz=100.0,
            telemetry_hz=20.0,
            heartbeat_timeout_s=2.0,
            hold_after_run_s=0.0,
        )
        digest = chunk_digest(0, points, final)
        run.append_chunk(0, points, final, digest)
        return run, events

    def test_duplicate_chunk_is_idempotent(self) -> None:
        points = [
            TrajectoryPoint(0, (0.0,) * 6),
            TrajectoryPoint(100_000, (1.0, 0, 0, 0, 0, 0)),
        ]
        run, _ = self.make_run(points, RecordingBackend())
        digest = chunk_digest(0, points, True)
        first = run.chunk_receipts[0][1]
        second = run.append_chunk(0, points, True, digest)
        self.assertEqual(first, second)
        self.assertEqual(len(run.points), 2)

    def test_disconnect_releases_a_never_started_run(self) -> None:
        points = [
            TrajectoryPoint(0, (0.0,) * 6),
            TrajectoryPoint(100_000, (1.0, 0, 0, 0, 0, 0)),
        ]
        run, _ = self.make_run(points, RecordingBackend())
        run.connection_lost()
        self.assertEqual(run.snapshot()["state"], "stopped")
        self.assertTrue(run.terminal_event.is_set())

    def test_simulated_relative_run_completes_and_disables_torque(self) -> None:
        points = [
            TrajectoryPoint(0, (0.0,) * 6),
            TrajectoryPoint(100_000, (1.0, 0, 0, 0, 0, 0)),
        ]
        backend = RecordingBackend()
        run, events = self.make_run(points, backend)
        run.start()
        self.assertTrue(run.terminal_event.wait(2.0))
        self.assertEqual(run.snapshot()["state"], "completed")
        self.assertTrue(backend.ever_enabled)
        self.assertFalse(backend.torque_enabled)
        self.assertTrue(backend.closed)
        self.assertEqual(events[-1]["type"], "run_finished")

    def test_start_pose_mismatch_never_enables_torque(self) -> None:
        points = [
            TrajectoryPoint(0, (0.0,) * 6),
            TrajectoryPoint(100_000, (1.0, 0, 0, 0, 0, 0)),
        ]
        backend = RecordingBackend(np.array([50.0] * 6))
        run, events = self.make_run(
            points,
            backend,
            coordinate_mode="absolute_deg",
        )
        run.start()
        self.assertTrue(run.terminal_event.wait(2.0))
        self.assertEqual(run.snapshot()["state"], "faulted")
        self.assertFalse(backend.ever_enabled)
        self.assertTrue(any(event.get("code") == "START_STATE_MISMATCH" for event in events))

    def test_unsealed_stream_faults_on_buffer_underrun(self) -> None:
        points = [
            TrajectoryPoint(0, (0.0,) * 6),
            TrajectoryPoint(100_000, (1.0, 0, 0, 0, 0, 0)),
        ]
        backend = RecordingBackend()
        run, events = self.make_run(
            points,
            backend,
            final=False,
            run_mode="streaming",
        )
        run.start()
        self.assertTrue(run.terminal_event.wait(2.0))
        self.assertEqual(run.snapshot()["state"], "faulted")
        self.assertTrue(any(event.get("code") == "BUFFER_UNDERRUN" for event in events))


if __name__ == "__main__":
    unittest.main()
