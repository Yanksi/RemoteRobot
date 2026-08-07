"""Canada-side SO-101 trajectory server.

The WAN carries timestamped trajectory chunks and telemetry.  Interpolation,
motor reads/writes, tracking supervision, heartbeat handling, and safe shutdown
all run on the computer physically connected to the arm.
"""

from __future__ import annotations

import argparse
import asyncio
import hmac
import json
import logging
import math
import os
import secrets
import signal
import threading
import time
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any, Protocol

import numpy as np
from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed

from robot_protocol import (
    JOINTS,
    PROTOCOL,
    ProgramHeader,
    ProtocolError,
    RUN_MODES,
    SafetyPolicy,
    TrajectoryPoint,
    chunk_digest,
    interpolate_quintic,
    program_digest,
)


LOG = logging.getLogger("robot_server")
BASE_DIR = Path(__file__).resolve().parent
TERMINAL_STATES = {"completed", "stopped", "faulted"}
# The gripper is supervised separately: its units are aperture percent, not
# degrees, so it cannot share the body joints' tracking threshold.
GRIPPER_INDEX = JOINTS.index("gripper")
BODY_INDICES = [index for index in range(len(JOINTS)) if index != GRIPPER_INDEX]


class MotorBackend(Protocol):
    """True external-dependency seam: real Feetech bus or simulator."""

    def prepare_disabled(self) -> np.ndarray: ...

    def enable_at_current(self, measured_q: np.ndarray) -> None: ...

    def read_q(self) -> np.ndarray: ...

    def send_q(self, q_deg: np.ndarray) -> None: ...

    def close(self) -> None: ...


def prepare_connected_robot_disabled(robot: Any, position_mode: int) -> np.ndarray:
    """Connect/configure while torque is off and overwrite every stale goal."""

    bus = robot.bus
    bus.disable_torque()
    bus.configure_motors()
    for motor in bus.motors:
        bus.write("Operating_Mode", motor, position_mode)
        bus.write("P_Coefficient", motor, robot.config.position_p_coefficient)
        bus.write("I_Coefficient", motor, robot.config.position_i_coefficient)
        bus.write("D_Coefficient", motor, robot.config.position_d_coefficient)
        if motor == "gripper":
            bus.write("Max_Torque_Limit", motor, 500)
            bus.write("Protection_Current", motor, 250)
            bus.write("Overload_Torque", motor, 25)

    present = bus.sync_read("Present_Position", num_retry=robot.config.num_read_retries)
    # Critical reconnect invariant: never energize a goal left in servo RAM by
    # an earlier process or a broken network session.
    bus.sync_write("Goal_Position", present)
    return np.array([present[name] for name in JOINTS], dtype=float)


def configure_connected_robot_safely(
    robot: Any,
    should_continue: Callable[[], bool],
    position_mode: int,
) -> None:
    """Compatibility helper retained for focused tests and one-shot callers."""

    prepare_connected_robot_disabled(robot, position_mode)
    if not should_continue():
        raise RuntimeError("control lease expired during robot setup")
    robot.bus.enable_torque()


class SO101Backend:
    def __init__(
        self,
        serial_port: str,
        robot_id: str,
        calibration_dir: Path,
        serial_write_timeout_s: float,
    ) -> None:
        self.serial_port = serial_port
        self.robot_id = robot_id
        self.calibration_dir = calibration_dir
        self.serial_write_timeout_s = serial_write_timeout_s
        self.robot: Any | None = None

    def prepare_disabled(self) -> np.ndarray:
        from lerobot.motors.feetech import OperatingMode
        from lerobot.robots.so_follower import SO101Follower, SO101FollowerConfig

        if self.robot is not None:
            raise RuntimeError("robot backend is already connected")
        robot = SO101Follower(
            SO101FollowerConfig(
                port=self.serial_port,
                id=self.robot_id,
                calibration_dir=self.calibration_dir,
            )
        )
        if not robot.calibration:
            raise RuntimeError(
                f"missing calibration file: {robot.calibration_fpath}; "
                "remote automatic calibration is intentionally disabled"
            )
        robot.bus.connect()
        self.robot = robot
        try:
            serial_handle = getattr(robot.bus.port_handler, "ser", None)
            if serial_handle is not None:
                serial_handle.write_timeout = self.serial_write_timeout_s
            if not robot.is_calibrated:
                raise RuntimeError(
                    "calibration file doesn't match motor registers; "
                    "remote automatic calibration is intentionally disabled"
                )
            return prepare_connected_robot_disabled(robot, OperatingMode.POSITION.value)
        except Exception:
            self.close()
            raise

    def enable_at_current(self, measured_q: np.ndarray) -> None:
        robot = self._require_robot()
        robot.bus.sync_write(
            "Goal_Position",
            {name: float(value) for name, value in zip(JOINTS, measured_q, strict=True)},
        )
        robot.bus.enable_torque()

    def read_q(self) -> np.ndarray:
        robot = self._require_robot()
        values = robot.bus.sync_read(
            "Present_Position",
            num_retry=robot.config.num_read_retries,
        )
        return np.array([values[name] for name in JOINTS], dtype=float)

    def send_q(self, q_deg: np.ndarray) -> None:
        robot = self._require_robot()
        robot.bus.sync_write(
            "Goal_Position",
            {name: float(value) for name, value in zip(JOINTS, q_deg, strict=True)},
        )

    def close(self) -> None:
        robot, self.robot = self.robot, None
        if robot is None:
            return
        try:
            robot.bus.disable_torque(num_retry=5)
        except Exception:  # Best effort: a physical power E-stop remains mandatory.
            LOG.exception("failed to disable motor torque")
        finally:
            try:
                robot.bus.disconnect(disable_torque=False)
            except Exception:
                LOG.exception("failed to close the motor serial port")

    def _require_robot(self) -> Any:
        if self.robot is None:
            raise RuntimeError("robot isn't connected")
        return self.robot


def limits_from_calibration(
    calibration_path: Path,
    policy: SafetyPolicy,
    margin_deg: float,
) -> SafetyPolicy:
    """Clamp the policy to the arm's recorded travel.

    The servos hold their own Min/Max_Position_Limit registers, so an out-of-range
    goal is clamped in firmware rather than driven into a stop.  But lerobot's
    unnormalize step bounds only the 0-100 joints, never a DEGREES one, so a policy
    wider than the calibrated travel turns into silent under-travel: the arm stops
    short of the commanded angle and the residual is usually too small to trip the
    tracking check.  Deriving the limits here makes that an explicit rejection at
    validation time, before torque.
    """

    from lerobot.motors.feetech import FeetechMotorsBus

    resolution = FeetechMotorsBus.model_resolution_table["sts3215"] - 1
    calibration = json.loads(calibration_path.read_text(encoding="utf-8"))
    missing = set(JOINTS) - set(calibration)
    if missing:
        raise SystemExit(f"{calibration_path} is missing joints: {sorted(missing)}")

    lower: list[float] = []
    upper: list[float] = []
    for index, name in enumerate(JOINTS):
        entry = calibration[name]
        raw_min, raw_max = float(entry["range_min"]), float(entry["range_max"])
        if name == "gripper":
            # RANGE_0_100 aperture percent, not degrees.  Full close/open must
            # stay reachable, so no margin is subtracted here.
            joint_min, joint_max = 0.0, 100.0
        else:
            mid = (raw_min + raw_max) / 2
            joint_min = (raw_min - mid) * 360 / resolution + margin_deg
            joint_max = (raw_max - mid) * 360 / resolution - margin_deg
        if joint_min >= joint_max:
            raise SystemExit(f"calibrated travel for {name} is smaller than the {margin_deg} deg margin")
        lower.append(max(joint_min, policy.joint_min_deg[index]))
        upper.append(min(joint_max, policy.joint_max_deg[index]))

    for name, low, high in zip(JOINTS, lower, upper, strict=True):
        LOG.info("joint limit %-14s %+8.2f .. %+8.2f", name, low, high)
    return replace(policy, joint_min_deg=tuple(lower), joint_max_deg=tuple(upper))


class SimulatedBackend:
    """Deterministic hardware-free adapter used by tests and deployment checks."""

    def __init__(self) -> None:
        self.q = np.array([0.0, -45.0, 75.0, 35.0, 0.0, 20.0], dtype=float)
        self.connected = False
        self.torque_enabled = False

    def prepare_disabled(self) -> np.ndarray:
        self.connected = True
        self.torque_enabled = False
        return self.q.copy()

    def enable_at_current(self, measured_q: np.ndarray) -> None:
        if not self.connected:
            raise RuntimeError("simulated robot isn't connected")
        self.q = measured_q.copy()
        self.torque_enabled = True

    def read_q(self) -> np.ndarray:
        if not self.connected:
            raise RuntimeError("simulated robot isn't connected")
        return self.q.copy()

    def send_q(self, q_deg: np.ndarray) -> None:
        if not self.connected or not self.torque_enabled:
            raise RuntimeError("simulated robot isn't enabled")
        self.q = np.asarray(q_deg, dtype=float).copy()

    def close(self) -> None:
        self.torque_enabled = False
        self.connected = False


class ProgramRun:
    """Deep module owning validation, bounded buffering, execution and safety."""

    def __init__(
        self,
        run_id: str,
        header: ProgramHeader,
        run_mode: str,
        expected_digest: str,
        disconnect_policy: str,
        backend_factory: Callable[[], MotorBackend],
        event_sink: Callable[[dict[str, Any]], None],
        policy: SafetyPolicy,
        control_hz: float,
        telemetry_hz: float,
        heartbeat_timeout_s: float,
        hold_after_run_s: float,
    ) -> None:
        self.run_id = run_id
        self.header = header
        self.run_mode = run_mode
        self.expected_digest = expected_digest
        self.disconnect_policy = disconnect_policy
        self.backend_factory = backend_factory
        self.event_sink = event_sink
        self.policy = policy
        self.control_hz = control_hz
        self.telemetry_hz = telemetry_hz
        self.heartbeat_timeout_s = heartbeat_timeout_s
        self.hold_after_run_s = hold_after_run_s

        self.lock = threading.RLock()
        self.points: list[TrajectoryPoint] = []
        self.chunk_receipts: dict[int, tuple[str, dict[str, Any]]] = {}
        self.expected_chunk_seq = 0
        self.final_received = False
        self.state = "buffering"
        self.message = "waiting for validated trajectory chunks"
        self.cursor_us = 0
        self.last_heartbeat = time.monotonic()
        self.origin_q: np.ndarray | None = None
        self.measured_q: np.ndarray | None = None
        self.target_q: np.ndarray | None = None
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.backend: MotorBackend | None = None
        self.torque_enabled = False
        self.terminal_event = threading.Event()

    def append_chunk(
        self,
        chunk_seq: int,
        points: list[TrajectoryPoint],
        final: bool,
        supplied_digest: str,
    ) -> dict[str, Any]:
        actual_digest = chunk_digest(chunk_seq, points, final)
        if not hmac.compare_digest(supplied_digest, actual_digest):
            raise ProtocolError("HASH_MISMATCH", "chunk digest doesn't match its payload")

        with self.lock:
            old = self.chunk_receipts.get(chunk_seq)
            if old is not None:
                old_digest, receipt = old
                if not hmac.compare_digest(old_digest, actual_digest):
                    raise ProtocolError("DUPLICATE_CONFLICT", "chunk sequence was reused with new data")
                return receipt.copy()
            if chunk_seq != self.expected_chunk_seq:
                raise ProtocolError(
                    "OUT_OF_ORDER",
                    f"expected chunk {self.expected_chunk_seq}, received {chunk_seq}",
                    details={"expected_chunk_seq": self.expected_chunk_seq},
                )
            if self.state not in {"buffering", "running"} or self.final_received:
                raise ProtocolError("RUN_STATE_CONFLICT", f"cannot append while run is {self.state}")
            if not points or len(points) > self.policy.max_chunk_points:
                raise ProtocolError(
                    "CHUNK_SIZE",
                    f"a chunk must contain 1..{self.policy.max_chunk_points} points",
                )

            previous = self.points[-1] if self.points else None
            for point in points:
                if previous is None:
                    if point.t_us != 0:
                        raise ProtocolError("INVALID_TIME", "the first point must have t_us=0")
                    if self.header.coordinate_mode == "relative_deg" and any(
                        abs(value) > 1e-9 for value in point.q_deg
                    ):
                        raise ProtocolError(
                            "START_STATE_MISMATCH",
                            "a relative trajectory must start at all-zero offsets",
                        )
                else:
                    self.policy.validate_segment(previous, point)
                self.policy.validate_position(point, self.header.coordinate_mode)
                self._validate_resolved_position(point)
                previous = point

            proposed_count = len(self.points) + len(points)
            proposed_end = points[-1].t_us
            if proposed_count > self.policy.max_points or proposed_end > self.policy.max_duration_us:
                raise ProtocolError("RESOURCE_LIMIT", "program exceeds the server resource limits")
            if final and proposed_count < 2:
                raise ProtocolError("SCHEMA_INVALID", "a complete program needs at least two points")
            if (
                self.run_mode == "streaming"
                and proposed_end - self.cursor_us > self.policy.max_horizon_us
            ):
                raise ProtocolError(
                    "QUEUE_FULL",
                    "chunk is beyond the server's bounded lookahead horizon",
                    retryable=True,
                    details={"available_horizon_us": self.available_horizon_us_locked()},
                )

            self.points.extend(points)
            self.expected_chunk_seq += 1
            self.final_received = final
            if final:
                digest = program_digest(self.header, self.points)
                if not hmac.compare_digest(digest, self.expected_digest):
                    self.points[-len(points) :] = []
                    self.expected_chunk_seq -= 1
                    self.final_received = False
                    raise ProtocolError("HASH_MISMATCH", "complete program digest doesn't match")
                self.message = "complete program validated"

            receipt = {
                "type": "chunk_accepted",
                "run_id": self.run_id,
                "chunk_seq": chunk_seq,
                "accepted_until_us": proposed_end,
                "execution_cursor_us": self.cursor_us,
                "queued_ahead_us": max(0, proposed_end - self.cursor_us),
                "available_horizon_us": self.available_horizon_us_locked(),
                "final": final,
            }
            self.chunk_receipts[chunk_seq] = (actual_digest, receipt.copy())
            return receipt

    def start(self) -> dict[str, Any]:
        with self.lock:
            if self.state != "buffering":
                raise ProtocolError("RUN_STATE_CONFLICT", f"cannot start while run is {self.state}")
            if len(self.points) < 2:
                raise ProtocolError("NOT_READY", "at least two validated points are required")
            if self.run_mode == "sealed" and not self.final_received:
                raise ProtocolError("NOT_READY", "sealed mode requires the entire program first")
            if (
                self.run_mode == "streaming"
                and not self.final_received
                and self.points[-1].t_us < self.policy.start_buffer_us
            ):
                raise ProtocolError(
                    "NOT_READY",
                    "streaming mode hasn't reached the start buffer watermark",
                    details={"start_buffer_us": self.policy.start_buffer_us},
                )
            self.state = "arming"
            self.message = "opening the local motor bus with torque disabled"
            self.thread = threading.Thread(target=self._execute, name=f"run-{self.run_id}", daemon=True)
            self.thread.start()
            event = self.snapshot()
            event["type"] = "run_state"
            return event

    def heartbeat(self) -> dict[str, Any]:
        with self.lock:
            self.last_heartbeat = time.monotonic()
            return {
                "type": "heartbeat_ack",
                "run_id": self.run_id,
                "server_monotonic_us": time.monotonic_ns() // 1000,
                "state": self.state,
            }

    def request_stop(self, reason: str) -> dict[str, Any]:
        with self.lock:
            self.stop_event.set()
            if self.state not in TERMINAL_STATES:
                self.message = reason
            return {
                "type": "stop_accepted",
                "run_id": self.run_id,
                "state": self.state,
                "reason": reason,
            }

    def connection_lost(self) -> None:
        """Release never-started runs; running runs use the local watchdog."""

        with self.lock:
            if self.state == "buffering":
                self.state = "stopped"
                self.message = "client disconnected before execution"
                self.terminal_event.set()
            elif self.state == "arming":
                self.stop_event.set()

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            accepted_until = self.points[-1].t_us if self.points else 0
            return {
                "run_id": self.run_id,
                "state": self.state,
                "message": self.message,
                "coordinate_mode": self.header.coordinate_mode,
                "run_mode": self.run_mode,
                "final_received": self.final_received,
                "expected_chunk_seq": self.expected_chunk_seq,
                "execution_cursor_us": self.cursor_us,
                "accepted_until_us": accepted_until,
                "queued_ahead_us": max(0, accepted_until - self.cursor_us),
                "available_horizon_us": self.available_horizon_us_locked(),
                "measured_q_deg": None if self.measured_q is None else self.measured_q.tolist(),
                "target_q_deg": None if self.target_q is None else self.target_q.tolist(),
            }

    def available_horizon_us_locked(self) -> int:
        accepted_until = self.points[-1].t_us if self.points else 0
        used = max(0, accepted_until - self.cursor_us)
        return max(0, self.policy.max_horizon_us - used)

    def _validate_resolved_position(self, point: TrajectoryPoint) -> None:
        if self.header.coordinate_mode != "relative_deg" or self.origin_q is None:
            return
        resolved = self.origin_q + np.asarray(point.q_deg)
        for index, (name, value) in enumerate(zip(JOINTS, resolved, strict=True)):
            if not self.policy.joint_min_deg[index] <= value <= self.policy.joint_max_deg[index]:
                raise ProtocolError(
                    "LIMIT_VIOLATION",
                    f"resolved {name} target is outside server limits",
                    details={"joint": name, "t_us": point.t_us},
                )

    def _execute(self) -> None:
        outcome = "completed"
        code: str | None = None
        try:
            backend = self.backend_factory()
            self.backend = backend
            measured = backend.prepare_disabled()
            if measured.shape != (len(JOINTS),) or not np.all(np.isfinite(measured)):
                raise ProtocolError("HARDWARE_FAULT", "motor bus returned an invalid start pose")
            with self.lock:
                self.origin_q = measured.copy()
                self.measured_q = measured.copy()
                points = list(self.points)

            if self.header.coordinate_mode == "absolute_deg":
                start_error = float(np.max(np.abs(np.asarray(points[0].q_deg) - measured)))
                if start_error > self.policy.start_tolerance_deg:
                    raise ProtocolError(
                        "START_STATE_MISMATCH",
                        f"first point is {start_error:.2f} deg from the measured pose",
                        details={"limit": self.policy.start_tolerance_deg},
                    )
            else:
                for point in points:
                    self._validate_resolved_position(point)

            if self.stop_event.is_set():
                raise ProtocolError("CANCELLED", "run was stopped before torque enable")
            backend.enable_at_current(measured)
            self.torque_enabled = True
            with self.lock:
                self.state = "running"
                self.message = "local closed-loop execution is running"
            self._emit({"type": "run_state", **self.snapshot()})

            started = time.monotonic()
            period_s = 1.0 / self.control_hz
            telemetry_period_s = 1.0 / self.telemetry_hz
            next_tick = started
            last_telemetry = -math.inf
            segment_index = 0
            tracking_violations = 0

            while True:
                now = time.monotonic()
                if self.stop_event.is_set():
                    outcome, code = "stopped", "CANCELLED"
                    break
                with self.lock:
                    heartbeat_age = now - self.last_heartbeat
                    ignore_heartbeat = self.final_received and self.disconnect_policy == "complete_if_sealed"
                if not ignore_heartbeat and heartbeat_age > self.heartbeat_timeout_s:
                    outcome, code = "faulted", "HEARTBEAT_LOST"
                    raise ProtocolError(code, "client heartbeat timed out")

                cursor_us = max(0, int((now - started) * 1_000_000))
                with self.lock:
                    points = list(self.points)
                    final_received = self.final_received
                while segment_index + 1 < len(points) and cursor_us > points[segment_index + 1].t_us:
                    segment_index += 1

                if segment_index + 1 >= len(points):
                    if final_received and cursor_us >= points[-1].t_us:
                        backend.send_q(self._resolve(points[-1]))
                        with self.lock:
                            self.cursor_us = points[-1].t_us
                        break
                    outcome, code = "faulted", "BUFFER_UNDERRUN"
                    raise ProtocolError(code, "validated trajectory buffer ran dry")

                logical_target = interpolate_quintic(
                    points[segment_index], points[segment_index + 1], cursor_us
                )
                target = self._resolve(TrajectoryPoint(cursor_us, logical_target))
                measured = backend.read_q()
                deviation = np.abs(target - measured)
                tracking_error = float(np.max(deviation[BODY_INDICES]))
                gripper_error = float(deviation[GRIPPER_INDEX])
                gripper_limit = self.policy.gripper_tracking_error_pct
                exceeded = tracking_error > self.policy.tracking_error_deg or (
                    gripper_limit is not None and gripper_error > gripper_limit
                )
                tracking_violations = tracking_violations + 1 if exceeded else 0
                if tracking_violations >= 5:
                    outcome, code = "faulted", "TRACKING_ERROR"
                    raise ProtocolError(
                        code,
                        f"body tracking error stayed above {self.policy.tracking_error_deg:g} deg"
                        if tracking_error > self.policy.tracking_error_deg
                        else f"gripper deviation stayed above {gripper_limit:g}%",
                        details={
                            "measured_error_deg": tracking_error,
                            "gripper_error_pct": gripper_error,
                        },
                    )
                backend.send_q(target)

                with self.lock:
                    self.cursor_us = cursor_us
                    self.measured_q = measured.copy()
                    self.target_q = target.copy()
                    accepted_until = self.points[-1].t_us
                if now - last_telemetry >= telemetry_period_s:
                    self._emit(
                        {
                            "type": "telemetry",
                            **self.snapshot(),
                            "tracking_error_deg": tracking_error,
                            "gripper_error_pct": gripper_error,
                            "server_monotonic_us": time.monotonic_ns() // 1000,
                            "low_water": accepted_until - cursor_us < self.policy.low_water_us,
                        }
                    )
                    last_telemetry = now

                next_tick += period_s
                delay = next_tick - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                elif delay < -max(0.25, 5 * period_s):
                    outcome, code = "faulted", "CONTROL_CYCLE_OVERRUN"
                    raise ProtocolError(code, "local motor loop missed its timing budget")

        except ProtocolError as exc:
            outcome = "stopped" if exc.code == "CANCELLED" else "faulted"
            code = exc.code
            self._emit(
                {
                    "type": "fault",
                    "run_id": self.run_id,
                    "code": exc.code,
                    "message": exc.message,
                    "retryable": exc.retryable,
                    "details": exc.details,
                    "safety_action": "hold_then_disable_torque",
                }
            )
        except Exception as exc:
            outcome, code = "faulted", "HARDWARE_FAULT"
            LOG.exception("run %s failed", self.run_id)
            self._emit(
                {
                    "type": "fault",
                    "run_id": self.run_id,
                    "code": code,
                    "message": str(exc),
                    "retryable": False,
                    "details": {},
                    "safety_action": "hold_then_disable_torque",
                }
            )
        finally:
            self._hold_then_close()
            with self.lock:
                self.state = outcome
                self.message = "trajectory completed" if outcome == "completed" else (code or outcome)
            self.terminal_event.set()
            self._emit(
                {
                    "type": "run_finished",
                    **self.snapshot(),
                    "outcome": outcome,
                    "fault_code": code,
                }
            )

    def _resolve(self, point: TrajectoryPoint) -> np.ndarray:
        q = np.asarray(point.q_deg, dtype=float)
        if self.header.coordinate_mode == "relative_deg":
            if self.origin_q is None:
                raise RuntimeError("relative trajectory has no measured origin")
            q = self.origin_q + q
        return q

    def _hold_then_close(self) -> None:
        backend, self.backend = self.backend, None
        if backend is None:
            return
        try:
            if self.torque_enabled:
                measured = backend.read_q()
                backend.send_q(measured)
                if self.hold_after_run_s > 0:
                    time.sleep(self.hold_after_run_s)
        except Exception:
            LOG.exception("couldn't command the local hold pose")
        finally:
            backend.close()
            self.torque_enabled = False

    def _emit(self, event: dict[str, Any]) -> None:
        try:
            self.event_sink(event)
        except Exception:
            LOG.exception("failed to enqueue run event")


class ProgramController:
    def __init__(
        self,
        backend_factory: Callable[[], MotorBackend],
        policy: SafetyPolicy,
        control_hz: float,
        telemetry_hz: float,
        heartbeat_timeout_s: float,
        hold_after_run_s: float,
    ) -> None:
        self.backend_factory = backend_factory
        self.policy = policy
        self.control_hz = control_hz
        self.telemetry_hz = telemetry_hz
        self.heartbeat_timeout_s = heartbeat_timeout_s
        self.hold_after_run_s = hold_after_run_s
        self.lock = threading.Lock()
        self.active: ProgramRun | None = None

    def open_run(
        self,
        run_id: str,
        header: ProgramHeader,
        run_mode: str,
        expected_digest: str,
        disconnect_policy: str,
        event_sink: Callable[[dict[str, Any]], None],
    ) -> ProgramRun:
        if run_mode not in RUN_MODES:
            raise ProtocolError("SCHEMA_INVALID", f"unsupported run_mode: {run_mode}")
        if disconnect_policy not in {"stop_after_timeout", "complete_if_sealed"}:
            raise ProtocolError("SCHEMA_INVALID", "unsupported disconnect_policy")
        if disconnect_policy == "complete_if_sealed" and run_mode != "sealed":
            raise ProtocolError(
                "SCHEMA_INVALID",
                "complete_if_sealed is only valid for sealed runs",
            )
        if not run_id or len(run_id) > 128:
            raise ProtocolError("SCHEMA_INVALID", "run_id must contain 1..128 characters")
        if len(expected_digest) != 64:
            raise ProtocolError("SCHEMA_INVALID", "program_digest must be a SHA-256 hex digest")
        with self.lock:
            if self.active is not None and self.active.snapshot()["state"] not in TERMINAL_STATES:
                raise ProtocolError("RUN_STATE_CONFLICT", "another run already owns the robot")
            run = ProgramRun(
                run_id,
                header,
                run_mode,
                expected_digest,
                disconnect_policy,
                self.backend_factory,
                event_sink,
                self.policy,
                self.control_hz,
                self.telemetry_hz,
                self.heartbeat_timeout_s,
                self.hold_after_run_s,
            )
            self.active = run
            return run

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            if self.active is None:
                return {"state": "idle", "message": "motors are disconnected"}
            return self.active.snapshot()

    def emergency_stop(self, reason: str) -> dict[str, Any]:
        with self.lock:
            if self.active is None:
                return {"type": "stop_accepted", "state": "idle", "reason": reason}
            return self.active.request_stop(reason)

    def wait_for_idle(self, timeout_s: float) -> bool:
        with self.lock:
            run = self.active
        if run is None:
            return True
        return run.terminal_event.wait(timeout_s)


def decode_message(raw: str | bytes) -> dict[str, Any]:
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ProtocolError("SCHEMA_INVALID", "binary frames must contain UTF-8 JSON") from exc
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ProtocolError("SCHEMA_INVALID", f"invalid JSON: {exc.msg}") from exc
    if not isinstance(value, dict) or not isinstance(value.get("type"), str):
        raise ProtocolError("SCHEMA_INVALID", "every message must be an object with a type")
    return value


async def connection_handler(
    websocket: ServerConnection,
    controller: ProgramController,
    token: str,
) -> None:
    loop = asyncio.get_running_loop()
    outgoing: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()

    def emit_from_thread(event: dict[str, Any]) -> None:
        loop.call_soon_threadsafe(outgoing.put_nowait, event)

    async def sender() -> None:
        while True:
            event = await outgoing.get()
            if event is None:
                return
            await websocket.send(json.dumps(event, ensure_ascii=False, separators=(",", ":")))

    sender_task = asyncio.create_task(sender())
    run: ProgramRun | None = None
    try:
        raw = await asyncio.wait_for(websocket.recv(), timeout=5.0)
        hello = decode_message(raw)
        supplied_token = str(hello.get("token", ""))
        if (
            hello.get("type") != "hello"
            or hello.get("protocol") != PROTOCOL
            or not hmac.compare_digest(supplied_token, token)
        ):
            await outgoing.put(
                {
                    "type": "error",
                    "code": "AUTH_FAILED",
                    "message": "authentication or protocol negotiation failed",
                    "retryable": False,
                    "details": {},
                }
            )
            await asyncio.sleep(0)
            await websocket.close(code=1008, reason="authentication failed")
            return

        await outgoing.put(
            {
                "type": "welcome",
                "protocol": PROTOCOL,
                "server_boot_id": SERVER_BOOT_ID,
                "robot_model": "so101-follower",
                "joints": list(JOINTS),
                "limits": {
                    "joint_min_deg": list(controller.policy.joint_min_deg),
                    "joint_max_deg": list(controller.policy.joint_max_deg),
                    "max_velocity_deg_s": controller.policy.max_velocity_deg_s,
                    "max_acceleration_deg_s2": controller.policy.max_acceleration_deg_s2,
                    "max_chunk_points": controller.policy.max_chunk_points,
                    "max_horizon_us": controller.policy.max_horizon_us,
                    "start_buffer_us": controller.policy.start_buffer_us,
                },
                "status": controller.snapshot(),
            }
        )

        async for raw in websocket:
            try:
                message = decode_message(raw)
                kind = message["type"]
                if kind == "open_run":
                    if run is not None:
                        raise ProtocolError("RUN_STATE_CONFLICT", "this connection already opened a run")
                    header_value = message.get("header")
                    if not isinstance(header_value, dict):
                        raise ProtocolError("SCHEMA_INVALID", "open_run.header must be an object")
                    run = controller.open_run(
                        str(message.get("run_id", "")),
                        ProgramHeader.from_record(header_value),
                        str(message.get("run_mode", "")),
                        str(message.get("program_digest", "")),
                        str(message.get("disconnect_policy", "stop_after_timeout")),
                        emit_from_thread,
                    )
                    await outgoing.put({"type": "run_opened", **run.snapshot()})
                elif kind == "chunk":
                    if run is None:
                        raise ProtocolError("RUN_STATE_CONFLICT", "open_run is required first")
                    point_values = message.get("points")
                    if not isinstance(point_values, list):
                        raise ProtocolError("SCHEMA_INVALID", "chunk.points must be an array")
                    points = [TrajectoryPoint.from_record(value) for value in point_values]
                    receipt = run.append_chunk(
                        int(message.get("chunk_seq", -1)),
                        points,
                        bool(message.get("final", False)),
                        str(message.get("digest", "")),
                    )
                    await outgoing.put(receipt)
                elif kind == "start":
                    if run is None:
                        raise ProtocolError("RUN_STATE_CONFLICT", "open_run is required first")
                    await outgoing.put(run.start())
                elif kind == "heartbeat":
                    if run is not None:
                        await outgoing.put(run.heartbeat())
                    else:
                        await outgoing.put(
                            {
                                "type": "heartbeat_ack",
                                "server_monotonic_us": time.monotonic_ns() // 1000,
                                "state": controller.snapshot()["state"],
                            }
                        )
                elif kind == "status":
                    await outgoing.put({"type": "status", **controller.snapshot()})
                elif kind == "cancel":
                    if run is None:
                        raise ProtocolError("RUN_STATE_CONFLICT", "this connection has no run")
                    await outgoing.put(run.request_stop(str(message.get("reason", "client cancel"))))
                elif kind == "emergency_stop":
                    await outgoing.put(controller.emergency_stop("remote emergency-stop request"))
                else:
                    raise ProtocolError("UNKNOWN_MESSAGE", f"unknown message type: {kind}")
            except (ProtocolError, TypeError, ValueError) as exc:
                if not isinstance(exc, ProtocolError):
                    exc = ProtocolError("SCHEMA_INVALID", str(exc))
                await outgoing.put(exc.event())
    except (ConnectionClosed, asyncio.TimeoutError):
        pass
    finally:
        if run is not None:
            run.connection_lost()
        await outgoing.put(None)
        try:
            await sender_task
        except ConnectionClosed:
            pass


SERVER_BOOT_ID = secrets.token_hex(16)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1", help="bind the Canada PC's ZeroTier IP")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--backend", choices=("sim", "so101"), default="sim")
    parser.add_argument("--serial-port", default="COM3")
    parser.add_argument("--robot-id", default="my_follower_arm")
    parser.add_argument("--calibration-dir", type=Path, default=BASE_DIR / "calibration")
    parser.add_argument(
        "--limit-margin-deg",
        type=float,
        default=5.0,
        help="keep validated targets this far inside the calibrated travel",
    )
    parser.add_argument(
        "--gripper-tracking-error-pct",
        type=float,
        default=None,
        help="fault when gripper deviation exceeds this percent; off by default "
        "because a grasp that stalls on an object holds a large error by design",
    )
    parser.add_argument("--token-env", default="ROBOT_SERVER_TOKEN")
    parser.add_argument("--control-hz", type=float, default=20.0)
    parser.add_argument("--telemetry-hz", type=float, default=5.0)
    parser.add_argument("--heartbeat-timeout", type=float, default=3.0)
    parser.add_argument("--serial-write-timeout", type=float, default=0.25)
    parser.add_argument("--hold-after-run", type=float, default=0.5)
    parser.add_argument("--log-level", default="INFO")
    return parser


async def run_server(args: argparse.Namespace) -> None:
    token = os.environ.get(args.token_env, "")
    if len(token) < 32:
        raise SystemExit(f"{args.token_env} must contain a random token of at least 32 characters")
    if args.control_hz <= 0 or args.telemetry_hz <= 0:
        raise SystemExit("control and telemetry rates must be positive")
    if args.telemetry_hz > args.control_hz:
        raise SystemExit("telemetry-hz cannot exceed control-hz")

    if args.gripper_tracking_error_pct is not None and args.gripper_tracking_error_pct <= 0:
        raise SystemExit("--gripper-tracking-error-pct must be positive when set")
    policy = replace(
        SafetyPolicy(), gripper_tracking_error_pct=args.gripper_tracking_error_pct
    )
    if args.backend == "sim":
        backend_factory: Callable[[], MotorBackend] = SimulatedBackend
    else:
        calibration_path = args.calibration_dir / f"{args.robot_id}.json"
        if not calibration_path.is_file():
            raise SystemExit(
                f"missing calibration file: {calibration_path}; "
                "remote automatic calibration is intentionally disabled"
            )
        policy = limits_from_calibration(calibration_path, policy, args.limit_margin_deg)
        backend_factory = lambda: SO101Backend(
            args.serial_port,
            args.robot_id,
            args.calibration_dir,
            args.serial_write_timeout,
        )
    controller = ProgramController(
        backend_factory,
        policy,
        args.control_hz,
        args.telemetry_hz,
        args.heartbeat_timeout,
        args.hold_after_run,
    )

    stop = asyncio.get_running_loop().create_future()

    def request_shutdown() -> None:
        controller.emergency_stop("server shutdown")
        if not stop.done():
            stop.set_result(None)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, request_shutdown)
        except (NotImplementedError, RuntimeError):
            pass

    async with serve(
        lambda ws: connection_handler(ws, controller, token),
        args.host,
        args.port,
        subprotocols=[PROTOCOL],
        max_size=1_048_576,
        ping_interval=20,
        ping_timeout=20,
    ):
        LOG.info("robot stream server listening on ws://%s:%d (%s backend)", args.host, args.port, args.backend)
        await stop

    # The execution thread is a daemon, so returning here would let interpreter
    # shutdown kill it mid-motion and leave the motors energized.  Block until it
    # has finished its own hold-then-disable path.
    shutdown_timeout_s = args.hold_after_run + 5.0
    if not await asyncio.to_thread(controller.wait_for_idle, shutdown_timeout_s):
        LOG.error(
            "run did not reach a terminal state within %.1fs of shutdown; "
            "motor torque may still be enabled — use the physical E-stop",
            shutdown_timeout_s,
        )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(run_server(args))
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
