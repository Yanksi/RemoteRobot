"""Versioned trajectory format and validation for remote robot execution.

The network protocol carries declarative, timestamped joint trajectories.  It
never carries Python code, shell commands, serial writes, or torque settings.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


PROTOCOL = "robot-stream.v1"
PROGRAM_FORMAT = "so101-joint-trajectory"
PROGRAM_VERSION = 1
JOINTS = (
    "shoulder_pan",
    "shoulder_lift",
    "elbow_flex",
    "wrist_flex",
    "wrist_roll",
    "gripper",
)
COORDINATE_MODES = {"absolute_deg", "relative_deg"}
RUN_MODES = {"sealed", "streaming"}


class ProtocolError(ValueError):
    """Stable, client-visible protocol or trajectory rejection."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.details = details or {}

    def event(self) -> dict[str, Any]:
        return {
            "type": "error",
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
            "details": self.details,
        }


@dataclass(frozen=True)
class ProgramHeader:
    program_id: str
    coordinate_mode: str = "absolute_deg"
    interpolation: str = "quintic_stop"
    model: str = "so101-follower"

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> "ProgramHeader":
        required = {
            "type",
            "format",
            "version",
            "program_id",
            "model",
            "joints",
            "coordinate_mode",
            "interpolation",
        }
        if set(record) != required:
            raise ProtocolError(
                "SCHEMA_INVALID",
                "header fields don't match the v1 schema",
                details={
                    "missing": sorted(required - set(record)),
                    "unexpected": sorted(set(record) - required),
                },
            )
        if record["type"] != "header":
            raise ProtocolError("SCHEMA_INVALID", "first record must be a header")
        if record["format"] != PROGRAM_FORMAT or record["version"] != PROGRAM_VERSION:
            raise ProtocolError("VERSION_UNSUPPORTED", "unsupported trajectory format/version")
        if record["model"] != "so101-follower":
            raise ProtocolError("MODEL_MISMATCH", "program isn't for an SO-101 follower")
        if tuple(record["joints"]) != JOINTS:
            raise ProtocolError(
                "JOINT_SET_MISMATCH",
                "joint order must exactly match the server's canonical joint order",
                details={"expected": list(JOINTS)},
            )
        coordinate_mode = str(record["coordinate_mode"])
        if coordinate_mode not in COORDINATE_MODES:
            raise ProtocolError("SCHEMA_INVALID", f"unsupported coordinate_mode: {coordinate_mode}")
        if record["interpolation"] != "quintic_stop":
            raise ProtocolError("SCHEMA_INVALID", "v1 only supports quintic_stop interpolation")
        program_id = str(record["program_id"])
        if not program_id or len(program_id) > 128:
            raise ProtocolError("SCHEMA_INVALID", "program_id must contain 1..128 characters")
        return cls(program_id=program_id, coordinate_mode=coordinate_mode)

    def record(self) -> dict[str, Any]:
        return {
            "type": "header",
            "format": PROGRAM_FORMAT,
            "version": PROGRAM_VERSION,
            "program_id": self.program_id,
            "model": self.model,
            "joints": list(JOINTS),
            "coordinate_mode": self.coordinate_mode,
            "interpolation": self.interpolation,
        }


@dataclass(frozen=True)
class TrajectoryPoint:
    t_us: int
    q_deg: tuple[float, ...]

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> "TrajectoryPoint":
        if set(record) != {"type", "t_us", "q_deg"} or record.get("type") != "point":
            raise ProtocolError("SCHEMA_INVALID", "point must contain type, t_us, and q_deg")
        if isinstance(record["t_us"], bool) or not isinstance(record["t_us"], int):
            raise ProtocolError("INVALID_TIME", "t_us must be an integer")
        try:
            q = tuple(float(value) for value in record["q_deg"])
        except (TypeError, ValueError) as exc:
            raise ProtocolError("SCHEMA_INVALID", "q_deg must be a numeric array") from exc
        if len(q) != len(JOINTS) or not all(math.isfinite(value) for value in q):
            raise ProtocolError(
                "SCHEMA_INVALID",
                f"q_deg must contain {len(JOINTS)} finite numbers",
            )
        return cls(record["t_us"], q)

    def record(self) -> dict[str, Any]:
        # JSON distinguishes `0` from `0.0` textually even though the protocol
        # treats both as the same joint value.  Normalize before hashing so a
        # serialize/parse round trip cannot change a chunk digest.
        return {
            "type": "point",
            "t_us": int(self.t_us),
            "q_deg": [float(value) for value in self.q_deg],
        }


@dataclass(frozen=True)
class SafetyPolicy:
    joint_min_deg: tuple[float, ...] = (-100.0,) * 6
    joint_max_deg: tuple[float, ...] = (100.0,) * 6
    max_relative_deg: float = 45.0
    max_velocity_deg_s: float = 30.0
    max_acceleration_deg_s2: float = 120.0
    min_segment_us: int = 100_000
    max_points: int = 100_000
    max_duration_us: int = 3_600_000_000
    max_chunk_points: int = 512
    max_horizon_us: int = 30_000_000
    start_buffer_us: int = 2_000_000
    low_water_us: int = 1_000_000
    start_tolerance_deg: float = 5.0
    tracking_error_deg: float = 15.0

    def validate_position(self, point: TrajectoryPoint, coordinate_mode: str) -> None:
        for index, (name, value) in enumerate(zip(JOINTS, point.q_deg, strict=True)):
            if coordinate_mode == "relative_deg":
                if abs(value) > self.max_relative_deg:
                    raise ProtocolError(
                        "LIMIT_VIOLATION",
                        f"relative {name} value {value:g} exceeds ±{self.max_relative_deg:g} deg",
                        details={"joint": name, "t_us": point.t_us},
                    )
            elif not self.joint_min_deg[index] <= value <= self.joint_max_deg[index]:
                raise ProtocolError(
                    "LIMIT_VIOLATION",
                    f"{name} value {value:g} is outside server limits",
                    details={"joint": name, "t_us": point.t_us},
                )

    def validate_segment(self, previous: TrajectoryPoint, current: TrajectoryPoint) -> None:
        dt_us = current.t_us - previous.t_us
        if dt_us < self.min_segment_us:
            raise ProtocolError(
                "INVALID_TIME",
                f"segments must be at least {self.min_segment_us} us",
                details={"previous_t_us": previous.t_us, "t_us": current.t_us},
            )
        dt_s = dt_us / 1_000_000
        # For smootherstep s(u)=6u^5-15u^4+10u^3, these are the exact peak
        # derivative multipliers. Every segment stops at both endpoints.
        for name, q0, q1 in zip(JOINTS, previous.q_deg, current.q_deg, strict=True):
            distance = abs(q1 - q0)
            peak_velocity = 1.875 * distance / dt_s
            peak_acceleration = 5.773503 * distance / (dt_s * dt_s)
            if peak_velocity > self.max_velocity_deg_s:
                raise ProtocolError(
                    "VELOCITY_LIMIT",
                    f"{name} would reach {peak_velocity:.2f} deg/s",
                    details={"joint": name, "limit": self.max_velocity_deg_s},
                )
            if peak_acceleration > self.max_acceleration_deg_s2:
                raise ProtocolError(
                    "ACCELERATION_LIMIT",
                    f"{name} would reach {peak_acceleration:.2f} deg/s²",
                    details={"joint": name, "limit": self.max_acceleration_deg_s2},
                )


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def chunk_digest(chunk_seq: int, points: Iterable[TrajectoryPoint], final: bool) -> str:
    payload = {
        "chunk_seq": chunk_seq,
        "final": final,
        "points": [point.record() for point in points],
    }
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def program_digest(header: ProgramHeader, points: Iterable[TrajectoryPoint]) -> str:
    digest = hashlib.sha256()
    digest.update((canonical_json(header.record()) + "\n").encode("utf-8"))
    for point in points:
        digest.update((canonical_json(point.record()) + "\n").encode("utf-8"))
    return digest.hexdigest()


def load_program(path: Path, policy: SafetyPolicy | None = None) -> tuple[ProgramHeader, list[TrajectoryPoint]]:
    policy = policy or SafetyPolicy()
    records: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as source:
            for line_number, line in enumerate(source, 1):
                if not line.strip() or line.lstrip().startswith("#"):
                    continue
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise ProtocolError("SCHEMA_INVALID", f"line {line_number} isn't an object")
                records.append(value)
    except json.JSONDecodeError as exc:
        raise ProtocolError("SCHEMA_INVALID", f"invalid JSON on line {exc.lineno}: {exc.msg}") from exc
    if len(records) < 2:
        raise ProtocolError("SCHEMA_INVALID", "program needs one header and at least one point")

    header = ProgramHeader.from_record(records[0])
    points = [TrajectoryPoint.from_record(record) for record in records[1:]]
    validate_points(header, points, policy, require_complete=True)
    return header, points


def validate_points(
    header: ProgramHeader,
    points: list[TrajectoryPoint],
    policy: SafetyPolicy,
    *,
    require_complete: bool,
) -> None:
    if not points:
        raise ProtocolError("SCHEMA_INVALID", "trajectory contains no points")
    if len(points) > policy.max_points:
        raise ProtocolError("RESOURCE_LIMIT", "trajectory contains too many points")
    if points[0].t_us != 0:
        raise ProtocolError("INVALID_TIME", "the first point must have t_us=0")
    if header.coordinate_mode == "relative_deg" and any(abs(v) > 1e-9 for v in points[0].q_deg):
        raise ProtocolError("START_STATE_MISMATCH", "a relative trajectory must start at all-zero offsets")
    previous: TrajectoryPoint | None = None
    for point in points:
        if point.t_us < 0 or point.t_us > policy.max_duration_us:
            raise ProtocolError("INVALID_TIME", "point time is outside the server duration limit")
        policy.validate_position(point, header.coordinate_mode)
        if previous is not None:
            policy.validate_segment(previous, point)
        previous = point
    if require_complete and len(points) < 2:
        raise ProtocolError("SCHEMA_INVALID", "a complete trajectory needs at least two points")


def interpolate_quintic(previous: TrajectoryPoint, current: TrajectoryPoint, t_us: int) -> tuple[float, ...]:
    if t_us <= previous.t_us:
        return previous.q_deg
    if t_us >= current.t_us:
        return current.q_deg
    u = (t_us - previous.t_us) / (current.t_us - previous.t_us)
    blend = u * u * u * (10.0 + u * (-15.0 + 6.0 * u))
    return tuple(a + (b - a) * blend for a, b in zip(previous.q_deg, current.q_deg, strict=True))
