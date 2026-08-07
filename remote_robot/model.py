"""Immutable capability model for Remote Robot Protocol v2.

The values in this module describe what a registered robot can do.  They do
not contain live hardware state and, except for the deliberately private
``connection`` mapping, are safe inputs to a public capability manifest.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from math import isfinite
from types import MappingProxyType
from typing import Any, Literal, Mapping, TypeAlias


AxisKind: TypeAlias = Literal["revolute", "prismatic", "normalized"]

_CANONICAL_UNITS: dict[AxisKind, str] = {
    "revolute": "rad",
    "prismatic": "m",
    "normalized": "1",
}


def _require_identifier(value: str, field_name: str) -> None:
    if not value or not value[0].isalpha():
        raise ValueError(f"{field_name} must start with a letter")
    if any(not (character.isalnum() or character in "_-") for character in value):
        raise ValueError(
            f"{field_name} may contain only letters, numbers, '_' and '-': {value!r}"
        )


def _require_unique(values: tuple[str, ...], field_name: str) -> None:
    if len(set(values)) != len(values):
        raise ValueError(f"{field_name} contains duplicates")


def freeze_value(value: Any) -> Any:
    """Recursively make an adapter-owned configuration value immutable."""

    if isinstance(value, Mapping):
        return MappingProxyType({str(key): freeze_value(item) for key, item in value.items()})
    if isinstance(value, list | tuple):
        return tuple(freeze_value(item) for item in value)
    return value


@dataclass(frozen=True, slots=True)
class AxisSpec:
    """One ordered axis in a dense actuator group."""

    axis_id: str
    kind: AxisKind
    unit: str
    minimum: float
    maximum: float
    max_velocity: float | None = None

    def __post_init__(self) -> None:
        _require_identifier(self.axis_id, "axis_id")
        if self.kind not in _CANONICAL_UNITS:
            raise ValueError(f"unsupported axis kind: {self.kind!r}")
        required_unit = _CANONICAL_UNITS[self.kind]
        if self.unit != required_unit:
            raise ValueError(
                f"{self.kind} axis {self.axis_id!r} must use canonical unit {required_unit!r}"
            )
        if not isfinite(self.minimum) or not isfinite(self.maximum):
            raise ValueError("axis bounds must be finite")
        if self.minimum >= self.maximum:
            raise ValueError("axis minimum must be less than maximum")
        if self.max_velocity is not None and (
            not isfinite(self.max_velocity) or self.max_velocity <= 0
        ):
            raise ValueError("max_velocity must be finite and positive")
        if self.kind == "normalized" and (self.minimum < 0 or self.maximum > 1):
            raise ValueError("normalized axis bounds must lie within [0, 1]")

    def public_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "axis_id": self.axis_id,
            "kind": self.kind,
            "unit": self.unit,
            "minimum": self.minimum,
            "maximum": self.maximum,
        }
        if self.max_velocity is not None:
            result["max_velocity"] = self.max_velocity
        return result


@dataclass(frozen=True, slots=True)
class ActuatorGroupSpec:
    """A fixed-order, arbitrary-dimensional dense command group."""

    group_id: str
    axes: tuple[AxisSpec, ...]
    command_schemas: tuple[str, ...]
    telemetry_schemas: tuple[str, ...]
    control_rate_hz: float

    def __post_init__(self) -> None:
        _require_identifier(self.group_id, "group_id")
        if not self.axes:
            raise ValueError(f"group {self.group_id!r} must contain at least one axis")
        _require_unique(tuple(axis.axis_id for axis in self.axes), "group axis IDs")
        if not self.command_schemas:
            raise ValueError("a group must publish at least one command schema")
        if not self.telemetry_schemas:
            raise ValueError("a group must publish at least one telemetry schema")
        _require_unique(self.command_schemas, "command_schemas")
        _require_unique(self.telemetry_schemas, "telemetry_schemas")
        for schema in self.command_schemas + self.telemetry_schemas:
            if not schema or "/" not in schema:
                raise ValueError(f"schema must be a non-empty versioned name: {schema!r}")
        if not isfinite(self.control_rate_hz) or self.control_rate_hz <= 0:
            raise ValueError("control_rate_hz must be finite and positive")

    @property
    def dimension(self) -> int:
        return len(self.axes)

    def public_dict(self) -> dict[str, object]:
        return {
            "group_id": self.group_id,
            "dimension": self.dimension,
            "axes": [axis.public_dict() for axis in self.axes],
            "command_schemas": list(self.command_schemas),
            "telemetry_schemas": list(self.telemetry_schemas),
            "control_rate_hz": self.control_rate_hz,
        }


@dataclass(frozen=True, slots=True)
class WatchdogSpec:
    strategy: str
    deadline_ms: int | None
    expiry_action: str
    worker_crash_safe: bool
    unattended_operation_allowed: bool

    def __post_init__(self) -> None:
        if not self.strategy:
            raise ValueError("watchdog strategy must not be empty")
        if self.deadline_ms is not None and self.deadline_ms <= 0:
            raise ValueError("watchdog deadline_ms must be positive")
        if not self.expiry_action:
            raise ValueError("watchdog expiry_action must not be empty")
        if self.unattended_operation_allowed and not self.worker_crash_safe:
            raise ValueError(
                "unattended operation requires an independent worker-crash-safe watchdog"
            )

    def public_dict(self) -> dict[str, object]:
        return {
            "strategy": self.strategy,
            "deadline_ms": self.deadline_ms,
            "expiry_action": self.expiry_action,
            "worker_crash_safe": self.worker_crash_safe,
            "unattended_operation_allowed": self.unattended_operation_allowed,
        }


@dataclass(frozen=True, slots=True)
class ChildRef:
    role: str
    node_id: str
    alias: bool = False

    def __post_init__(self) -> None:
        _require_identifier(self.role, "child role")
        _require_identifier(self.node_id, "child node_id")


@dataclass(frozen=True, slots=True)
class PhysicalNode:
    node_id: str
    revision: int
    exposed_as_run_root: bool
    adapter: str
    adapter_revision: int
    configuration_revision: int
    groups: tuple[ActuatorGroupSpec, ...]
    resources: tuple[str, ...] = ()
    safety_domains: tuple[str, ...] = ()
    safety_policies: tuple[str, ...] = ()
    cross_collision_checked: bool = False
    watchdog: WatchdogSpec | None = None
    connection: Mapping[str, Any] = field(
        default_factory=lambda: MappingProxyType({}), repr=False, compare=False
    )

    def __post_init__(self) -> None:
        _require_identifier(self.node_id, "node_id")
        if (
            self.revision <= 0
            or self.adapter_revision <= 0
            or self.configuration_revision <= 0
        ):
            raise ValueError(
                "node, adapter, and configuration revisions must be positive"
            )
        if not self.adapter:
            raise ValueError("adapter must not be empty")
        if not self.groups:
            raise ValueError("physical node must publish at least one group")
        _require_unique(tuple(group.group_id for group in self.groups), "group IDs")
        _require_unique(self.resources, "resources")
        _require_unique(self.safety_domains, "safety_domains")
        _require_unique(self.safety_policies, "safety_policies")
        object.__setattr__(self, "connection", freeze_value(self.connection))

    @property
    def kind(self) -> Literal["physical"]:
        return "physical"

    def group(self, group_id: str) -> ActuatorGroupSpec:
        for group in self.groups:
            if group.group_id == group_id:
                return group
        raise KeyError(group_id)


@dataclass(frozen=True, slots=True)
class CompositeNode:
    node_id: str
    revision: int
    exposed_as_run_root: bool
    children: tuple[ChildRef, ...]
    resources: tuple[str, ...] = ()
    safety_domains: tuple[str, ...] = ()
    safety_policies: tuple[str, ...] = ()
    cross_collision_checked: bool = False
    max_start_skew_us: int | None = None

    def __post_init__(self) -> None:
        _require_identifier(self.node_id, "node_id")
        if self.revision <= 0:
            raise ValueError("node revision must be positive")
        if not self.children:
            raise ValueError("composite node must contain at least one child")
        _require_unique(tuple(child.role for child in self.children), "child roles")
        _require_unique(self.resources, "resources")
        _require_unique(self.safety_domains, "safety_domains")
        _require_unique(self.safety_policies, "safety_policies")
        if self.max_start_skew_us is not None and self.max_start_skew_us <= 0:
            raise ValueError("max_start_skew_us must be positive")

    @property
    def kind(self) -> Literal["composite"]:
        return "composite"

    def child(self, role: str) -> ChildRef:
        for child in self.children:
            if child.role == role:
                return child
        raise KeyError(role)


ControlNode: TypeAlias = PhysicalNode | CompositeNode


@dataclass(frozen=True, slots=True)
class ResolvedGroup:
    root_node_id: str
    role_path: str
    physical_node_id: str
    group_id: str
    group: ActuatorGroupSpec

    @property
    def canonical_id(self) -> str:
        return f"{self.physical_node_id}.{self.group_id}"


@dataclass(frozen=True, slots=True)
class IdentitySpec:
    identity_id: str
    credential_env: str
    discover: tuple[str, ...]
    observe: tuple[str, ...]
    control: tuple[str, ...]
    emergency_stop: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_identifier(self.identity_id, "identity_id")
        if not self.credential_env:
            raise ValueError("credential_env must not be empty")
        _require_unique(self.discover, "discover scopes")
        _require_unique(self.observe, "observe scopes")
        _require_unique(self.control, "control scopes")
        _require_unique(self.emergency_stop, "emergency_stop scopes")

    def can_discover(self, node_id: str) -> bool:
        return "*" in self.discover or node_id in self.discover

    def can_observe(self, node_id: str) -> bool:
        return "*" in self.observe or node_id in self.observe

    def can_emergency_stop(self, node_id: str) -> bool:
        return "*" in self.emergency_stop or node_id in self.emergency_stop

    def can_control(self, node_id: str, role_path: str | None = None) -> bool:
        if "*" in self.control or node_id in self.control:
            return True
        if f"{node_id}.*" in self.control:
            return True
        return role_path is not None and f"{node_id}.{role_path}" in self.control
