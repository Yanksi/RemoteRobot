"""Strict, local-only capability registry for Remote Robot Protocol v2."""

from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
from pathlib import Path
import tomllib
from typing import Any, Iterable, Mapping

import cbor2

from .model import (
    ActuatorGroupSpec,
    AxisSpec,
    ChildRef,
    CompositeNode,
    ControlNode,
    IdentitySpec,
    PhysicalNode,
    ResolvedGroup,
    WatchdogSpec,
)


class RegistryError(ValueError):
    """A registry file is invalid or a requested capability doesn't exist."""


def canonical_cbor_sha256(value: object) -> str:
    """Return the stable digest used to pin public manifests."""

    return sha256(cbor2.dumps(value, canonical=True)).hexdigest()


def _check_keys(
    table: Mapping[str, Any],
    *,
    context: str,
    required: Iterable[str] = (),
    optional: Iterable[str] = (),
) -> None:
    required_set = set(required)
    allowed = required_set | set(optional)
    missing = required_set - table.keys()
    unknown = table.keys() - allowed
    if missing:
        raise RegistryError(f"{context}: missing keys: {', '.join(sorted(missing))}")
    if unknown:
        raise RegistryError(f"{context}: unknown keys: {', '.join(sorted(unknown))}")


def _mapping(value: Any, context: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise RegistryError(f"{context} must be a TOML table")
    return value


def _list(value: Any, context: str) -> list[Any]:
    if not isinstance(value, list):
        raise RegistryError(f"{context} must be a TOML array")
    return value


def _str(value: Any, context: str) -> str:
    if not isinstance(value, str) or not value:
        raise RegistryError(f"{context} must be a non-empty string")
    return value


def _int(value: Any, context: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RegistryError(f"{context} must be an integer")
    return value


def _number(value: Any, context: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise RegistryError(f"{context} must be a number")
    return float(value)


def _bool(value: Any, context: str) -> bool:
    if not isinstance(value, bool):
        raise RegistryError(f"{context} must be a boolean")
    return value


def _strings(value: Any, context: str) -> tuple[str, ...]:
    return tuple(_str(item, f"{context} item") for item in _list(value, context))


def _optional_strings(table: Mapping[str, Any], key: str, context: str) -> tuple[str, ...]:
    return _strings(table.get(key, []), f"{context}.{key}")


def _read_toml(path: Path) -> Mapping[str, Any]:
    try:
        with path.open("rb") as handle:
            return tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise RegistryError(f"cannot load {path}: {exc}") from exc


def _parse_axis(raw: Any, context: str) -> AxisSpec:
    table = _mapping(raw, context)
    _check_keys(
        table,
        context=context,
        required=("id", "kind", "unit", "minimum", "maximum"),
        optional=("max_velocity",),
    )
    max_velocity = table.get("max_velocity")
    try:
        return AxisSpec(
            axis_id=_str(table["id"], f"{context}.id"),
            kind=_str(table["kind"], f"{context}.kind"),  # type: ignore[arg-type]
            unit=_str(table["unit"], f"{context}.unit"),
            minimum=_number(table["minimum"], f"{context}.minimum"),
            maximum=_number(table["maximum"], f"{context}.maximum"),
            max_velocity=(
                None
                if max_velocity is None
                else _number(max_velocity, f"{context}.max_velocity")
            ),
        )
    except ValueError as exc:
        raise RegistryError(f"{context}: {exc}") from exc


def _parse_group(raw: Any, context: str) -> ActuatorGroupSpec:
    table = _mapping(raw, context)
    _check_keys(
        table,
        context=context,
        required=(
            "id",
            "control_rate_hz",
            "command_schemas",
            "telemetry_schemas",
            "axes",
        ),
    )
    axes = tuple(
        _parse_axis(axis, f"{context}.axes[{index}]")
        for index, axis in enumerate(_list(table["axes"], f"{context}.axes"))
    )
    try:
        return ActuatorGroupSpec(
            group_id=_str(table["id"], f"{context}.id"),
            axes=axes,
            command_schemas=_strings(
                table["command_schemas"], f"{context}.command_schemas"
            ),
            telemetry_schemas=_strings(
                table["telemetry_schemas"], f"{context}.telemetry_schemas"
            ),
            control_rate_hz=_number(table["control_rate_hz"], f"{context}.control_rate_hz"),
        )
    except ValueError as exc:
        raise RegistryError(f"{context}: {exc}") from exc


def _parse_watchdog(raw: Any, context: str) -> WatchdogSpec:
    table = _mapping(raw, context)
    _check_keys(
        table,
        context=context,
        required=(
            "strategy",
            "expiry_action",
            "worker_crash_safe",
            "unattended_operation_allowed",
        ),
        optional=("deadline_ms",),
    )
    deadline = table.get("deadline_ms")
    try:
        return WatchdogSpec(
            strategy=_str(table["strategy"], f"{context}.strategy"),
            deadline_ms=None if deadline is None else _int(deadline, f"{context}.deadline_ms"),
            expiry_action=_str(table["expiry_action"], f"{context}.expiry_action"),
            worker_crash_safe=_bool(
                table["worker_crash_safe"], f"{context}.worker_crash_safe"
            ),
            unattended_operation_allowed=_bool(
                table["unattended_operation_allowed"],
                f"{context}.unattended_operation_allowed",
            ),
        )
    except ValueError as exc:
        raise RegistryError(f"{context}: {exc}") from exc


_COMMON_NODE_KEYS = {
    "id",
    "kind",
    "revision",
    "exposed_as_run_root",
    "resources",
    "safety_domains",
    "safety_policies",
    "cross_collision_checked",
}


def _parse_node(path: Path) -> ControlNode:
    document = _read_toml(path)
    _check_keys(document, context=str(path), required=("node",))
    table = _mapping(document["node"], f"{path}: node")
    kind = _str(table.get("kind"), f"{path}: node.kind")
    common_required = {
        "id",
        "kind",
        "revision",
        "exposed_as_run_root",
        "cross_collision_checked",
    }
    common_optional = {"resources", "safety_domains", "safety_policies"}
    context = f"{path}: node"

    if kind == "physical":
        _check_keys(
            table,
            context=context,
            required=common_required
            | {
                "adapter",
                "adapter_revision",
                "configuration_revision",
                "groups",
                "watchdog",
            },
            optional=common_optional | {"connection"},
        )
        groups = tuple(
            _parse_group(group, f"{context}.groups[{index}]")
            for index, group in enumerate(_list(table["groups"], f"{context}.groups"))
        )
        connection = _mapping(table.get("connection", {}), f"{context}.connection")
        try:
            return PhysicalNode(
                node_id=_str(table["id"], f"{context}.id"),
                revision=_int(table["revision"], f"{context}.revision"),
                exposed_as_run_root=_bool(
                    table["exposed_as_run_root"], f"{context}.exposed_as_run_root"
                ),
                adapter=_str(table["adapter"], f"{context}.adapter"),
                adapter_revision=_int(
                    table["adapter_revision"], f"{context}.adapter_revision"
                ),
                configuration_revision=_int(
                    table["configuration_revision"],
                    f"{context}.configuration_revision",
                ),
                groups=groups,
                resources=_optional_strings(table, "resources", context),
                safety_domains=_optional_strings(table, "safety_domains", context),
                safety_policies=_optional_strings(table, "safety_policies", context),
                cross_collision_checked=_bool(
                    table["cross_collision_checked"],
                    f"{context}.cross_collision_checked",
                ),
                watchdog=_parse_watchdog(table["watchdog"], f"{context}.watchdog"),
                connection=connection,
            )
        except ValueError as exc:
            raise RegistryError(f"{context}: {exc}") from exc

    if kind == "composite":
        _check_keys(
            table,
            context=context,
            required=common_required | {"children"},
            optional=common_optional | {"max_start_skew_us"},
        )
        children: list[ChildRef] = []
        for index, raw_child in enumerate(_list(table["children"], f"{context}.children")):
            child_context = f"{context}.children[{index}]"
            child = _mapping(raw_child, child_context)
            _check_keys(
                child,
                context=child_context,
                required=("role", "node"),
                optional=("alias",),
            )
            try:
                children.append(
                    ChildRef(
                        role=_str(child["role"], f"{child_context}.role"),
                        node_id=_str(child["node"], f"{child_context}.node"),
                        alias=_bool(child.get("alias", False), f"{child_context}.alias"),
                    )
                )
            except ValueError as exc:
                raise RegistryError(f"{child_context}: {exc}") from exc
        max_skew = table.get("max_start_skew_us")
        try:
            return CompositeNode(
                node_id=_str(table["id"], f"{context}.id"),
                revision=_int(table["revision"], f"{context}.revision"),
                exposed_as_run_root=_bool(
                    table["exposed_as_run_root"], f"{context}.exposed_as_run_root"
                ),
                children=tuple(children),
                resources=_optional_strings(table, "resources", context),
                safety_domains=_optional_strings(table, "safety_domains", context),
                safety_policies=_optional_strings(table, "safety_policies", context),
                cross_collision_checked=_bool(
                    table["cross_collision_checked"],
                    f"{context}.cross_collision_checked",
                ),
                max_start_skew_us=(
                    None if max_skew is None else _int(max_skew, f"{context}.max_start_skew_us")
                ),
            )
        except ValueError as exc:
            raise RegistryError(f"{context}: {exc}") from exc

    raise RegistryError(f"{context}.kind: expected 'physical' or 'composite', got {kind!r}")


def _parse_identity(path: Path) -> IdentitySpec:
    document = _read_toml(path)
    _check_keys(document, context=str(path), required=("identity",))
    table = _mapping(document["identity"], f"{path}: identity")
    context = f"{path}: identity"
    _check_keys(
        table,
        context=context,
        required=(
            "id",
            "credential_env",
            "discover",
            "observe",
            "control",
            "emergency_stop",
        ),
    )
    try:
        return IdentitySpec(
            identity_id=_str(table["id"], f"{context}.id"),
            credential_env=_str(table["credential_env"], f"{context}.credential_env"),
            discover=_strings(table["discover"], f"{context}.discover"),
            observe=_strings(table["observe"], f"{context}.observe"),
            control=_strings(table["control"], f"{context}.control"),
            emergency_stop=_strings(
                table["emergency_stop"], f"{context}.emergency_stop"
            ),
        )
    except ValueError as exc:
        raise RegistryError(f"{context}: {exc}") from exc


class CapabilityRegistry:
    """Immutable, validated same-server registry and discovery interface."""

    def __init__(
        self,
        *,
        server_id: str,
        nodes: Mapping[str, ControlNode],
        identities: Mapping[str, IdentitySpec],
    ) -> None:
        self.server_id = server_id
        self._nodes = dict(nodes)
        self._identities = dict(identities)
        self._manifest_cache: dict[str, dict[str, object]] = {}
        self._validate()
        for node_id in sorted(self._nodes):
            self._manifest(node_id)

    @classmethod
    def load(cls, root: str | Path) -> "CapabilityRegistry":
        root_path = Path(root)
        server_path = root_path / "server.toml"
        server_document = _read_toml(server_path)
        _check_keys(server_document, context=str(server_path), required=("server",))
        server = _mapping(server_document["server"], f"{server_path}: server")
        _check_keys(
            server,
            context=f"{server_path}: server",
            required=("id", "registry_version"),
        )
        if _int(server["registry_version"], "server.registry_version") != 2:
            raise RegistryError("server.registry_version must be 2")
        server_id = _str(server["id"], "server.id")

        nodes: dict[str, ControlNode] = {}
        robots_dir = root_path / "robots"
        if not robots_dir.is_dir():
            raise RegistryError(f"missing robots directory: {robots_dir}")
        for path in sorted(robots_dir.glob("*.toml")):
            node = _parse_node(path)
            if node.node_id in nodes:
                raise RegistryError(f"duplicate node ID: {node.node_id!r}")
            nodes[node.node_id] = node
        if not nodes:
            raise RegistryError("registry contains no robot nodes")

        identities: dict[str, IdentitySpec] = {}
        identities_dir = root_path / "identities"
        if identities_dir.exists() and not identities_dir.is_dir():
            raise RegistryError(f"identities path is not a directory: {identities_dir}")
        if identities_dir.is_dir():
            for path in sorted(identities_dir.glob("*.toml")):
                identity = _parse_identity(path)
                if identity.identity_id in identities:
                    raise RegistryError(f"duplicate identity ID: {identity.identity_id!r}")
                identities[identity.identity_id] = identity

        try:
            return cls(server_id=server_id, nodes=nodes, identities=identities)
        except ValueError as exc:
            if isinstance(exc, RegistryError):
                raise
            raise RegistryError(str(exc)) from exc

    @property
    def node_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._nodes))

    @property
    def identity_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._identities))

    def node(self, node_id: str) -> ControlNode:
        try:
            return self._nodes[node_id]
        except KeyError as exc:
            raise RegistryError(f"unknown node: {node_id!r}") from exc

    def identity(self, identity_id: str) -> IdentitySpec:
        try:
            return self._identities[identity_id]
        except KeyError as exc:
            raise RegistryError(f"unknown identity: {identity_id!r}") from exc

    def manifest_hash(self, node_id: str) -> str:
        return str(self._manifest(node_id)["manifest_hash"])

    def describe(self, node_id: str, identity_id: str | None = None) -> dict[str, object]:
        if identity_id is not None and not self.identity(identity_id).can_discover(node_id):
            raise RegistryError(
                f"identity {identity_id!r} is not allowed to discover {node_id!r}"
            )
        return deepcopy(self._manifest(node_id))

    def list_robots(self, identity_id: str) -> list[dict[str, object]]:
        identity = self.identity(identity_id)
        return [
            self.describe(node_id)
            for node_id, node in sorted(self._nodes.items())
            if node.exposed_as_run_root and identity.can_discover(node_id)
        ]

    def resolve_group(self, root_id: str, role_path: str) -> ResolvedGroup:
        if not role_path or role_path.startswith(".") or role_path.endswith("."):
            raise RegistryError("role path must not be empty or have empty segments")
        segments = role_path.split(".")
        if any(not segment for segment in segments):
            raise RegistryError("role path contains an empty segment")
        current = self.node(root_id)
        index = 0
        while isinstance(current, CompositeNode):
            if index >= len(segments):
                raise RegistryError(f"role path {role_path!r} stops at a composite node")
            role = segments[index]
            try:
                child = current.child(role)
            except KeyError as exc:
                raise RegistryError(
                    f"composite {current.node_id!r} has no child role {role!r}"
                ) from exc
            current = self.node(child.node_id)
            index += 1
        if index != len(segments) - 1:
            raise RegistryError(
                f"role path {role_path!r} has extra segments after physical node {current.node_id!r}"
            )
        group_id = segments[index]
        try:
            group = current.group(group_id)
        except KeyError as exc:
            raise RegistryError(
                f"physical node {current.node_id!r} has no group {group_id!r}"
            ) from exc
        return ResolvedGroup(
            root_node_id=root_id,
            role_path=role_path,
            physical_node_id=current.node_id,
            group_id=group_id,
            group=group,
        )

    def _validate(self) -> None:
        if not self.server_id:
            raise RegistryError("server_id must not be empty")
        self._validate_graph()
        for node in self._nodes.values():
            if isinstance(node, CompositeNode):
                self._validate_composite_aliases(node)
        for identity in self._identities.values():
            self._validate_identity_scopes(identity)

    def _validate_graph(self) -> None:
        for node in self._nodes.values():
            if isinstance(node, CompositeNode):
                for child in node.children:
                    if child.node_id not in self._nodes:
                        raise RegistryError(
                            f"composite {node.node_id!r} references missing node {child.node_id!r}"
                        )

        visiting: list[str] = []
        visited: set[str] = set()

        def visit(node_id: str) -> None:
            if node_id in visiting:
                start = visiting.index(node_id)
                cycle = visiting[start:] + [node_id]
                raise RegistryError(f"control-node cycle: {' -> '.join(cycle)}")
            if node_id in visited:
                return
            visiting.append(node_id)
            node = self._nodes[node_id]
            if isinstance(node, CompositeNode):
                for child in node.children:
                    visit(child.node_id)
            visiting.pop()
            visited.add(node_id)

        for node_id in sorted(self._nodes):
            visit(node_id)

    def _leaf_paths(
        self,
        node_id: str,
        *,
        prefix: tuple[str, ...] = (),
        aliased: bool = False,
    ) -> list[tuple[tuple[str, str], str, bool]]:
        node = self._nodes[node_id]
        if isinstance(node, PhysicalNode):
            return [
                ((node.node_id, group.group_id), ".".join(prefix + (group.group_id,)), aliased)
                for group in node.groups
            ]
        leaves: list[tuple[tuple[str, str], str, bool]] = []
        for child in node.children:
            leaves.extend(
                self._leaf_paths(
                    child.node_id,
                    prefix=prefix + (child.role,),
                    aliased=aliased or child.alias,
                )
            )
        return leaves

    def _validate_composite_aliases(self, node: CompositeNode) -> None:
        by_leaf: dict[tuple[str, str], list[tuple[str, bool]]] = {}
        for canonical, path, aliased in self._leaf_paths(node.node_id):
            by_leaf.setdefault(canonical, []).append((path, aliased))
        for canonical, paths in by_leaf.items():
            if len(paths) == 1:
                continue
            primary = [path for path, aliased in paths if not aliased]
            if len(primary) != 1:
                rendered = ", ".join(
                    f"{path}{' (alias)' if aliased else ''}" for path, aliased in paths
                )
                raise RegistryError(
                    f"composite {node.node_id!r} reaches leaf {canonical[0]}.{canonical[1]} "
                    f"through ambiguous paths: {rendered}; mark every path except one alias=true"
                )

    def _validate_identity_scopes(self, identity: IdentitySpec) -> None:
        for scope_name in ("discover", "observe", "emergency_stop"):
            for value in getattr(identity, scope_name):
                if value != "*" and value not in self._nodes:
                    raise RegistryError(
                        f"identity {identity.identity_id!r} has unknown {scope_name} scope {value!r}"
                    )
        for value in identity.control:
            if value == "*":
                continue
            root_id, separator, role_path = value.partition(".")
            if root_id not in self._nodes:
                raise RegistryError(
                    f"identity {identity.identity_id!r} has unknown control root {root_id!r}"
                )
            if separator and role_path != "*":
                self.resolve_group(root_id, role_path)

    def _common_manifest(self, node: ControlNode) -> dict[str, object]:
        return {
            "protocol": "robot-stream.v2",
            "server_id": self.server_id,
            "node_id": node.node_id,
            "kind": node.kind,
            "revision": node.revision,
            "exposed_as_run_root": node.exposed_as_run_root,
            "resources": list(node.resources),
            "safety_domains": list(node.safety_domains),
            "safety": {
                "policies": list(node.safety_policies),
                "cross_collision_checked": node.cross_collision_checked,
            },
        }

    def _manifest(self, node_id: str) -> dict[str, object]:
        cached = self._manifest_cache.get(node_id)
        if cached is not None:
            return cached
        node = self.node(node_id)
        body = self._common_manifest(node)
        if isinstance(node, PhysicalNode):
            body.update(
                {
                    "adapter": {
                        "name": node.adapter,
                        "revision": node.adapter_revision,
                    },
                    "configuration_revision": node.configuration_revision,
                    "groups": [group.public_dict() for group in node.groups],
                    "watchdog": None if node.watchdog is None else node.watchdog.public_dict(),
                }
            )
        else:
            child_manifests: list[dict[str, object]] = []
            for child in node.children:
                child_manifest = self._manifest(child.node_id)
                child_manifests.append(
                    {
                        "role": child.role,
                        "node_id": child.node_id,
                        "alias": child.alias,
                        "manifest_hash": child_manifest["manifest_hash"],
                        "manifest": child_manifest,
                    }
                )
            leaf_watchdogs = [
                physical.watchdog
                for physical in self._physical_descendants(node.node_id)
                if physical.watchdog is not None
            ]
            physical_count = len(self._physical_descendants(node.node_id))
            body.update(
                {
                    "children": child_manifests,
                    "max_start_skew_us": node.max_start_skew_us,
                    "assurances": {
                        "worker_crash_safe": physical_count > 0
                        and len(leaf_watchdogs) == physical_count
                        and all(watchdog.worker_crash_safe for watchdog in leaf_watchdogs),
                        "unattended_operation_allowed": physical_count > 0
                        and len(leaf_watchdogs) == physical_count
                        and all(
                            watchdog.unattended_operation_allowed
                            for watchdog in leaf_watchdogs
                        ),
                    },
                }
            )
        result = body | {"manifest_hash": canonical_cbor_sha256(body)}
        self._manifest_cache[node_id] = result
        return result

    def _physical_descendants(self, node_id: str) -> tuple[PhysicalNode, ...]:
        result: dict[str, PhysicalNode] = {}

        def collect(current_id: str) -> None:
            node = self._nodes[current_id]
            if isinstance(node, PhysicalNode):
                result[node.node_id] = node
                return
            for child in node.children:
                collect(child.node_id)

        collect(node_id)
        return tuple(result[key] for key in sorted(result))
