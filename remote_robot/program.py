"""Compile human-readable v2 templates into deterministic robot programs.

The external interface is deliberately small: load a template, compile it
against one immutable registry manifest, then serialize/inspect the resulting
artifact. Role resolution, dense-axis validation, units, phase digests, and
canonical encoding remain inside this Module.
"""

from __future__ import annotations

import hashlib
import json
import math
import tomllib
from dataclasses import asdict, dataclass, is_dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol

import cbor2


TEMPLATE_FORMAT = "robot-program-template"
COMPILED_FORMAT = "robot-program"
PROGRAM_VERSION = 2
POSITION_SCHEMA = "joint_position_trajectory/v1"
NORMALIZED_POSITION_SCHEMA = "normalized_position_trajectory/v1"
POSITION_SCHEMAS = {POSITION_SCHEMA, NORMALIZED_POSITION_SCHEMA}
REFERENCES = {"absolute", "phase_start_measured"}
DEFAULT_COMPLETION = {"schema": "all_sequences_finished/v1"}


class ProgramError(ValueError):
    """Stable compilation/decoding rejection with a machine-readable code."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        path: str = "",
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.path = path
        self.details = dict(details or {})


class RegistryView(Protocol):
    """The narrow registry seam used by compilation."""

    server_id: str

    def describe(self, node_id: str) -> Mapping[str, Any]: ...

    def manifest_hash(self, node_id: str) -> str: ...

    def resolve_group(self, root_id: str, role_path: str) -> Any: ...


def canonical_cbor(value: Any) -> bytes:
    return cbor2.dumps(value, canonical=True)


def digest_value(value: Any) -> str:
    return hashlib.sha256(canonical_cbor(value)).hexdigest()


@dataclass(frozen=True)
class CompiledProgram:
    """Immutable, manifest-bound executable artifact."""

    body: Mapping[str, Any]
    sha256: str

    @property
    def server_id(self) -> str:
        return str(self.body["server_id"])

    @property
    def root_id(self) -> str:
        return str(self.body["root_id"])

    @property
    def manifest_hash(self) -> str:
        return str(self.body["manifest_hash"])

    @property
    def outline(self) -> Mapping[str, Any]:
        return self.body["outline"]

    @property
    def phases(self) -> tuple[Mapping[str, Any], ...]:
        return tuple(self.body["phases"])

    def to_bytes(self) -> bytes:
        return canonical_cbor({"body": dict(self.body), "sha256": self.sha256})

    def write(self, path: Path) -> None:
        path.write_bytes(self.to_bytes())

    def inspect(self) -> dict[str, Any]:
        return {
            "format": self.body["format"],
            "version": self.body["version"],
            "server_id": self.server_id,
            "root_id": self.root_id,
            "manifest_hash": self.manifest_hash,
            "artifact_sha256": self.sha256,
            "outline_sha256": self.body["outline_sha256"],
            "phase_count": len(self.phases),
            "phases": [
                {
                    "phase_id": phase["phase_id"],
                    "duration_us": phase["duration_us"],
                    "sequence_count": len(phase["sequences"]),
                    "sha256": phase["sha256"],
                }
                for phase in self.phases
            ],
        }

    @classmethod
    def from_bytes(cls, data: bytes) -> "CompiledProgram":
        try:
            value = cbor2.loads(data)
        except Exception as exc:
            raise ProgramError("INVALID_CBOR", f"cannot decode compiled program: {exc}") from exc
        if data != canonical_cbor(value):
            raise ProgramError(
                "NONCANONICAL_ARTIFACT",
                "compiled program must be one canonical CBOR value with no trailing data",
            )
        if not isinstance(value, dict) or set(value) != {"body", "sha256"}:
            raise ProgramError("INVALID_ARTIFACT", "compiled program envelope is invalid")
        body = value["body"]
        supplied = value["sha256"]
        if not isinstance(body, dict) or not isinstance(supplied, str):
            raise ProgramError("INVALID_ARTIFACT", "compiled program body/digest types are invalid")
        if body.get("format") != COMPILED_FORMAT or body.get("version") != PROGRAM_VERSION:
            raise ProgramError("VERSION_UNSUPPORTED", "compiled program version isn't supported")
        actual = digest_value(body)
        if supplied != actual:
            raise ProgramError(
                "HASH_MISMATCH",
                "compiled program digest doesn't match its canonical body",
                details={"expected": supplied, "actual": actual},
            )
        _validate_compiled_internal_hashes(body)
        return cls(body=body, sha256=supplied)

    @classmethod
    def read(cls, path: Path) -> "CompiledProgram":
        return cls.from_bytes(path.read_bytes())


class ProgramCompiler:
    """Deep compilation Module; callers supply only a template and root."""

    def __init__(self, registry: RegistryView) -> None:
        self.registry = registry

    def compile(self, template: Mapping[str, Any]) -> CompiledProgram:
        _require_exact_fields(
            template,
            required={"format", "version", "root_id", "phases"},
            optional={"program_id"},
            path="template",
        )
        if template["format"] != TEMPLATE_FORMAT or template["version"] != PROGRAM_VERSION:
            raise ProgramError("VERSION_UNSUPPORTED", "template format/version isn't supported")
        root_id = _short_id(template["root_id"], "template.root_id")
        manifest = self.registry.describe(root_id)
        if not isinstance(manifest, Mapping):
            raise ProgramError(
                "INVALID_MANIFEST", "registry returned an invalid root manifest"
            )
        if manifest.get("exposed_as_run_root") is not True:
            raise ProgramError(
                "ROOT_NOT_EXPOSED",
                f"node {root_id!r} is not exposed as a selectable run root",
                path="template.root_id",
            )
        manifest_hash = self.registry.manifest_hash(root_id)
        raw_phases = template["phases"]
        if not isinstance(raw_phases, list) or not raw_phases:
            raise ProgramError("SCHEMA_INVALID", "template.phases must be a non-empty array")

        phase_payloads: list[dict[str, Any]] = []
        outline_phases: list[dict[str, Any]] = []
        seen_phase_ids: set[str] = set()
        for phase_index, raw_phase in enumerate(raw_phases):
            phase_path = f"template.phases[{phase_index}]"
            phase, outline = self._compile_phase(root_id, raw_phase, phase_path)
            phase_id = phase["phase_id"]
            if phase_id in seen_phase_ids:
                raise ProgramError(
                    "DUPLICATE_PHASE",
                    f"phase_id {phase_id!r} is duplicated",
                    path=f"{phase_path}.phase_id",
                )
            seen_phase_ids.add(phase_id)
            phase_payloads.append(phase)
            outline_phases.append(outline)

        outline = {
            "phase_count": len(outline_phases),
            "phases": outline_phases,
        }
        body = {
            "format": COMPILED_FORMAT,
            "version": PROGRAM_VERSION,
            "program_id": _short_id(
                template.get("program_id", "program"), "template.program_id"
            ),
            "server_id": self.registry.server_id,
            "root_id": root_id,
            "manifest_hash": manifest_hash,
            "outline": outline,
            "outline_sha256": digest_value(outline),
            "phases": phase_payloads,
        }
        return CompiledProgram(body=body, sha256=digest_value(body))

    def compile_file(self, template_path: Path, output_path: Path | None = None) -> CompiledProgram:
        template = load_template(template_path)
        compiled = self.compile(template)
        if output_path is not None:
            compiled.write(output_path)
        return compiled

    def validate_for_current_registry(self, program: CompiledProgram) -> None:
        """Validate an untrusted artifact against this exact registry.

        Artifact digests detect accidental corruption; they are not signatures.
        A remote caller can recompute every digest after changing the CBOR, so
        execution validation must also rebuild the canonical compiler output.
        This keeps role resolution, schemas, dense dimensions, units, limits,
        and outline relationships behind the compiler seam.
        """

        if program.sha256 != digest_value(program.body):
            raise ProgramError("HASH_MISMATCH", "compiled program body digest doesn't match")
        _validate_compiled_internal_hashes(program.body)
        if program.server_id != self.registry.server_id:
            raise ProgramError("SERVER_MISMATCH", "program was compiled for a different server")
        current = self.registry.manifest_hash(program.root_id)
        if current != program.manifest_hash:
            raise ProgramError(
                "MANIFEST_MISMATCH",
                "program was compiled against a stale robot manifest",
                details={"compiled": program.manifest_hash, "current": current},
            )
        template = _template_from_compiled(program.body)
        rebuilt = self.compile(template)
        if rebuilt.body != program.body or rebuilt.sha256 != program.sha256:
            raise ProgramError(
                "NONCANONICAL_ARTIFACT",
                "compiled program isn't the canonical output for its pinned registry",
            )

    def _compile_phase(
        self,
        root_id: str,
        raw_phase: Any,
        path: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if not isinstance(raw_phase, Mapping):
            raise ProgramError("SCHEMA_INVALID", "phase must be an object", path=path)
        _require_exact_fields(
            raw_phase,
            required={"phase_id", "sequences"},
            optional={"completion", "transitions"},
            path=path,
        )
        phase_id = _short_id(raw_phase["phase_id"], f"{path}.phase_id")
        completion = raw_phase.get("completion", DEFAULT_COMPLETION)
        if completion != DEFAULT_COMPLETION:
            raise ProgramError(
                "CAPABILITY_MISSING",
                "v2 milestone 1 only implements all_sequences_finished/v1",
                path=f"{path}.completion",
            )
        transitions = raw_phase.get("transitions", [])
        if not isinstance(transitions, list):
            raise ProgramError("SCHEMA_INVALID", "transitions must be an array", path=path)
        if transitions:
            raise ProgramError(
                "CAPABILITY_MISSING",
                "mode transitions are reserved by the v2 format but not implemented",
                path=f"{path}.transitions",
            )
        # Preserve the typed field in the compiled format so a later milestone
        # can add resolved transition edges without changing the phase shape.
        compiled_transitions = [
            _compile_transition(item, f"{path}.transitions[{index}]")
            for index, item in enumerate(transitions)
        ]

        raw_sequences = raw_phase["sequences"]
        if not isinstance(raw_sequences, list) or not raw_sequences:
            raise ProgramError("SCHEMA_INVALID", "phase sequences must be non-empty", path=path)
        sequences: list[dict[str, Any]] = []
        outline_sequences: list[dict[str, Any]] = []
        canonical_targets: set[tuple[str, str]] = set()
        duration_us = 0
        for sequence_index, raw_sequence in enumerate(raw_sequences):
            sequence_path = f"{path}.sequences[{sequence_index}]"
            sequence = self._compile_sequence(root_id, raw_sequence, sequence_path)
            target = (
                str(sequence["resolved_target"]["physical_node_id"]),
                str(sequence["resolved_target"]["group_id"]),
            )
            if target in canonical_targets:
                raise ProgramError(
                    "DUPLICATE_GROUP_SEQUENCE",
                    "a phase may contain at most one sequence per resolved group",
                    path=sequence_path,
                    details={"physical_node_id": target[0], "group_id": target[1]},
                )
            canonical_targets.add(target)
            sequence_sha = digest_value(sequence)
            sequence["sha256"] = sequence_sha
            duration_us = max(duration_us, int(sequence["duration_us"]))
            sequences.append(sequence)
            outline_sequences.append(
                {
                    "role_path": sequence["role_path"],
                    "resolved_target": sequence["resolved_target"],
                    "command_schema": sequence["command_schema"],
                    "reference": sequence["reference"],
                    "duration_us": sequence["duration_us"],
                    "sha256": sequence_sha,
                }
            )

        phase_without_hash = {
            "phase_id": phase_id,
            "duration_us": duration_us,
            "completion": dict(completion),
            "transitions": compiled_transitions,
            "sequences": sequences,
        }
        phase_sha = digest_value(phase_without_hash)
        phase = {**phase_without_hash, "sha256": phase_sha}
        outline = {
            "phase_id": phase_id,
            "duration_us": duration_us,
            "participants": [item["resolved_target"] for item in outline_sequences],
            "sequences": outline_sequences,
            "transitions": compiled_transitions,
            "payload_sha256": phase_sha,
        }
        return phase, outline

    def _compile_sequence(
        self,
        root_id: str,
        raw_sequence: Any,
        path: str,
    ) -> dict[str, Any]:
        if not isinstance(raw_sequence, Mapping):
            raise ProgramError("SCHEMA_INVALID", "sequence must be an object", path=path)
        _require_exact_fields(
            raw_sequence,
            required={"group", "command_schema", "reference", "samples"},
            optional=set(),
            path=path,
        )
        role_path = _role_path(raw_sequence["group"], f"{path}.group")
        resolved = self.registry.resolve_group(root_id, role_path)
        resolved_dict, group_dict = _normalize_resolution(resolved)
        command_schema = str(raw_sequence["command_schema"])
        supported = set(group_dict.get("command_schemas", []))
        if command_schema not in supported:
            raise ProgramError(
                "CAPABILITY_MISSING",
                f"group {role_path!r} doesn't support {command_schema!r}",
                path=f"{path}.command_schema",
                details={"supported": sorted(supported)},
            )
        if command_schema not in POSITION_SCHEMAS:
            raise ProgramError(
                "CAPABILITY_MISSING",
                f"compiler doesn't implement command schema {command_schema!r}",
                path=f"{path}.command_schema",
            )
        reference = str(raw_sequence["reference"])
        if reference not in REFERENCES:
            raise ProgramError("SCHEMA_INVALID", f"unsupported reference {reference!r}", path=path)

        axes = list(group_dict.get("axes", []))
        if not axes:
            raise ProgramError("INVALID_MANIFEST", "resolved group has no axes", path=path)
        samples = _compile_dense_samples(
            raw_sequence["samples"], axes, reference, command_schema, path
        )
        return {
            "role_path": role_path,
            "resolved_target": resolved_dict,
            "command_schema": command_schema,
            "reference": reference,
            "axes": [
                {
                    "name": str(axis["name"]),
                    "kind": str(axis["kind"]),
                    "unit": str(axis["unit"]),
                }
                for axis in axes
            ],
            "duration_us": samples[-1]["t_us"],
            "samples": samples,
        }


def _template_from_compiled(body: Mapping[str, Any]) -> dict[str, Any]:
    """Project an untrusted compiled body back onto the public template shape."""

    try:
        raw_phases = body["phases"]
        if not isinstance(raw_phases, list):
            raise TypeError("phases must be an array")
        phases: list[dict[str, Any]] = []
        for phase in raw_phases:
            if not isinstance(phase, Mapping):
                raise TypeError("phase must be an object")
            raw_sequences = phase["sequences"]
            if not isinstance(raw_sequences, list):
                raise TypeError("phase sequences must be an array")
            sequences: list[dict[str, Any]] = []
            for sequence in raw_sequences:
                if not isinstance(sequence, Mapping):
                    raise TypeError("sequence must be an object")
                sequences.append(
                    {
                        "group": sequence["role_path"],
                        "command_schema": sequence["command_schema"],
                        "reference": sequence["reference"],
                        "samples": sequence["samples"],
                    }
                )
            phases.append(
                {
                    "phase_id": phase["phase_id"],
                    "completion": phase["completion"],
                    "transitions": phase["transitions"],
                    "sequences": sequences,
                }
            )
        return {
            "format": TEMPLATE_FORMAT,
            "version": PROGRAM_VERSION,
            "program_id": body["program_id"],
            "root_id": body["root_id"],
            "phases": phases,
        }
    except (KeyError, TypeError) as exc:
        raise ProgramError(
            "INVALID_ARTIFACT", f"compiled program cannot be reconstructed: {exc}"
        ) from exc


def load_template(path: Path) -> Mapping[str, Any]:
    suffix = path.suffix.lower()
    try:
        if suffix == ".toml":
            value = tomllib.loads(path.read_text(encoding="utf-8"))
        elif suffix in {".json", ".jsonl"}:
            value = json.loads(path.read_text(encoding="utf-8"))
        else:
            raise ProgramError(
                "UNSUPPORTED_TEMPLATE",
                "template must use .json, .jsonl, or .toml",
            )
    except (json.JSONDecodeError, tomllib.TOMLDecodeError) as exc:
        raise ProgramError("SCHEMA_INVALID", f"cannot parse template: {exc}") from exc
    if not isinstance(value, Mapping):
        raise ProgramError("SCHEMA_INVALID", "template root must be an object")
    return value


def _compile_dense_samples(
    raw_samples: Any,
    axes: list[Mapping[str, Any]],
    reference: str,
    command_schema: str,
    path: str,
) -> list[dict[str, Any]]:
    if not isinstance(raw_samples, list) or len(raw_samples) < 2:
        raise ProgramError("SCHEMA_INVALID", "sequence needs at least two samples", path=path)
    result: list[dict[str, Any]] = []
    previous_t: int | None = None
    previous_values: list[float] | None = None
    for index, raw_sample in enumerate(raw_samples):
        sample_path = f"{path}.samples[{index}]"
        if not isinstance(raw_sample, Mapping):
            raise ProgramError("SCHEMA_INVALID", "sample must be an object", path=sample_path)
        _require_exact_fields(
            raw_sample,
            required={"t_us", "values"},
            optional=set(),
            path=sample_path,
        )
        t_us = raw_sample["t_us"]
        if isinstance(t_us, bool) or not isinstance(t_us, int) or t_us < 0:
            raise ProgramError("INVALID_TIME", "t_us must be a non-negative integer", path=sample_path)
        if previous_t is None and t_us != 0:
            raise ProgramError("INVALID_TIME", "first sample must start at t_us=0", path=sample_path)
        if previous_t is not None and t_us <= previous_t:
            raise ProgramError("INVALID_TIME", "sample times must strictly increase", path=sample_path)
        raw_values = raw_sample["values"]
        if not isinstance(raw_values, list) or len(raw_values) != len(axes):
            raise ProgramError(
                "DENSE_AXIS_MISMATCH",
                f"sample must provide exactly {len(axes)} axis values",
                path=f"{sample_path}.values",
            )
        if any(isinstance(value, bool) for value in raw_values):
            raise ProgramError(
                "SCHEMA_INVALID",
                "axis values must be numbers, not booleans",
                path=f"{sample_path}.values",
            )
        try:
            values = [float(value) for value in raw_values]
        except (OverflowError, TypeError, ValueError) as exc:
            raise ProgramError("SCHEMA_INVALID", "axis values must be numeric", path=sample_path) from exc
        if not all(math.isfinite(value) for value in values):
            raise ProgramError("NON_FINITE_VALUE", "axis values must be finite", path=sample_path)
        if reference == "phase_start_measured" and index == 0 and any(
            abs(value) > 1e-12 for value in values
        ):
            raise ProgramError(
                "START_STATE_MISMATCH",
                "relative sequence must begin with an all-zero dense sample",
                path=sample_path,
            )
        if reference == "absolute":
            for axis, value in zip(axes, values, strict=True):
                lower = axis.get("lower")
                upper = axis.get("upper")
                if lower is not None and value < float(lower) or upper is not None and value > float(upper):
                    raise ProgramError(
                        "LIMIT_VIOLATION",
                        f"axis {axis['name']!r} value {value:g} is outside the manifest limits",
                        path=sample_path,
                    )
        if command_schema == NORMALIZED_POSITION_SCHEMA:
            invalid_normalized = (
                any(value < 0.0 or value > 1.0 for value in values)
                if reference == "absolute"
                else any(value < -1.0 or value > 1.0 for value in values)
            )
            if invalid_normalized:
                expected = "[0, 1]" if reference == "absolute" else "[-1, 1] offsets"
                raise ProgramError(
                    "LIMIT_VIOLATION",
                    f"normalized position values must be in {expected}",
                    path=sample_path,
                )
        if previous_t is not None and previous_values is not None:
            _validate_segment_rates(
                axes,
                previous_values,
                values,
                t_us - previous_t,
                sample_path,
            )
        result.append({"t_us": t_us, "values": values})
        previous_t = t_us
        previous_values = values
    return result


def _validate_segment_rates(
    axes: list[Mapping[str, Any]],
    before: list[float],
    after: list[float],
    duration_us: int,
    path: str,
) -> None:
    duration_s = duration_us / 1_000_000
    for axis, start, end in zip(axes, before, after, strict=True):
        distance = abs(end - start)
        max_velocity = axis.get("max_velocity")
        max_acceleration = axis.get("max_acceleration")
        peak_velocity = 1.875 * distance / duration_s
        peak_acceleration = 5.773503 * distance / (duration_s * duration_s)
        if max_velocity is not None and peak_velocity > float(max_velocity):
            raise ProgramError(
                "VELOCITY_LIMIT",
                f"axis {axis['name']!r} would reach {peak_velocity:g} {axis['unit']}/s",
                path=path,
            )
        if max_acceleration is not None and peak_acceleration > float(max_acceleration):
            raise ProgramError(
                "ACCELERATION_LIMIT",
                f"axis {axis['name']!r} would reach {peak_acceleration:g} {axis['unit']}/s²",
                path=path,
            )


def _compile_transition(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ProgramError("SCHEMA_INVALID", "transition must be an object", path=path)
    _require_exact_fields(
        value,
        required={"group", "from_schema", "to_schema"},
        optional=set(),
        path=path,
    )
    return {
        "group": _role_path(value["group"], f"{path}.group"),
        "from_schema": _schema_id(value["from_schema"], f"{path}.from_schema"),
        "to_schema": _schema_id(value["to_schema"], f"{path}.to_schema"),
    }


def _normalize_resolution(resolved: Any) -> tuple[dict[str, str], dict[str, Any]]:
    value = _plain(resolved)
    if not isinstance(value, Mapping):
        raise ProgramError("INVALID_MANIFEST", "registry returned an invalid group resolution")
    physical_node_id = value.get("physical_node_id", value.get("node_id"))
    group_id = value.get("group_id")
    group = value.get("group", value.get("group_spec"))
    group_value = _plain(group)
    if not isinstance(physical_node_id, str) or not isinstance(group_id, str):
        raise ProgramError("INVALID_MANIFEST", "group resolution is missing canonical IDs")
    if not isinstance(group_value, Mapping):
        raise ProgramError("INVALID_MANIFEST", "group resolution is missing a group manifest")
    normalized_group = dict(group_value)
    normalized_axes: list[dict[str, Any]] = []
    for axis in normalized_group.get("axes", []):
        axis_value = _plain(axis)
        if not isinstance(axis_value, Mapping):
            raise ProgramError("INVALID_MANIFEST", "group axis record is invalid")
        normalized_axis = dict(axis_value)
        # Registry model names remain domain-oriented; the compiler's internal
        # form uses concise mathematical names. Keep that translation private
        # to this Module rather than leaking either representation to callers.
        if "name" not in normalized_axis and "axis_id" in normalized_axis:
            normalized_axis["name"] = normalized_axis["axis_id"]
        if "lower" not in normalized_axis and "minimum" in normalized_axis:
            normalized_axis["lower"] = normalized_axis["minimum"]
        if "upper" not in normalized_axis and "maximum" in normalized_axis:
            normalized_axis["upper"] = normalized_axis["maximum"]
        normalized_axes.append(normalized_axis)
    normalized_group["axes"] = normalized_axes
    return (
        {"physical_node_id": physical_node_id, "group_id": group_id},
        normalized_group,
    )


def _plain(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def _validate_compiled_internal_hashes(body: Mapping[str, Any]) -> None:
    outline = body.get("outline")
    if not isinstance(outline, Mapping) or body.get("outline_sha256") != digest_value(outline):
        raise ProgramError("HASH_MISMATCH", "outline digest doesn't match")
    phases = body.get("phases")
    if not isinstance(phases, list):
        raise ProgramError("INVALID_ARTIFACT", "compiled program phases are invalid")
    outline_phases = outline.get("phases")
    if not isinstance(outline_phases, list) or len(outline_phases) != len(phases):
        raise ProgramError("INVALID_ARTIFACT", "outline and phase payload counts differ")
    for index, (phase, phase_outline) in enumerate(zip(phases, outline_phases, strict=True)):
        if not isinstance(phase, Mapping) or not isinstance(phase_outline, Mapping):
            raise ProgramError("INVALID_ARTIFACT", "phase record is invalid")
        without_hash = {key: value for key, value in phase.items() if key != "sha256"}
        actual = digest_value(without_hash)
        if phase.get("sha256") != actual or phase_outline.get("payload_sha256") != actual:
            raise ProgramError("HASH_MISMATCH", f"phase {index} digest doesn't match")
        for sequence in phase.get("sequences", []):
            without_sequence_hash = {
                key: value for key, value in sequence.items() if key != "sha256"
            }
            if sequence.get("sha256") != digest_value(without_sequence_hash):
                raise ProgramError("HASH_MISMATCH", f"phase {index} sequence digest doesn't match")


def _require_exact_fields(
    value: Mapping[str, Any],
    *,
    required: set[str],
    optional: set[str],
    path: str,
) -> None:
    keys = set(value)
    missing = required - keys
    unexpected = keys - required - optional
    if missing or unexpected:
        raise ProgramError(
            "SCHEMA_INVALID",
            "object fields don't match the v2 schema",
            path=path,
            details={"missing": sorted(missing), "unexpected": sorted(unexpected)},
        )


def _short_id(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 128:
        raise ProgramError("SCHEMA_INVALID", "ID must contain 1..128 characters", path=path)
    if any(character.isspace() for character in value):
        raise ProgramError("SCHEMA_INVALID", "ID cannot contain whitespace", path=path)
    return value


def _role_path(value: Any, path: str) -> str:
    result = _short_id(value, path)
    if result.startswith(".") or result.endswith(".") or ".." in result:
        raise ProgramError("SCHEMA_INVALID", "role path contains an empty segment", path=path)
    return result


def _schema_id(value: Any, path: str) -> str:
    result = _short_id(value, path)
    if "/" not in result:
        raise ProgramError("SCHEMA_INVALID", "schema ID must end with /version", path=path)
    return result
