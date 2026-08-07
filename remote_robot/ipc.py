"""Canonical CBOR framing for the local robot-worker protocol.

The wire format is deliberately small and language-neutral::

    uint32 payload_length (network byte order)
    canonical-CBOR payload

Only ordinary data values are admitted.  In particular, no Python object
serialization, CBOR tags, non-finite floats, or non-string mapping keys cross
the process boundary.
"""

from __future__ import annotations

import asyncio
import math
import struct
from collections.abc import Mapping
from typing import Any

import cbor2


DEFAULT_MAX_FRAME_BYTES = 4 * 1024 * 1024
MAX_VALUE_DEPTH = 32
PROTOCOL_VERSION = 1
PROTOCOL_NAME = "remote-robot-worker"
_HEADER = struct.Struct(">I")


class IPCError(Exception):
    """Base error for a malformed or failed IPC frame."""


class FrameTooLargeError(IPCError):
    """A frame exceeded the configured transport limit."""


class TruncatedFrameError(IPCError):
    """The stream ended part-way through a frame."""


class IPCSchemaError(IPCError):
    """A decoded value does not satisfy the worker wire schema."""


class NonCanonicalFrameError(IPCError):
    """The payload is valid CBOR but is not its canonical representation."""


def _validate_value(value: Any, *, path: str = "$", depth: int = 0) -> None:
    if depth > MAX_VALUE_DEPTH:
        raise IPCSchemaError(f"{path}: value nesting exceeds {MAX_VALUE_DEPTH}")
    if value is None or isinstance(value, (str, bytes, bool)):
        return
    if isinstance(value, int):
        if not -(2**63) <= value < 2**63:
            raise IPCSchemaError(f"{path}: integer is outside signed 64-bit range")
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise IPCSchemaError(f"{path}: floats must be finite")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_value(item, path=f"{path}[{index}]", depth=depth + 1)
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise IPCSchemaError(f"{path}: mapping keys must be strings")
            _validate_value(item, path=f"{path}.{key}", depth=depth + 1)
        return
    raise IPCSchemaError(f"{path}: unsupported value type {type(value).__name__}")


def canonical_payload(message: Mapping[str, Any]) -> bytes:
    """Return the unique canonical-CBOR representation of a message."""

    if not isinstance(message, Mapping):
        raise IPCSchemaError("$: IPC messages must be mappings")
    plain = dict(message)
    _validate_value(plain)
    return cbor2.dumps(plain, canonical=True)


def encode_frame(
    message: Mapping[str, Any], *, max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES
) -> bytes:
    """Encode one length-prefixed canonical-CBOR frame."""

    if not isinstance(max_frame_bytes, int) or isinstance(max_frame_bytes, bool):
        raise ValueError("max_frame_bytes must be an integer")
    if max_frame_bytes <= 0 or max_frame_bytes > 0xFFFFFFFF:
        raise ValueError("max_frame_bytes must be between 1 and 2^32-1")
    payload = canonical_payload(message)
    if not payload:
        raise IPCSchemaError("empty CBOR payload")
    if len(payload) > max_frame_bytes:
        raise FrameTooLargeError(
            f"frame payload is {len(payload)} bytes; limit is {max_frame_bytes}"
        )
    return _HEADER.pack(len(payload)) + payload


def decode_payload(
    payload: bytes, *, max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES
) -> dict[str, Any]:
    """Decode and validate a complete canonical-CBOR payload."""

    if not payload:
        raise IPCSchemaError("empty CBOR payload")
    if len(payload) > max_frame_bytes:
        raise FrameTooLargeError(
            f"frame payload is {len(payload)} bytes; limit is {max_frame_bytes}"
        )
    try:
        value = cbor2.loads(payload)
    except Exception as exc:  # cbor2 exposes several decoder exception types.
        raise IPCSchemaError(f"invalid CBOR payload: {exc}") from exc
    if not isinstance(value, dict):
        raise IPCSchemaError("$: IPC messages must decode to a mapping")
    _validate_value(value)
    canonical = cbor2.dumps(value, canonical=True)
    if canonical != payload:
        raise NonCanonicalFrameError("CBOR payload is not canonical")
    return value


def decode_frame(
    frame: bytes, *, max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES
) -> dict[str, Any]:
    """Decode exactly one complete frame held in memory."""

    if len(frame) < _HEADER.size:
        raise TruncatedFrameError("frame header is truncated")
    (length,) = _HEADER.unpack(frame[: _HEADER.size])
    if length > max_frame_bytes:
        raise FrameTooLargeError(
            f"frame payload declares {length} bytes; limit is {max_frame_bytes}"
        )
    expected = _HEADER.size + length
    if len(frame) < expected:
        raise TruncatedFrameError(
            f"frame payload is truncated: expected {length}, got {len(frame) - 4}"
        )
    if len(frame) > expected:
        raise IPCSchemaError("trailing bytes after IPC frame")
    return decode_payload(frame[_HEADER.size :], max_frame_bytes=max_frame_bytes)


async def read_frame(
    reader: asyncio.StreamReader,
    *,
    max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
    allow_eof: bool = False,
) -> dict[str, Any] | None:
    """Read one frame, optionally returning ``None`` for a clean EOF."""

    try:
        header = await reader.readexactly(_HEADER.size)
    except asyncio.IncompleteReadError as exc:
        if allow_eof and not exc.partial:
            return None
        raise TruncatedFrameError(
            f"frame header is truncated: got {len(exc.partial)} of {_HEADER.size} bytes"
        ) from exc
    (length,) = _HEADER.unpack(header)
    if length > max_frame_bytes:
        raise FrameTooLargeError(
            f"frame payload declares {length} bytes; limit is {max_frame_bytes}"
        )
    try:
        payload = await reader.readexactly(length)
    except asyncio.IncompleteReadError as exc:
        raise TruncatedFrameError(
            f"frame payload is truncated: expected {length}, got {len(exc.partial)}"
        ) from exc
    return decode_payload(payload, max_frame_bytes=max_frame_bytes)


async def write_frame(
    writer: asyncio.StreamWriter,
    message: Mapping[str, Any],
    *,
    max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
) -> None:
    """Write and drain one frame."""

    writer.write(encode_frame(message, max_frame_bytes=max_frame_bytes))
    await writer.drain()


def _expect_exact_keys(
    message: Mapping[str, Any], required: set[str], optional: set[str] = frozenset()
) -> None:
    keys = set(message)
    missing = required - keys
    extra = keys - required - optional
    if missing:
        raise IPCSchemaError(f"message is missing fields: {sorted(missing)}")
    if extra:
        raise IPCSchemaError(f"message has unknown fields: {sorted(extra)}")


def _expect_int(value: Any, field: str, *, minimum: int = 0) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise IPCSchemaError(f"{field} must be an integer >= {minimum}")
    return value


def validate_wire_message(message: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a worker-protocol envelope and return a plain dictionary."""

    if not isinstance(message, Mapping):
        raise IPCSchemaError("wire message must be a mapping")
    result = dict(message)
    _validate_value(result)
    kind = result.get("kind")
    if not isinstance(kind, str):
        raise IPCSchemaError("kind must be a string")

    if kind == "hello":
        _expect_exact_keys(
            result,
            {"kind", "protocol", "version", "adapter", "capabilities"},
        )
        if result["protocol"] != PROTOCOL_NAME:
            raise IPCSchemaError("unsupported worker protocol name")
        _expect_int(result["version"], "version", minimum=1)
        if result["version"] != PROTOCOL_VERSION:
            raise IPCSchemaError("unsupported worker protocol version")
        if not isinstance(result["adapter"], str) or not result["adapter"]:
            raise IPCSchemaError("adapter must be a non-empty string")
        if not isinstance(result["capabilities"], list) or not all(
            isinstance(item, str) for item in result["capabilities"]
        ):
            raise IPCSchemaError("capabilities must be a list of strings")
        return result

    _expect_int(result.get("version"), "version", minimum=1)
    if result["version"] != PROTOCOL_VERSION:
        raise IPCSchemaError("unsupported worker protocol version")

    if kind == "request":
        _expect_exact_keys(
            result, {"kind", "version", "request_id", "method", "params"}
        )
        _expect_int(result["request_id"], "request_id", minimum=1)
        if not isinstance(result["method"], str) or not result["method"]:
            raise IPCSchemaError("method must be a non-empty string")
        if not isinstance(result["params"], dict):
            raise IPCSchemaError("params must be a mapping")
    elif kind == "response":
        required = {"kind", "version", "request_id", "ok"}
        if result.get("ok") is True:
            _expect_exact_keys(result, required | {"result"})
            if not isinstance(result["result"], dict):
                raise IPCSchemaError("successful result must be a mapping")
        elif result.get("ok") is False:
            _expect_exact_keys(result, required | {"error"})
            error = result["error"]
            if not isinstance(error, dict) or set(error) != {"code", "message"}:
                raise IPCSchemaError("error must contain exactly code and message")
            if not all(isinstance(error[key], str) for key in ("code", "message")):
                raise IPCSchemaError("error code and message must be strings")
        else:
            raise IPCSchemaError("ok must be a boolean")
        _expect_int(result["request_id"], "request_id", minimum=1)
    elif kind == "event":
        _expect_exact_keys(result, {"kind", "version", "event"})
        if not isinstance(result["event"], dict):
            raise IPCSchemaError("event must be a mapping")
    else:
        raise IPCSchemaError(f"unknown wire message kind: {kind!r}")
    return result
