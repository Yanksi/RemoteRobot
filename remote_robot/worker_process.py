"""Built-in subprocess host for a Remote Robot worker.

This module intentionally has no ``--module`` or import-path option.  A server
administrator selects only adapters registered in ``BUILTIN_ADAPTERS``; remote
clients can never turn the worker boundary into arbitrary code execution.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import threading
from collections.abc import Mapping
from typing import Any, BinaryIO

from .ipc import (
    DEFAULT_MAX_FRAME_BYTES,
    PROTOCOL_NAME,
    PROTOCOL_VERSION,
    FrameTooLargeError,
    IPCError,
    IPCSchemaError,
    TruncatedFrameError,
    decode_payload,
    encode_frame,
    validate_wire_message,
)
from .worker import InMemoryWorker, WorkerClosedError, WorkerError, WorkerPort


METHODS = (
    "prepare_disabled",
    "prepare_phase",
    "start_phase",
    "stop",
    "state",
    "close",
)


class TestEchoWorker(InMemoryWorker):
    """A deterministic adapter exposed solely for transport integration tests."""

    def __init__(self) -> None:
        super().__init__(adapter_name="test-echo")


class TestHangWorker(InMemoryWorker):
    """A built-in fault injector whose state request never finishes itself."""

    def __init__(self) -> None:
        super().__init__(adapter_name="test-hang")

    async def state(self) -> dict[str, Any]:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


BUILTIN_ADAPTERS: dict[str, type[InMemoryWorker]] = {
    "simulator": InMemoryWorker,
    "test-echo": TestEchoWorker,
    "test-hang": TestHangWorker,
}


def _read_exact(stream: BinaryIO, length: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < length:
        chunk = stream.read(length - len(chunks))
        if not chunk:
            if not chunks:
                return b""
            raise TruncatedFrameError(
                f"stdio frame ended after {len(chunks)} of {length} bytes"
            )
        chunks.extend(chunk)
    return bytes(chunks)


def _read_blocking(stream: BinaryIO, max_frame_bytes: int) -> dict[str, Any] | None:
    header = _read_exact(stream, 4)
    if not header:
        return None
    if len(header) != 4:
        raise TruncatedFrameError("stdio frame header is truncated")
    length = int.from_bytes(header, "big")
    if length > max_frame_bytes:
        raise FrameTooLargeError(
            f"stdio frame declares {length} bytes; limit is {max_frame_bytes}"
        )
    payload = _read_exact(stream, length)
    if len(payload) != length:
        raise TruncatedFrameError("stdio frame payload is truncated")
    return decode_payload(payload, max_frame_bytes=max_frame_bytes)


class StdioChannel:
    def __init__(self, max_frame_bytes: int) -> None:
        self.max_frame_bytes = max_frame_bytes
        self._write_lock = threading.Lock()
        self._input = sys.stdin.buffer
        self._output = sys.stdout.buffer

    async def read(self) -> dict[str, Any] | None:
        return await asyncio.to_thread(
            _read_blocking, self._input, self.max_frame_bytes
        )

    def _write_blocking(self, message: Mapping[str, Any]) -> None:
        frame = encode_frame(message, max_frame_bytes=self.max_frame_bytes)
        with self._write_lock:
            self._output.write(frame)
            self._output.flush()

    async def write(self, message: Mapping[str, Any]) -> None:
        await asyncio.to_thread(self._write_blocking, message)


def _require_params(
    params: Mapping[str, Any], required: set[str]
) -> dict[str, Any]:
    if set(params) != required:
        missing = required - set(params)
        extra = set(params) - required
        raise IPCSchemaError(
            f"invalid params; missing={sorted(missing)}, extra={sorted(extra)}"
        )
    return dict(params)


async def _dispatch(
    worker: WorkerPort, method: str, params: Mapping[str, Any]
) -> dict[str, Any]:
    if method == "prepare_disabled":
        _require_params(params, set())
        return await worker.prepare_disabled()
    if method == "prepare_phase":
        values = _require_params(params, {"phase_plan"})
        if not isinstance(values["phase_plan"], dict):
            raise IPCSchemaError("phase_plan must be a mapping")
        return await worker.prepare_phase(values["phase_plan"])
    if method == "start_phase":
        values = _require_params(params, {"monotonic_deadline_us"})
        return await worker.start_phase(values["monotonic_deadline_us"])
    if method == "stop":
        values = _require_params(params, {"fault_context"})
        if not isinstance(values["fault_context"], dict):
            raise IPCSchemaError("fault_context must be a mapping")
        return await worker.stop(values["fault_context"])
    if method == "state":
        _require_params(params, set())
        return await worker.state()
    if method == "close":
        _require_params(params, set())
        await worker.close()
        return {"status": "closed"}
    raise WorkerError(f"unknown method {method!r}")


async def _pump_events(worker: WorkerPort, channel: StdioChannel) -> None:
    while True:
        event = await worker.next_event()
        await channel.write(
            {"kind": "event", "version": PROTOCOL_VERSION, "event": event}
        )


def _error_record(exc: BaseException) -> dict[str, str]:
    if isinstance(exc, IPCSchemaError):
        code = "INVALID_ARGUMENT"
    elif isinstance(exc, WorkerClosedError):
        code = "WORKER_CLOSED"
    elif isinstance(exc, WorkerError):
        code = "WORKER_ERROR"
    else:
        code = "INTERNAL_ERROR"
    return {"code": code, "message": str(exc) or type(exc).__name__}


async def serve(adapter_name: str, max_frame_bytes: int) -> int:
    adapter_type = BUILTIN_ADAPTERS[adapter_name]
    if adapter_name == "simulator":
        worker: WorkerPort = adapter_type(adapter_name="simulator")
    else:
        worker = adapter_type()
    channel = StdioChannel(max_frame_bytes)
    await channel.write(
        {
            "kind": "hello",
            "protocol": PROTOCOL_NAME,
            "version": PROTOCOL_VERSION,
            "adapter": adapter_name,
            "capabilities": list(METHODS),
        }
    )
    event_task = asyncio.create_task(_pump_events(worker, channel))
    try:
        while True:
            try:
                raw = await channel.read()
                if raw is None:
                    return 0
                request = validate_wire_message(raw)
                if request["kind"] != "request":
                    raise IPCSchemaError("worker host accepts only request messages")
            except IPCError as exc:
                print(f"fatal worker IPC error: {exc}", file=sys.stderr, flush=True)
                return 2

            request_id = request["request_id"]
            try:
                result = await _dispatch(
                    worker, request["method"], request["params"]
                )
                response = {
                    "kind": "response",
                    "version": PROTOCOL_VERSION,
                    "request_id": request_id,
                    "ok": True,
                    "result": result,
                }
            except BaseException as exc:
                if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                    raise
                response = {
                    "kind": "response",
                    "version": PROTOCOL_VERSION,
                    "request_id": request_id,
                    "ok": False,
                    "error": _error_record(exc),
                }
            await channel.write(response)
            if request["method"] == "close" and response["ok"]:
                return 0
    finally:
        event_task.cancel()
        await asyncio.gather(event_task, return_exceptions=True)
        await worker.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--adapter", choices=sorted(BUILTIN_ADAPTERS), required=True)
    parser.add_argument(
        "--max-frame-bytes",
        type=int,
        default=DEFAULT_MAX_FRAME_BYTES,
        help="hard upper bound for one CBOR payload",
    )
    args = parser.parse_args(argv)
    if args.max_frame_bytes <= 0 or args.max_frame_bytes > 0xFFFFFFFF:
        parser.error("--max-frame-bytes must be between 1 and 2^32-1")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return asyncio.run(serve(args.adapter, args.max_frame_bytes))


if __name__ == "__main__":
    raise SystemExit(main())
