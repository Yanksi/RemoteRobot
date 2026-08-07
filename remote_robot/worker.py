"""WorkerPort implementations for local execution workers.

The orchestrator talks only to the small asynchronous ``WorkerPort`` seam.
Production workers use a supervised subprocess; deterministic tests can use
the in-memory implementation without changing orchestration code.
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
import time
from collections import deque
from collections.abc import Mapping, Sequence
from typing import Any, Protocol, runtime_checkable

from .ipc import (
    DEFAULT_MAX_FRAME_BYTES,
    PROTOCOL_NAME,
    PROTOCOL_VERSION,
    IPCError,
    IPCSchemaError,
    canonical_payload,
    decode_payload,
    encode_frame,
    read_frame,
    validate_wire_message,
)


class WorkerError(Exception):
    """Base class for worker lifecycle and transport failures."""


class WorkerStateError(WorkerError):
    """A lifecycle operation is invalid in the worker's current state."""


class WorkerClosedError(WorkerError):
    """The worker process or port is no longer available."""


class WorkerProtocolError(WorkerError):
    """The worker violated the local IPC protocol."""


class WorkerTimeoutError(WorkerError):
    """A bounded worker operation did not finish in time."""


class WorkerRemoteError(WorkerError):
    """A worker returned a structured operation error."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


@runtime_checkable
class WorkerPort(Protocol):
    """The complete remote-owned worker interface used by the orchestrator."""

    async def prepare_disabled(self) -> dict[str, Any]: ...

    async def prepare_phase(self, phase_plan: dict[str, Any]) -> dict[str, Any]: ...

    async def start_phase(self, monotonic_deadline_us: int) -> dict[str, Any]: ...

    async def stop(self, fault_context: dict[str, Any]) -> dict[str, Any]: ...

    async def state(self) -> dict[str, Any]: ...

    async def next_event(self, timeout: float | None = None) -> dict[str, Any]: ...

    async def close(self) -> None: ...


def _data_copy(value: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and copy a serializable dictionary through canonical CBOR."""

    payload = canonical_payload(value)
    return decode_payload(payload)


class InMemoryWorker:
    """Deterministic worker state machine for orchestration tests."""

    def __init__(self, *, adapter_name: str = "in-memory", event_limit: int = 256) -> None:
        if event_limit <= 0:
            raise ValueError("event_limit must be positive")
        self.adapter_name = adapter_name
        self.events: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=event_limit)
        self._status = "created"
        self._phase_plan: dict[str, Any] | None = None
        self._last_start_deadline_us: int | None = None
        self._last_stop: dict[str, Any] | None = None
        self._phase_task: asyncio.Task[None] | None = None

    def _ensure_open(self) -> None:
        if self._status == "closed":
            raise WorkerClosedError("worker is closed")

    def _emit(self, event: Mapping[str, Any]) -> None:
        try:
            self.events.put_nowait(_data_copy(event))
        except asyncio.QueueFull as exc:
            raise WorkerError("worker event queue is full") from exc

    def _phase_identity(self) -> dict[str, str]:
        plan = self._phase_plan or {}
        return {
            "run_id": str(plan.get("run_id", "")),
            "phase_id": str(plan.get("phase_id", "")),
            "phase_execution_id": str(plan.get("phase_execution_id", "")),
        }

    async def prepare_disabled(self) -> dict[str, Any]:
        self._ensure_open()
        if self._status in {"scheduled", "running"}:
            raise WorkerStateError("cannot prepare hardware while a phase is running")
        self._status = "disabled"
        self._phase_plan = None
        result = {"status": self._status, "adapter": self.adapter_name}
        self._emit({"type": "worker_disabled", **result})
        return result

    async def prepare_phase(self, phase_plan: dict[str, Any]) -> dict[str, Any]:
        self._ensure_open()
        if self._status not in {"disabled", "stopped", "prepared", "completed"}:
            raise WorkerStateError(
                f"prepare_phase requires disabled or stopped state, got {self._status}"
            )
        if not isinstance(phase_plan, dict):
            raise IPCSchemaError("phase_plan must be a dictionary")
        self._phase_plan = _data_copy(phase_plan)
        for field in ("run_id", "phase_id", "phase_execution_id"):
            value = self._phase_plan.get(field)
            if not isinstance(value, str) or not value:
                self._phase_plan = None
                raise IPCSchemaError(f"phase_plan.{field} must be a non-empty string")
        duration_us = self._phase_plan.get("duration_us", 0)
        if (
            not isinstance(duration_us, int)
            or isinstance(duration_us, bool)
            or duration_us < 0
        ):
            self._phase_plan = None
            raise IPCSchemaError("phase_plan.duration_us must be a non-negative integer")
        self._status = "prepared"
        result = {
            "status": self._status,
            **self._phase_identity(),
        }
        self._emit({"type": "phase_prepared", **result})
        return result

    async def start_phase(self, monotonic_deadline_us: int) -> dict[str, Any]:
        self._ensure_open()
        if self._status != "prepared":
            raise WorkerStateError(
                f"start_phase requires prepared state, got {self._status}"
            )
        if (
            not isinstance(monotonic_deadline_us, int)
            or isinstance(monotonic_deadline_us, bool)
            or monotonic_deadline_us < 0
        ):
            raise IPCSchemaError("monotonic_deadline_us must be a non-negative integer")
        self._last_start_deadline_us = monotonic_deadline_us
        self._status = "scheduled"
        result = {
            "status": self._status,
            **self._phase_identity(),
            "requested_start_us": monotonic_deadline_us,
        }
        self._phase_task = asyncio.create_task(
            self._execute_phase(monotonic_deadline_us),
            name=f"{self.adapter_name}-phase-execution",
        )
        return result

    async def _execute_phase(self, monotonic_deadline_us: int) -> None:
        try:
            now_us = time.monotonic_ns() // 1_000
            delay_us = monotonic_deadline_us - now_us
            if delay_us > 0:
                await asyncio.sleep(delay_us / 1_000_000)
            if self._status != "scheduled":
                return
            self._status = "running"
            actual_start_us = time.monotonic_ns() // 1_000
            self._emit(
                {
                    "type": "phase_started",
                    "status": self._status,
                    **self._phase_identity(),
                    "requested_start_us": monotonic_deadline_us,
                    "actual_start_us": actual_start_us,
                    "start_skew_us": actual_start_us - monotonic_deadline_us,
                }
            )
            duration_us = int((self._phase_plan or {}).get("duration_us", 0))
            if duration_us:
                await asyncio.sleep(duration_us / 1_000_000)
            if self._status != "running":
                return
            self._status = "completed"
            self._emit(
                {
                    "type": "phase_completed",
                    "status": self._status,
                    **self._phase_identity(),
                    "requested_start_us": monotonic_deadline_us,
                    "actual_start_us": actual_start_us,
                    "actual_completed_us": time.monotonic_ns() // 1_000,
                    "completed_at_us": time.monotonic_ns() // 1_000,
                }
            )
        finally:
            if self._phase_task is asyncio.current_task():
                self._phase_task = None

    async def _cancel_phase_task(self) -> None:
        task = self._phase_task
        self._phase_task = None
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def stop(self, fault_context: dict[str, Any]) -> dict[str, Any]:
        self._ensure_open()
        if not isinstance(fault_context, dict):
            raise IPCSchemaError("fault_context must be a dictionary")
        await self._cancel_phase_task()
        self._last_stop = _data_copy(fault_context)
        self._status = "stopped"
        result = {
            "status": self._status,
            **self._phase_identity(),
            "fault_context": self._last_stop,
        }
        self._emit({"type": "worker_stopped", **result})
        return result

    async def state(self) -> dict[str, Any]:
        self._ensure_open()
        return _data_copy(
            {
                "status": self._status,
                "adapter": self.adapter_name,
                **self._phase_identity(),
                "last_start_deadline_us": self._last_start_deadline_us,
                "last_stop": self._last_stop,
            }
        )

    async def next_event(self, timeout: float | None = None) -> dict[str, Any]:
        self._ensure_open()
        if timeout is None:
            return await self.events.get()
        try:
            async with asyncio.timeout(timeout):
                return await self.events.get()
        except TimeoutError as exc:
            raise WorkerTimeoutError("timed out waiting for worker event") from exc

    async def close(self) -> None:
        if self._status == "closed":
            return
        await self._cancel_phase_task()
        self._status = "closed"


class SubprocessWorker:
    """Supervise one worker process and multiplex RPC responses and events."""

    def __init__(
        self,
        process: asyncio.subprocess.Process,
        *,
        request_timeout: float,
        max_frame_bytes: int,
        event_limit: int,
    ) -> None:
        if process.stdin is None or process.stdout is None or process.stderr is None:
            raise ValueError("worker subprocess must use piped stdin/stdout/stderr")
        self._process = process
        self._stdin = process.stdin
        self._stdout = process.stdout
        self._stderr = process.stderr
        self._request_timeout = request_timeout
        self._max_frame_bytes = max_frame_bytes
        self._events: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=event_limit)
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._next_request_id = 1
        self._write_lock = asyncio.Lock()
        self._hello: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._reader_error: BaseException | None = None
        self._closed = False
        self._closing = False
        self._stderr_tail: deque[str] = deque(maxlen=50)
        self._reader_task = asyncio.create_task(self._reader_loop(), name="worker-ipc-reader")
        self._stderr_task = asyncio.create_task(self._stderr_loop(), name="worker-stderr-reader")

    @classmethod
    async def spawn(
        cls,
        adapter: str = "simulator",
        *,
        request_timeout: float = 2.0,
        handshake_timeout: float = 5.0,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        event_limit: int = 256,
        python_executable: str | None = None,
        extra_args: Sequence[str] = (),
    ) -> "SubprocessWorker":
        """Start one of the worker host's explicitly built-in adapters."""

        if request_timeout <= 0 or handshake_timeout <= 0:
            raise ValueError("worker timeouts must be positive")
        if event_limit <= 0:
            raise ValueError("event_limit must be positive")
        if max_frame_bytes <= 0 or max_frame_bytes > 0xFFFFFFFF:
            raise ValueError("max_frame_bytes must be between 1 and 2^32-1")
        executable = python_executable or sys.executable
        process = await asyncio.create_subprocess_exec(
            executable,
            "-m",
            "remote_robot.worker_process",
            "--adapter",
            adapter,
            "--max-frame-bytes",
            str(max_frame_bytes),
            *extra_args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        worker = cls(
            process,
            request_timeout=request_timeout,
            max_frame_bytes=max_frame_bytes,
            event_limit=event_limit,
        )
        try:
            async with asyncio.timeout(handshake_timeout):
                hello = await asyncio.shield(worker._hello)
        except TimeoutError as exc:
            worker._closing = True
            await worker._terminate()
            for task in (worker._reader_task, worker._stderr_task):
                task.cancel()
            await asyncio.gather(
                worker._reader_task, worker._stderr_task, return_exceptions=True
            )
            if not worker._hello.done():
                worker._hello.cancel()
            raise WorkerTimeoutError(
                f"worker handshake exceeded {handshake_timeout:.3f}s"
            ) from exc
        except BaseException:
            worker._closing = True
            await worker._terminate()
            for task in (worker._reader_task, worker._stderr_task):
                task.cancel()
            await asyncio.gather(
                worker._reader_task, worker._stderr_task, return_exceptions=True
            )
            if not worker._hello.done():
                worker._hello.cancel()
            raise
        if hello["protocol"] != PROTOCOL_NAME or hello["version"] != PROTOCOL_VERSION:
            await worker.close()
            raise WorkerProtocolError("worker handshake negotiated an unsupported protocol")
        worker.adapter_name = hello["adapter"]
        worker.capabilities = tuple(hello["capabilities"])
        return worker

    @property
    def returncode(self) -> int | None:
        return self._process.returncode

    @property
    def stderr_tail(self) -> tuple[str, ...]:
        return tuple(self._stderr_tail)

    async def _stderr_loop(self) -> None:
        try:
            while line := await self._stderr.readline():
                self._stderr_tail.append(line.decode("utf-8", errors="replace").rstrip())
        except (asyncio.CancelledError, Exception):
            return

    def _fail_transport(self, error: BaseException) -> None:
        if self._reader_error is None:
            self._reader_error = error
        if not self._hello.done():
            self._hello.set_exception(error)
        pending = tuple(self._pending.values())
        self._pending.clear()
        for future in pending:
            if not future.done():
                future.set_exception(error)

    async def _reader_loop(self) -> None:
        try:
            while True:
                raw = await read_frame(
                    self._stdout,
                    max_frame_bytes=self._max_frame_bytes,
                    allow_eof=True,
                )
                if raw is None:
                    if not self._closing:
                        detail = "; ".join(self._stderr_tail)
                        suffix = f": {detail}" if detail else ""
                        raise WorkerClosedError(f"worker process closed its output{suffix}")
                    return
                try:
                    message = validate_wire_message(raw)
                except IPCError as exc:
                    raise WorkerProtocolError(str(exc)) from exc
                kind = message["kind"]
                if kind == "hello":
                    if self._hello.done():
                        raise WorkerProtocolError("worker sent more than one hello")
                    self._hello.set_result(message)
                elif kind == "response":
                    request_id = message["request_id"]
                    future = self._pending.pop(request_id, None)
                    if future is None:
                        # A late response after a bounded timeout is expected.
                        continue
                    if message["ok"]:
                        future.set_result(message["result"])
                    else:
                        error = message["error"]
                        future.set_exception(
                            WorkerRemoteError(error["code"], error["message"])
                        )
                elif kind == "event":
                    try:
                        self._events.put_nowait(message["event"])
                    except asyncio.QueueFull as exc:
                        raise WorkerProtocolError("worker event queue overflow") from exc
                else:
                    raise WorkerProtocolError(f"unexpected worker message kind {kind!r}")
        except asyncio.CancelledError:
            return
        except BaseException as exc:
            self._fail_transport(exc)

    async def _rpc(self, method: str, params: Mapping[str, Any]) -> dict[str, Any]:
        if self._closed:
            raise WorkerClosedError("worker is closed")
        if self._reader_error is not None:
            raise WorkerClosedError(f"worker transport failed: {self._reader_error}")
        request_id = self._next_request_id
        self._next_request_id += 1
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending[request_id] = future
        request = {
            "kind": "request",
            "version": PROTOCOL_VERSION,
            "request_id": request_id,
            "method": method,
            "params": dict(params),
        }
        try:
            frame = encode_frame(request, max_frame_bytes=self._max_frame_bytes)
            async with self._write_lock:
                self._stdin.write(frame)
                await self._stdin.drain()
        except BaseException:
            self._pending.pop(request_id, None)
            future.cancel()
            raise
        try:
            async with asyncio.timeout(self._request_timeout):
                return await asyncio.shield(future)
        except TimeoutError as exc:
            self._pending.pop(request_id, None)
            future.cancel()
            timeout_error = WorkerTimeoutError(
                f"worker method {method!r} exceeded {self._request_timeout:.3f}s"
            )
            # A timed-out request has an unknown remote outcome.  Keeping the
            # channel alive would allow it (or a queued command) to execute
            # later, so timeout is a terminal transport failure.
            await self._abort_transport(timeout_error)
            raise timeout_error from exc

    async def prepare_disabled(self) -> dict[str, Any]:
        return await self._rpc("prepare_disabled", {})

    async def prepare_phase(self, phase_plan: dict[str, Any]) -> dict[str, Any]:
        return await self._rpc("prepare_phase", {"phase_plan": phase_plan})

    async def start_phase(self, monotonic_deadline_us: int) -> dict[str, Any]:
        return await self._rpc(
            "start_phase", {"monotonic_deadline_us": monotonic_deadline_us}
        )

    async def stop(self, fault_context: dict[str, Any]) -> dict[str, Any]:
        return await self._rpc("stop", {"fault_context": fault_context})

    async def state(self) -> dict[str, Any]:
        return await self._rpc("state", {})

    async def next_event(self, timeout: float | None = None) -> dict[str, Any]:
        if self._closed:
            raise WorkerClosedError("worker is closed")
        try:
            if timeout is None:
                return await self._events.get()
            async with asyncio.timeout(timeout):
                return await self._events.get()
        except TimeoutError as exc:
            raise WorkerTimeoutError("timed out waiting for worker event") from exc

    async def _terminate(self) -> None:
        if self._process.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                self._process.terminate()
            try:
                async with asyncio.timeout(2.0):
                    await self._process.wait()
            except TimeoutError:
                with contextlib.suppress(ProcessLookupError):
                    self._process.kill()
                await self._process.wait()

    async def _abort_transport(self, error: BaseException) -> None:
        self._closing = True
        self._closed = True
        self._fail_transport(error)
        with contextlib.suppress(BrokenPipeError, ConnectionError):
            self._stdin.close()
        await self._terminate()
        for task in (self._reader_task, self._stderr_task):
            if not task.done() and task is not asyncio.current_task():
                task.cancel()
        await asyncio.gather(
            *(task for task in (self._reader_task, self._stderr_task) if task is not asyncio.current_task()),
            return_exceptions=True,
        )

    async def close(self) -> None:
        if self._closed:
            return
        self._closing = True
        try:
            if self._process.returncode is None and self._reader_error is None:
                with contextlib.suppress(WorkerError, BrokenPipeError, ConnectionError):
                    await self._rpc("close", {})
            with contextlib.suppress(BrokenPipeError, ConnectionError):
                self._stdin.close()
                await self._stdin.wait_closed()
            if self._process.returncode is None:
                try:
                    async with asyncio.timeout(2.0):
                        await self._process.wait()
                except TimeoutError:
                    await self._terminate()
        finally:
            self._closed = True
            self._fail_transport(WorkerClosedError("worker is closed"))
            for task in (self._reader_task, self._stderr_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(
                self._reader_task, self._stderr_task, return_exceptions=True
            )
