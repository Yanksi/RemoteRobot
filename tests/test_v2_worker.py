from __future__ import annotations

import asyncio
import struct
import time
import unittest

import cbor2

from remote_robot.ipc import (
    FrameTooLargeError,
    IPCSchemaError,
    NonCanonicalFrameError,
    TruncatedFrameError,
    decode_frame,
    decode_payload,
    encode_frame,
    read_frame,
)
from remote_robot.worker import (
    InMemoryWorker,
    SubprocessWorker,
    WorkerClosedError,
    WorkerStateError,
    WorkerTimeoutError,
)


class CanonicalCBORTests(unittest.TestCase):
    def test_round_trip_is_canonical_and_independent_of_map_insertion_order(self) -> None:
        left = {"kind": "event", "event": {"z": 1, "a": [1.0, True, None]}}
        right = {"event": {"a": [1.0, True, None], "z": 1}, "kind": "event"}
        left_frame = encode_frame(left)
        right_frame = encode_frame(right)
        self.assertEqual(left_frame, right_frame)
        self.assertEqual(decode_frame(left_frame), left)

    def test_noncanonical_payload_is_rejected(self) -> None:
        # Insertion order differs from canonical key ordering.
        payload = cbor2.dumps({"long-key": 1, "a": 2}, canonical=False)
        self.assertNotEqual(payload, cbor2.dumps(cbor2.loads(payload), canonical=True))
        with self.assertRaises(NonCanonicalFrameError):
            decode_payload(payload)

    def test_schema_rejects_python_specific_or_ambiguous_values(self) -> None:
        with self.assertRaises(IPCSchemaError):
            encode_frame({"bad": float("nan")})
        with self.assertRaises(IPCSchemaError):
            encode_frame({"bad": {1: "non-string-key"}})
        with self.assertRaises(IPCSchemaError):
            encode_frame({"bad": object()})

    def test_oversized_frame_is_rejected_from_header_without_reading_payload(self) -> None:
        frame = struct.pack(">I", 129)
        with self.assertRaises(FrameTooLargeError):
            decode_frame(frame, max_frame_bytes=128)

    def test_truncated_frame_and_trailing_data_are_rejected(self) -> None:
        frame = encode_frame({"ok": True})
        with self.assertRaises(TruncatedFrameError):
            decode_frame(frame[:-1])
        with self.assertRaises(IPCSchemaError):
            decode_frame(frame + b"x")


class AsyncFrameTests(unittest.IsolatedAsyncioTestCase):
    async def test_stream_reader_reports_truncated_payload(self) -> None:
        reader = asyncio.StreamReader()
        reader.feed_data(struct.pack(">I", 10) + b"short")
        reader.feed_eof()
        with self.assertRaises(TruncatedFrameError):
            await read_frame(reader)

    async def test_clean_eof_is_distinct_from_a_truncated_header(self) -> None:
        clean = asyncio.StreamReader()
        clean.feed_eof()
        self.assertIsNone(await read_frame(clean, allow_eof=True))

        partial = asyncio.StreamReader()
        partial.feed_data(b"\x00")
        partial.feed_eof()
        with self.assertRaises(TruncatedFrameError):
            await read_frame(partial, allow_eof=True)


class InMemoryWorkerTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def phase_plan(phase_id: str = "pick", *, duration_us: int = 0) -> dict:
        return {
            "run_id": "run-1",
            "phase_id": phase_id,
            "phase_execution_id": f"run-1:{phase_id}:1",
            "duration_us": duration_us,
            "tracks": [{"group": "arm", "samples": []}],
        }

    async def test_lifecycle_and_events_use_serializable_dictionaries(self) -> None:
        worker = InMemoryWorker()
        self.assertEqual((await worker.state())["status"], "created")
        self.assertEqual((await worker.prepare_disabled())["status"], "disabled")
        prepared = await worker.prepare_phase(self.phase_plan(duration_us=1_000))
        self.assertEqual(prepared["status"], "prepared")
        self.assertEqual(prepared["phase_execution_id"], "run-1:pick:1")
        scheduled = await worker.start_phase(time.monotonic_ns() // 1_000 + 10_000)
        self.assertEqual(scheduled["status"], "scheduled")
        self.assertEqual(scheduled["run_id"], "run-1")
        self.assertEqual(scheduled["phase_id"], "pick")
        self.assertEqual(scheduled["phase_execution_id"], "run-1:pick:1")

        events = [(await worker.next_event(0.2)) for _ in range(4)]
        self.assertEqual(
            [event["type"] for event in events],
            ["worker_disabled", "phase_prepared", "phase_started", "phase_completed"],
        )
        for event in events[1:]:
            self.assertEqual(event["run_id"], "run-1")
            self.assertEqual(event["phase_id"], "pick")
            self.assertEqual(event["phase_execution_id"], "run-1:pick:1")
        self.assertIn("actual_start_us", events[2])
        self.assertIn("start_skew_us", events[2])

        stopped = await worker.stop({"code": "TEST_STOP", "details": {}})
        self.assertEqual(stopped["status"], "stopped")
        stopped_event = await worker.next_event(0.1)
        self.assertEqual(stopped_event["type"], "worker_stopped")
        self.assertEqual(stopped_event["phase_execution_id"], "run-1:pick:1")
        await worker.close()
        await worker.close()  # idempotent
        with self.assertRaises(WorkerClosedError):
            await worker.state()

    async def test_invalid_state_transition_is_rejected(self) -> None:
        worker = InMemoryWorker()
        with self.assertRaises(WorkerStateError):
            await worker.start_phase(0)
        await worker.close()

    async def test_stop_cancels_a_future_scheduled_phase(self) -> None:
        worker = InMemoryWorker()
        await worker.prepare_disabled()
        await worker.prepare_phase(self.phase_plan())
        await worker.start_phase(time.monotonic_ns() // 1_000 + 500_000)
        await worker.stop({"code": "CANCELLED"})
        queued = [await worker.next_event(0.1) for _ in range(3)]
        self.assertEqual(
            [event["type"] for event in queued],
            ["worker_disabled", "phase_prepared", "worker_stopped"],
        )
        await asyncio.sleep(0.05)
        self.assertTrue(worker.events.empty())
        await worker.close()


class SubprocessWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_handshake_rpc_event_multiplexing_and_clean_close(self) -> None:
        worker = await SubprocessWorker.spawn("test-echo")
        try:
            self.assertEqual(worker.adapter_name, "test-echo")
            self.assertIn("prepare_phase", worker.capabilities)
            disabled = await worker.prepare_disabled()
            prepared = await worker.prepare_phase(
                {
                    "run_id": "run-subprocess",
                    "phase_id": "phase-7",
                    "phase_execution_id": "run-subprocess:phase-7:1",
                    "duration_us": 0,
                }
            )
            self.assertEqual(disabled["status"], "disabled")
            self.assertEqual(prepared["phase_id"], "phase-7")

            # Events and request responses share stdout but are dispatched to
            # independent consumers by request ID / envelope kind.
            event = await worker.next_event(1.0)
            self.assertEqual(event["type"], "worker_disabled")
            state = await worker.state()
            self.assertEqual(state["status"], "prepared")
        finally:
            await worker.close()
        self.assertEqual(worker.returncode, 0)
        with self.assertRaises(WorkerClosedError):
            await worker.state()

    async def test_future_start_is_acknowledged_before_deadline_then_emits_events(self) -> None:
        worker = await SubprocessWorker.spawn(
            "test-echo", request_timeout=0.2, handshake_timeout=2.0
        )
        try:
            await worker.prepare_disabled()
            await worker.prepare_phase(
                {
                    "run_id": "future-run",
                    "phase_id": "future-phase",
                    "phase_execution_id": "future-run:future-phase:1",
                    "duration_us": 1_000,
                }
            )
            # Ignore preparation events so correlation assertions below apply
            # only to actual phase execution events.
            await worker.next_event(0.2)
            await worker.next_event(0.2)
            deadline_us = time.monotonic_ns() // 1_000 + 500_000
            before = time.monotonic()
            scheduled = await worker.start_phase(deadline_us)
            elapsed = time.monotonic() - before
            self.assertLess(elapsed, 0.2)
            self.assertEqual(scheduled["status"], "scheduled")
            self.assertEqual(scheduled["requested_start_us"], deadline_us)
            self.assertEqual(
                scheduled["phase_execution_id"], "future-run:future-phase:1"
            )

            started = await worker.next_event(1.0)
            completed = await worker.next_event(0.5)
            self.assertEqual(started["type"], "phase_started")
            self.assertEqual(completed["type"], "phase_completed")
            for event in (started, completed):
                self.assertEqual(event["run_id"], "future-run")
                self.assertEqual(event["phase_id"], "future-phase")
                self.assertEqual(
                    event["phase_execution_id"], "future-run:future-phase:1"
                )
        finally:
            await worker.close()

    async def test_request_timeout_is_bounded_and_close_terminates_hung_worker(self) -> None:
        worker = await SubprocessWorker.spawn(
            "test-hang", request_timeout=0.1, handshake_timeout=2.0
        )
        with self.assertRaises(WorkerTimeoutError):
            await worker.state()
        # Timeout is terminal: the supervisor kills the process before the
        # timeout is reported, so the unknown request cannot execute later.
        self.assertIsNotNone(worker.returncode)
        with self.assertRaises(WorkerClosedError):
            await worker.state()
        await worker.close()


if __name__ == "__main__":
    unittest.main()
