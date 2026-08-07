"""Client for the bidirectional SO-101 trajectory stream."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from robot_protocol import (
    PROTOCOL,
    ProgramHeader,
    ProtocolError,
    SafetyPolicy,
    TrajectoryPoint,
    chunk_digest,
    load_program,
    program_digest,
)


TERMINAL_TYPES = {"run_finished"}


class RobotStreamClient:
    def __init__(self, websocket: ClientConnection) -> None:
        self.websocket = websocket
        self.events: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.receipts: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self.progress_changed = asyncio.Event()
        self.cursor_us = 0
        self.accepted_until_us = 0
        self.receiver_task = asyncio.create_task(self._receive())

    @classmethod
    async def open(cls, url: str, token: str) -> "RobotStreamClient":
        websocket = await connect(
            url,
            subprotocols=[PROTOCOL],
            open_timeout=5,
            ping_interval=20,
            ping_timeout=20,
            max_size=1_048_576,
            proxy=None,
        )
        await websocket.send(
            json.dumps(
                {"type": "hello", "protocol": PROTOCOL, "token": token},
                separators=(",", ":"),
            )
        )
        client = cls(websocket)
        welcome = await client.wait_for("welcome")
        if welcome.get("protocol") != PROTOCOL:
            await client.close()
            raise RuntimeError("server negotiated an unexpected protocol")
        return client

    async def close(self) -> None:
        await self.websocket.close()
        if not self.receiver_task.done():
            self.receiver_task.cancel()
        await asyncio.gather(self.receiver_task, return_exceptions=True)

    async def send(self, message: dict[str, Any]) -> None:
        await self.websocket.send(json.dumps(message, ensure_ascii=False, separators=(",", ":")))

    async def wait_for(self, kind: str) -> dict[str, Any]:
        while True:
            event = await self.events.get()
            if event.get("type") == "error":
                raise RuntimeError(f"{event.get('code')}: {event.get('message')}")
            if event.get("type") == kind:
                return event

    async def send_chunk(
        self,
        chunk_seq: int,
        points: list[TrajectoryPoint],
        final: bool,
    ) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        receipt: asyncio.Future[dict[str, Any]] = loop.create_future()
        self.receipts[chunk_seq] = receipt
        await self.send(
            {
                "type": "chunk",
                "chunk_seq": chunk_seq,
                "points": [point.record() for point in points],
                "final": final,
                "digest": chunk_digest(chunk_seq, points, final),
            }
        )
        try:
            return await receipt
        finally:
            self.receipts.pop(chunk_seq, None)

    async def _receive(self) -> None:
        try:
            async for raw in self.websocket:
                event = json.loads(raw)
                if not isinstance(event, dict):
                    continue
                if event.get("type") == "chunk_accepted":
                    chunk_seq = int(event["chunk_seq"])
                    receipt = self.receipts.get(chunk_seq)
                    if receipt is not None and not receipt.done():
                        receipt.set_result(event)
                elif event.get("type") == "error":
                    error = RuntimeError(f"{event.get('code')}: {event.get('message')}")
                    for receipt in self.receipts.values():
                        if not receipt.done():
                            receipt.set_exception(error)
                if "execution_cursor_us" in event:
                    self.cursor_us = int(event["execution_cursor_us"])
                if "accepted_until_us" in event:
                    self.accepted_until_us = int(event["accepted_until_us"])
                self.progress_changed.set()
                await self.events.put(event)
        except ConnectionClosed as exc:
            await self.events.put(
                {
                    "type": "connection_closed",
                    "code": "CONNECTION_CLOSED",
                    "message": str(exc),
                }
            )
        except Exception as exc:
            await self.events.put(
                {"type": "connection_closed", "code": "CLIENT_RECEIVE_FAILED", "message": str(exc)}
            )
        finally:
            for receipt in self.receipts.values():
                if not receipt.done():
                    receipt.set_exception(RuntimeError("connection closed before chunk ACK"))


def split_chunks(points: list[TrajectoryPoint], chunk_points: int) -> list[list[TrajectoryPoint]]:
    return [points[index : index + chunk_points] for index in range(0, len(points), chunk_points)]


async def heartbeat_loop(client: RobotStreamClient, interval_s: float) -> None:
    while True:
        await asyncio.sleep(interval_s)
        await client.send({"type": "heartbeat"})


async def stream_remaining_chunks(
    client: RobotStreamClient,
    chunks: list[list[TrajectoryPoint]],
    start_index: int,
    target_ahead_us: int,
) -> None:
    for chunk_seq in range(start_index, len(chunks)):
        chunk = chunks[chunk_seq]
        final = chunk_seq == len(chunks) - 1
        while chunk[-1].t_us - client.cursor_us > target_ahead_us:
            client.progress_changed.clear()
            await client.progress_changed.wait()
        receipt = await client.send_chunk(chunk_seq, chunk, final)
        print(
            f"ACK chunk={chunk_seq} accepted_until={receipt['accepted_until_us'] / 1e6:.2f}s "
            f"queued={receipt['queued_ahead_us'] / 1e6:.2f}s"
        )


async def run_program(args: argparse.Namespace, token: str) -> int:
    header, points = load_program(args.program)
    chunks = split_chunks(points, args.chunk_points)
    digest = program_digest(header, points)
    client = await RobotStreamClient.open(args.url, token)
    heartbeat: asyncio.Task[None] | None = None
    producer: asyncio.Task[None] | None = None
    run_started = False
    try:
        await client.send(
            {
                "type": "open_run",
                "run_id": args.run_id or uuid.uuid4().hex,
                "header": header.record(),
                "run_mode": args.mode,
                "program_digest": digest,
                "disconnect_policy": args.disconnect_policy,
            }
        )
        opened = await client.wait_for("run_opened")
        print(f"opened run {opened['run_id']} ({args.mode}, {header.coordinate_mode})")
        # Uploading a large sealed program can itself take longer than the
        # execution watchdog. Keep the control lease alive from open_run, not
        # merely from start.
        heartbeat = asyncio.create_task(heartbeat_loop(client, args.heartbeat_interval))

        next_chunk = 0
        if args.mode == "sealed":
            for next_chunk, chunk in enumerate(chunks):
                receipt = await client.send_chunk(next_chunk, chunk, next_chunk == len(chunks) - 1)
                print(
                    f"ACK chunk={next_chunk} accepted_until={receipt['accepted_until_us'] / 1e6:.2f}s"
                )
            next_chunk = len(chunks)
        else:
            prebuffer_us = min(int(args.target_ahead * 1_000_000), points[-1].t_us)
            while next_chunk < len(chunks):
                chunk = chunks[next_chunk]
                receipt = await client.send_chunk(
                    next_chunk,
                    chunk,
                    next_chunk == len(chunks) - 1,
                )
                print(
                    f"ACK chunk={next_chunk} accepted_until={receipt['accepted_until_us'] / 1e6:.2f}s"
                )
                next_chunk += 1
                if receipt["accepted_until_us"] >= prebuffer_us:
                    break

        await client.send({"type": "start"})
        run_started = True
        producer = asyncio.create_task(
            stream_remaining_chunks(
                client,
                chunks,
                next_chunk,
                int(args.target_ahead * 1_000_000),
            )
        )

        last_telemetry_second = -1
        while True:
            event_task = asyncio.create_task(client.events.get())
            wait_set: set[asyncio.Task[Any]] = {event_task}
            if producer is not None:
                wait_set.add(producer)
            done, _ = await asyncio.wait(wait_set, return_when=asyncio.FIRST_COMPLETED)
            if producer in done:
                error = producer.exception()
                producer = None
                if error is not None:
                    raise error
            if event_task not in done:
                event_task.cancel()
                await asyncio.gather(event_task, return_exceptions=True)
                continue

            event = event_task.result()
            kind = event.get("type")
            if kind == "telemetry":
                second = int(event.get("execution_cursor_us", 0)) // 1_000_000
                if second != last_telemetry_second:
                    print(
                        f"RUN t={event['execution_cursor_us'] / 1e6:.2f}s "
                        f"ahead={event['queued_ahead_us'] / 1e6:.2f}s "
                        f"error={event['tracking_error_deg']:.2f}°"
                    )
                    last_telemetry_second = second
            elif kind == "fault":
                print(
                    f"FAULT {event.get('code')}: {event.get('message')} "
                    f"[{event.get('safety_action')}]",
                    file=sys.stderr,
                )
            elif kind == "run_finished":
                outcome = event.get("outcome")
                print(f"finished: {outcome} ({event.get('fault_code') or 'no fault'})")
                return 0 if outcome == "completed" else 1
            elif kind == "error":
                raise RuntimeError(f"{event.get('code')}: {event.get('message')}")
            elif kind == "connection_closed":
                raise RuntimeError(f"connection closed: {event.get('message')}")
    except (KeyboardInterrupt, asyncio.CancelledError):
        if run_started:
            try:
                await client.send({"type": "cancel", "reason": "client interrupted"})
                await asyncio.sleep(0.1)
            except Exception:
                pass
        raise
    except Exception:
        if run_started:
            try:
                await client.send({"type": "cancel", "reason": "client-side failure"})
            except Exception:
                pass
        raise
    finally:
        for task in (heartbeat, producer):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (heartbeat, producer) if task is not None),
            return_exceptions=True,
        )
        await client.close()


async def one_shot(args: argparse.Namespace, token: str, message: dict[str, Any], reply: str) -> int:
    client = await RobotStreamClient.open(args.url, token)
    try:
        await client.send(message)
        result = await client.wait_for(reply)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0
    finally:
        await client.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="ws://10.115.253.69:8765")
    parser.add_argument("--token-env", default="ROBOT_SERVER_TOKEN")
    commands = parser.add_subparsers(dest="command", required=True)

    validate = commands.add_parser("validate", help="validate a program locally without a robot")
    validate.add_argument("program", type=Path)

    run = commands.add_parser("run", help="stream and execute a trajectory program")
    run.add_argument("program", type=Path)
    run.add_argument("--mode", choices=("sealed", "streaming"), default="sealed")
    run.add_argument("--run-id")
    run.add_argument("--chunk-points", type=int, default=64)
    run.add_argument("--target-ahead", type=float, default=5.0, help="streaming lookahead seconds")
    run.add_argument("--heartbeat-interval", type=float, default=0.5)
    run.add_argument(
        "--disconnect-policy",
        choices=("stop_after_timeout", "complete_if_sealed"),
        default="stop_after_timeout",
    )
    commands.add_parser("status", help="show current server/run state")
    commands.add_parser("stop", help="send an out-of-band emergency stop request")
    return parser


def validate_cli(path: Path) -> int:
    header, points = load_program(path, SafetyPolicy())
    print(
        json.dumps(
            {
                "valid": True,
                "program_id": header.program_id,
                "coordinate_mode": header.coordinate_mode,
                "points": len(points),
                "duration_s": points[-1].t_us / 1_000_000,
                "sha256": program_digest(header, points),
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "validate":
        try:
            return validate_cli(args.program)
        except ProtocolError as exc:
            print(f"{exc.code}: {exc.message}", file=sys.stderr)
            return 2

    token = os.environ.get(args.token_env, "")
    if len(token) < 32:
        raise SystemExit(f"{args.token_env} must match the server's 32+ character token")
    try:
        if args.command == "run":
            if not 1 <= args.chunk_points <= SafetyPolicy().max_chunk_points:
                raise SystemExit("--chunk-points is outside the server limit")
            if not 2.0 <= args.target_ahead <= 25.0:
                raise SystemExit("--target-ahead must be between 2 and 25 seconds")
            return asyncio.run(run_program(args, token))
        if args.command == "status":
            return asyncio.run(one_shot(args, token, {"type": "status"}, "status"))
        if args.command == "stop":
            return asyncio.run(
                one_shot(args, token, {"type": "emergency_stop"}, "stop_accepted")
            )
    except KeyboardInterrupt:
        return 130
    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
