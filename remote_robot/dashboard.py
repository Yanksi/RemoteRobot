"""Loopback-only read-only operations dashboard over HTTP and SSE."""

from __future__ import annotations

import asyncio
import contextlib
import ipaddress
import json
from pathlib import Path
from typing import Any

from aiohttp import web

from .operations import OperationsProjection


ASSET_ROOT = Path(__file__).with_name("dashboard_assets")


def _loopback_host(value: str) -> str:
    try:
        address = ipaddress.ip_address(value)
    except ValueError as exc:
        raise ValueError("dashboard host must be a loopback IP literal") from exc
    if not address.is_loopback:
        raise ValueError("dashboard is read-only but must still bind to loopback")
    return value


@web.middleware
async def _security_headers(
    request: web.Request, handler: Any
) -> web.StreamResponse:
    response = await handler(request)
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; base-uri 'none'; frame-ancestors 'none'; "
        "connect-src 'self'; img-src 'self' data:; script-src 'self'; style-src 'self'"
    )
    return response


class DashboardServer:
    """HTTP/SSE Adapter for ``OperationsProjection``; never accepts commands."""

    def __init__(
        self,
        projection: OperationsProjection,
        *,
        host: str = "127.0.0.1",
        port: int = 8080,
        refresh_interval_s: float = 1.0,
    ) -> None:
        self.projection = projection
        self.host = _loopback_host(host)
        if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65535:
            raise ValueError("dashboard port must be between 0 and 65535")
        if refresh_interval_s < 0.1:
            raise ValueError("dashboard refresh interval must be at least 0.1 seconds")
        self.port = port
        self.refresh_interval_s = float(refresh_interval_s)
        self.bound_port: int | None = None
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._poll_task: asyncio.Task[None] | None = None
        self._closed = False
        self.app = web.Application(middlewares=[_security_headers])
        self.app.add_routes(
            [
                web.get("/", self._index),
                web.get("/favicon.svg", self._favicon),
                web.get("/assets/dashboard.css", self._css),
                web.get("/assets/dashboard.js", self._javascript),
                web.get("/api/health", self._health),
                web.get("/api/snapshot", self._snapshot),
                web.get("/api/events", self._events),
            ]
        )

    @property
    def url(self) -> str:
        if self.bound_port is None:
            raise RuntimeError("dashboard has not started")
        host = f"[{self.host}]" if ":" in self.host else self.host
        return f"http://{host}:{self.bound_port}"

    async def start(self) -> None:
        if self._runner is not None:
            return
        if self._closed:
            raise RuntimeError("dashboard cannot restart after close")
        await self.projection.refresh()
        self._runner = web.AppRunner(self.app, access_log=None)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self.host, self.port)
        try:
            await self._site.start()
            server = self._site._server
            if server is None or not server.sockets:
                raise RuntimeError("dashboard didn't expose a listening socket")
            self.bound_port = int(server.sockets[0].getsockname()[1])
            self._poll_task = asyncio.create_task(
                self._poll(), name="operations-dashboard-poll"
            )
        except BaseException:
            await self._runner.cleanup()
            self._runner = None
            self._site = None
            raise

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._poll_task is not None:
            self._poll_task.cancel()
            await asyncio.gather(self._poll_task, return_exceptions=True)
            self._poll_task = None
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
            self._site = None
        self.bound_port = None

    async def _poll(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.refresh_interval_s)
                await self.projection.refresh()
            except asyncio.CancelledError:
                return
            except Exception:
                # The next poll may recover. Individual worker failures are
                # represented in the projection rather than raised here.
                await asyncio.sleep(self.refresh_interval_s)

    @staticmethod
    def _asset(name: str) -> web.FileResponse:
        path = ASSET_ROOT / name
        if not path.is_file():
            raise web.HTTPNotFound()
        return web.FileResponse(path)

    async def _index(self, request: web.Request) -> web.StreamResponse:
        del request
        return self._asset("index.html")

    async def _favicon(self, request: web.Request) -> web.StreamResponse:
        del request
        return self._asset("favicon.svg")

    async def _css(self, request: web.Request) -> web.StreamResponse:
        del request
        return self._asset("dashboard.css")

    async def _javascript(self, request: web.Request) -> web.StreamResponse:
        del request
        return self._asset("dashboard.js")

    async def _health(self, request: web.Request) -> web.Response:
        del request
        snapshot = await self.projection.snapshot()
        return web.json_response(
            {
                "ok": True,
                "read_only": True,
                "server_id": snapshot["server"]["server_id"],
                "operations_event_seq": snapshot["operations_event_seq"],
                "generated_at_ns": snapshot["generated_at_ns"],
            }
        )

    async def _snapshot(self, request: web.Request) -> web.Response:
        del request
        return web.json_response(await self.projection.snapshot())

    async def _events(self, request: web.Request) -> web.StreamResponse:
        raw_cursor = request.headers.get("Last-Event-ID", request.query.get("after", "0"))
        try:
            after_event_seq = int(raw_cursor)
        except ValueError as exc:
            raise web.HTTPBadRequest(text="Last-Event-ID must be an integer") from exc
        if after_event_seq < 0:
            raise web.HTTPBadRequest(text="Last-Event-ID must be non-negative")

        response = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "text/event-stream; charset=utf-8",
                "Cache-Control": "no-cache, no-store",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )
        await response.prepare(request)
        stream = self.projection.subscribe(after_event_seq)
        next_event = asyncio.create_task(anext(stream))
        try:
            while True:
                done, _ = await asyncio.wait({next_event}, timeout=15.0)
                if not done:
                    await response.write(b": operations keepalive\n\n")
                    continue
                event = next_event.result()
                payload = json.dumps(
                    event.public_dict(),
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                frame = (
                    f"id: {event.event_seq}\n"
                    f"event: operations\n"
                    f"data: {payload}\n\n"
                ).encode("utf-8")
                await response.write(frame)
                next_event = asyncio.create_task(anext(stream))
        except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError, StopAsyncIteration):
            return response
        finally:
            next_event.cancel()
            await asyncio.gather(next_event, return_exceptions=True)
            with contextlib.suppress(RuntimeError):
                await stream.aclose()
