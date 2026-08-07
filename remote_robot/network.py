"""Robot Stream v2 management connection and discovery client.

Run execution deliberately isn't multiplexed onto this connection. Each future
active run gets its own stream; management remains responsive even when a robot
produces heavy telemetry or a worker faults.
"""

from __future__ import annotations

import asyncio
import hmac
import json
import os
from typing import Any, Mapping

from websockets.asyncio.client import ClientConnection, connect
from websockets.asyncio.server import ServerConnection
from websockets.exceptions import ConnectionClosed

from .registry import CapabilityRegistry, RegistryError


PROTOCOL_V2 = "robot-stream.v2"
MAX_MANAGEMENT_MESSAGE = 4 * 1024 * 1024


class NetworkError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


def _decode(raw: str | bytes) -> dict[str, Any]:
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise NetworkError("SCHEMA_INVALID", "management binary frame isn't UTF-8") from exc
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise NetworkError("SCHEMA_INVALID", f"invalid JSON: {exc.msg}") from exc
    if not isinstance(value, dict):
        raise NetworkError("SCHEMA_INVALID", "message must be an object")
    return value


def _exact(value: Mapping[str, Any], required: set[str], optional: set[str] = frozenset()) -> None:
    missing = required - set(value)
    extra = set(value) - required - optional
    if missing or extra:
        raise NetworkError(
            "SCHEMA_INVALID",
            f"message fields mismatch; missing={sorted(missing)}, unexpected={sorted(extra)}",
        )


def _response(request_id: int, result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "type": "response",
        "request_id": request_id,
        "ok": True,
        "result": dict(result),
    }


def _error(request_id: int | None, code: str, message: str) -> dict[str, Any]:
    return {
        "type": "response" if request_id is not None else "error",
        "request_id": request_id,
        "ok": False,
        "error": {"code": code, "message": message},
    }


class ManagementEndpoint:
    """Server-side discovery Module with local registry-owned authorization."""

    def __init__(self, registry: CapabilityRegistry) -> None:
        self.registry = registry

    def authenticate(self, identity_id: str, supplied_token: str) -> None:
        try:
            identity = self.registry.identity(identity_id)
        except RegistryError as exc:
            raise NetworkError("AUTH_FAILED", "unknown identity or invalid credential") from exc
        expected = os.environ.get(identity.credential_env, "")
        if len(expected) < 32 or not hmac.compare_digest(supplied_token, expected):
            raise NetworkError("AUTH_FAILED", "unknown identity or invalid credential")

    def dispatch(
        self,
        identity_id: str,
        method: str,
        params: Mapping[str, Any],
    ) -> dict[str, Any]:
        if method == "list_robots":
            _exact(params, set())
            return {"robots": self.registry.list_robots(identity_id)}
        if method == "describe_robot":
            _exact(params, {"node_id"})
            node_id = params["node_id"]
            if not isinstance(node_id, str):
                raise NetworkError("SCHEMA_INVALID", "node_id must be a string")
            try:
                manifest = self.registry.describe(node_id, identity_id)
            except RegistryError as exc:
                raise NetworkError("NOT_FOUND", "robot isn't visible to this identity") from exc
            return {"manifest": manifest}
        if method == "server_info":
            _exact(params, set())
            return {
                "protocol": PROTOCOL_V2,
                "server_id": self.registry.server_id,
                "capabilities": ["registry-discovery/v1"],
                "execution_available": False,
            }
        raise NetworkError("METHOD_NOT_FOUND", f"unknown management method {method!r}")

    async def handle(self, websocket: ServerConnection) -> None:
        identity_id: str | None = None
        try:
            raw = await asyncio.wait_for(websocket.recv(), timeout=5.0)
            hello = _decode(raw)
            _exact(hello, {"type", "protocol", "role", "identity_id", "token"})
            if (
                hello["type"] != "hello"
                or hello["protocol"] != PROTOCOL_V2
                or hello["role"] != "management"
                or not isinstance(hello["identity_id"], str)
                or not isinstance(hello["token"], str)
            ):
                raise NetworkError("AUTH_FAILED", "management negotiation failed")
            identity_id = hello["identity_id"]
            self.authenticate(identity_id, hello["token"])
            await websocket.send(
                json.dumps(
                    {
                        "type": "welcome",
                        "protocol": PROTOCOL_V2,
                        "role": "management",
                        "server_id": self.registry.server_id,
                        "identity_id": identity_id,
                        "capabilities": ["registry-discovery/v1"],
                        "execution_available": False,
                    },
                    separators=(",", ":"),
                )
            )
            async for raw in websocket:
                request_id: int | None = None
                try:
                    request = _decode(raw)
                    _exact(request, {"type", "request_id", "method", "params"})
                    request_id = request["request_id"]
                    if (
                        request["type"] != "request"
                        or isinstance(request_id, bool)
                        or not isinstance(request_id, int)
                        or request_id <= 0
                        or not isinstance(request["method"], str)
                        or not isinstance(request["params"], dict)
                    ):
                        raise NetworkError("SCHEMA_INVALID", "invalid request envelope")
                    result = self.dispatch(identity_id, request["method"], request["params"])
                    message = _response(request_id, result)
                except (NetworkError, RegistryError) as exc:
                    code = getattr(exc, "code", "REGISTRY_ERROR")
                    message = _error(request_id, code, str(exc))
                await websocket.send(json.dumps(message, ensure_ascii=False, separators=(",", ":")))
        except NetworkError as exc:
            await websocket.send(json.dumps(_error(None, exc.code, exc.message), separators=(",", ":")))
            await websocket.close(code=1008, reason="authentication failed")
        except (ConnectionClosed, asyncio.TimeoutError):
            return


class RegistryClient:
    """High-level caller interface for authorized v2 discovery."""

    def __init__(self, websocket: ClientConnection, welcome: Mapping[str, Any]) -> None:
        self.websocket = websocket
        self.welcome = dict(welcome)
        self._next_request_id = 1
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._closed = False
        self._reader_task = asyncio.create_task(self._reader(), name="v2-management-reader")

    @classmethod
    async def connect(cls, url: str, identity_id: str, token: str) -> "RegistryClient":
        websocket = await connect(
            url,
            subprotocols=[PROTOCOL_V2],
            open_timeout=5,
            ping_interval=20,
            ping_timeout=20,
            max_size=MAX_MANAGEMENT_MESSAGE,
            proxy=None,
        )
        try:
            await websocket.send(
                json.dumps(
                    {
                        "type": "hello",
                        "protocol": PROTOCOL_V2,
                        "role": "management",
                        "identity_id": identity_id,
                        "token": token,
                    },
                    separators=(",", ":"),
                )
            )
            raw = await asyncio.wait_for(websocket.recv(), timeout=5.0)
            welcome = _decode(raw)
            if welcome.get("type") == "error":
                error = welcome.get("error", {})
                raise NetworkError(str(error.get("code", "AUTH_FAILED")), str(error.get("message", "")))
            if welcome.get("type") != "welcome" or welcome.get("protocol") != PROTOCOL_V2:
                raise NetworkError("NEGOTIATION_FAILED", "server didn't return a v2 welcome")
            return cls(websocket, welcome)
        except BaseException:
            await websocket.close()
            raise

    async def request(self, method: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        if self._closed:
            raise NetworkError("CONNECTION_CLOSED", "management client is closed")
        request_id = self._next_request_id
        self._next_request_id += 1
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        await self.websocket.send(
            json.dumps(
                {
                    "type": "request",
                    "request_id": request_id,
                    "method": method,
                    "params": dict(params or {}),
                },
                separators=(",", ":"),
            )
        )
        try:
            async with asyncio.timeout(5.0):
                return await asyncio.shield(future)
        except TimeoutError as exc:
            self._pending.pop(request_id, None)
            future.cancel()
            raise NetworkError("TIMEOUT", f"management method {method!r} timed out") from exc

    async def list_robots(self) -> list[dict[str, Any]]:
        return list((await self.request("list_robots"))["robots"])

    async def describe_robot(self, node_id: str) -> dict[str, Any]:
        return dict((await self.request("describe_robot", {"node_id": node_id}))["manifest"])

    async def server_info(self) -> dict[str, Any]:
        return await self.request("server_info")

    async def _reader(self) -> None:
        error: BaseException | None = None
        try:
            async for raw in self.websocket:
                message = _decode(raw)
                if message.get("type") != "response":
                    continue
                request_id = message.get("request_id")
                if not isinstance(request_id, int):
                    continue
                future = self._pending.pop(request_id, None)
                if future is None or future.done():
                    continue
                if message.get("ok") is True and isinstance(message.get("result"), dict):
                    future.set_result(message["result"])
                else:
                    remote = message.get("error", {})
                    future.set_exception(
                        NetworkError(
                            str(remote.get("code", "REMOTE_ERROR")),
                            str(remote.get("message", "management request failed")),
                        )
                    )
        except BaseException as exc:
            error = exc
        finally:
            failure = error or NetworkError("CONNECTION_CLOSED", "management connection closed")
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(failure)
            self._pending.clear()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self.websocket.close()
        if not self._reader_task.done():
            self._reader_task.cancel()
        await asyncio.gather(self._reader_task, return_exceptions=True)
