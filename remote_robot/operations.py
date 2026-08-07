"""Read-only operational projection for dashboards and diagnostic clients.

``OperationsProjection`` is the deep Module between mutable runtime internals
and observers. Callers learn only two operations: obtain a self-consistent
snapshot, or subscribe to changed snapshots. Registry traversal, lease
correlation, journal reduction, worker polling, freshness, and redaction stay
inside the implementation.
"""

from __future__ import annotations

import asyncio
from collections import deque
import copy
from dataclasses import dataclass
import hashlib
import json
import time
from typing import Any, AsyncIterator, Mapping

from .journal import RunJournal
from .lease import LeaseManager, LeaseReceipt
from .worker import WorkerPort


OPERATIONS_FORMAT = "remote-robot-operations"
OPERATIONS_VERSION = 1
DEFAULT_RECENT_RUNS = 24
DEFAULT_RECENT_EVENTS = 80


class OperationsError(RuntimeError):
    """Operational state could not be projected safely."""


@dataclass(frozen=True, slots=True)
class OperationsEvent:
    event_seq: int
    recorded_at_ns: int
    event_type: str
    snapshot: Mapping[str, Any]

    def public_dict(self) -> dict[str, Any]:
        return {
            "event_seq": self.event_seq,
            "recorded_at_ns": self.recorded_at_ns,
            "type": self.event_type,
            "snapshot": copy.deepcopy(dict(self.snapshot)),
        }


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, bytes):
        return {"encoding": "hex", "value": value.hex()}
    if isinstance(value, Mapping):
        return {str(key): _json_safe(child) for key, child in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_safe(child) for child in value]
    return str(value)


def _fingerprint(value: Mapping[str, Any]) -> str:
    def stable(child: Any) -> Any:
        if isinstance(child, Mapping):
            return {
                str(key): stable(item)
                for key, item in child.items()
                if key not in {"polled_at_ns"}
            }
        if isinstance(child, (list, tuple, set, frozenset)):
            return [stable(item) for item in child]
        return child

    payload = json.dumps(
        _json_safe(stable(value)),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _worker_display_state(raw_status: str) -> str:
    return {
        "created": "offline",
        "disabled": "idle",
        "stopped": "idle",
        "completed": "idle",
        "prepared": "preparing",
        "scheduled": "scheduled",
        "running": "running",
        "closed": "offline",
    }.get(raw_status, "unknown")


def _run_display_state(event_type: str | None, *, active: bool) -> str:
    if event_type == "run.completed":
        return "completed"
    if event_type == "run.faulted":
        return "faulted"
    if event_type == "phase.started":
        return "running"
    if event_type == "phase.scheduled":
        return "scheduled"
    if event_type in {
        "run.opened",
        "workers.disabled",
        "phase.prepared",
        "phase.completed",
    }:
        return "preparing" if active else "interrupted"
    return "occupied" if active else "unknown"


_STATE_PRIORITY = {
    "faulted": 90,
    "running": 80,
    "scheduled": 70,
    "preparing": 60,
    "occupied": 50,
    "offline": 40,
    "unknown": 30,
    "idle": 10,
}


class OperationsProjection:
    """Project registry, leases, journals, and workers into observer-safe state."""

    def __init__(
        self,
        registry: Any,
        lease_manager: LeaseManager,
        journal: RunJournal,
        workers: Mapping[str, WorkerPort] | None = None,
        *,
        execution_available: bool = False,
        event_capacity: int = 256,
        recent_runs: int = DEFAULT_RECENT_RUNS,
        recent_events: int = DEFAULT_RECENT_EVENTS,
    ) -> None:
        if event_capacity <= 0 or recent_runs <= 0 or recent_events <= 0:
            raise ValueError("operations projection capacities must be positive")
        self.registry = registry
        self.lease_manager = lease_manager
        self.journal = journal
        self.workers = dict(workers or {})
        self.execution_available = bool(execution_available)
        self.recent_runs = recent_runs
        self.recent_events = recent_events
        self._started_monotonic_ns = time.monotonic_ns()
        self._events: deque[OperationsEvent] = deque(maxlen=event_capacity)
        self._event_seq = 0
        self._snapshot: dict[str, Any] | None = None
        self._state_fingerprint: str | None = None
        self._refresh_lock = asyncio.Lock()
        self._condition = asyncio.Condition()
        self._closed = False

    async def snapshot(self) -> dict[str, Any]:
        """Return a fresh, self-consistent snapshot with no private config."""

        await self.refresh()
        if self._snapshot is None:  # pragma: no cover - defensive invariant
            raise OperationsError("operations snapshot was not initialized")
        return copy.deepcopy(self._snapshot)

    async def refresh(self) -> bool:
        """Poll workers and publish one event only when meaningful state changed."""

        if self._closed:
            raise OperationsError("operations projection is closed")
        async with self._refresh_lock:
            worker_states = await self._poll_workers()
            state = self._build_state(worker_states)
            fingerprint = _fingerprint(state)
            changed = fingerprint != self._state_fingerprint
            if changed:
                self._event_seq += 1
            snapshot = {
                **state,
                "generated_at_ns": time.time_ns(),
                "uptime_ms": (time.monotonic_ns() - self._started_monotonic_ns)
                // 1_000_000,
                "operations_event_seq": self._event_seq,
            }
            self._snapshot = snapshot
            if changed:
                self._state_fingerprint = fingerprint
                event = OperationsEvent(
                    event_seq=self._event_seq,
                    recorded_at_ns=time.time_ns(),
                    event_type="operations.snapshot",
                    snapshot=copy.deepcopy(snapshot),
                )
                self._events.append(event)
                async with self._condition:
                    self._condition.notify_all()
            return changed

    async def subscribe(self, after_event_seq: int = 0) -> AsyncIterator[OperationsEvent]:
        """Yield changed snapshots after ``after_event_seq`` until closed."""

        if isinstance(after_event_seq, bool) or not isinstance(after_event_seq, int):
            raise TypeError("after_event_seq must be an integer")
        if after_event_seq < 0:
            raise ValueError("after_event_seq must be non-negative")
        cursor = after_event_seq
        while True:
            async with self._condition:
                pending = [event for event in self._events if event.event_seq > cursor]
                while not pending and not self._closed:
                    await self._condition.wait()
                    pending = [
                        event for event in self._events if event.event_seq > cursor
                    ]
                if self._closed:
                    return
            for event in pending:
                cursor = event.event_seq
                yield OperationsEvent(
                    event.event_seq,
                    event.recorded_at_ns,
                    event.event_type,
                    copy.deepcopy(dict(event.snapshot)),
                )

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        async with self._condition:
            self._condition.notify_all()

    async def _poll_workers(self) -> dict[str, dict[str, Any]]:
        async def poll(worker_id: str, worker: WorkerPort) -> tuple[str, dict[str, Any]]:
            try:
                async with asyncio.timeout(1.0):
                    state = await worker.state()
                return worker_id, {
                    "reachable": True,
                    "polled_at_ns": time.time_ns(),
                    "state": _json_safe(state),
                }
            except BaseException as exc:
                if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                    raise
                return worker_id, {
                    "reachable": False,
                    "polled_at_ns": time.time_ns(),
                    "error_type": type(exc).__name__,
                }

        results = await asyncio.gather(
            *(poll(worker_id, worker) for worker_id, worker in sorted(self.workers.items()))
        )
        return dict(results)

    def _build_state(self, worker_states: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
        grants = self.lease_manager.active_grants()
        runs, recent_events = self._project_runs(grants)
        node_records: dict[str, dict[str, Any]] = {}
        manifests = {
            node_id: self.registry.describe(node_id) for node_id in self.registry.node_ids
        }
        leaf_cache: dict[str, frozenset[str]] = {}

        def leaves_for(node_id: str) -> frozenset[str]:
            cached = leaf_cache.get(node_id)
            if cached is not None:
                return cached
            manifest = manifests[node_id]
            if manifest.get("kind") == "physical":
                result = frozenset(
                    f"{node_id}.{group['group_id']}"
                    for group in manifest.get("groups", [])
                    if isinstance(group, Mapping) and isinstance(group.get("group_id"), str)
                )
            else:
                result = frozenset(
                    leaf
                    for child in manifest.get("children", [])
                    if isinstance(child, Mapping)
                    for leaf in leaves_for(str(child.get("node_id")))
                )
            leaf_cache[node_id] = result
            return result

        def occupying_runs(node_id: str) -> list[str]:
            manifest = manifests[node_id]
            leaves = leaves_for(node_id)
            resources = set(manifest.get("resources", []))
            domains = set(manifest.get("safety_domains", []))
            return sorted(
                receipt.run_id
                for receipt in grants
                if leaves.intersection(receipt.leaves)
                or resources.intersection(receipt.resources)
                or domains.intersection(receipt.safety_domains)
            )

        runtime_cache: dict[str, dict[str, Any] | None] = {}
        display_cache: dict[str, str] = {}

        def runtime_for(node_id: str) -> dict[str, Any]:
            cached = runtime_cache.get(node_id)
            if cached is not None:
                return cached
            polled = worker_states.get(node_id)
            if polled is None:
                runtime = {
                    "reachable": False,
                    "registered": False,
                    "display_state": "offline",
                    "freshness": "unavailable",
                }
            elif polled.get("reachable") is True:
                raw = polled.get("state", {})
                raw_status = (
                    str(raw.get("status", "unknown"))
                    if isinstance(raw, Mapping)
                    else "unknown"
                )
                runtime = {
                    **copy.deepcopy(dict(polled)),
                    "registered": True,
                    "display_state": _worker_display_state(raw_status),
                    "freshness": "fresh",
                }
            else:
                runtime = {
                    **copy.deepcopy(dict(polled)),
                    "registered": True,
                    "display_state": "faulted",
                    "freshness": "stale",
                }
            runtime_cache[node_id] = runtime
            return runtime

        def display_for(node_id: str) -> str:
            cached = display_cache.get(node_id)
            if cached is not None:
                return cached
            manifest = manifests[node_id]
            occupied_by = occupying_runs(node_id)
            if manifest.get("kind") == "physical":
                state = runtime_for(node_id)["display_state"]
            else:
                child_states = [
                    display_for(str(child.get("node_id")))
                    for child in manifest.get("children", [])
                    if isinstance(child, Mapping)
                ]
                state = max(
                    child_states or ["idle"],
                    key=lambda value: _STATE_PRIORITY.get(value, 0),
                )
            if occupied_by and state in {"idle", "offline"}:
                state = "occupied"
            display_cache[node_id] = state
            return state

        for node_id in self.registry.node_ids:
            manifest = manifests[node_id]
            kind = str(manifest.get("kind", "unknown"))
            occupied_by = occupying_runs(node_id)
            if kind == "physical":
                runtime = runtime_for(node_id)
            else:
                runtime = None
            display_state = display_for(node_id)

            safety = manifest.get("safety", {})
            watchdog = manifest.get("watchdog")
            assurances = manifest.get("assurances", {})
            attention: list[str] = []
            if isinstance(safety, Mapping) and safety.get("cross_collision_checked") is False:
                attention.append("cross_collision_unchecked")
            unattended = (
                assurances.get("unattended_operation_allowed")
                if isinstance(assurances, Mapping)
                else None
            )
            if unattended is None and isinstance(watchdog, Mapping):
                unattended = watchdog.get("unattended_operation_allowed")
            if unattended is False:
                attention.append("supervised_only")

            node_records[node_id] = {
                "node_id": node_id,
                "kind": kind,
                "revision": manifest.get("revision"),
                "manifest_hash": manifest.get("manifest_hash"),
                "exposed_as_run_root": manifest.get("exposed_as_run_root") is True,
                "display_state": display_state,
                "occupied_by": occupied_by,
                "canonical_leaves": sorted(leaves_for(node_id)),
                "resources": list(manifest.get("resources", [])),
                "safety_domains": list(manifest.get("safety_domains", [])),
                "safety": _json_safe(safety),
                "watchdog": _json_safe(watchdog),
                "assurances": _json_safe(assurances),
                "attention": attention,
                "groups": _json_safe(manifest.get("groups", [])),
                "children": [
                    {
                        "role": child.get("role"),
                        "node_id": child.get("node_id"),
                        "alias": child.get("alias") is True,
                    }
                    for child in manifest.get("children", [])
                    if isinstance(child, Mapping)
                ],
                "runtime": runtime,
                "telemetry": {
                    "available": False,
                    "last_update_ns": None,
                    "freshness": "unavailable",
                },
            }

        roots = [
            node_records[node_id]
            for node_id in self.registry.node_ids
            if node_records[node_id]["exposed_as_run_root"]
        ]
        active_runs = [run for run in runs if run["active"]]
        return {
            "format": OPERATIONS_FORMAT,
            "version": OPERATIONS_VERSION,
            "server": {
                "server_id": self.registry.server_id,
                "protocol": "robot-stream.v2",
                "execution_available": self.execution_available,
                "dashboard_read_only": True,
            },
            "summary": {
                "root_count": len(roots),
                "physical_count": sum(
                    record["kind"] == "physical" for record in node_records.values()
                ),
                "active_run_count": len(active_runs),
                "occupied_leaf_count": len(
                    {leaf for receipt in grants for leaf in receipt.leaves}
                ),
                "faulted_run_count": sum(run["state"] == "faulted" for run in runs),
                "attention_count": sum(
                    len(record["attention"]) for record in node_records.values()
                ),
            },
            "roots": roots,
            "nodes": [node_records[node_id] for node_id in self.registry.node_ids],
            "leases": [self._lease_dict(receipt) for receipt in grants],
            "runs": runs,
            "recent_events": recent_events,
        }

    def _project_runs(
        self, grants: tuple[LeaseReceipt, ...]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        active_by_run = {receipt.run_id: receipt for receipt in grants}
        run_records: list[dict[str, Any]] = []
        all_events: list[dict[str, Any]] = []
        for run_id in self.journal.run_ids():
            events = self.journal.replay(run_id)
            if not events:
                continue
            opened = next((event for event in events if event.event_type == "run.opened"), None)
            latest = events[-1]
            current_phase = next(
                (
                    event.payload.get("phase_id")
                    for event in reversed(events)
                    if event.payload.get("phase_id") is not None
                ),
                None,
            )
            active = run_id in active_by_run
            run_records.append(
                {
                    "run_id": run_id,
                    "identity_id": None if opened is None else opened.payload.get("identity_id"),
                    "root_id": None if opened is None else opened.payload.get("root_id"),
                    "program_sha256": None
                    if opened is None
                    else opened.payload.get("program_sha256"),
                    "state": _run_display_state(latest.event_type, active=active),
                    "active": active,
                    "current_phase": current_phase,
                    "latest_event_type": latest.event_type,
                    "latest_event_seq": latest.event_seq,
                    "updated_at_ns": latest.recorded_at_ns,
                    "event_count": len(events),
                }
            )
            all_events.extend(
                {
                    "run_id": event.run_id,
                    "event_seq": event.event_seq,
                    "recorded_at_ns": event.recorded_at_ns,
                    "type": event.event_type,
                    "phase_id": event.payload.get("phase_id"),
                    "error_type": event.payload.get("error_type"),
                }
                for event in events
            )

        for run_id, receipt in active_by_run.items():
            if any(record["run_id"] == run_id for record in run_records):
                continue
            run_records.append(
                {
                    "run_id": run_id,
                    "identity_id": None,
                    "root_id": None,
                    "program_sha256": None,
                    "state": "occupied",
                    "active": True,
                    "current_phase": None,
                    "latest_event_type": None,
                    "latest_event_seq": 0,
                    "updated_at_ns": time.time_ns()
                    - max(0, time.monotonic_ns() - receipt.acquired_at_monotonic_ns),
                    "event_count": 0,
                }
            )

        run_records.sort(key=lambda run: (run["updated_at_ns"], run["run_id"]), reverse=True)
        all_events.sort(
            key=lambda event: (event["recorded_at_ns"], event["run_id"], event["event_seq"]),
            reverse=True,
        )
        return (
            run_records[: self.recent_runs],
            all_events[: self.recent_events],
        )

    @staticmethod
    def _lease_dict(receipt: LeaseReceipt) -> dict[str, Any]:
        return {
            "run_id": receipt.run_id,
            "leaves": sorted(receipt.leaves),
            "resources": sorted(receipt.resources),
            "safety_domains": sorted(receipt.safety_domains),
            "acquired_at_monotonic_ns": receipt.acquired_at_monotonic_ns,
        }
