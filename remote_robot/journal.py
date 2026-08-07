"""Append-only run event stores used by the v2 orchestrator.

``InMemoryRunJournal`` and ``FileRunJournal`` intentionally expose the same
small multi-run interface.  A file store opened after a server restart makes
all recovered run IDs read-only: their evidence can be replayed, but the
journal can never be treated as authority to resume physical execution.
"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterator, Mapping
import copy
from dataclasses import dataclass
import hashlib
import math
import os
from pathlib import Path
import struct
import threading
import time
from typing import Any, Protocol, runtime_checkable

import cbor2


_MAGIC = b"RRJ2"
_FORMAT = "remote-robot-run-journal"
_FORMAT_VERSION = 2
_MAX_FRAME_BYTES = 16 * 1024 * 1024
_RESERVED_EVENT_KEYS = {"run_id", "event_seq", "recorded_at_ns"}
_FORBIDDEN_KEYS = {
    "api_key",
    "authorization",
    "cookie",
    "credential",
    "credentials",
    "password",
    "private_key",
    "refresh_token",
    "secret",
    "serial_credentials",
    "set_cookie",
    "token",
}


class SecretDataError(ValueError):
    """Journal input looked like authentication or connection secret data."""


class JournalCorruptionError(RuntimeError):
    """A file journal is truncated, altered, or structurally invalid."""


class JournalReadOnlyError(RuntimeError):
    """A recovered run cannot receive new events after server restart."""


def _validate_run_id(run_id: str) -> str:
    if not isinstance(run_id, str):
        raise TypeError("run_id must be a string")
    if not run_id or run_id != run_id.strip():
        raise ValueError("run_id must be a non-empty canonical string")
    if len(run_id) > 512 or any(ord(character) < 32 for character in run_id):
        raise ValueError("run_id contains invalid characters")
    return run_id


def _looks_secret(key: str) -> bool:
    normalized = key.casefold().replace("-", "_").replace(" ", "_")
    return normalized in _FORBIDDEN_KEYS or normalized.endswith(
        ("_password", "_secret", "_credential", "_credentials", "_token")
    )


def _validate_plain_data(value: Any, path: str = "event") -> None:
    if value is None or isinstance(value, (bool, int, str, bytes)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} contains a non-finite float")
        return
    if isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} mapping keys must be strings")
            if _looks_secret(key):
                raise SecretDataError(f"refusing to journal secret-like field {path}.{key}")
            _validate_plain_data(child, f"{path}.{key}")
        return
    if isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            _validate_plain_data(child, f"{path}[{index}]")
        return
    raise TypeError(f"{path} contains unsupported value type {type(value).__name__}")


def _event_data(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("event must be a mapping")
    overlap = _RESERVED_EVENT_KEYS.intersection(value)
    if overlap:
        raise ValueError(f"event cannot supply reserved fields: {sorted(overlap)}")
    _validate_plain_data(value)
    return copy.deepcopy(dict(value))


@dataclass(frozen=True, slots=True)
class JournalEvent(Mapping[str, Any]):
    """One reliable event, usable through attributes or mapping access."""

    run_id: str
    event_seq: int
    recorded_at_ns: int
    event: Mapping[str, Any]

    @property
    def payload(self) -> Mapping[str, Any]:
        return self.event

    @property
    def event_type(self) -> str | None:
        value = self.event.get("type")
        return value if isinstance(value, str) else None

    def __getitem__(self, key: str) -> Any:
        if key == "run_id":
            return self.run_id
        if key == "event_seq":
            return self.event_seq
        if key == "recorded_at_ns":
            return self.recorded_at_ns
        return self.event[key]

    def __iter__(self) -> Iterator[str]:
        yield "run_id"
        yield "event_seq"
        yield "recorded_at_ns"
        yield from self.event

    def __len__(self) -> int:
        return 3 + len(self.event)

    def _record(self) -> dict[str, Any]:
        return {
            "version": _FORMAT_VERSION,
            "run_id": self.run_id,
            "event_seq": self.event_seq,
            "recorded_at_ns": self.recorded_at_ns,
            "event": dict(self.event),
        }


def _copy_event(event: JournalEvent) -> JournalEvent:
    return JournalEvent(
        event.run_id,
        event.event_seq,
        event.recorded_at_ns,
        copy.deepcopy(dict(event.event)),
    )


@runtime_checkable
class RunJournal(Protocol):
    """Storage seam shared by production and simulator orchestrators."""

    def append(self, run_id: str, event: Mapping[str, Any]) -> JournalEvent: ...

    def replay(
        self, run_id: str, after_event_seq: int = 0
    ) -> tuple[JournalEvent, ...]: ...

    def latest(self, run_id: str) -> JournalEvent | None: ...

    def run_ids(self) -> tuple[str, ...]: ...

    def close(self) -> None: ...


class _JournalBase:
    def __init__(
        self,
        *,
        events_by_run: Mapping[str, list[JournalEvent]] | None = None,
        recovered_run_ids: frozenset[str] = frozenset(),
    ) -> None:
        self._events_by_run = {
            run_id: list(events) for run_id, events in (events_by_run or {}).items()
        }
        self._recovered_run_ids = recovered_run_ids
        self._lock = threading.RLock()
        self._closed = False

    @property
    def recovered_run_ids(self) -> frozenset[str]:
        return self._recovered_run_ids

    @property
    def execution_resume_allowed(self) -> bool:
        return False

    def append(self, run_id: str, event: Mapping[str, Any]) -> JournalEvent:
        owner = _validate_run_id(run_id)
        safe_event = _event_data(event)
        with self._lock:
            if self._closed:
                raise ValueError("journal is closed")
            if owner in self._recovered_run_ids:
                raise JournalReadOnlyError(
                    f"run {owner!r} was recovered after restart and cannot be resumed"
                )
            events = self._events_by_run.setdefault(owner, [])
            item = JournalEvent(
                run_id=owner,
                event_seq=len(events) + 1,
                recorded_at_ns=time.time_ns(),
                event=safe_event,
            )
            self._persist_record(item._record())
            events.append(item)
            return _copy_event(item)

    @abstractmethod
    def _persist_record(self, record: Mapping[str, Any]) -> None:
        raise NotImplementedError

    def replay(
        self, run_id: str, after_event_seq: int = 0
    ) -> tuple[JournalEvent, ...]:
        owner = _validate_run_id(run_id)
        if isinstance(after_event_seq, bool) or not isinstance(after_event_seq, int):
            raise TypeError("after_event_seq must be an integer")
        if after_event_seq < 0:
            raise ValueError("after_event_seq must be non-negative")
        with self._lock:
            return tuple(
                _copy_event(event)
                for event in self._events_by_run.get(owner, ())
                if event.event_seq > after_event_seq
            )

    def latest(self, run_id: str) -> JournalEvent | None:
        owner = _validate_run_id(run_id)
        with self._lock:
            events = self._events_by_run.get(owner)
            return _copy_event(events[-1]) if events else None

    def run_ids(self) -> tuple[str, ...]:
        """Return a stable diagnostic index without exposing journal storage."""

        with self._lock:
            return tuple(sorted(self._events_by_run))

    def close(self) -> None:
        with self._lock:
            self._closed = True

    def __enter__(self) -> _JournalBase:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class InMemoryRunJournal(_JournalBase):
    def __init__(self) -> None:
        super().__init__()

    def _persist_record(self, record: Mapping[str, Any]) -> None:
        del record


def _canonical_cbor(value: Mapping[str, Any]) -> bytes:
    return cbor2.dumps(dict(value), canonical=True)


def _frame(value: Mapping[str, Any]) -> bytes:
    payload = _canonical_cbor(value)
    if not payload or len(payload) > _MAX_FRAME_BYTES:
        raise ValueError("journal record is too large")
    return struct.pack(">I", len(payload)) + hashlib.sha256(payload).digest() + payload


def _decode_frame(file_handle: Any, *, location: str) -> Mapping[str, Any] | None:
    size_bytes = file_handle.read(4)
    if not size_bytes:
        return None
    if len(size_bytes) != 4:
        raise JournalCorruptionError(f"truncated frame length at {location}")
    size = struct.unpack(">I", size_bytes)[0]
    if size == 0 or size > _MAX_FRAME_BYTES:
        raise JournalCorruptionError(f"invalid frame length {size} at {location}")
    checksum = file_handle.read(32)
    if len(checksum) != 32:
        raise JournalCorruptionError(f"truncated frame checksum at {location}")
    payload = file_handle.read(size)
    if len(payload) != size:
        raise JournalCorruptionError(f"truncated frame payload at {location}")
    if hashlib.sha256(payload).digest() != checksum:
        raise JournalCorruptionError(f"frame checksum mismatch at {location}")
    try:
        value = cbor2.loads(payload)
    except Exception as exc:  # cbor2 exposes multiple decoder exception types.
        raise JournalCorruptionError(f"invalid CBOR at {location}") from exc
    if not isinstance(value, Mapping):
        raise JournalCorruptionError(f"journal frame is not a map at {location}")
    try:
        if _canonical_cbor(value) != payload:
            raise JournalCorruptionError(f"non-canonical CBOR at {location}")
    except (TypeError, ValueError) as exc:
        raise JournalCorruptionError(f"unsupported CBOR at {location}") from exc
    return value


def _event_from_record(
    record: Mapping[str, Any], expected_sequences: dict[str, int]
) -> JournalEvent:
    required = {
        "version",
        "run_id",
        "event_seq",
        "recorded_at_ns",
        "event",
    }
    if set(record) != required or record["version"] != _FORMAT_VERSION:
        raise JournalCorruptionError("invalid event record fields or version")
    try:
        owner = _validate_run_id(record["run_id"])
    except (TypeError, ValueError) as exc:
        raise JournalCorruptionError("invalid event run id") from exc
    expected = expected_sequences.get(owner, 0) + 1
    sequence = record["event_seq"]
    timestamp = record["recorded_at_ns"]
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence != expected:
        raise JournalCorruptionError(f"event sequence discontinuity for run {owner!r}")
    if isinstance(timestamp, bool) or not isinstance(timestamp, int) or timestamp < 0:
        raise JournalCorruptionError("invalid event timestamp")
    try:
        event = _event_data(record["event"])
    except (TypeError, ValueError, SecretDataError) as exc:
        raise JournalCorruptionError("invalid or secret-bearing event data") from exc
    expected_sequences[owner] = sequence
    return JournalEvent(owner, sequence, timestamp, event)


class FileRunJournal(_JournalBase):
    """Canonical-CBOR server journal supporting independently sequenced runs."""

    def __init__(self, path: str | Path, *, fsync: bool = False) -> None:
        journal_path = Path(path)
        journal_path.parent.mkdir(parents=True, exist_ok=True)
        events: dict[str, list[JournalEvent]] = {}
        if journal_path.exists():
            events = self._load(journal_path)
        else:
            header = {"format": _FORMAT, "version": _FORMAT_VERSION}
            with journal_path.open("xb") as file_handle:
                file_handle.write(_MAGIC)
                file_handle.write(_frame(header))
                file_handle.flush()
                if fsync:
                    os.fsync(file_handle.fileno())

        self.path = journal_path
        self._fsync = fsync
        self._file_handle = journal_path.open("ab")
        super().__init__(
            events_by_run=events,
            recovered_run_ids=frozenset(events),
        )

    @staticmethod
    def _load(path: Path) -> dict[str, list[JournalEvent]]:
        with path.open("rb") as file_handle:
            if file_handle.read(len(_MAGIC)) != _MAGIC:
                raise JournalCorruptionError("invalid or truncated journal magic")
            header = _decode_frame(file_handle, location="header")
            if header != {"format": _FORMAT, "version": _FORMAT_VERSION}:
                raise JournalCorruptionError("invalid journal header")
            events: dict[str, list[JournalEvent]] = {}
            expected_sequences: dict[str, int] = {}
            frame_index = 1
            while True:
                record = _decode_frame(file_handle, location=f"event frame {frame_index}")
                if record is None:
                    return events
                item = _event_from_record(record, expected_sequences)
                events.setdefault(item.run_id, []).append(item)
                frame_index += 1

    def _persist_record(self, record: Mapping[str, Any]) -> None:
        self._file_handle.write(_frame(record))
        self._file_handle.flush()
        if self._fsync:
            os.fsync(self._file_handle.fileno())

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._file_handle.close()
                self._closed = True


# Earlier design notes used this spelling.  Keep it as a source-compatible
# alias while the public v2 contract standardizes on ``FileRunJournal``.
PersistentRunJournal = FileRunJournal
