"""Atomic ownership of robot leaves and server-local exclusive resources.

The lease manager deliberately has a synchronous, non-blocking interface.  Each
operation is a small in-memory transaction guarded by a thread lock, so callers
may use it from either synchronous code or an asyncio event loop without an
``await`` boundary in the middle of acquisition.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import threading
import time
from typing import Iterable, Literal


LeaseKind = Literal["leaf", "resource", "safety_domain"]


def _canonical_keys(values: Iterable[str], label: str) -> frozenset[str]:
    if isinstance(values, (str, bytes)):
        raise TypeError(f"{label} must be an iterable of keys, not one string")
    result: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            raise TypeError(f"{label} keys must be strings")
        if not value or value != value.strip():
            raise ValueError(f"{label} keys must be non-empty canonical strings")
        if len(value) > 512 or any(ord(character) < 32 for character in value):
            raise ValueError(f"invalid {label} key: {value!r}")
        result.add(value)
    return frozenset(result)


def _run_id(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("run_id must be a string")
    if not value or value != value.strip():
        raise ValueError("run_id must be a non-empty canonical string")
    if len(value) > 512 or any(ord(character) < 32 for character in value):
        raise ValueError("run_id contains invalid characters")
    return value


@dataclass(frozen=True, slots=True)
class LeaseTarget:
    """One name in an exclusive ownership namespace."""

    kind: LeaseKind
    key: str

    def __str__(self) -> str:
        return f"{self.kind}:{self.key}"


@dataclass(frozen=True, slots=True)
class LeaseRequest:
    """The complete, immutable lock set needed by one run."""

    leaf_ids: frozenset[str] = field(default_factory=frozenset)
    resource_keys: frozenset[str] = field(default_factory=frozenset)
    safety_domain_keys: frozenset[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        object.__setattr__(self, "leaf_ids", _canonical_keys(self.leaf_ids, "leaf"))
        object.__setattr__(
            self, "resource_keys", _canonical_keys(self.resource_keys, "resource")
        )
        object.__setattr__(
            self,
            "safety_domain_keys",
            _canonical_keys(self.safety_domain_keys, "safety domain"),
        )

    @property
    def targets(self) -> tuple[LeaseTarget, ...]:
        targets = [LeaseTarget("leaf", key) for key in self.leaf_ids]
        targets.extend(LeaseTarget("resource", key) for key in self.resource_keys)
        targets.extend(
            LeaseTarget("safety_domain", key) for key in self.safety_domain_keys
        )
        return tuple(sorted(targets, key=lambda target: (target.kind, target.key)))


@dataclass(frozen=True, slots=True)
class LeaseConflictItem:
    target: LeaseTarget
    owner_run_id: str


class LeaseConflict(RuntimeError):
    """Raised without changing ownership when any requested target is busy."""

    def __init__(self, run_id: str, conflicts: Iterable[LeaseConflictItem]) -> None:
        self.run_id = run_id
        self.conflicts = tuple(conflicts)
        details = ", ".join(
            f"{conflict.target} is owned by {conflict.owner_run_id!r}"
            for conflict in self.conflicts
        )
        super().__init__(f"cannot acquire lease for {run_id!r}: {details}")


class LeaseRequestMismatchError(RuntimeError):
    """A duplicate run id attempted to change its already-sealed lock set."""


@dataclass(frozen=True, slots=True)
class LeaseReceipt:
    run_id: str
    leaves: frozenset[str]
    resources: frozenset[str]
    safety_domains: frozenset[str]
    acquired_at_monotonic_ns: int

    @property
    def request(self) -> LeaseRequest:
        return LeaseRequest(self.leaves, self.resources, self.safety_domains)


class LeaseManager:
    """Atomically owns expanded leaf/resource/domain unions for active runs."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._owners: dict[LeaseTarget, str] = {}
        self._grants: dict[str, LeaseReceipt] = {}

    def acquire(
        self,
        run_id: str,
        *,
        leaves: Iterable[str] = (),
        resources: Iterable[str] = (),
        safety_domains: Iterable[str] = (),
    ) -> LeaseReceipt:
        """Acquire all requested targets or none of them.

        Repeating the exact request for the same run returns its original grant.
        Changing a live run's request is rejected because the program outline is
        sealed before acquisition.
        """

        owner = _run_id(run_id)
        request = LeaseRequest(
            leaf_ids=leaves,
            resource_keys=resources,
            safety_domain_keys=safety_domains,
        )

        with self._lock:
            existing = self._grants.get(owner)
            if existing is not None:
                if existing.request == request:
                    return existing
                raise LeaseRequestMismatchError(
                    f"run {owner!r} already holds a different lease request"
                )

            conflicts = tuple(
                LeaseConflictItem(target, held_by)
                for target in request.targets
                if (held_by := self._owners.get(target)) is not None
                and held_by != owner
            )
            if conflicts:
                raise LeaseConflict(owner, conflicts)

            # No mutation occurs before every conflict has been collected.
            grant = LeaseReceipt(
                owner,
                request.leaf_ids,
                request.resource_keys,
                request.safety_domain_keys,
                time.monotonic_ns(),
            )
            for target in request.targets:
                self._owners[target] = owner
            self._grants[owner] = grant
            return grant

    def release(self, run_id: str) -> bool:
        """Release exactly the lease owned by ``run_id``.

        Unknown/non-owning run IDs are harmless and cannot release another
        owner's targets.
        """

        owner = _run_id(run_id)
        with self._lock:
            grant = self._grants.pop(owner, None)
            if grant is None:
                return False
            for target in grant.request.targets:
                if self._owners.get(target) == owner:
                    del self._owners[target]
            return True

    def owner_of(self, target: LeaseTarget) -> str | None:
        if not isinstance(target, LeaseTarget):
            raise TypeError("target must be a LeaseTarget")
        with self._lock:
            return self._owners.get(target)

    def grant_for(self, run_id: str) -> LeaseReceipt | None:
        owner = _run_id(run_id)
        with self._lock:
            return self._grants.get(owner)

    def active_grants(self) -> tuple[LeaseReceipt, ...]:
        """Return a stable diagnostic snapshot ordered by run id."""

        with self._lock:
            return tuple(self._grants[key] for key in sorted(self._grants))


# The v2 public name is ``LeaseConflict``.  This alias makes early callers that
# used the longer spelling fail in the same typed way during the milestone.
LeaseConflictError = LeaseConflict
LeaseGrant = LeaseReceipt
