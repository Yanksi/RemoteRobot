"""Safe, same-server execution orchestration for compiled v2 programs.

This milestone owns the run lifecycle through terminal phase events.  It does
not treat a worker's ``start_phase`` acknowledgement as completion: every
participating worker must emit ``phase_completed`` before the next barrier is
crossed or the run can release its leases.
"""

from __future__ import annotations

import asyncio
import copy
from dataclasses import dataclass
import time
from typing import Any, Iterable, Mapping, Protocol

from .journal import JournalEvent, RunJournal
from .lease import LeaseManager, LeaseReceipt
from .program import CompiledProgram, DEFAULT_COMPLETION, ProgramCompiler, ProgramError
from .worker import WorkerPort


class OrchestratorError(RuntimeError):
    """Base class for run lifecycle errors outside program compilation."""


class AuthorizationError(OrchestratorError):
    pass


class MissingWorkerError(OrchestratorError):
    pass


class UnsupportedProgramFeatureError(OrchestratorError):
    pass


class RunAlreadyActiveError(OrchestratorError):
    pass


class RunIdReuseError(OrchestratorError):
    pass


class PhaseExecutionError(OrchestratorError):
    pass


class RunExecutionError(OrchestratorError):
    def __init__(self, run_id: str, cause: BaseException) -> None:
        self.run_id = run_id
        self.cause = cause
        super().__init__(f"run {run_id!r} failed: {type(cause).__name__}: {cause}")


class RuntimeRegistry(Protocol):
    server_id: str

    def manifest_hash(self, node_id: str) -> str: ...

    def describe(self, node_id: str) -> Mapping[str, Any]: ...

    def resolve_group(self, root_id: str, role_path: str) -> Any: ...

    def identity(self, identity_id: str) -> Any: ...


class ControlAuthorizer(Protocol):
    """Replaceable authorization seam for a future external identity service."""

    def require_control(
        self, identity_id: str, root_id: str, role_paths: Iterable[str]
    ) -> None: ...


class RegistryControlAuthorizer:
    def __init__(self, registry: RuntimeRegistry) -> None:
        self._registry = registry

    def require_control(
        self, identity_id: str, root_id: str, role_paths: Iterable[str]
    ) -> None:
        try:
            identity = self._registry.identity(identity_id)
        except Exception as exc:
            raise AuthorizationError(f"unknown identity {identity_id!r}") from exc
        denied = sorted(
            role_path
            for role_path in set(role_paths)
            if not identity.can_control(root_id, role_path)
        )
        if denied:
            raise AuthorizationError(
                f"identity {identity_id!r} cannot control {root_id!r} roles {denied}"
            )


@dataclass(frozen=True, slots=True)
class LockRequirements:
    leaves: frozenset[str]
    resources: frozenset[str]
    safety_domains: frozenset[str]


@dataclass(frozen=True, slots=True)
class RunResult:
    run_id: str
    status: str
    phase_count: int
    program_sha256: str
    terminal_event_seq: int


def _plain_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ProgramError("INVALID_ARTIFACT", f"{label} must be a mapping")
    return value


def _string_list(value: Any, label: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ProgramError("INVALID_MANIFEST", f"{label} must be an array of strings")
    return tuple(value)


class RunOrchestrator:
    """Validate, authorize, lease, synchronize, and safely terminate one run."""

    def __init__(
        self,
        registry: RuntimeRegistry,
        workers: Mapping[str, WorkerPort],
        lease_manager: LeaseManager,
        journal: RunJournal,
        *,
        authorizer: ControlAuthorizer | None = None,
        start_lead_us: int = 100_000,
        phase_timeout_margin_s: float = 2.0,
    ) -> None:
        if isinstance(start_lead_us, bool) or not isinstance(start_lead_us, int):
            raise TypeError("start_lead_us must be an integer")
        if start_lead_us < 0:
            raise ValueError("start_lead_us must be non-negative")
        if phase_timeout_margin_s <= 0:
            raise ValueError("phase_timeout_margin_s must be positive")
        self.registry = registry
        self.workers = dict(workers)
        self.lease_manager = lease_manager
        self.journal = journal
        self.authorizer = authorizer or RegistryControlAuthorizer(registry)
        self.start_lead_us = start_lead_us
        self.phase_timeout_margin_s = float(phase_timeout_margin_s)
        self._active_run_ids: set[str] = set()
        self._active_lock = asyncio.Lock()

    async def execute(
        self, *, run_id: str, identity_id: str, program: CompiledProgram
    ) -> RunResult:
        """Execute every deterministic phase and return only after safe idle.

        Validation and authorization finish before lease acquisition or worker
        access.  Once leased, all failure paths coordinate a stop and release
        the complete lock union before raising ``RunExecutionError``.
        """

        async with self._active_lock:
            if run_id in self._active_run_ids:
                raise RunAlreadyActiveError(f"run {run_id!r} is already active")
            self._active_run_ids.add(run_id)

        try:
            if self.journal.latest(run_id) is not None:
                raise RunIdReuseError(
                    f"run_id {run_id!r} already has journal history and cannot be reused"
                )
            checked = self._preflight(identity_id, program)
            requirements = self.lock_requirements(checked)
            worker_ids = self._participant_worker_ids(checked)
            missing = sorted(worker_ids - self.workers.keys())
            if missing:
                raise MissingWorkerError(f"no worker registered for physical nodes {missing}")
            participating = {worker_id: self.workers[worker_id] for worker_id in worker_ids}

            receipt = self.lease_manager.acquire(
                run_id,
                leaves=requirements.leaves,
                resources=requirements.resources,
                safety_domains=requirements.safety_domains,
            )
            try:
                return await self._execute_leased(
                    run_id, identity_id, checked, receipt, participating
                )
            finally:
                self.lease_manager.release(run_id)
        finally:
            async with self._active_lock:
                self._active_run_ids.discard(run_id)

    def _preflight(
        self, identity_id: str, program: CompiledProgram
    ) -> CompiledProgram:
        if not isinstance(program, CompiledProgram):
            raise TypeError("program must be a CompiledProgram")
        # Re-decode the envelope so a manually mutated in-memory body cannot
        # bypass the internal phase/outline digests.
        checked = CompiledProgram.from_bytes(program.to_bytes())
        ProgramCompiler(self.registry).validate_for_current_registry(checked)

        role_paths: set[str] = set()
        for phase in checked.phases:
            if phase.get("completion") != DEFAULT_COMPLETION:
                raise UnsupportedProgramFeatureError(
                    "only all_sequences_finished/v1 completion is implemented"
                )
            if phase.get("transitions"):
                raise UnsupportedProgramFeatureError(
                    "mode transitions are retained by the format but not executable yet"
                )
            for sequence in phase.get("sequences", ()):
                sequence_map = _plain_mapping(sequence, "phase sequence")
                role_path = sequence_map.get("role_path")
                if not isinstance(role_path, str):
                    raise ProgramError("INVALID_ARTIFACT", "sequence role_path is invalid")
                role_paths.add(role_path)

        # This seam can later delegate to a policy service without weakening
        # the pre-worker ordering guarantee.
        self.authorizer.require_control(identity_id, checked.root_id, role_paths)
        return checked

    def lock_requirements(self, program: CompiledProgram) -> LockRequirements:
        """Expand the sealed outline into its one atomic lease request."""

        manifest = _plain_mapping(
            self.registry.describe(program.root_id), "root capability manifest"
        )
        leaves: set[str] = set()
        resources: set[str] = set()
        safety_domains: set[str] = set()

        outline = _plain_mapping(program.outline, "program outline")
        phases = outline.get("phases")
        if not isinstance(phases, list):
            raise ProgramError("INVALID_ARTIFACT", "outline phases must be an array")
        for phase in phases:
            phase_map = _plain_mapping(phase, "outline phase")
            sequences = phase_map.get("sequences")
            if not isinstance(sequences, list):
                raise ProgramError("INVALID_ARTIFACT", "outline sequences must be an array")
            for sequence in sequences:
                item = _plain_mapping(sequence, "outline sequence")
                target = _plain_mapping(item.get("resolved_target"), "resolved target")
                physical_id = target.get("physical_node_id")
                group_id = target.get("group_id")
                role_path = item.get("role_path")
                if not all(isinstance(value, str) for value in (physical_id, group_id, role_path)):
                    raise ProgramError("INVALID_ARTIFACT", "outline target is invalid")
                leaves.add(f"{physical_id}.{group_id}")
                self._collect_role_branch_locks(
                    manifest,
                    role_path,
                    physical_id,
                    resources,
                    safety_domains,
                )
        return LockRequirements(
            frozenset(leaves), frozenset(resources), frozenset(safety_domains)
        )

    def _collect_role_branch_locks(
        self,
        root_manifest: Mapping[str, Any],
        role_path: str,
        expected_physical_id: str,
        resources: set[str],
        safety_domains: set[str],
    ) -> None:
        current = root_manifest
        segments = role_path.split(".")
        while True:
            resources.update(_string_list(current.get("resources", []), "manifest resources"))
            safety_domains.update(
                _string_list(current.get("safety_domains", []), "manifest safety_domains")
            )
            kind = current.get("kind")
            if kind == "physical":
                if current.get("node_id") != expected_physical_id or len(segments) != 1:
                    raise ProgramError(
                        "INVALID_ARTIFACT", "role path and resolved physical target disagree"
                    )
                return
            if kind != "composite" or len(segments) < 2:
                raise ProgramError("INVALID_MANIFEST", "role path cannot traverse manifest")
            role = segments.pop(0)
            children = current.get("children")
            if not isinstance(children, list):
                raise ProgramError("INVALID_MANIFEST", "composite children are invalid")
            matching = [child for child in children if isinstance(child, Mapping) and child.get("role") == role]
            if len(matching) != 1:
                raise ProgramError("INVALID_MANIFEST", f"manifest has no unique child role {role!r}")
            current = _plain_mapping(matching[0].get("manifest"), "child manifest")

    @staticmethod
    def _participant_worker_ids(program: CompiledProgram) -> set[str]:
        result: set[str] = set()
        for phase in program.phases:
            for sequence in phase.get("sequences", ()):
                target = _plain_mapping(sequence.get("resolved_target"), "resolved target")
                physical_id = target.get("physical_node_id")
                if not isinstance(physical_id, str):
                    raise ProgramError("INVALID_ARTIFACT", "physical node id is invalid")
                result.add(physical_id)
        return result

    async def _execute_leased(
        self,
        run_id: str,
        identity_id: str,
        program: CompiledProgram,
        receipt: LeaseReceipt,
        workers: Mapping[str, WorkerPort],
    ) -> RunResult:
        current_phase: str | None = None
        current_phase_execution_id: str | None = None
        self.journal.append(
            run_id,
            {
                "type": "run.opened",
                "identity_id": identity_id,
                "root_id": program.root_id,
                "manifest_hash": program.manifest_hash,
                "program_sha256": program.sha256,
                "lease": {
                    "leaves": sorted(receipt.leaves),
                    "resources": sorted(receipt.resources),
                    "safety_domains": sorted(receipt.safety_domains),
                },
            },
        )
        try:
            disabled = await self._call_all(workers, "prepare_disabled")
            self.journal.append(
                run_id,
                {"type": "workers.disabled", "workers": self._safe_results(disabled)},
            )

            for phase_index, phase in enumerate(program.phases):
                current_phase = str(phase["phase_id"])
                plans = self._worker_phase_plans(
                    run_id, program, phase_index, phase
                )
                phase_workers = {worker_id: workers[worker_id] for worker_id in plans}
                current_phase_execution_id = next(
                    iter(plans.values())
                )["phase_execution_id"]
                prepared = await self._prepare_all(phase_workers, plans)
                self.journal.append(
                    run_id,
                    {
                        "type": "phase.prepared",
                        "phase_id": current_phase,
                        "phase_execution_id": current_phase_execution_id,
                        "phase_index": phase_index,
                        "workers": self._safe_results(prepared),
                    },
                )

                start_at_us = time.monotonic_ns() // 1_000 + self.start_lead_us
                scheduled = await self._start_all(
                    phase_workers,
                    start_at_us,
                    run_id,
                    current_phase_execution_id,
                )
                self.journal.append(
                    run_id,
                    {
                        "type": "phase.scheduled",
                        "phase_id": current_phase,
                        "phase_execution_id": current_phase_execution_id,
                        "phase_index": phase_index,
                        "start_at_monotonic_us": start_at_us,
                        "workers": self._safe_results(scheduled),
                    },
                )
                started = await self._wait_all_for_event(
                    phase_workers,
                    run_id=run_id,
                    phase_execution_id=current_phase_execution_id,
                    expected_type="phase_started",
                    timeout_s=max(
                        0.0,
                        (start_at_us - time.monotonic_ns() // 1_000) / 1_000_000,
                    )
                    + self.phase_timeout_margin_s,
                )
                observed_skew_us = self._observed_start_skew_us(started)
                max_skew_us = self._max_start_skew_us(program.root_id)
                self.journal.append(
                    run_id,
                    {
                        "type": "phase.started",
                        "phase_id": current_phase,
                        "phase_execution_id": current_phase_execution_id,
                        "phase_index": phase_index,
                        "start_at_monotonic_us": start_at_us,
                        "observed_start_skew_us": observed_skew_us,
                        "max_start_skew_us": max_skew_us,
                        "workers": self._safe_results(started),
                    },
                )
                if max_skew_us is not None and observed_skew_us > max_skew_us:
                    raise PhaseExecutionError(
                        f"phase start skew {observed_skew_us}us exceeds "
                        f"root limit {max_skew_us}us"
                    )

                duration_us = int(phase["duration_us"])
                completed = await self._wait_all_for_event(
                    phase_workers,
                    run_id=run_id,
                    phase_execution_id=current_phase_execution_id,
                    expected_type="phase_completed",
                    timeout_s=duration_us / 1_000_000
                    + self.phase_timeout_margin_s,
                )
                self.journal.append(
                    run_id,
                    {
                        "type": "phase.completed",
                        "phase_id": current_phase,
                        "phase_execution_id": current_phase_execution_id,
                        "phase_index": phase_index,
                        "workers": self._safe_results(completed),
                    },
                )

            stop_results = await self._stop_all(
                workers,
                "RUN_COMPLETED",
                run_id,
                current_phase,
                current_phase_execution_id,
            )
            stop_errors = [value for value in stop_results.values() if isinstance(value, BaseException)]
            if stop_errors:
                raise PhaseExecutionError("one or more workers failed to enter safe idle")
            terminal = self.journal.append(
                run_id,
                {
                    "type": "run.completed",
                    "phase_count": len(program.phases),
                    "safe_idle": self._safe_results(stop_results),
                },
            )
            return RunResult(run_id, "completed", len(program.phases), program.sha256, terminal.event_seq)
        except BaseException as exc:
            stop_results = await self._stop_all(
                workers,
                "ORCHESTRATION_FAILURE",
                run_id,
                current_phase,
                current_phase_execution_id,
            )
            try:
                self.journal.append(
                    run_id,
                    {
                        "type": "run.faulted",
                        "phase_id": current_phase,
                        "phase_execution_id": current_phase_execution_id,
                        "error_type": type(exc).__name__,
                        "safe_stop": self._safe_results(stop_results),
                    },
                )
            except BaseException:
                # Preserve the initiating failure if the evidence store itself
                # is the failed component.
                pass
            if isinstance(exc, asyncio.CancelledError):
                raise
            raise RunExecutionError(run_id, exc) from exc

    async def _call_all(
        self, workers: Mapping[str, WorkerPort], method: str
    ) -> dict[str, Any]:
        names = sorted(workers)
        results = await asyncio.gather(
            *(getattr(workers[name], method)() for name in names),
            return_exceptions=True,
        )
        mapped = dict(zip(names, results, strict=True))
        errors = [value for value in mapped.values() if isinstance(value, BaseException)]
        if errors:
            raise PhaseExecutionError(f"{method} failed for {len(errors)} worker(s)") from errors[0]
        return mapped

    async def _prepare_all(
        self,
        workers: Mapping[str, WorkerPort],
        plans: Mapping[str, dict[str, Any]],
    ) -> dict[str, Any]:
        names = sorted(workers)
        results = await asyncio.gather(
            *(workers[name].prepare_phase(plans[name]) for name in names),
            return_exceptions=True,
        )
        mapped = dict(zip(names, results, strict=True))
        errors = [value for value in mapped.values() if isinstance(value, BaseException)]
        if errors:
            raise PhaseExecutionError(f"prepare_phase failed for {len(errors)} worker(s)") from errors[0]
        return mapped

    async def _start_all(
        self,
        workers: Mapping[str, WorkerPort],
        start_at_us: int,
        run_id: str,
        phase_execution_id: str,
    ) -> dict[str, Any]:
        names = sorted(workers)
        results = await asyncio.gather(
            *(workers[name].start_phase(start_at_us) for name in names),
            return_exceptions=True,
        )
        mapped = dict(zip(names, results, strict=True))
        errors = [value for value in mapped.values() if isinstance(value, BaseException)]
        if errors:
            raise PhaseExecutionError(f"start_phase failed for {len(errors)} worker(s)") from errors[0]
        for name, result in mapped.items():
            if (
                not isinstance(result, Mapping)
                or result.get("status") != "scheduled"
                or result.get("run_id") != run_id
                or result.get("phase_execution_id") != phase_execution_id
            ):
                raise PhaseExecutionError(
                    f"worker {name!r} returned an invalid scheduled acknowledgement"
                )
        return mapped

    async def _wait_all_for_event(
        self,
        workers: Mapping[str, WorkerPort],
        *,
        run_id: str,
        phase_execution_id: str,
        expected_type: str,
        timeout_s: float,
    ) -> dict[str, Any]:
        names = sorted(workers)

        async def wait_one(name: str) -> Mapping[str, Any]:
            async with asyncio.timeout(timeout_s):
                while True:
                    event = await workers[name].next_event()
                    event_type = event.get("type")
                    matches = (
                        event.get("run_id") == run_id
                        and event.get("phase_execution_id") == phase_execution_id
                    )
                    if matches and event_type == expected_type:
                        return event
                    if matches and event_type in {
                        "worker_fault",
                        "worker_faulted",
                        "worker_stopped",
                    }:
                        raise PhaseExecutionError(
                            f"worker {name!r} terminated during {phase_execution_id!r}"
                        )
                    if (
                        matches
                        and expected_type == "phase_started"
                        and event_type == "phase_completed"
                    ):
                        raise PhaseExecutionError(
                            f"worker {name!r} completed before reporting phase_started"
                        )

        tasks = {
            asyncio.create_task(
                wait_one(name), name=f"wait-{phase_execution_id}-{expected_type}-{name}"
            ): name
            for name in names
        }
        pending = set(tasks)
        mapped: dict[str, Any] = {}
        try:
            while pending:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    error = task.exception()
                    if error is not None:
                        for unfinished in pending:
                            unfinished.cancel()
                        await asyncio.gather(*pending, return_exceptions=True)
                        raise PhaseExecutionError(
                            f"{expected_type} wait failed for worker {tasks[task]!r}"
                        ) from error
                    mapped[tasks[task]] = task.result()
            return mapped
        finally:
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    @staticmethod
    def _observed_start_skew_us(starts: Mapping[str, Any]) -> int:
        actual_starts: list[int] = []
        for worker_id, result in starts.items():
            if not isinstance(result, Mapping):
                raise PhaseExecutionError(
                    f"worker {worker_id!r} start acknowledgement is not a mapping"
                )
            actual = result.get("actual_start_us")
            if isinstance(actual, bool) or not isinstance(actual, int) or actual < 0:
                raise PhaseExecutionError(
                    f"worker {worker_id!r} did not report a valid actual_start_us"
                )
            actual_starts.append(actual)
        if not actual_starts:
            raise PhaseExecutionError("phase has no worker start acknowledgements")
        return max(actual_starts) - min(actual_starts)

    def _max_start_skew_us(self, root_id: str) -> int | None:
        manifest = _plain_mapping(
            self.registry.describe(root_id), "root capability manifest"
        )
        value = manifest.get("max_start_skew_us")
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ProgramError(
                "INVALID_MANIFEST", "max_start_skew_us must be a positive integer"
            )
        return value

    @staticmethod
    def _worker_phase_plans(
        run_id: str,
        program: CompiledProgram,
        phase_index: int,
        phase: Mapping[str, Any],
    ) -> dict[str, dict[str, Any]]:
        phase_sha = phase.get("sha256")
        if not isinstance(phase_sha, str) or len(phase_sha) < 12:
            raise ProgramError("INVALID_ARTIFACT", "phase digest is invalid")
        phase_execution_id = f"{run_id}:{phase_index}:{phase_sha[:12]}"
        tracks: dict[str, list[dict[str, Any]]] = {}
        for sequence in phase["sequences"]:
            target = _plain_mapping(sequence["resolved_target"], "resolved target")
            physical_id = str(target["physical_node_id"])
            tracks.setdefault(physical_id, []).append(copy.deepcopy(dict(sequence)))
        return {
            physical_id: {
                "run_id": run_id,
                "phase_execution_id": phase_execution_id,
                "root_id": program.root_id,
                "manifest_hash": program.manifest_hash,
                "phase_id": phase["phase_id"],
                "duration_us": phase["duration_us"],
                "completion": copy.deepcopy(dict(phase["completion"])),
                "transitions": [],
                "tracks": group_tracks,
            }
            for physical_id, group_tracks in tracks.items()
        }

    async def _stop_all(
        self,
        workers: Mapping[str, WorkerPort],
        code: str,
        run_id: str,
        phase_id: str | None,
        phase_execution_id: str | None,
    ) -> dict[str, Any]:
        names = sorted(workers)
        context = {
            "code": code,
            "details": {
                "run_id": run_id,
                "phase_id": phase_id,
                "phase_execution_id": phase_execution_id,
            },
        }
        results = await asyncio.gather(
            *(workers[name].stop(copy.deepcopy(context)) for name in names),
            return_exceptions=True,
        )
        return dict(zip(names, results, strict=True))

    @staticmethod
    def _safe_results(results: Mapping[str, Any]) -> dict[str, Any]:
        safe: dict[str, Any] = {}
        for name, value in results.items():
            if isinstance(value, BaseException):
                safe[name] = {"ok": False, "error_type": type(value).__name__}
            elif isinstance(value, Mapping):
                safe[name] = {"ok": True, "result": copy.deepcopy(dict(value))}
            else:
                safe[name] = {"ok": True, "result_type": type(value).__name__}
        return safe
