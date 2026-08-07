from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any
import unittest

from remote_robot.journal import InMemoryRunJournal
from remote_robot.lease import LeaseManager
from remote_robot.model import IdentitySpec
from remote_robot.orchestrator import (
    AuthorizationError,
    RunExecutionError,
    RunIdReuseError,
    RunOrchestrator,
)
from remote_robot.program import ProgramCompiler, ProgramError
from remote_robot.registry import CapabilityRegistry
from remote_robot.worker import InMemoryWorker


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def registry() -> CapabilityRegistry:
    return CapabilityRegistry.load(PROJECT_ROOT / "registry.example")


def short_program(
    capability_registry: CapabilityRegistry, *, phase_count: int = 1
):
    template = json.loads(
        (PROJECT_ROOT / "programs" / "v2_workcell_demo.json").read_text(
            encoding="utf-8"
        )
    )
    base = template["phases"][0]
    phases = []
    for phase_index in range(phase_count):
        phase = copy.deepcopy(base)
        phase["phase_id"] = f"phase-{phase_index}"
        for sequence in phase["sequences"]:
            # A zero-distance trajectory keeps this orchestration test fast
            # without weakening compiler velocity/acceleration validation.
            for sample_index, sample in enumerate(sequence["samples"]):
                sample["t_us"] = sample_index * 5_000
                sample["values"] = [0.0] * len(sample["values"])
        phases.append(phase)
    template["phases"] = phases
    return ProgramCompiler(capability_registry).compile(template)


class RecordingLeaseManager(LeaseManager):
    def __init__(self) -> None:
        super().__init__()
        self.last_request: dict[str, frozenset[str]] | None = None
        self.acquisition_count = 0

    def acquire(
        self,
        run_id: str,
        *,
        leaves=(),
        resources=(),
        safety_domains=(),
    ):
        self.acquisition_count += 1
        self.last_request = {
            "leaves": frozenset(leaves),
            "resources": frozenset(resources),
            "safety_domains": frozenset(safety_domains),
        }
        return super().acquire(
            run_id,
            leaves=self.last_request["leaves"],
            resources=self.last_request["resources"],
            safety_domains=self.last_request["safety_domains"],
        )


class RecordingWorker(InMemoryWorker):
    def __init__(self, worker_id: str, trace: list[tuple[str, str, str]]) -> None:
        super().__init__(adapter_name=worker_id)
        self.worker_id = worker_id
        self.trace = trace
        self.plans: list[dict[str, Any]] = []
        self.start_deadlines: list[int] = []
        self.stop_contexts: list[dict[str, Any]] = []

    async def prepare_disabled(self):
        self.trace.append(("disabled", self.worker_id, ""))
        return await super().prepare_disabled()

    async def prepare_phase(self, phase_plan):
        self.plans.append(copy.deepcopy(phase_plan))
        self.trace.append(("prepare", self.worker_id, phase_plan["phase_id"]))
        return await super().prepare_phase(phase_plan)

    async def start_phase(self, monotonic_deadline_us):
        self.start_deadlines.append(monotonic_deadline_us)
        phase_id = self.plans[-1]["phase_id"]
        self.trace.append(("start", self.worker_id, phase_id))
        return await super().start_phase(monotonic_deadline_us)

    async def stop(self, fault_context):
        self.stop_contexts.append(copy.deepcopy(fault_context))
        self.trace.append(("stop", self.worker_id, str(fault_context["code"])))
        return await super().stop(fault_context)


class FailingPrepareWorker(RecordingWorker):
    async def prepare_phase(self, phase_plan):
        self.plans.append(copy.deepcopy(phase_plan))
        self.trace.append(("prepare", self.worker_id, phase_plan["phase_id"]))
        raise RuntimeError("injected prepare failure")


class SkewedStartWorker(RecordingWorker):
    async def next_event(self, timeout=None):
        event = await super().next_event(timeout)
        if event.get("type") == "phase_started":
            return {**event, "actual_start_us": event["actual_start_us"] + 60_000}
        return event


class RunOrchestratorTests(unittest.IsolatedAsyncioTestCase):
    def make_orchestrator(self, workers, lease=None, journal=None, capability_registry=None):
        return RunOrchestrator(
            capability_registry or registry(),
            workers,
            lease or RecordingLeaseManager(),
            journal or InMemoryRunJournal(),
            start_lead_us=5_000,
            phase_timeout_margin_s=0.2,
        )

    async def test_multi_worker_phases_use_atomic_union_and_shared_deadlines(self) -> None:
        capabilities = registry()
        program = short_program(capabilities, phase_count=2)
        trace: list[tuple[str, str, str]] = []
        primary = RecordingWorker("so101_canada", trace)
        assistant = RecordingWorker("simulator_7dof", trace)
        leases = RecordingLeaseManager()
        journal = InMemoryRunJournal()
        orchestrator = self.make_orchestrator(
            {"so101_canada": primary, "simulator_7dof": assistant},
            leases,
            journal,
            capabilities,
        )

        result = await orchestrator.execute(
            run_id="coordinated-run",
            identity_id="workcell_operator",
            program=program,
        )

        self.assertEqual(result.status, "completed")
        self.assertEqual(result.phase_count, 2)
        self.assertEqual(leases.active_grants(), ())
        self.assertEqual(
            leases.last_request["leaves"],  # type: ignore[index]
            {
                "so101_canada.arm",
                "so101_canada.gripper",
                "simulator_7dof.arm",
            },
        )
        self.assertEqual(
            leases.last_request["resources"],  # type: ignore[index]
            {
                "camera:overhead_1",
                "serial:so101_canada",
                "simulator:seven_axis",
            },
        )
        self.assertEqual(
            leases.last_request["safety_domains"],  # type: ignore[index]
            {
                "combined_demo_workspace",
                "table_workspace_A",
                "table_workspace_B",
            },
        )
        for phase_index in range(2):
            self.assertEqual(
                primary.start_deadlines[phase_index], assistant.start_deadlines[phase_index]
            )
            phase_id = f"phase-{phase_index}"
            positions = [
                index
                for index, (action, _worker, traced_phase) in enumerate(trace)
                if traced_phase == phase_id and action in {"prepare", "start"}
            ]
            prepare_positions = [
                index
                for index in positions
                if trace[index][0] == "prepare"
            ]
            start_positions = [index for index in positions if trace[index][0] == "start"]
            self.assertLess(max(prepare_positions), min(start_positions))
        self.assertEqual(len(primary.plans[0]["tracks"]), 2)
        self.assertEqual(len(assistant.plans[0]["tracks"]), 1)
        self.assertEqual(primary.stop_contexts[-1]["code"], "RUN_COMPLETED")
        self.assertEqual(
            primary.stop_contexts[-1]["details"]["run_id"], "coordinated-run"
        )
        self.assertEqual(
            primary.stop_contexts[-1]["details"]["phase_execution_id"],
            primary.plans[-1]["phase_execution_id"],
        )
        self.assertEqual(journal.latest("coordinated-run")["type"], "run.completed")  # type: ignore[index]

    async def test_prepare_failure_stops_every_participant_and_releases_union(self) -> None:
        capabilities = registry()
        program = short_program(capabilities)
        trace: list[tuple[str, str, str]] = []
        primary = RecordingWorker("so101_canada", trace)
        assistant = FailingPrepareWorker("simulator_7dof", trace)
        leases = RecordingLeaseManager()
        journal = InMemoryRunJournal()
        orchestrator = self.make_orchestrator(
            {"so101_canada": primary, "simulator_7dof": assistant},
            leases,
            journal,
            capabilities,
        )

        with self.assertRaises(RunExecutionError):
            await orchestrator.execute(
                run_id="failing-run",
                identity_id="administrator",
                program=program,
            )

        self.assertEqual(leases.active_grants(), ())
        self.assertEqual(primary.stop_contexts[-1]["code"], "ORCHESTRATION_FAILURE")
        self.assertEqual(assistant.stop_contexts[-1]["code"], "ORCHESTRATION_FAILURE")
        self.assertEqual(journal.latest("failing-run")["type"], "run.faulted")  # type: ignore[index]

    async def test_stale_events_are_ignored_and_distinct_runs_can_execute_once(self) -> None:
        capabilities = registry()
        program = short_program(capabilities)
        trace: list[tuple[str, str, str]] = []
        primary = RecordingWorker("so101_canada", trace)
        assistant = RecordingWorker("simulator_7dof", trace)
        for worker in (primary, assistant):
            for event_type in (
                "phase_started",
                "worker_fault",
                "worker_stopped",
                "phase_completed",
            ):
                worker._emit(
                    {
                        "type": event_type,
                        "run_id": "old-run",
                        "phase_id": "phase-0",
                        "phase_execution_id": "old-run:0:stale",
                        "actual_start_us": 1,
                    }
                )
        leases = RecordingLeaseManager()
        journal = InMemoryRunJournal()
        orchestrator = self.make_orchestrator(
            {"so101_canada": primary, "simulator_7dof": assistant},
            leases,
            journal,
            capabilities,
        )

        first = await orchestrator.execute(
            run_id="first-run", identity_id="administrator", program=program
        )
        second = await orchestrator.execute(
            run_id="second-run", identity_id="administrator", program=program
        )
        self.assertEqual((first.status, second.status), ("completed", "completed"))
        self.assertEqual(leases.acquisition_count, 2)
        self.assertEqual(leases.active_grants(), ())
        self.assertEqual(journal.latest("first-run")["type"], "run.completed")  # type: ignore[index]
        self.assertEqual(journal.latest("second-run")["type"], "run.completed")  # type: ignore[index]

        trace_before_reuse = list(trace)
        with self.assertRaises(RunIdReuseError):
            await orchestrator.execute(
                run_id="first-run", identity_id="administrator", program=program
            )
        self.assertEqual(leases.acquisition_count, 2)
        self.assertEqual(trace, trace_before_reuse)

    async def test_excess_start_skew_faults_before_phase_completion(self) -> None:
        capabilities = registry()
        program = short_program(capabilities)
        trace: list[tuple[str, str, str]] = []
        primary = RecordingWorker("so101_canada", trace)
        assistant = SkewedStartWorker("simulator_7dof", trace)
        leases = RecordingLeaseManager()
        journal = InMemoryRunJournal()
        orchestrator = self.make_orchestrator(
            {"so101_canada": primary, "simulator_7dof": assistant},
            leases,
            journal,
            capabilities,
        )

        with self.assertRaises(RunExecutionError):
            await orchestrator.execute(
                run_id="skewed-run", identity_id="administrator", program=program
            )

        events = journal.replay("skewed-run")
        started = next(event for event in events if event["type"] == "phase.started")
        self.assertGreater(started["observed_start_skew_us"], 50_000)
        self.assertEqual(started["max_start_skew_us"], 50_000)
        self.assertEqual(journal.latest("skewed-run")["type"], "run.faulted")  # type: ignore[index]
        self.assertEqual(primary.stop_contexts[-1]["code"], "ORCHESTRATION_FAILURE")
        self.assertEqual(assistant.stop_contexts[-1]["code"], "ORCHESTRATION_FAILURE")
        self.assertEqual(leases.active_grants(), ())

    async def test_manifest_pin_and_role_scope_fail_before_workers_or_leases(self) -> None:
        capabilities = registry()
        program = short_program(capabilities)
        trace: list[tuple[str, str, str]] = []
        workers = {
            "so101_canada": RecordingWorker("so101_canada", trace),
            "simulator_7dof": RecordingWorker("simulator_7dof", trace),
        }

        class StaleRegistry:
            server_id = capabilities.server_id

            def __getattr__(self, name):
                return getattr(capabilities, name)

            def manifest_hash(self, node_id):
                return "0" * 64

        stale_leases = RecordingLeaseManager()
        stale = self.make_orchestrator(
            workers, stale_leases, InMemoryRunJournal(), StaleRegistry()
        )
        with self.assertRaises(ProgramError) as raised:
            await stale.execute(
                run_id="stale-run", identity_id="administrator", program=program
            )
        self.assertEqual(raised.exception.code, "MANIFEST_MISMATCH")
        self.assertIsNone(stale_leases.last_request)
        self.assertEqual(trace, [])

        limited_identity = IdentitySpec(
            identity_id="limited",
            credential_env="LIMITED_TOKEN",
            discover=("demo_workcell",),
            observe=("demo_workcell",),
            control=("demo_workcell.primary_arm.arm",),
            emergency_stop=(),
        )
        limited_registry = CapabilityRegistry(
            server_id=capabilities.server_id,
            nodes={node_id: capabilities.node(node_id) for node_id in capabilities.node_ids},
            identities={"limited": limited_identity},
        )
        limited_program = short_program(limited_registry)
        limited_leases = RecordingLeaseManager()
        denied = self.make_orchestrator(
            workers, limited_leases, InMemoryRunJournal(), limited_registry
        )
        with self.assertRaises(AuthorizationError):
            await denied.execute(
                run_id="denied-run", identity_id="limited", program=limited_program
            )
        self.assertIsNone(limited_leases.last_request)
        self.assertEqual(trace, [])

if __name__ == "__main__":
    unittest.main()
