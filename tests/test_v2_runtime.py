from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import unittest

from remote_robot.journal import (
    FileRunJournal,
    InMemoryRunJournal,
    JournalCorruptionError,
    JournalReadOnlyError,
    RunJournal,
    SecretDataError,
)
from remote_robot.lease import (
    LeaseConflict,
    LeaseManager,
    LeaseRequestMismatchError,
    LeaseTarget,
)


class LeaseManagerTests(unittest.TestCase):
    def test_conflict_is_atomic_and_same_request_is_idempotent(self) -> None:
        manager = LeaseManager()
        first = manager.acquire(
            "run-a",
            leaves={"arm-017/arm"},
            resources={"serial:COM3"},
            safety_domains={"workspace:table-a"},
        )
        duplicate = manager.acquire(
            "run-a",
            leaves={"arm-017/arm"},
            resources={"serial:COM3"},
            safety_domains={"workspace:table-a"},
        )
        self.assertIs(first, duplicate)

        with self.assertRaises(LeaseConflict) as raised:
            manager.acquire(
                "run-b",
                leaves={"conveyor-04/drive"},
                resources={"serial:COM3"},
                safety_domains={"workspace:table-b"},
            )

        self.assertEqual(raised.exception.conflicts[0].target.key, "serial:COM3")
        self.assertEqual(raised.exception.conflicts[0].owner_run_id, "run-a")
        # No unrelated target from the rejected all-or-nothing request leaked.
        self.assertIsNone(manager.owner_of(LeaseTarget("leaf", "conveyor-04/drive")))
        self.assertIsNone(
            manager.owner_of(LeaseTarget("safety_domain", "workspace:table-b"))
        )

    def test_dag_views_contend_on_shared_local_resource(self) -> None:
        manager = LeaseManager()
        manager.acquire(
            "packing-cell-run",
            leaves={"arm-017/arm", "conveyor-04/drive"},
            resources={"camera:overhead-1"},
            safety_domains={"workspace:packing-cell"},
        )

        with self.assertRaises(LeaseConflict) as raised:
            manager.acquire(
                "inspection-cell-run",
                leaves={"arm-023/arm"},
                resources={"camera:overhead-1"},
                safety_domains={"workspace:inspection-cell"},
            )
        self.assertIn("camera:overhead-1", str(raised.exception))

        # A non-owner release is harmless and does not unlock the shared camera.
        self.assertFalse(manager.release("inspection-cell-run"))
        self.assertEqual(
            manager.owner_of(LeaseTarget("resource", "camera:overhead-1")),
            "packing-cell-run",
        )
        self.assertTrue(manager.release("packing-cell-run"))
        manager.acquire(
            "inspection-cell-run",
            leaves={"arm-023/arm"},
            resources={"camera:overhead-1"},
        )

    def test_live_run_cannot_change_its_sealed_lock_set(self) -> None:
        manager = LeaseManager()
        manager.acquire("run-a", leaves={"arm/arm"})
        with self.assertRaises(LeaseRequestMismatchError):
            manager.acquire("run-a", leaves={"arm/arm", "arm/gripper"})

    def test_simultaneous_acquisition_has_exactly_one_owner(self) -> None:
        manager = LeaseManager()

        def acquire(run_id: str) -> str:
            try:
                return manager.acquire(run_id, leaves={"shared/arm"}).run_id
            except LeaseConflict:
                return "conflict"

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(acquire, (f"run-{index}" for index in range(8))))
        self.assertEqual(results.count("conflict"), 7)
        self.assertEqual(len(manager.active_grants()), 1)


class JournalContractTests(unittest.TestCase):
    def _exercise_replay(self, journal: RunJournal, run_id: str) -> None:
        first = journal.append(run_id, {"type": "run.opened", "root_id": "packing-cell"})
        second = journal.append(
            run_id,
            {"type": "run.state", "state": "ARMED", "phase_index": 0},
        )
        third = journal.append(run_id, {"type": "phase.started", "phase_index": 0})
        self.assertEqual((first.event_seq, second.event_seq, third.event_seq), (1, 2, 3))
        self.assertEqual(
            [event.event_seq for event in journal.replay(run_id, after_event_seq=1)],
            [2, 3],
        )
        latest = journal.latest(run_id)
        self.assertIsNotNone(latest)
        self.assertEqual(latest["type"], "phase.started")  # type: ignore[index]
        self.assertEqual(latest["event_seq"], 3)  # type: ignore[index]
        self.assertFalse(journal.execution_resume_allowed)  # type: ignore[attr-defined]

    def test_in_memory_adapter_replay_is_per_run_and_rejects_secrets(self) -> None:
        journal = InMemoryRunJournal()
        self.assertIsInstance(journal, RunJournal)
        self._exercise_replay(journal, "run-memory")
        other = journal.append("other-run", {"type": "run.opened"})
        self.assertEqual(other.event_seq, 1)
        with self.assertRaises(SecretDataError):
            journal.append(
                "run-memory", {"type": "bad", "nested": [{"access-token": "no"}]}
            )
        # Rejected input doesn't consume an event sequence.
        self.assertEqual(
            journal.append("run-memory", {"type": "run.completed"}).event_seq, 4
        )

    def test_file_replay_after_restart_never_resumes_recovered_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runs.rrj"
            journal = FileRunJournal(path, fsync=True)
            self._exercise_replay(journal, "run-42")
            journal.close()

            recovered = FileRunJournal(path)
            self.assertIn("run-42", recovered.recovered_run_ids)
            self.assertEqual(
                [event.event_type for event in recovered.replay("run-42", 1)],
                ["run.state", "phase.started"],
            )
            with self.assertRaises(JournalReadOnlyError):
                recovered.append("run-42", {"type": "run.resumed"})
            # The server may journal a new run after restart; its sequence is local.
            self.assertEqual(
                recovered.append("run-after-restart", {"type": "run.opened"}).event_seq,
                1,
            )
            recovered.close()

    def test_checksum_and_truncation_are_detected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runs.rrj"
            journal = FileRunJournal(path)
            journal.append("run-corrupt", {"type": "run.opened", "root_id": "arm"})
            journal.close()
            original = path.read_bytes()

            corrupted = Path(directory) / "corrupted.rrj"
            altered = bytearray(original)
            altered[-1] ^= 1
            corrupted.write_bytes(altered)
            with self.assertRaises(JournalCorruptionError):
                FileRunJournal(corrupted)

            truncated = Path(directory) / "truncated.rrj"
            truncated.write_bytes(original[:-1])
            with self.assertRaises(JournalCorruptionError):
                FileRunJournal(truncated)


if __name__ == "__main__":
    unittest.main()
