from __future__ import annotations

import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path

from remote_robot.cli import main


class V2CliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.project_root = Path(__file__).resolve().parents[1]
        self.registry = self.project_root / "registry.example"
        self.template = self.project_root / "programs" / "v2_workcell_demo.json"

    def invoke(self, *arguments: str) -> tuple[int, str, str]:
        output, error = StringIO(), StringIO()
        with redirect_stdout(output), redirect_stderr(error):
            code = main(["--registry", str(self.registry), *arguments])
        return code, output.getvalue(), error.getvalue()

    def test_list_is_identity_filtered(self) -> None:
        code, output, error = self.invoke("list", "--identity", "workcell_operator")
        self.assertEqual((code, error), (0, ""))
        roots = json.loads(output)
        self.assertEqual([item["node_id"] for item in roots], ["demo_workcell"])

    def test_compile_inspect_and_validate_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "demo.rrp"
            code, _, error = self.invoke("compile", str(self.template), str(target))
            self.assertEqual((code, error), (0, ""))
            self.assertTrue(target.is_file())
            code, output, error = self.invoke("inspect", str(target))
            self.assertEqual((code, error), (0, ""))
            self.assertIn('"phase_count": 1', output)
            code, output, error = self.invoke("validate", str(target))
            self.assertEqual((code, error), (0, ""))
            self.assertIn('"valid": true', output)


if __name__ == "__main__":
    unittest.main()
