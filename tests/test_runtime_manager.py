from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import runtime_manager


class RuntimeManagerTests(unittest.TestCase):
    def test_usable_cores_halves_detected_cores(self) -> None:
        self.assertEqual(runtime_manager.usable_cores_for(1), 1)
        self.assertEqual(runtime_manager.usable_cores_for(2), 1)
        self.assertEqual(runtime_manager.usable_cores_for(7), 3)
        self.assertEqual(runtime_manager.usable_cores_for(8), 4)

    def test_load_runtime_config_adds_default_local_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "missing.json"
            with patch.object(runtime_manager, "RUNTIME_CONFIG_PATH", config_path):
                payload = runtime_manager.load_runtime_config()
        runtimes = payload["runtimes"]
        self.assertTrue(any(runtime["id"] == "local" for runtime in runtimes))

    def test_queue_state_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_root = Path(tmp_dir) / "runtime_state"
            with patch.object(runtime_manager, "RUNTIME_STATE_ROOT", state_root):
                saved = runtime_manager.save_queue_state(
                    "vm01",
                    active_run_id="run_a",
                    queued_run_ids=["run_b", "run_c"],
                )
                loaded = runtime_manager.load_queue_state("vm01")
        self.assertEqual(saved["active_run_id"], "run_a")
        self.assertEqual(loaded["queued_run_ids"], ["run_b", "run_c"])
        self.assertEqual(loaded["runtime_id"], "vm01")


if __name__ == "__main__":
    unittest.main()
