from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import lieferdaten.runtime as lieferdaten_runtime
from instance_payload import import_instance_payload, load_instance_payload


def payload_with_stores(stores: list[dict[str, object]]) -> dict[str, object]:
    return {
        "schema_version": 1,
        "instance_id": "test-instance",
        "clustering_method": "angular_slices",
        "warehouse": {"latitude": 48.0, "longitude": 11.0},
        "stores": stores,
        "demand_rows": [
            {"delivery_date": "2026-01-15", "client_num": row["client_num"], "demand_kg": 100.0}
            for row in stores
        ],
    }


class InstancePayloadTests(unittest.TestCase):
    def write_payload(self, root: Path, payload: dict[str, object]) -> Path:
        path = root / "payload.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def test_cluster_ids_are_all_or_none(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            base_store = {"client_num": 1, "store_id": "A", "latitude": 48.1, "longitude": 11.1}
            clustered_store = {"client_num": 2, "store_id": "B", "latitude": 48.2, "longitude": 11.2, "cluster_id": 1}

            load_instance_payload(self.write_payload(root, payload_with_stores([base_store])))
            load_instance_payload(self.write_payload(root, payload_with_stores([{**base_store, "cluster_id": 0}, clustered_store])))

            with self.assertRaisesRegex(ValueError, "cluster_id must be provided for all stores or none"):
                load_instance_payload(self.write_payload(root, payload_with_stores([base_store, clustered_store])))

    def test_import_strips_public_clustering_method_and_omits_empty_assignments(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            payload_path = self.write_payload(
                root,
                payload_with_stores([{"client_num": 1, "store_id": "A", "latitude": 48.1, "longitude": 11.1}]),
            )
            run_dir = root / "run"

            with patch.object(lieferdaten_runtime, "LATEST_RUN_PATH", root / "state" / "latest_run.json"):
                import_instance_payload(payload_path, run_dir)

            stored_payload = json.loads((run_dir / "prep" / "instance" / "payload.json").read_text(encoding="utf-8"))
            self.assertNotIn("clustering_method", stored_payload)
            self.assertFalse((run_dir / "prep" / "clustering" / "assignments.json").exists())

    def test_import_writes_assignments_when_all_stores_are_clustered(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            payload_path = self.write_payload(
                root,
                payload_with_stores(
                    [
                        {"client_num": 1, "store_id": "A", "latitude": 48.1, "longitude": 11.1, "cluster_id": 0},
                        {"client_num": 2, "store_id": "B", "latitude": 48.2, "longitude": 11.2, "cluster_id": 1},
                    ]
                ),
            )
            run_dir = root / "run"

            with patch.object(lieferdaten_runtime, "LATEST_RUN_PATH", root / "state" / "latest_run.json"):
                import_instance_payload(payload_path, run_dir)

            assignments = json.loads((run_dir / "prep" / "clustering" / "assignments.json").read_text(encoding="utf-8"))
            self.assertEqual(len(assignments["assignments"]), 2)


if __name__ == "__main__":
    unittest.main()
