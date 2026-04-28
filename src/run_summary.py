from __future__ import annotations

from pathlib import Path
from typing import Any

from json_artifacts import read_json, write_json
from lieferdaten.runtime import get_run_layout, resolve_run_root


def _status(path: Path) -> str:
    return "completed" if path.exists() else "missing"


def build_run_summary(run_dir: str | Path) -> dict[str, Any]:
    run_root = resolve_run_root(run_dir)
    layout = get_run_layout(run_root)
    config = read_json(layout["run_config"], {}) or {}
    summary = {
        "schema_version": 1,
        "run": {
            "run_id": run_root.name,
            "run_dir": str(run_root),
            "instance_id": config.get("instance_id"),
            "clustering_method": config.get("clustering_method"),
            "vehicle_type": config.get("vehicle_type"),
            "scenarios_to_use": config.get("scenarios_to_use"),
            "last_stage_recorded": config.get("last_stage"),
        },
        "stage_status": {
            "first_stage": _status(layout["cspp_first_stage"] / "cluster_summary.json"),
            "scenario_evaluation": _status(layout["cspp_scenario_evaluation"] / "cluster_summary.json"),
            "reoptimization": _status(layout["cspp_reopt"] / "current_state.json"),
        },
    }
    return summary


def write_run_summary(run_dir: str | Path) -> Path:
    run_root = resolve_run_root(run_dir)
    path = run_root / "summary.json"
    write_json(path, build_run_summary(run_root))
    return path
