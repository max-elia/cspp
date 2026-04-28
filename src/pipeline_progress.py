from __future__ import annotations

from pathlib import Path
from typing import Any

from json_artifacts import read_json, write_json
from lieferdaten.runtime import get_run_layout, resolve_run_root

DEFAULT_CSPP_TIMELIMITS = {
    "timelimit_master_iter": 600.0,
    "second_stage_eval_timelimit": 300.0,
}


def _stage_status(path: Path) -> str:
    return "completed" if path.exists() else "missing"


def build_pipeline_progress(run_dir: str | Path, *, summary: dict[str, Any] | None = None) -> dict[str, Any]:
    run_root = resolve_run_root(run_dir)
    layout = get_run_layout(run_root)
    job = read_json(run_root / "state" / "pipeline_job.json", {}) or {}
    stages = {
        "first_stage": {
            "stage_key": "first_stage",
            "title": "Stage 1",
            "status": _stage_status(layout["cspp_first_stage"] / "cluster_summary.json"),
            "summary": {},
            "entities": [],
            "active_entities": [],
            "recent_events": [],
        },
        "scenario_evaluation": {
            "stage_key": "scenario_evaluation",
            "title": "Stage 2",
            "status": _stage_status(layout["cspp_scenario_evaluation"] / "cluster_summary.json"),
            "summary": {},
            "entities": [],
            "active_entities": [],
            "recent_events": [],
        },
        "reoptimization": {
            "stage_key": "reoptimization",
            "title": "Stage 3",
            "status": _stage_status(layout["cspp_reopt"] / "current_state.json"),
            "summary": read_json(layout["cspp_reopt"] / "current_state.json", {}) or {},
            "entities": [],
            "active_entities": [],
            "recent_events": [],
        },
    }
    return {
        "schema_version": 1,
        "run_id": run_root.name,
        "status": job.get("status") or "idle",
        "current_stage_key": job.get("current_stage_key"),
        "current_step_key": job.get("current_step_key"),
        "stages": stages,
        "recent_events": [],
        "alerts": [],
        "estimate": {"total_sec": None, "elapsed_sec": None, "remaining_sec": None},
    }


def pipeline_progress_path(run_dir: str | Path) -> Path:
    return resolve_run_root(run_dir) / "frontend" / "pipeline" / "progress.json"


def write_pipeline_job_state(run_dir: str | Path, *, summary: dict[str, Any] | None = None) -> Path:
    path = pipeline_progress_path(run_dir)
    write_json(path, build_pipeline_progress(run_dir, summary=summary))
    return path
