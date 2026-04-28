from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from webserver_runtime import ensure_webserver_dirs

from . import pipeline_jobs
from . import runtime
from . import storage


def _active_runs_for_instance(instance_id: str) -> list[dict[str, Any]]:
    rows = []
    for row in storage.list_runs_from_manifests()["runs"]:
        if str(row.get("instance_id") or "").strip() != instance_id:
            continue
        if str(row.get("status") or "").strip().lower() in runtime.TERMINAL_STATES:
            continue
        rows.append(row)
    return rows


def _optional_float(payload: dict[str, Any], key: str) -> float | None:
    value = payload.get(key)
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"{key} must be a number") from exc
    if parsed < 0:
        raise HTTPException(status_code=400, detail=f"{key} must be non-negative")
    return parsed


def _optional_int(payload: dict[str, Any], key: str) -> int | None:
    value = payload.get(key)
    if value is None or value == "":
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"{key} must be an integer") from exc
    if parsed < 0:
        raise HTTPException(status_code=400, detail=f"{key} must be non-negative")
    return parsed


def _parse_run_parameters(parameters_json: str | None) -> dict[str, Any]:
    if not parameters_json or not parameters_json.strip():
        return {}
    try:
        raw = json.loads(parameters_json)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="parameters_json must be valid JSON") from exc
    if not isinstance(raw, dict):
        raise HTTPException(status_code=400, detail="parameters_json must be a JSON object")

    params: dict[str, Any] = {}
    for key in (
        "d_cost",
        "h",
        "charger_cost_multiplier",
        "gap",
        "second_stage_eval_mipgap",
        "reopt_eval_mipgap",
        "timelimit_master_iter",
        "heur_timelimit",
        "var_timelimit_minimum",
        "second_stage_eval_timelimit",
    ):
        value = _optional_float(raw, key)
        if value is not None:
            params[key] = value
    for key in ("max_tours_per_truck", "scenarios_to_use", "gurobi_threads", "parallel_total_threads", "stage1_max_iterations", "reopt_max_iterations"):
        value = _optional_int(raw, key)
        if value is not None:
            params[key] = value
    vehicle_type = str(raw.get("vehicle_type") or "").strip()
    if vehicle_type:
        if vehicle_type not in {"mercedes", "volvo"}:
            raise HTTPException(status_code=400, detail="vehicle_type must be mercedes or volvo")
        params["vehicle_type"] = vehicle_type
    compute_profile = str(raw.get("compute_profile") or "").strip()
    if compute_profile:
        if compute_profile not in {"light", "heavy"}:
            raise HTTPException(status_code=400, detail="compute_profile must be light or heavy")
        params["compute_profile"] = compute_profile
    return params


def create_run(*, instance_id: str, runtime_id: str = "local", parameters_json: str | None = None) -> dict[str, Any]:
    ensure_webserver_dirs()
    if not storage.instance_dir(instance_id).exists():
        raise HTTPException(status_code=404, detail=f"instance not found: {instance_id}")
    active = _active_runs_for_instance(instance_id)
    if active:
        descriptions = [
            f"{row['run_id']} on {row.get('runtime_label') or row.get('runtime_id') or 'unknown'} ({row.get('status') or 'active'})"
            for row in active
        ]
        raise HTTPException(status_code=409, detail=f"instance {instance_id} already has active run(s): {', '.join(descriptions)}")
    run_parameters = _parse_run_parameters(parameters_json)
    run_root = pipeline_jobs.create_run_workspace(instance_id, runtime_id, run_parameters=run_parameters)
    try:
        job = pipeline_jobs.prepare_run_for_runtime(run_root, runtime_id=runtime_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    runtime.enqueue_run(run_root.name, runtime_id)
    return {"updated_at": storage.now_iso(), "run_id": run_root.name, "job": job, "manifest": storage.read_run_manifest(run_root.name)}


def delete_run(run_id: str) -> dict[str, Any]:
    run_root = storage.run_dir(run_id)
    if not run_root.exists():
        raise HTTPException(status_code=404, detail=f"run not found: {run_id}")
    status = storage.read_pipeline_job(run_id)
    if str(status.get("status") or "").strip().lower() not in runtime.TERMINAL_STATES:
        raise HTTPException(status_code=409, detail=f"run is not terminal: {status.get('status')}")
    pipeline_jobs.delete_run_directory(run_id)
    storage.list_runs_from_manifests()
    storage.list_instances_from_manifests()
    return {"updated_at": storage.now_iso(), "deleted": True, "run_id": run_id}


def stop_run(run_id: str) -> dict[str, Any]:
    job = runtime.stop_run(run_id)
    storage.list_runs_from_manifests()
    storage.list_instances_from_manifests()
    return {"updated_at": storage.now_iso(), "run_id": run_id, "job": job}
