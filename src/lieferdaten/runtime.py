from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from json_artifacts import now_iso
from json_artifacts import write_json

PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXPORT_ROOT = Path(os.environ.get("THESIS_EXPORT_ROOT", PROJECT_ROOT / "exports")).expanduser().resolve()
RUNS_DIR = EXPORT_ROOT / "runs"
STATE_DIR = EXPORT_ROOT / "state"
LATEST_RUN_PATH = STATE_DIR / "latest_run.json"


RUN_STEP_FOLDERS = {
    "process_tour_data": "01_process_tour_data",
    "generate_instance_data": "02_generate_instance_data",
    "combine_tours": "03_combine_tours",
    "clustering": "04_clustering",
    "solve_clusters_first_stage": "05_solve_clusters_first_stage",
    "scenario_evaluation": "06_scenario_evaluation",
    "cluster_reoptimization": "07_cluster_reoptimization",
}


def parse_optional_non_negative_float(raw_value: str):
    """Parse non-negative float values and allow disabling with 'none'."""
    if raw_value is None:
        return None

    normalized = str(raw_value).strip().lower()
    if normalized in {"none", "null", "off", "disable", "disabled"}:
        return None

    try:
        parsed = float(raw_value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid numeric value: {raw_value}") from exc

    if parsed < 0:
        raise argparse.ArgumentTypeError("Distance threshold must be >= 0")
    return parsed


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip())
    cleaned = cleaned.strip("-_.")
    return cleaned or "run"


def _scalar_slug(value: Any) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return str(value).replace(".", "p")
    return _slugify(str(value))


def max_distance_slug(max_distance_km: float | None) -> str:
    if max_distance_km is None:
        return "maxdistall"
    if float(max_distance_km).is_integer():
        return f"maxdist{int(max_distance_km)}"
    compact = str(max_distance_km).replace(".", "p")
    return f"maxdist{compact}"


def build_run_folder_name(
    vehicle_type: str = "mercedes",
    clustering_method: str = "geographic",
    scenarios_to_use: int | None = None,
    run_name: str | None = None,
) -> str:
    """Build a short run folder name."""
    if run_name:
        return _slugify(run_name)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S-%f")[:-3]
    vehicle_token = vehicle_type or "mercedes"
    clustering_token = clustering_method or "geographic"
    parts = [timestamp, _slugify(vehicle_token), _slugify(clustering_token)]
    return "_".join(part for part in parts if part)


def build_run_dir(
    max_distance_km: float | None = None,
    vehicle_type: str = "mercedes",
    clustering_method: str = "geographic",
    scenarios_to_use: int | None = None,
    run_name: str | None = None,
) -> Path:
    return (
        RUNS_DIR
        / build_run_folder_name(
            vehicle_type=vehicle_type,
            clustering_method=clustering_method,
            scenarios_to_use=scenarios_to_use,
            run_name=run_name,
        )
    ).resolve()


def resolve_run_dir(
    output_run_dir: str | None = None,
    run_name: str | None = None,
    max_distance_km: float | None = None,
    vehicle_type: str = "mercedes",
    clustering_method: str = "geographic",
    scenarios_to_use: int | None = None,
) -> Path:
    if output_run_dir:
        return Path(output_run_dir).expanduser().resolve()

    env_run_dir = os.environ.get("RUN_DIR")
    if env_run_dir:
        return Path(env_run_dir).expanduser().resolve()

    return build_run_dir(
        max_distance_km=max_distance_km,
        vehicle_type=vehicle_type,
        clustering_method=clustering_method,
        scenarios_to_use=scenarios_to_use,
        run_name=run_name,
    )


def resolve_run_root(path: str | Path) -> Path:
    """Resolve a run root from either the run dir or any path inside it."""
    candidate = Path(path).expanduser().resolve()
    if candidate.is_file():
        candidate = candidate.parent

    for current in (candidate, *candidate.parents):
        if (current / "run_config.json").exists() or (current / "manifest.json").exists():
            return current

    raise FileNotFoundError(f"Could not resolve run root from path: {candidate}")


def get_run_layout(run_dir: Path) -> Dict[str, Path]:
    run_dir = Path(run_dir).expanduser().resolve()
    try:
        run_dir = resolve_run_root(run_dir)
    except FileNotFoundError:
        pass

    process_stage = run_dir / RUN_STEP_FOLDERS["process_tour_data"]
    instance_stage = run_dir / RUN_STEP_FOLDERS["generate_instance_data"]
    combine_stage = run_dir / RUN_STEP_FOLDERS["combine_tours"]
    clustering_stage = run_dir / RUN_STEP_FOLDERS["clustering"]
    first_stage = run_dir / RUN_STEP_FOLDERS["solve_clusters_first_stage"]
    scenario_stage = run_dir / RUN_STEP_FOLDERS["scenario_evaluation"]
    reopt_stage = run_dir / RUN_STEP_FOLDERS["cluster_reoptimization"]

    return {
        "run": run_dir,
        "run_config": run_dir / "run_config.json",
        "results_json": run_dir / "results.json",
        "manifest": run_dir / "manifest.json",
        # Global catalog
        "catalog": EXPORT_ROOT / "catalog.json",
        "catalog_scenarios": EXPORT_ROOT / "catalog_scenarios.json",
        # Step roots
        "process_stage": process_stage,
        "instance_stage": instance_stage,
        "combine_stage": combine_stage,
        "clustering_stage": clustering_stage,
        "cspp_first_stage_root": first_stage,
        "cspp_scenario_stage": scenario_stage,
        "cspp_reopt_stage": reopt_stage,
        # Step 1: process_tour_data
        "process_tour_data": process_stage,
        "process_data": process_stage / "data",
        "process_reports": process_stage / "reports",
        "process_figures": process_stage / "figures",
        # Step 2: generate_instance_data
        "generate_instance_data": instance_stage,
        "instance_reports": instance_stage / "reports",
        "instance_figures": instance_stage / "figures",
        "instance_demand_maps": instance_stage / "figures" / "demand_maps",
        "instance_tour_maps": instance_stage / "figures" / "tour_maps",
        "cspp_data": instance_stage / "data",
        # Step 3: combine_tours
        "combine_tours": combine_stage,
        "combine_reports": combine_stage / "reports",
        "combine_figures": combine_stage / "figures",
        "combine_tables": combine_stage / "tables",
        # Shared / auxiliary exports
        "unloading_time": EXPORT_ROOT / "unloading_time_model",
        "unloading_reports": EXPORT_ROOT / "unloading_time_model" / "reports",
        "unloading_figures": EXPORT_ROOT / "unloading_time_model" / "figures",
        # Step 4: clustering
        "clustering": clustering_stage,
        "clustering_data": clustering_stage / "data",
        "clustering_reports": clustering_stage / "reports",
        "clustering_figures": clustering_stage / "figures",
        # Step 5: CSPP first stage
        "cspp_first_stage": first_stage / "results",
        "cspp_first_stage_logs": first_stage / "logs",
        "cspp_first_stage_figures": first_stage / "figures",
        "cspp_first_stage_route_maps": first_stage / "figures" / "route_maps",
        "cspp_first_stage_solver": first_stage / "solver",
        "cspp_first_stage_json": first_stage / "solver" / "first_stage_jsons",
        # Step 6: scenario evaluation
        "cspp_scenario_evaluation": scenario_stage / "results",
        "cspp_scenario_figures": scenario_stage / "figures",
        "cspp_scenario_route_maps": scenario_stage / "figures" / "route_maps",
        "cspp_scenario_solver": scenario_stage / "solver",
        "cspp_scenario_warmstarts": scenario_stage / "solver" / "warmstarts",
        # Step 7: reoptimization
        "cspp_reopt": reopt_stage / "results",
        "cspp_reopt_summary": reopt_stage / "summary",
        "cspp_reopt_iterations": reopt_stage / "iterations",
        "cspp_reopt_figures": reopt_stage / "figures",
        "cspp_reopt_route_maps": reopt_stage / "figures" / "route_maps",
        "cspp_reopt_solver": reopt_stage / "solver",
        "cspp_reopt_cache": reopt_stage / "solver" / "cache",
    }


def ensure_run_subdirs(run_dir: Path, *, debug: bool = False) -> Dict[str, Path]:
    subdirs = get_run_layout(run_dir)
    dir_keys = [
        "run",
        "process_stage",
        "process_data",
        "process_reports",
        "process_figures",
        "instance_stage",
        "instance_reports",
        "instance_figures",
        "instance_demand_maps",
        "instance_tour_maps",
        "cspp_data",
        "combine_stage",
        "combine_reports",
        "combine_figures",
        "combine_tables",
        "clustering_stage",
        "clustering_data",
        "clustering_reports",
        "clustering_figures",
        "cspp_first_stage_root",
        "cspp_first_stage",
        "cspp_first_stage_logs",
        "cspp_first_stage_figures",
        "cspp_first_stage_route_maps",
        "cspp_first_stage_solver",
        "cspp_first_stage_json",
        "cspp_scenario_stage",
        "cspp_scenario_evaluation",
        "cspp_scenario_figures",
        "cspp_scenario_route_maps",
        "cspp_scenario_solver",
        "cspp_scenario_warmstarts",
        "cspp_reopt_stage",
        "cspp_reopt",
        "cspp_reopt_summary",
        "cspp_reopt_iterations",
        "cspp_reopt_figures",
        "cspp_reopt_route_maps",
        "cspp_reopt_solver",
        "cspp_reopt_cache",
        "unloading_time",
        "unloading_reports",
        "unloading_figures",
    ]
    for key in dir_keys:
        subdirs[key].mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    write_json(
        LATEST_RUN_PATH,
        {
            "schema_version": 1,
            "updated_at": now_iso(),
            "run_id": Path(run_dir).name,
            "run_dir": str(Path(run_dir).expanduser().resolve()),
        },
    )
    return subdirs


def _make_json_safe(value: Any):
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _make_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_make_json_safe(v) for v in value]
    return value


def merge_run_config(run_dir: Path, updates: Dict[str, Any]) -> Path:
    run_dir = Path(run_dir).expanduser().resolve()
    layout = get_run_layout(run_dir)
    config_path = layout["run_config"]
    manifest_path = layout["manifest"]
    existing: Dict[str, Any] = {}
    for metadata_path in (config_path, manifest_path):
        if not metadata_path.exists():
            continue
        try:
            existing = json.loads(metadata_path.read_text(encoding="utf-8"))
            break
        except Exception:
            existing = {}

    for key, value in updates.items():
        existing[key] = _make_json_safe(value)

    existing.setdefault("schema_version", 4)
    existing["run_dir"] = str(run_dir)

    payload = json.dumps(existing, indent=2, sort_keys=True)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(payload, encoding="utf-8")
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(payload, encoding="utf-8")
    return config_path


def merge_results_json(run_dir: Path, section: str, data: Dict[str, Any]) -> Path:
    """Incrementally build results.json by merging *data* under *section*.

    Sections are top-level keys such as ``"params"``, ``"instance"``,
    ``"metrics"`` (or sub-keys like ``"metrics.stage2_cluster_eval"``).
    Dot-separated section names create nested dicts.
    """
    run_dir = Path(run_dir).expanduser().resolve()
    results_path = run_dir / "results.json"
    existing: Dict[str, Any] = {}
    if results_path.exists():
        try:
            existing = json.loads(results_path.read_text(encoding="utf-8"))
        except Exception:
            existing = {}

    existing.setdefault("schema_version", 4)
    existing["run_id"] = run_dir.name

    parts = section.split(".")
    target = existing
    for part in parts[:-1]:
        target = target.setdefault(part, {})
    leaf = parts[-1]

    if isinstance(target.get(leaf), dict) and isinstance(data, dict):
        target[leaf].update(_make_json_safe(data))
    else:
        target[leaf] = _make_json_safe(data)

    existing["updated_at"] = datetime.now(timezone.utc).isoformat()

    results_path.parent.mkdir(parents=True, exist_ok=True)
    results_path.write_text(
        json.dumps(existing, indent=2, sort_keys=False),
        encoding="utf-8",
    )
    return results_path


def is_debug_run(run_dir: Path) -> bool:
    """Check whether a run was started with --debug."""
    run_dir = Path(run_dir).expanduser().resolve()
    config_path = run_dir / "run_config.json"
    if not config_path.exists():
        return False
    try:
        cfg = json.loads(config_path.read_text(encoding="utf-8"))
        return bool(cfg.get("debug", False))
    except Exception:
        return False
