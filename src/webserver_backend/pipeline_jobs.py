from __future__ import annotations

import math
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from clustering.config import CLUSTERING_DIR
from clustering.config import PYTHONPATH_ENTRIES
from frontend_exports import export_frontend_contract
from json_artifacts import read_table_rows
from json_artifacts import write_table
from lieferdaten.runtime import get_run_layout
from lieferdaten.runtime import merge_run_config
from pipeline_progress import write_pipeline_job_state
from runtime_manager import delete_remote_run_dir
from runtime_manager import get_runtime
from runtime_manager import list_runtimes
from runtime_manager import load_queue_state
from runtime_manager import probe_runtime
from runtime_manager import read_remote_run_status
from runtime_manager import remote_run_paths
from runtime_manager import save_queue_state
from runtime_manager import start_remote_run
from runtime_manager import stop_remote_run
from runtime_manager import sync_run_from_runtime
from runtime_manager import usable_cores_for
from webserver_runtime import WEBSERVER_EXPORT_ROOT
from webserver_runtime import WEBSERVER_INSTANCE_ROOT
from webserver_runtime import WEBSERVER_ROOT
from webserver_runtime import ensure_webserver_dirs

from . import storage


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_STATUS_DIR = WEBSERVER_ROOT / "state" / "pipeline_jobs"
PIPELINE_LOG_TAIL_LIMIT = 120

_PIPELINE_JOBS: dict[str, dict[str, object]] = {}
_PIPELINE_LOCK = threading.Lock()
_PIPELINE_PROCESSES: dict[str, subprocess.Popen[str]] = {}
_RUNTIME_QUEUE_LOCK = threading.Lock()

_SSH_MONITORS: dict[str, threading.Thread] = {}
_POLLER_STOP = threading.Event()
_POLLER_THREAD: threading.Thread | None = None
_POLLER_INTERVAL_SECONDS = 2.0


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> dict[str, object]:
    payload = storage.read_json(path, {}) or {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: object) -> None:
    storage.write_json(path, payload)


def _safe_float(value: object | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"none", "null", "nan"}:
        return None
    try:
        return float(text)
    except Exception:
        return None


def safe_int(value: object | None) -> int | None:
    parsed = _safe_float(value)
    if parsed is None:
        return None
    return int(parsed)


def _append_run_flag(args: list[str], flag: str, value: object | None) -> None:
    if value is None:
        return
    if isinstance(value, str) and not value.strip():
        return
    args.extend([flag, str(value)])


def _instance_dir(instance_id: str) -> Path:
    return WEBSERVER_INSTANCE_ROOT / instance_id


def _run_dir(run_id: str) -> Path:
    return WEBSERVER_EXPORT_ROOT / "runs" / run_id


def _read_instance_manifest(instance_id: str) -> dict[str, object]:
    return _read_json(_instance_dir(instance_id) / "manifest.json")


def _read_instance_payload_dir(instance_dir: Path) -> dict[str, object]:
    return _read_json(instance_dir / "prep" / "instance" / "payload.json")


def _write_table_rows(path: Path, columns: list[str], rows: list[dict[str, object]]) -> None:
    write_table(path, columns, rows)


def _read_table_dict_rows(path: Path) -> list[dict[str, object]]:
    return read_table_rows(path)


def _tail_log(path: Path, limit: int = PIPELINE_LOG_TAIL_LIMIT) -> list[str]:
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return []
    return lines[-limit:]


def _run_terminal(status: str | None) -> bool:
    return str(status or "").strip().lower() in {"idle", "completed", "failed", "stopped"}


def job_log_path(run_id: str) -> Path:
    return PIPELINE_STATUS_DIR / f"{run_id}.log"


def job_status(run_id: str) -> dict[str, object] | None:
    with _PIPELINE_LOCK:
        existing = _PIPELINE_JOBS.get(run_id)
        if existing is None:
            return None
        return dict(existing)


def _set_job_status(run_id: str, **updates: object) -> dict[str, object]:
    with _PIPELINE_LOCK:
        current = dict(_PIPELINE_JOBS.get(run_id, {}))
        current.update(updates)
        _PIPELINE_JOBS[run_id] = current
        return dict(current)


def _register_pipeline_process(run_id: str, process: subprocess.Popen[str] | None) -> None:
    with _PIPELINE_LOCK:
        if process is None:
            _PIPELINE_PROCESSES.pop(run_id, None)
        else:
            _PIPELINE_PROCESSES[run_id] = process


def _active_pipeline_processes() -> list[tuple[str, subprocess.Popen[str]]]:
    with _PIPELINE_LOCK:
        return list(_PIPELINE_PROCESSES.items())


def _process_exists(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False
    return True


def _queue_position(runtime_id: str, run_id: str) -> int | None:
    queue_state = load_queue_state(runtime_id)
    queued_run_ids = [str(item) for item in (queue_state.get("queued_run_ids") or []) if str(item).strip()]
    if run_id not in queued_run_ids:
        return None
    return queued_run_ids.index(run_id) + 1


def _persist_runtime_queue(runtime_id: str, *, active_run_id: str | None, queued_run_ids: list[str]) -> dict[str, object]:
    with _RUNTIME_QUEUE_LOCK:
        return save_queue_state(runtime_id, active_run_id=active_run_id, queued_run_ids=queued_run_ids)


def _read_persisted_job_status(run_id: str) -> dict[str, object]:
    return storage.read_pipeline_job(run_id)


def _stop_requested(run_id: str) -> bool:
    payload = _read_persisted_job_status(run_id)
    return str(payload.get("status") or "").strip().lower() == "stopped"


def persist_pipeline_job_state(run_id: str) -> dict[str, object] | None:
    run_dir = _run_dir(run_id)
    if not run_dir.exists():
        return None
    status = job_status(run_id) or {}
    log_path = Path(status.get("log_path") or job_log_path(run_id))
    runtime_id = str(status.get("runtime_id") or "").strip() or None
    payload = {
        "run_id": run_id,
        "status": status.get("status", "idle"),
        "started_at": status.get("started_at"),
        "queued_at": status.get("queued_at"),
        "finished_at": status.get("finished_at"),
        "current_stage_key": status.get("current_stage_key"),
        "current_stage_label": status.get("current_stage_label"),
        "current_step_key": status.get("current_step_key") or status.get("current_step"),
        "current_step_label": status.get("current_step_label") or status.get("current_step"),
        "error": status.get("error"),
        "pid": status.get("pid"),
        "steps": status.get("steps") or [],
        "returncode": status.get("returncode"),
        "log_tail": _tail_log(log_path),
        "log_path": str(log_path),
        "runtime_id": runtime_id,
        "runtime_label": status.get("runtime_label"),
        "runtime_kind": status.get("runtime_kind"),
        "queue_position": _queue_position(runtime_id, run_id) if runtime_id else None,
        "runtime_probe": status.get("runtime_probe"),
        "runtime_system_cores": status.get("runtime_system_cores"),
        "runtime_usable_cores": status.get("runtime_usable_cores"),
        "remote_run_dir": status.get("remote_run_dir"),
        "remote_pid_path": status.get("remote_pid_path"),
        "remote_exit_path": status.get("remote_exit_path"),
        "last_sync_at": status.get("last_sync_at"),
        "sync_status": status.get("sync_status"),
        "updated_at": _now_iso(),
    }
    write_pipeline_job_state(run_dir, payload)
    storage.write_sync_manifest(
        run_id,
        {
            "runtime_id": runtime_id,
            "runtime_kind": status.get("runtime_kind"),
            "sync_status": status.get("sync_status"),
            "last_sync_at": status.get("last_sync_at"),
            "remote_run_dir": status.get("remote_run_dir"),
            "remote_pid_path": status.get("remote_pid_path"),
            "remote_exit_path": status.get("remote_exit_path"),
            "stop_requested": str(payload.get("status") or "").strip().lower() == "stopped",
        },
    )
    try:
        export_frontend_contract(run_dir)
    except Exception:
        pass
    storage.rebuild_run_artifact_manifest(run_id)
    storage.list_runs_from_manifests()
    return payload


def load_persisted_pipeline_jobs() -> None:
    runs_dir = WEBSERVER_EXPORT_ROOT / "runs"
    if not runs_dir.exists():
        return
    for run_dir in sorted(path for path in runs_dir.iterdir() if path.is_dir()):
        payload = _read_json(run_dir / "state" / "pipeline_job.json")
        if not payload:
            continue
        run_id = run_dir.name
        _set_job_status(
            run_id,
            status=payload.get("status", "idle"),
            started_at=payload.get("started_at"),
            queued_at=payload.get("queued_at"),
            finished_at=payload.get("finished_at"),
            current_stage_key=payload.get("current_stage_key"),
            current_stage_label=payload.get("current_stage_label"),
            current_step=payload.get("current_step_key") or payload.get("current_step_label"),
            current_step_key=payload.get("current_step_key"),
            current_step_label=payload.get("current_step_label"),
            error=payload.get("error"),
            pid=payload.get("pid"),
            steps=payload.get("steps") or [],
            returncode=payload.get("returncode"),
            log_path=payload.get("log_path") or str(job_log_path(run_id)),
            runtime_id=payload.get("runtime_id"),
            runtime_label=payload.get("runtime_label"),
            runtime_kind=payload.get("runtime_kind"),
            runtime_probe=payload.get("runtime_probe"),
            runtime_system_cores=payload.get("runtime_system_cores"),
            runtime_usable_cores=payload.get("runtime_usable_cores"),
            remote_run_dir=payload.get("remote_run_dir"),
            remote_pid_path=payload.get("remote_pid_path"),
            remote_exit_path=payload.get("remote_exit_path"),
            last_sync_at=payload.get("last_sync_at"),
            sync_status=payload.get("sync_status"),
        )


def _reconcile_pipeline_jobs_on_startup() -> None:
    for run_id in list(_PIPELINE_JOBS):
        status = job_status(run_id) or {}
        state = str(status.get("status") or "").strip().lower()
        pid = safe_int(status.get("pid"))
        runtime_kind = str(status.get("runtime_kind") or "").strip().lower()
        if state == "queued":
            continue
        if state not in {"preparing", "syncing_to_runtime", "running", "syncing_from_runtime"}:
            continue
        if runtime_kind == "ssh":
            continue
        if runtime_kind == "local" and _process_exists(pid):
            continue
        _set_job_status(
            run_id,
            status="stopped",
            current_stage_key=None,
            current_stage_label=None,
            current_step=None,
            current_step_key=None,
            current_step_label=None,
            finished_at=_now_iso(),
            pid=None,
            error="Pipeline stopped because the worker shut down.",
        )
        persist_pipeline_job_state(run_id)


def _reconcile_runtime_queues_on_startup() -> None:
    for runtime in list_runtimes():
        runtime_id = str(runtime.get("id") or "").strip()
        if not runtime_id:
            continue
        queue_state = load_queue_state(runtime_id)
        active_run_id = str(queue_state.get("active_run_id") or "").strip() or None
        queued_run_ids = [str(item) for item in (queue_state.get("queued_run_ids") or []) if str(item).strip()]
        runtime_kind = str(runtime.get("kind") or "").strip().lower()
        keep_active_run_id: str | None = None
        if runtime_kind == "ssh" and active_run_id:
            active_status = job_status(active_run_id) or {}
            remote_pid_path = str(active_status.get("remote_pid_path") or "").strip()
            remote_exit_path = str(active_status.get("remote_exit_path") or "").strip()
            if remote_pid_path and remote_exit_path:
                remote_status = read_remote_run_status(
                    runtime_id,
                    remote_pid_path=remote_pid_path,
                    remote_exit_path=remote_exit_path,
                )
                if remote_status.get("state") == "running":
                    keep_active_run_id = active_run_id
        _persist_runtime_queue(runtime_id, active_run_id=keep_active_run_id, queued_run_ids=queued_run_ids)


def _safe_text(value: object | None) -> str:
    return str(value or "").strip()


def _configured_frontend_clustering_method(run_dir: Path) -> str | None:
    def canonical(method: str) -> str | None:
        value = method.strip()
        if value.startswith("angular_gap") or value.startswith("angular_slices"):
            return "angular_slices"
        if value in {"kmeans", "depot_regularized_kmeans", "hierarchical_ward", "geographic"}:
            return "geographic"
        if value in {"tour_containment", "angular_slices_store_count"}:
            return value
        if value in {"manual", "predefined", "predefined_cluster_ids", "imported_bundle"}:
            return None
        return None

    payload = _read_instance_payload(run_dir)
    payload_method = payload.get("clustering_method")
    if isinstance(payload_method, str) and payload_method.strip():
        method = canonical(payload_method)
        if method:
            return method
    config = _read_json(run_dir / "run_config.json")
    configured = config.get("clustering_method")
    if isinstance(configured, str) and configured.strip():
        return canonical(configured)
    return None


def _read_instance_payload(run_dir: Path) -> dict[str, object]:
    payload_path = run_dir / "prep" / "instance" / "payload.json"
    payload = _read_json(payload_path)
    if payload:
        return payload
    customers_json = _read_json(run_dir / "prep" / "instance" / "customers.json")
    customers = customers_json.get("customers") if isinstance(customers_json.get("customers"), list) else []
    demand_rows = _read_table_dict_rows(run_dir / "prep" / "instance" / "demand_long.json")
    map_summary = _read_json(run_dir / "prep" / "map" / "customers_summary.json")
    warehouse = map_summary.get("warehouse") if isinstance(map_summary.get("warehouse"), dict) else None
    payload = {
        "schema_version": 1,
        "run_id": run_dir.name,
        "generated_at": _now_iso(),
        "clustering_method": _configured_frontend_clustering_method(run_dir),
        "warehouse": warehouse,
        "customers": customers,
        "demand_rows": demand_rows,
    }
    _write_json(payload_path, payload)
    return payload


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return round(radius_km * c, 4)


def _write_wide_table(path: Path, columns: list[str], rows: list[dict[str, object]]) -> None:
    write_table(path, columns, rows)


def _has_prepared_manual_clustering(run_dir: Path) -> bool:
    prep_root = run_dir / "prep" / "clustering"
    assignments_payload = _read_json(prep_root / "assignments.json")
    if isinstance(assignments_payload.get("assignments"), list) and assignments_payload["assignments"]:
        return True
    if read_table_rows(prep_root / "cluster_assignments.json"):
        return True
    return bool(read_table_rows(prep_root / "manual_assignments.json"))


def _ensure_clustering_inputs(run_dir: Path) -> None:
    layout = get_run_layout(run_dir)
    clustering_data_dir = layout["clustering_data"]
    required_paths = [
        clustering_data_dir / "cluster_assignments.json",
        clustering_data_dir / "arc_set.json",
        clustering_data_dir / "arc_set_global.json",
    ]
    if all(path.exists() for path in required_paths):
        return
    if not _has_prepared_manual_clustering(run_dir):
        return
    env = os.environ.copy()
    current_pythonpath = env.get("PYTHONPATH", "")
    entries = [str(path) for path in PYTHONPATH_ENTRIES]
    env["PYTHONPATH"] = os.pathsep.join(entries + ([current_pythonpath] if current_pythonpath else []))
    env["RUN_DIR"] = str(run_dir)
    result = subprocess.run(
        [sys.executable, str(CLUSTERING_DIR / "build_clusters_manual.py")],
        cwd=PROJECT_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        message = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(message or f"manual clustering bootstrap failed with exit code {result.returncode}")


def _hydrate_instance_run_data(run_dir: Path) -> None:
    layout = get_run_layout(run_dir)
    cspp_data_dir = layout["cspp_data"]
    required_files = [
        cspp_data_dir / "coordinates.json",
        cspp_data_dir / "customer_id_mapping.json",
        cspp_data_dir / "demand_matrix.json",
        cspp_data_dir / "distances_matrix.json",
    ]
    if all(path.exists() for path in required_files):
        return
    payload = _read_instance_payload(run_dir)
    customers_raw = payload.get("stores")
    if not isinstance(customers_raw, list):
        customers_raw = payload.get("customers")
    demand_rows_raw = payload.get("demand_rows")
    warehouse_raw = payload.get("warehouse")
    if not isinstance(customers_raw, list) or not isinstance(demand_rows_raw, list):
        return
    customers: list[dict[str, object]] = []
    for row in customers_raw:
        if not isinstance(row, dict):
            continue
        client_num = safe_int(row.get("client_num"))
        latitude = _safe_float(row.get("latitude"))
        longitude = _safe_float(row.get("longitude"))
        if client_num is None or latitude is None or longitude is None:
            continue
        customers.append(
            {
                "client_num": client_num,
                "customer_id": _safe_text(row.get("customer_id") or row.get("store_id") or client_num),
                "latitude": latitude,
                "longitude": longitude,
            }
        )
    warehouse_lat = _safe_float(warehouse_raw.get("latitude")) if isinstance(warehouse_raw, dict) else None
    warehouse_lon = _safe_float(warehouse_raw.get("longitude")) if isinstance(warehouse_raw, dict) else None
    if not customers or warehouse_lat is None or warehouse_lon is None:
        return
    customers.sort(key=lambda row: int(row["client_num"]))
    cspp_data_dir.mkdir(parents=True, exist_ok=True)
    coordinates_rows: list[dict[str, object]] = [{"node_index": 0, "latitude": warehouse_lat, "longitude": warehouse_lon}]
    mapping_rows: list[dict[str, object]] = []
    customer_positions: dict[int, tuple[float, float]] = {0: (warehouse_lat, warehouse_lon)}
    for customer in customers:
        client_num = int(customer["client_num"])
        latitude = float(customer["latitude"])
        longitude = float(customer["longitude"])
        customer_positions[client_num] = (latitude, longitude)
        coordinates_rows.append({"node_index": client_num, "latitude": latitude, "longitude": longitude})
        mapping_rows.append({"customer_id": str(customer["customer_id"]), "client_num": client_num})
    demand_by_date: dict[str, dict[int, float]] = defaultdict(dict)
    for row in demand_rows_raw:
        if not isinstance(row, dict):
            continue
        delivery_date = _safe_text(row.get("delivery_date"))
        client_num = safe_int(row.get("client_num"))
        demand_kg = _safe_float(row.get("demand_kg"))
        if not delivery_date or client_num is None or demand_kg is None:
            continue
        demand_by_date[delivery_date][client_num] = demand_by_date[delivery_date].get(client_num, 0.0) + demand_kg
    client_columns = [str(int(customer["client_num"])) for customer in customers]
    demand_columns = ["delivery_date", *client_columns]
    demand_rows: list[dict[str, object]] = []
    for delivery_date in sorted(demand_by_date):
        row_out: dict[str, object] = {"delivery_date": delivery_date}
        for customer in customers:
            client_num = int(customer["client_num"])
            row_out[str(client_num)] = round(demand_by_date[delivery_date].get(client_num, 0.0), 4)
        demand_rows.append(row_out)
    node_ids = [0, *[int(customer["client_num"]) for customer in customers]]
    distance_columns = ["node_index", *[str(node_id) for node_id in node_ids]]
    distance_rows: list[dict[str, object]] = []
    for from_node in node_ids:
        from_lat, from_lon = customer_positions[from_node]
        row_out: dict[str, object] = {"node_index": from_node}
        for to_node in node_ids:
            to_lat, to_lon = customer_positions[to_node]
            row_out[str(to_node)] = _haversine_km(from_lat, from_lon, to_lat, to_lon)
        distance_rows.append(row_out)
    _write_wide_table(cspp_data_dir / "coordinates.json", ["node_index", "latitude", "longitude"], coordinates_rows)
    _write_wide_table(cspp_data_dir / "customer_id_mapping.json", ["customer_id", "client_num"], mapping_rows)
    _write_wide_table(cspp_data_dir / "demand_matrix.json", demand_columns, demand_rows)
    _write_wide_table(cspp_data_dir / "distances_matrix.json", distance_columns, distance_rows)
    merge_run_config(
        run_dir,
        {
            "selected_domain": "clustering",
            "selected_target": "full",
            "clustering_method": _configured_frontend_clustering_method(run_dir),
        },
    )


def reset_pipeline_runtime_outputs(run_dir: Path) -> None:
    layout = get_run_layout(run_dir)
    cleanup_targets = [
        layout["cspp_first_stage"] / "cluster_live",
        layout["cspp_first_stage_logs"],
        layout["cspp_first_stage"] / "cluster_progress_snapshot.json",
        layout["cspp_first_stage"] / "current_state.json",
        layout["cspp_scenario_evaluation"] / "solver_live",
        layout["cspp_scenario_evaluation"] / "scenario_progress_snapshot.json",
        layout["cspp_reopt"] / "cluster_live",
        layout["cspp_reopt"] / "solver_live",
        layout["cspp_reopt"] / "current_state.json",
        layout["cspp_reopt"] / "event_log.jsonl",
    ]
    for target in cleanup_targets:
        if not target.exists():
            continue
        if target.is_dir():
            for child in sorted(target.rglob("*"), reverse=True):
                if child.is_file() or child.is_symlink():
                    child.unlink()
                elif child.is_dir():
                    child.rmdir()
            target.rmdir()
        else:
            target.unlink()


def _set_runtime_defaults(run_dir: Path, runtime_probe: dict[str, object]) -> dict[str, object]:
    run_config = _read_json(run_dir / "run_config.json")
    system_cores = safe_int(runtime_probe.get("system_cores"))
    usable_cores = safe_int(runtime_probe.get("usable_cores")) or usable_cores_for(system_cores)
    compute_profile = str(run_config.get("compute_profile") or "heavy").strip().lower()
    if compute_profile not in {"light", "heavy"}:
        compute_profile = "heavy"
    selected_cores = usable_cores
    if usable_cores is not None and compute_profile == "light":
        selected_cores = max(1, int(usable_cores) // 2)
    updates: dict[str, object] = {"runtime_id": run_config.get("runtime_id"), "runtime_probe": runtime_probe}
    if selected_cores is not None and safe_int(run_config.get("gurobi_threads")) is None:
        updates["gurobi_threads"] = selected_cores
    if selected_cores is not None and safe_int(run_config.get("parallel_total_threads")) is None:
        updates["parallel_total_threads"] = selected_cores
    if len(updates) > 2 or "gurobi_threads" in updates or "parallel_total_threads" in updates:
        merge_run_config(run_dir, updates)
        run_config = _read_json(run_dir / "run_config.json")
    return run_config


def _runtime_extra_env(run_config: dict[str, object]) -> dict[str, str]:
    env: dict[str, str] = {}
    gurobi_threads = safe_int(run_config.get("gurobi_threads"))
    parallel_total_threads = safe_int(run_config.get("parallel_total_threads"))
    if gurobi_threads is not None:
        env["GUROBI_THREADS"] = str(gurobi_threads)
    if parallel_total_threads is not None:
        env["CSPP_PARALLEL_TOTAL_THREADS"] = str(parallel_total_threads)
    return env


def _pipeline_commands(run_dir: Path) -> list[tuple[str, list[str]]]:
    config = _read_json(run_dir / "run_config.json")
    clustering_method = _configured_frontend_clustering_method(run_dir) or "geographic"
    cspp_cmd = [
        sys.executable,
        str(PROJECT_ROOT / "src" / "run.py"),
        "run",
        "cspp",
        "full",
        "--output-run-dir",
        str(run_dir),
        "--clustering-method",
        clustering_method,
    ]
    _append_run_flag(cspp_cmd, "--vehicle-type", config.get("vehicle_type"))
    _append_run_flag(cspp_cmd, "--scenarios-to-use", config.get("scenarios_to_use"))
    _append_run_flag(cspp_cmd, "--gurobi-threads", config.get("gurobi_threads"))
    _append_run_flag(cspp_cmd, "--d-cost", config.get("d_cost"))
    _append_run_flag(cspp_cmd, "--h", config.get("h"))
    _append_run_flag(cspp_cmd, "--max-tours-per-truck", config.get("max_tours_per_truck"))
    _append_run_flag(cspp_cmd, "--charger-cost-multiplier", config.get("charger_cost_multiplier"))
    _append_run_flag(cspp_cmd, "--gap", config.get("gap"))
    _append_run_flag(cspp_cmd, "--stage1-max-iterations", config.get("stage1_max_iterations"))
    _append_run_flag(cspp_cmd, "--reopt-max-iterations", config.get("reopt_max_iterations"))
    _append_run_flag(cspp_cmd, "--second-stage-eval-mipgap", config.get("second_stage_eval_mipgap"))
    _append_run_flag(cspp_cmd, "--reopt-eval-mipgap", config.get("reopt_eval_mipgap"))
    _append_run_flag(cspp_cmd, "--timelimit-master-iter", config.get("timelimit_master_iter"))
    _append_run_flag(cspp_cmd, "--heur-timelimit", config.get("heur_timelimit"))
    _append_run_flag(cspp_cmd, "--var-timelimit-minimum", config.get("var_timelimit_minimum"))
    _append_run_flag(cspp_cmd, "--second-stage-eval-timelimit", config.get("second_stage_eval_timelimit"))
    if bool(config.get("debug")):
        cspp_cmd.append("--debug")
    return [("cspp", cspp_cmd)]


def _pipeline_preflight(run_dir: Path, commands: list[tuple[str, list[str]]]) -> list[str]:
    del commands
    issues: list[str] = []
    cspp_required = [
        run_dir / "02_generate_instance_data" / "data" / "coordinates.json",
        run_dir / "02_generate_instance_data" / "data" / "distances_matrix.json",
        run_dir / "02_generate_instance_data" / "data" / "demand_matrix.json",
        run_dir / "02_generate_instance_data" / "data" / "customer_id_mapping.json",
    ]
    prepared_clustering_dir = run_dir / "prep" / "clustering"
    payload = _read_instance_payload(run_dir)
    stores = payload.get("stores")
    if not isinstance(stores, list):
        stores = payload.get("customers")
    if not isinstance(stores, list) or not stores:
        issues.append(f"missing instance payload stores: {run_dir / 'prep' / 'instance' / 'payload.json'}")
    for path in cspp_required:
        if not path.exists():
            issues.append(f"missing CSPP input file: {path}")
    assignments_payload = _read_json(prepared_clustering_dir / "assignments.json")
    has_assignments = isinstance(assignments_payload.get("assignments"), list) and bool(assignments_payload["assignments"])
    has_assignment_table = bool(read_table_rows(prepared_clustering_dir / "cluster_assignments.json"))
    has_pipeline_assignment_table = bool(read_table_rows(run_dir / "04_clustering" / "data" / "cluster_assignments.json"))
    if not (has_assignments or has_assignment_table or has_pipeline_assignment_table):
        issues.append(f"missing clustering assignments: {prepared_clustering_dir / 'assignments.json'}")
    return issues


def create_run_workspace(instance_id: str, runtime_id: str, run_parameters: dict[str, object] | None = None) -> Path:
    instance_dir = _instance_dir(instance_id)
    if not instance_dir.exists():
        raise FileNotFoundError(f"instance not found: {instance_id}")
    instance_alias = storage.compact_identifier(instance_id, fallback="run")
    base_run_id = f"{instance_alias}-{runtime_id}-{storage.timestamp_id()}"
    run_id = base_run_id
    counter = 2
    while _run_dir(run_id).exists():
        run_id = f"{base_run_id}-{counter}"
        counter += 1
    run_dir = _run_dir(run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    shutil.copytree(instance_dir / "prep", run_dir / "prep", dirs_exist_ok=True)
    run_parameters = run_parameters or {}
    run_config = {
        "d_cost": 0.25,
        "h": 50,
        "max_tours_per_truck": 3,
        "charger_cost_multiplier": 1,
        "vehicle_type": "mercedes",
        "compute_profile": "heavy",
        "clustering_method": (_read_instance_manifest(instance_id).get("clustering_method") or _read_instance_payload_dir(instance_dir).get("clustering_method")),
        "instance_id": instance_id,
    }
    run_config.update({key: value for key, value in run_parameters.items() if value is not None})
    _write_json(run_dir / "run_config.json", run_config)
    _write_json(
        run_dir / "manifest.json",
        {
            "run_id": run_id,
            "instance_id": instance_id,
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
            "runtime_id": runtime_id,
            "clustering_method": run_config.get("clustering_method"),
            "max_distance_from_warehouse_km": _read_instance_manifest(instance_id).get("max_distance_from_warehouse_km"),
        },
    )
    _hydrate_instance_run_data(run_dir)
    _ensure_clustering_inputs(run_dir)
    _set_job_status(
        run_id,
        status="idle",
        current_stage_key=None,
        current_stage_label=None,
        current_step=None,
        current_step_key=None,
        current_step_label=None,
        queued_at=None,
        started_at=None,
        finished_at=None,
        error=None,
        pid=None,
        log_path=str(job_log_path(run_id)),
        steps=[],
        runtime_id=runtime_id,
        runtime_label=get_runtime(runtime_id).get("label"),
        runtime_kind=get_runtime(runtime_id).get("kind"),
        remote_pid_path=None,
        remote_exit_path=None,
        remote_run_dir=None,
        sync_status="idle",
    )
    persist_pipeline_job_state(run_id)
    return run_dir


def delete_run_directory(run_id: str) -> None:
    run_dir = _run_dir(run_id)
    live_state = dict(job_status(run_id) or {})
    persisted_state = _read_json(run_dir / "state" / "pipeline_job.json") if run_dir.exists() else {}
    manifest = _read_json(run_dir / "manifest.json") if run_dir.exists() else {}
    runtime_id = str(live_state.get("runtime_id") or persisted_state.get("runtime_id") or manifest.get("runtime_id") or "").strip()
    remote_run_dir = str(live_state.get("remote_run_dir") or persisted_state.get("remote_run_dir") or "").strip()
    if runtime_id and remote_run_dir:
        try:
            delete_remote_run_dir(runtime_id, remote_run_dir)
        except Exception:
            pass
    if run_dir.exists():
        shutil.rmtree(run_dir)
    with _PIPELINE_LOCK:
        _PIPELINE_JOBS.pop(run_id, None)
        _PIPELINE_PROCESSES.pop(run_id, None)
    log_path = job_log_path(run_id)
    if log_path.exists():
        log_path.unlink()
    for runtime in list_runtimes():
        runtime_id = str(runtime.get("id") or "").strip()
        if not runtime_id:
            continue
        queue_state = load_queue_state(runtime_id)
        active = str(queue_state.get("active_run_id") or "").strip() or None
        queued = [str(item) for item in (queue_state.get("queued_run_ids") or []) if str(item).strip()]
        new_active = None if active == run_id else active
        new_queued = [item for item in queued if item != run_id]
        if new_active != active or new_queued != queued:
            _persist_runtime_queue(runtime_id, active_run_id=new_active, queued_run_ids=new_queued)


def build_queued_job_payload(
    *,
    run_id: str,
    runtime_id: str,
    runtime_row: dict[str, Any],
    runtime_probe: dict[str, Any],
    log_path: Path,
) -> dict[str, Any]:
    position = _queue_position(runtime_id, run_id)
    return {
        "run_id": run_id,
        "status": "queued",
        "queued_at": storage.now_iso(),
        "started_at": None,
        "finished_at": None,
        "current_stage_key": "pipeline",
        "current_stage_label": "Solve Pipeline",
        "current_step_key": None,
        "current_step_label": None,
        "error": None,
        "pid": None,
        "steps": ["cspp"],
        "returncode": None,
        "log_tail": [],
        "log_path": str(log_path),
        "runtime_id": runtime_id,
        "runtime_label": runtime_row.get("label"),
        "runtime_kind": runtime_row.get("kind"),
        "queue_position": position,
        "runtime_probe": runtime_probe,
        "runtime_system_cores": runtime_probe.get("system_cores"),
        "runtime_usable_cores": runtime_probe.get("usable_cores"),
        "remote_run_dir": None,
        "remote_pid_path": None,
        "remote_exit_path": None,
        "last_sync_at": None,
        "sync_status": "queued",
        "updated_at": storage.now_iso(),
    }


def prepare_run_for_runtime(run_root: Path, *, runtime_id: str) -> dict[str, Any]:
    runtime_row, runtime_probe = runtime_ready(runtime_id)
    _hydrate_instance_run_data(run_root)
    reset_pipeline_runtime_outputs(run_root)
    merge_run_config(
        run_root,
        {
            "runtime_id": runtime_id,
            "runtime_label": runtime_row.get("label"),
            "runtime_kind": runtime_row.get("kind"),
            "runtime_probe": runtime_probe,
        },
    )
    _set_runtime_defaults(run_root, runtime_probe)
    commands = _pipeline_commands(run_root)
    preflight_issues = _pipeline_preflight(run_root, commands)
    if preflight_issues:
        raise RuntimeError("; ".join(preflight_issues))
    log_path = job_log_path(run_root.name)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text("", encoding="utf-8")
    manifest = storage.read_run_manifest(run_root.name)
    manifest.update(
        {
            "run_id": run_root.name,
            "runtime_id": runtime_id,
            "runtime_label": runtime_row.get("label"),
            "runtime_kind": runtime_row.get("kind"),
            "updated_at": storage.now_iso(),
        }
    )
    storage.write_json(storage.run_manifest_path(run_root.name), manifest)
    job = build_queued_job_payload(
        run_id=run_root.name,
        runtime_id=runtime_id,
        runtime_row=runtime_row,
        runtime_probe=runtime_probe,
        log_path=log_path,
    )
    storage.write_json(storage.pipeline_job_path(run_root.name), job)
    storage.write_sync_manifest(
        run_root.name,
        {
            "runtime_id": runtime_id,
            "runtime_kind": runtime_row.get("kind"),
            "sync_status": "queued",
            "last_sync_at": None,
            "remote_run_dir": None,
        },
    )
    storage.rebuild_run_artifact_manifest(run_root.name)
    storage.list_runs_from_manifests()
    storage.list_instances_from_manifests()
    return job


def runtime_ready(runtime_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    runtime = get_runtime(runtime_id)
    probe = probe_runtime(runtime_id, repair=True)
    if not probe.get("ready"):
        raise RuntimeError(str(probe.get("error") or f"runtime {runtime_id} is not ready"))
    return runtime, probe


def _remote_sync_issue_status(sync_issue: str) -> str:
    value = str(sync_issue or "").strip().lower()
    if value in {"status_error", "status_unknown", "sync_error"}:
        return value
    return "sync_error"


def _finalize_runtime_run(run_id: str) -> None:
    status = job_status(run_id) or {}
    runtime_id = str(status.get("runtime_id") or "").strip()
    if not runtime_id:
        return
    if not _run_terminal(str(status.get("status") or "")):
        return
    queue_state = load_queue_state(runtime_id)
    queued_run_ids = [str(item) for item in (queue_state.get("queued_run_ids") or []) if str(item).strip()]
    active_run_id = str(queue_state.get("active_run_id") or "").strip() or None
    if active_run_id == run_id:
        active_run_id = None
    _persist_runtime_queue(runtime_id, active_run_id=active_run_id, queued_run_ids=queued_run_ids)
    start_next_runtime_run(runtime_id)


def _run_local_pipeline_job(
    run_id: str,
    *,
    run_dir: Path,
    log_path: Path,
    commands: list[tuple[str, list[str]]],
    extra_env: dict[str, str],
) -> None:
    env = os.environ.copy()
    env["RUN_DIR"] = str(run_dir)
    env.update(extra_env)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"[{_now_iso()}] Starting pipeline for {run_id}\n")
        handle.flush()
        for step_name, cmd in commands:
            _set_job_status(
                run_id,
                status="running",
                current_stage_key="pipeline",
                current_stage_label="Solve Pipeline",
                current_step=step_name,
                current_step_key=step_name,
                current_step_label=step_name,
                command=cmd,
                finished_at=None,
                error=None,
                sync_status="running",
            )
            persist_pipeline_job_state(run_id)
            handle.write(f"\n[{_now_iso()}] Step {step_name}: {' '.join(cmd)}\n")
            handle.flush()
            process = subprocess.Popen(
                cmd,
                cwd=PROJECT_ROOT,
                env=env,
                stdout=handle,
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=True,
            )
            _register_pipeline_process(run_id, process)
            _set_job_status(run_id, pid=process.pid)
            persist_pipeline_job_state(run_id)
            returncode = process.wait()
            _register_pipeline_process(run_id, None)
            handle.write(f"[{_now_iso()}] Step {step_name} finished with code {returncode}\n")
            handle.flush()
            if _stop_requested(run_id):
                persist_pipeline_job_state(run_id)
                return
            if str((job_status(run_id) or {}).get("status") or "").strip().lower() == "stopped":
                persist_pipeline_job_state(run_id)
                return
            persist_pipeline_job_state(run_id)
            if returncode != 0:
                _set_job_status(
                    run_id,
                    status="failed",
                    current_stage_key="pipeline",
                    current_stage_label="Solve Pipeline",
                    current_step=step_name,
                    current_step_key=step_name,
                    current_step_label=step_name,
                    finished_at=_now_iso(),
                    returncode=returncode,
                    pid=None,
                    error=f"{step_name} failed with exit code {returncode}",
                    sync_status="finished",
                )
                persist_pipeline_job_state(run_id)
                return
    _set_job_status(
        run_id,
        status="completed",
        current_stage_key=None,
        current_stage_label=None,
        current_step=None,
        current_step_key=None,
        current_step_label=None,
        finished_at=_now_iso(),
        returncode=0,
        pid=None,
        error=None,
        sync_status="finished",
    )
    persist_pipeline_job_state(run_id)


def _monitor_ssh_pipeline_job(
    run_id: str,
    *,
    runtime: dict[str, object],
    run_dir: Path,
    step_name: str,
    remote_pid_path: str,
    remote_exit_path: str,
    remote_run_dir: str,
) -> None:
    poll_interval_sec = max(5, safe_int(runtime.get("poll_interval_sec")) or 60)
    runtime_id = str(runtime.get("id") or "ssh")
    while True:
        if _stop_requested(run_id):
            stop_remote_run(runtime_id, remote_pid_path=remote_pid_path)
            _set_job_status(run_id, status="stopped", finished_at=_now_iso(), error="Pipeline stopped by user.")
            persist_pipeline_job_state(run_id)
            _finalize_runtime_run(run_id)
            return
        if str((job_status(run_id) or {}).get("status") or "").strip().lower() == "stopped":
            stop_remote_run(runtime_id, remote_pid_path=remote_pid_path)
            return
        status = read_remote_run_status(runtime_id, remote_pid_path=remote_pid_path, remote_exit_path=remote_exit_path)
        state = str(status.get("state") or "").strip().lower()
        if state == "error":
            _set_job_status(run_id, status="running", sync_status=_remote_sync_issue_status("status_error"))
            persist_pipeline_job_state(run_id)
            time.sleep(poll_interval_sec)
            continue
        if state == "unknown":
            _set_job_status(run_id, status="running", sync_status=_remote_sync_issue_status("status_unknown"))
            persist_pipeline_job_state(run_id)
            time.sleep(poll_interval_sec)
            continue
        try:
            sync_run_from_runtime(runtime_id, remote_run_dir, run_dir)
        except Exception:
            _set_job_status(run_id, status="running", sync_status=_remote_sync_issue_status("sync_error"))
            persist_pipeline_job_state(run_id)
            time.sleep(poll_interval_sec)
            continue
        _set_job_status(run_id, status="running", last_sync_at=_now_iso(), sync_status=state)
        persist_pipeline_job_state(run_id)
        if state == "finished":
            returncode = safe_int(status.get("returncode")) or 0
            final_status = "completed" if returncode == 0 else "failed"
            _set_job_status(
                run_id,
                status=final_status,
                current_stage_key=None if returncode == 0 else "pipeline",
                current_stage_label=None if returncode == 0 else "Solve Pipeline",
                current_step=None if returncode == 0 else step_name,
                current_step_key=None if returncode == 0 else step_name,
                current_step_label=None if returncode == 0 else step_name,
                finished_at=_now_iso(),
                returncode=returncode,
                error=None if returncode == 0 else f"{step_name} failed with exit code {returncode}",
                sync_status="finished",
            )
            persist_pipeline_job_state(run_id)
            return
        time.sleep(poll_interval_sec)


def resume_ssh_pipeline_job(run_id: str) -> None:
    status = job_status(run_id) or {}
    runtime_id = str(status.get("runtime_id") or "").strip()
    if not runtime_id:
        return
    runtime = get_runtime(runtime_id)
    if str(runtime.get("kind") or "").strip().lower() != "ssh":
        return
    remote_pid_path = _safe_text(status.get("remote_pid_path"))
    remote_exit_path = _safe_text(status.get("remote_exit_path"))
    remote_run_dir = _safe_text(status.get("remote_run_dir"))
    if not remote_pid_path or not remote_exit_path or not remote_run_dir:
        return
    _monitor_ssh_pipeline_job(
        run_id,
        runtime=runtime,
        run_dir=_run_dir(run_id),
        step_name=_safe_text(status.get("current_step_key") or status.get("current_step") or "cspp"),
        remote_pid_path=remote_pid_path,
        remote_exit_path=remote_exit_path,
        remote_run_dir=remote_run_dir,
    )


def _run_ssh_pipeline_job(
    run_id: str,
    *,
    runtime: dict[str, object],
    run_dir: Path,
    commands: list[tuple[str, list[str]]],
    extra_env: dict[str, str],
) -> None:
    if not commands:
        raise RuntimeError("No commands configured for runtime run")
    step_name, cmd = commands[0]
    runtime_id = str(runtime.get("id") or "ssh")
    expected_paths = remote_run_paths(runtime_id, run_id)
    _set_job_status(
        run_id,
        status="syncing_to_runtime",
        current_stage_key="pipeline",
        current_stage_label="Solve Pipeline",
        current_step=step_name,
        current_step_key=step_name,
        current_step_label=step_name,
        pid=None,
        remote_pid_path=expected_paths["remote_pid_path"],
        remote_exit_path=expected_paths["remote_exit_path"],
        remote_run_dir=expected_paths["remote_run_dir"],
        log_path=str(run_dir / "state" / "runtime_job.log"),
        sync_status="syncing_to_runtime",
        last_sync_at=None,
    )
    persist_pipeline_job_state(run_id)
    remote_info = start_remote_run(runtime_id, local_run_dir=run_dir, remote_run_id=run_id, command=cmd, extra_env=extra_env)
    remote_pid_path = _safe_text(remote_info.get("remote_pid_path"))
    remote_exit_path = _safe_text(remote_info.get("remote_exit_path"))
    remote_run_dir = _safe_text(remote_info.get("remote_run_dir"))
    _set_job_status(
        run_id,
        status="running",
        current_stage_key="pipeline",
        current_stage_label="Solve Pipeline",
        current_step=step_name,
        current_step_key=step_name,
        current_step_label=step_name,
        pid=None,
        remote_pid_path=remote_pid_path,
        remote_exit_path=remote_exit_path,
        remote_run_dir=remote_run_dir,
        log_path=str(run_dir / "state" / "runtime_job.log"),
        sync_status="running",
        last_sync_at=None,
    )
    persist_pipeline_job_state(run_id)
    _monitor_ssh_pipeline_job(
        run_id,
        runtime=runtime,
        run_dir=run_dir,
        step_name=step_name,
        remote_pid_path=remote_pid_path,
        remote_exit_path=remote_exit_path,
        remote_run_dir=remote_run_dir,
    )


def _run_pipeline_job(run_id: str) -> None:
    status = job_status(run_id) or {}
    runtime_id = str(status.get("runtime_id") or "local").strip() or "local"
    runtime = get_runtime(runtime_id)
    run_dir = _run_dir(run_id)
    log_path = Path(status.get("log_path") or job_log_path(run_id))
    commands = _pipeline_commands(run_dir)
    run_config = _read_json(run_dir / "run_config.json")
    extra_env = _runtime_extra_env(run_config)
    try:
        _set_job_status(run_id, status="preparing", error=None, finished_at=None, started_at=status.get("started_at") or _now_iso())
        persist_pipeline_job_state(run_id)
        runtime_probe = probe_runtime(runtime_id, repair=True)
        if not runtime_probe.get("ready"):
            raise RuntimeError(str(runtime_probe.get("error") or f"runtime {runtime_id} is not ready"))
        _set_job_status(
            run_id,
            runtime_probe=runtime_probe,
            runtime_system_cores=runtime_probe.get("system_cores"),
            runtime_usable_cores=runtime_probe.get("usable_cores"),
        )
        _set_runtime_defaults(run_dir, runtime_probe)
        run_config = _read_json(run_dir / "run_config.json")
        extra_env = _runtime_extra_env(run_config)
        if str(runtime.get("kind") or "local") == "ssh":
            _set_job_status(run_id, status="syncing_to_runtime", sync_status="syncing_to_runtime")
            persist_pipeline_job_state(run_id)
            _run_ssh_pipeline_job(run_id, runtime=runtime, run_dir=run_dir, commands=commands, extra_env=extra_env)
        else:
            _run_local_pipeline_job(run_id, run_dir=run_dir, log_path=log_path, commands=commands, extra_env=extra_env)
    except Exception as exc:
        _register_pipeline_process(run_id, None)
        if str((job_status(run_id) or {}).get("status") or "").strip().lower() != "stopped":
            _set_job_status(
                run_id,
                status="failed",
                current_stage_key=None,
                current_stage_label=None,
                current_step=None,
                current_step_key=None,
                current_step_label=None,
                finished_at=_now_iso(),
                pid=None,
                error=str(exc),
            )
            persist_pipeline_job_state(run_id)
    finally:
        _finalize_runtime_run(run_id)


def start_next_runtime_run(runtime_id: str) -> None:
    queue_state = load_queue_state(runtime_id)
    active_run_id = str(queue_state.get("active_run_id") or "").strip() or None
    queued_run_ids = [str(item) for item in (queue_state.get("queued_run_ids") or []) if str(item).strip()]
    if active_run_id or not queued_run_ids:
        return
    next_run_id = queued_run_ids.pop(0)
    _persist_runtime_queue(runtime_id, active_run_id=next_run_id, queued_run_ids=queued_run_ids)
    thread = threading.Thread(target=_run_pipeline_job, args=(next_run_id,), daemon=True)
    thread.start()


def terminate_local_processes(*, reason: str) -> None:
    processes = dict(_active_pipeline_processes())
    for run_id, process in processes.items():
        if process.poll() is not None:
            _register_pipeline_process(run_id, None)
            continue
        try:
            pgid = os.getpgid(process.pid)
        except Exception:
            pgid = None
        try:
            if pgid is not None:
                os.killpg(pgid, signal.SIGTERM)
            else:
                process.terminate()
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                if pgid is not None:
                    os.killpg(pgid, signal.SIGKILL)
                else:
                    process.kill()
                process.wait(timeout=5)
            except Exception:
                pass
        except Exception:
            pass
        finally:
            _register_pipeline_process(run_id, None)
            _set_job_status(
                run_id,
                status="stopped",
                current_stage_key=None,
                current_stage_label=None,
                current_step=None,
                current_step_key=None,
                current_step_label=None,
                finished_at=_now_iso(),
                pid=None,
                error=reason,
            )
            persist_pipeline_job_state(run_id)


def startup() -> None:
    ensure_webserver_dirs()
    PIPELINE_STATUS_DIR.mkdir(parents=True, exist_ok=True)
    load_persisted_pipeline_jobs()
    _reconcile_pipeline_jobs_on_startup()
    _reconcile_runtime_queues_on_startup()
    for runtime in list_runtimes():
        runtime_id = str(runtime.get("id") or "").strip()
        if not runtime_id:
            continue
        queue_state = load_queue_state(runtime_id)
        active_run_id = str(queue_state.get("active_run_id") or "").strip() or None
        if active_run_id:
            active_status = job_status(active_run_id) or {}
            if str(active_status.get("runtime_kind") or "").strip().lower() == "ssh":
                thread = threading.Thread(target=resume_ssh_pipeline_job, args=(active_run_id,), daemon=True)
                thread.start()
        start_next_runtime_run(runtime_id)


def _poller_loop() -> None:
    while not _POLLER_STOP.is_set():
        load_persisted_pipeline_jobs()
        for runtime in list_runtimes():
            runtime_id = str(runtime.get("id") or "").strip()
            if not runtime_id:
                continue
            queue_state = load_queue_state(runtime_id)
            active_run_id = str(queue_state.get("active_run_id") or "").strip() or None
            if active_run_id:
                status = job_status(active_run_id) or {}
                runtime_kind = str(status.get("runtime_kind") or runtime.get("kind") or "").strip().lower()
                state = str(status.get("status") or "").strip().lower()
                monitor = _SSH_MONITORS.get(active_run_id)
                if runtime_kind == "ssh" and state in {"preparing", "syncing_to_runtime", "running", "syncing_from_runtime"}:
                    if monitor is None or not monitor.is_alive():
                        thread = threading.Thread(target=resume_ssh_pipeline_job, args=(active_run_id,), daemon=True)
                        _SSH_MONITORS[active_run_id] = thread
                        thread.start()
                elif active_run_id in _SSH_MONITORS and (monitor is None or not monitor.is_alive()):
                    _SSH_MONITORS.pop(active_run_id, None)
            start_next_runtime_run(runtime_id)
        if _POLLER_STOP.wait(_POLLER_INTERVAL_SECONDS):
            break


def start_poller() -> None:
    global _POLLER_THREAD
    if _POLLER_THREAD is not None and _POLLER_THREAD.is_alive():
        return
    _POLLER_STOP.clear()
    _POLLER_THREAD = threading.Thread(target=_poller_loop, name="pipeline-poller", daemon=True)
    _POLLER_THREAD.start()


def stop_poller() -> None:
    global _POLLER_THREAD
    _POLLER_STOP.set()
    thread = _POLLER_THREAD
    if thread is not None and thread.is_alive():
        thread.join(timeout=5.0)
    _POLLER_THREAD = None
    _POLLER_STOP.clear()


def shutdown() -> None:
    stop_poller()
    terminate_local_processes(reason="Pipeline stopped because the worker shut down.")
