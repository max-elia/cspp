from __future__ import annotations

import json
import mimetypes
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from webserver_runtime import WEBSERVER_EXPORT_ROOT
from webserver_runtime import WEBSERVER_INSTANCE_ROOT
from webserver_runtime import WEBSERVER_ROOT
from webserver_runtime import WEBSERVER_RUN_ROOT
from webserver_runtime import WEBSERVER_STATE_ROOT
from webserver_runtime import WEBSERVER_UPLOAD_ROOT
from webserver_runtime import ensure_webserver_dirs
from webserver_runtime import file_metadata
from webserver_runtime import resolve_server_relative_path


CATALOG_CACHE_PATH = WEBSERVER_EXPORT_ROOT / "state" / "web_app_catalog.json"
RUNS_CACHE_PATH = WEBSERVER_EXPORT_ROOT / "state" / "web_app_runs_index.json"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    return path


def remove_file(path: Path) -> None:
    if path.exists():
        path.unlink()


def slugify_identifier(value: object, *, fallback: str) -> str:
    text = "".join(ch if str(ch).isalnum() or ch in {"-", "_", "."} else "_" for ch in str(value or "").strip())
    text = text.strip("._-")
    return text or fallback


def compact_identifier(value: object, *, fallback: str, max_parts: int = 4, max_part_len: int = 12) -> str:
    slug = slugify_identifier(value, fallback=fallback).replace(".", "_")
    parts = [part for part in slug.replace("-", "_").split("_") if part]
    compact_parts: list[str] = []
    for part in parts:
        lowered = part.lower()
        if lowered in {"instance", "instances", "integration", "refactor"}:
            continue
        compact_parts.append(part[:max_part_len].lower())
        if len(compact_parts) >= max_parts:
            break
    if not compact_parts:
        compact_parts = [fallback.lower()[:max_part_len]]
    return "-".join(compact_parts)


def timestamp_id(*, with_seconds: bool = True) -> str:
    fmt = "%y%m%d-%H%M%S" if with_seconds else "%y%m%d-%H%M"
    return datetime.now().strftime(fmt)


def instance_dir(instance_id: str) -> Path:
    return WEBSERVER_INSTANCE_ROOT / instance_id


def run_dir(run_id: str) -> Path:
    return WEBSERVER_RUN_ROOT / run_id


def instance_manifest_path(instance_id: str) -> Path:
    return instance_dir(instance_id) / "manifest.json"


def run_manifest_path(run_id: str) -> Path:
    return run_dir(run_id) / "manifest.json"


def instance_artifact_manifest_path(instance_id: str) -> Path:
    return instance_dir(instance_id) / "artifacts" / "manifest.json"


def run_artifact_manifest_path(run_id: str) -> Path:
    return run_dir(run_id) / "artifacts" / "manifest.json"


def sync_manifest_path(run_id: str) -> Path:
    return run_dir(run_id) / "state" / "sync_manifest.json"


def pipeline_job_path(run_id: str) -> Path:
    return run_dir(run_id) / "state" / "pipeline_job.json"


def artifact_content_type(path: Path) -> str:
    mime_type, _ = mimetypes.guess_type(path.name)
    return mime_type or "application/octet-stream"


def relative_to_webserver_root(path: Path) -> str:
    return str(path.resolve().relative_to(WEBSERVER_ROOT.resolve()))


def build_artifact_entry(path: Path, *, key: str, category: str, derived: bool = False) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    stat = path.stat()
    return {
        "key": key,
        "category": category,
        "path": relative_to_webserver_root(path),
        "size": stat.st_size,
        "mtime_ms": int(stat.st_mtime * 1000),
        "content_type": artifact_content_type(path),
        "derived": derived,
    }


def _build_run_artifact_entries(run_root: Path) -> list[dict[str, Any]]:
    known_files: list[tuple[str, str, Path, bool]] = [
        ("run_manifest", "metadata", run_root / "manifest.json", False),
        ("run_config", "metadata", run_root / "run_config.json", False),
        ("pipeline_job", "state", run_root / "state" / "pipeline_job.json", False),
        ("sync_manifest", "state", run_root / "state" / "sync_manifest.json", False),
        ("summary", "result", run_root / "summary.json", False),
        ("frontend_manifest", "frontend", run_root / "frontend" / "manifest.json", True),
        ("frontend_overview", "frontend", run_root / "frontend" / "overview.json", True),
        ("pipeline_progress", "frontend", run_root / "frontend" / "pipeline_progress.json", True),
        ("map_geojson", "frontend", run_root / "frontend" / "map" / "customers.json", True),
        ("map_summary", "frontend", run_root / "frontend" / "map" / "customers_summary.json", True),
        ("activity_recent_events", "frontend", run_root / "frontend" / "activity" / "recent_events.json", True),
        ("activity_alerts", "frontend", run_root / "frontend" / "activity" / "alerts.json", True),
        ("runtime_log", "log", run_root / "state" / "runtime_job.log", False),
    ]
    entries: list[dict[str, Any]] = []
    for key, category, path, derived in known_files:
        entry = build_artifact_entry(path, key=key, category=category, derived=derived)
        if entry is not None:
            entries.append(entry)
    return entries


def _build_instance_artifact_entries(instance_root: Path) -> list[dict[str, Any]]:
    known_files: list[tuple[str, str, Path, bool]] = [
        ("instance_manifest", "metadata", instance_root / "manifest.json", False),
        ("instance_payload", "metadata", instance_root / "prep" / "instance" / "payload.json", False),
        ("instance_customers", "metadata", instance_root / "prep" / "instance" / "customers.json", False),
        ("cluster_assignments", "metadata", instance_root / "prep" / "clustering" / "assignments.json", False),
        ("cluster_assignments_csv", "metadata", instance_root / "prep" / "clustering" / "cluster_assignments.json", False),
        ("frontend_manifest", "frontend", instance_root / "frontend" / "manifest.json", True),
        ("frontend_overview", "frontend", instance_root / "frontend" / "overview.json", True),
        ("map_geojson", "frontend", instance_root / "frontend" / "map" / "customers.json", True),
        ("map_summary", "frontend", instance_root / "frontend" / "map" / "customers_summary.json", True),
    ]
    entries: list[dict[str, Any]] = []
    for key, category, path, derived in known_files:
        entry = build_artifact_entry(path, key=key, category=category, derived=derived)
        if entry is not None:
            entries.append(entry)
    return entries


def rebuild_run_artifact_manifest(run_id: str) -> dict[str, Any]:
    ensure_webserver_dirs()
    run_root = run_dir(run_id)
    payload = {
        "run_id": run_id,
        "updated_at": now_iso(),
        "artifacts": _build_run_artifact_entries(run_root),
    }
    write_json(run_artifact_manifest_path(run_id), payload)
    return payload


def rebuild_instance_artifact_manifest(instance_id: str) -> dict[str, Any]:
    ensure_webserver_dirs()
    instance_root = instance_dir(instance_id)
    payload = {
        "instance_id": instance_id,
        "updated_at": now_iso(),
        "artifacts": _build_instance_artifact_entries(instance_root),
    }
    write_json(instance_artifact_manifest_path(instance_id), payload)
    return payload


def write_sync_manifest(run_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    document = {
        "run_id": run_id,
        "updated_at": now_iso(),
        **payload,
    }
    write_json(sync_manifest_path(run_id), document)
    rebuild_run_artifact_manifest(run_id)
    return document


def read_run_artifact_manifest(run_id: str) -> dict[str, Any]:
    path = run_artifact_manifest_path(run_id)
    payload = read_json(path, {}) or {}
    if not payload and run_dir(run_id).exists():
        payload = rebuild_run_artifact_manifest(run_id)
    return payload


def read_instance_artifact_manifest(instance_id: str) -> dict[str, Any]:
    path = instance_artifact_manifest_path(instance_id)
    payload = read_json(path, {}) or {}
    if not payload and instance_dir(instance_id).exists():
        payload = rebuild_instance_artifact_manifest(instance_id)
    return payload


def artifact_by_key(manifest: dict[str, Any], artifact_key: str) -> dict[str, Any] | None:
    for row in manifest.get("artifacts") or []:
        if isinstance(row, dict) and str(row.get("key") or "") == artifact_key:
            return row
    return None


def resolve_artifact_path(artifact_row: dict[str, Any]) -> Path:
    path = str(artifact_row.get("path") or "").strip()
    if not path:
        raise FileNotFoundError("artifact path missing")
    return resolve_server_relative_path(path)


def read_instance_manifest(instance_id: str) -> dict[str, Any]:
    return read_json(instance_manifest_path(instance_id), {}) or {}


def read_run_manifest(run_id: str) -> dict[str, Any]:
    return read_json(run_manifest_path(run_id), {}) or {}


def read_instance_payload(instance_id: str) -> dict[str, Any]:
    return read_json(instance_dir(instance_id) / "prep" / "instance" / "payload.json", {}) or {}


def read_pipeline_job(run_id: str) -> dict[str, Any]:
    return read_json(pipeline_job_path(run_id), {}) or {}


def list_instances_from_manifests() -> dict[str, Any]:
    ensure_webserver_dirs()
    instance_rows: list[dict[str, Any]] = []
    run_rows_by_instance: dict[str, list[dict[str, Any]]] = {}
    runs_payload = list_runs_from_manifests()
    for row in runs_payload["runs"]:
        instance_id = str(row.get("instance_id") or "").strip()
        if not instance_id:
            continue
        run_rows_by_instance.setdefault(instance_id, []).append(row)
    for instance_root in sorted((path for path in WEBSERVER_INSTANCE_ROOT.iterdir() if path.is_dir()), key=lambda path: path.stat().st_mtime, reverse=True):
        manifest = read_json(instance_root / "manifest.json", {}) or {}
        payload = read_json(instance_root / "prep" / "instance" / "payload.json", {}) or {}
        instance_id = str(manifest.get("instance_id") or instance_root.name).strip() or instance_root.name
        runs = sorted(run_rows_by_instance.get(instance_id, []), key=lambda item: str(item.get("updated_at") or ""), reverse=True)
        instance_rows.append(
            {
                "instance_id": instance_id,
                "label": manifest.get("label") or instance_id,
                "created_at": manifest.get("created_at"),
                "updated_at": manifest.get("updated_at") or datetime.fromtimestamp(instance_root.stat().st_mtime, tz=timezone.utc).isoformat(),
                "source_instance_id": manifest.get("source_instance_id"),
                "clustering_method": manifest.get("clustering_method") or payload.get("clustering_method"),
                "max_distance_from_warehouse_km": manifest.get("max_distance_from_warehouse_km") or payload.get("max_distance_from_warehouse_km"),
                "customer_count": manifest.get("customer_count") or len(payload.get("customers") or []),
                "demand_row_count": manifest.get("demand_row_count") or len(payload.get("demand_rows") or []),
                "latest_run_id": runs[0]["run_id"] if runs else None,
                "run_count": len(runs),
                "runs": runs,
            }
        )
    latest_instance_id = instance_rows[0]["instance_id"] if instance_rows else None
    payload = {
        "updated_at": now_iso(),
        "latest_instance_id": latest_instance_id,
        "latest_run_id": runs_payload.get("latest_run_id"),
        "instances": instance_rows,
    }
    write_json(CATALOG_CACHE_PATH, payload)
    return payload


def list_runs_from_manifests() -> dict[str, Any]:
    ensure_webserver_dirs()
    rows: list[dict[str, Any]] = []
    for run_root in sorted((path for path in WEBSERVER_RUN_ROOT.iterdir() if path.is_dir()), key=lambda path: path.stat().st_mtime, reverse=True):
        manifest = read_json(run_root / "manifest.json", {}) or {}
        job = read_json(run_root / "state" / "pipeline_job.json", {}) or {}
        rows.append(
            {
                "run_id": run_root.name,
                "instance_id": manifest.get("instance_id"),
                "created_at": manifest.get("created_at"),
                "updated_at": job.get("updated_at") or manifest.get("updated_at") or datetime.fromtimestamp(run_root.stat().st_mtime, tz=timezone.utc).isoformat(),
                "status": job.get("status") or "idle",
                "runtime_id": job.get("runtime_id") or manifest.get("runtime_id"),
                "runtime_label": job.get("runtime_label") or manifest.get("runtime_label"),
                "runtime_kind": job.get("runtime_kind") or manifest.get("runtime_kind"),
                "queue_position": job.get("queue_position"),
                "started_at": job.get("started_at"),
                "finished_at": job.get("finished_at"),
            }
        )
    payload = {
        "updated_at": now_iso(),
        "latest_run_id": rows[0]["run_id"] if rows else None,
        "runs": rows,
    }
    write_json(RUNS_CACHE_PATH, payload)
    return payload


def cleanup_empty_parents(path: Path, *, stop_at: Path) -> None:
    parent = path.parent
    while parent != stop_at and parent.exists() and not any(parent.iterdir()):
        parent.rmdir()
        parent = parent.parent


def _frontend_manifest_paths(owner_root: Path) -> set[Path]:
    frontend_root = owner_root / "frontend"
    manifest_path = frontend_root / "manifest.json"
    manifest = read_json(manifest_path, {}) or {}
    paths: set[Path] = {manifest_path}

    for relative in (manifest.get("files") or {}).values():
        text = str(relative or "").strip().lstrip("/")
        if not text:
            continue
        paths.add(owner_root / text)

    standard_frontend_paths = (
        frontend_root / "overview.json",
        frontend_root / "pipeline" / "progress.json",
        frontend_root / "map" / "customers.json",
        frontend_root / "map" / "customers_summary.json",
        frontend_root / "stage_1" / "clusters.json",
        frontend_root / "stage_2" / "scenarios.json",
        frontend_root / "stage_3" / "scenarios.json",
        frontend_root / "stage_3" / "overview.json",
        frontend_root / "activity" / "recent_events.json",
        frontend_root / "activity" / "alerts.json",
    )
    paths.update(standard_frontend_paths)

    available_routes = manifest.get("available_routes") or {}
    route_specs = (
        ("stage_1_clusters", frontend_root / "stage_1" / "clusters"),
        ("stage_2_scenarios", frontend_root / "stage_2" / "scenarios"),
        ("stage_3_scenarios", frontend_root / "stage_3" / "scenarios"),
        ("stage_3_clusters", frontend_root / "stage_3" / "clusters"),
        ("stage_3_scopes", frontend_root / "stage_3" / "scopes"),
    )
    for key, base in route_specs:
        for item in available_routes.get(key) or []:
            name = slugify_identifier(item, fallback="item")
            paths.add(base / f"{name}.json")

    for cluster_id in available_routes.get("stage_1_clusters") or []:
        cluster_name = slugify_identifier(cluster_id, fallback="cluster")
        paths.add(owner_root / "05_solve_clusters_first_stage" / "logs" / f"cluster_{cluster_name}.log")
    return paths


def dashboard_file_index() -> list[dict[str, Any]]:
    ensure_webserver_dirs()
    list_runs_from_manifests()
    list_instances_from_manifests()

    candidate_paths: set[Path] = {
        CATALOG_CACHE_PATH,
        RUNS_CACHE_PATH,
    }

    for instance_root in (path for path in WEBSERVER_INSTANCE_ROOT.iterdir() if path.is_dir()):
        candidate_paths.add(instance_root / "manifest.json")
        candidate_paths.add(instance_root / "prep" / "instance" / "payload.json")
        candidate_paths.update(_frontend_manifest_paths(instance_root))

    for run_root in (path for path in WEBSERVER_RUN_ROOT.iterdir() if path.is_dir()):
        candidate_paths.add(run_root / "manifest.json")
        candidate_paths.add(run_root / "run_config.json")
        candidate_paths.add(run_root / "state" / "pipeline_job.json")
        candidate_paths.add(run_root / "state" / "runtime_job.log")
        candidate_paths.update(_frontend_manifest_paths(run_root))

    rows: list[dict[str, Any]] = []
    for path in sorted(candidate_paths):
        metadata = file_metadata(path)
        if metadata is not None:
            rows.append(metadata)
    return rows
