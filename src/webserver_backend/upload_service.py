from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import HTTPException

from webserver_runtime import WEBSERVER_ROOT
from webserver_runtime import WEBSERVER_UPLOAD_ROOT
from webserver_runtime import resolve_server_relative_path

from . import storage


def upload_instance_json(*, run_id: str, kind: str, filename: str, data: bytes) -> dict[str, Any]:
    allowed = {"stores", "customers", "demand_long", "cluster_assignments", "frontend_manifest"}
    if kind not in allowed:
        raise HTTPException(status_code=400, detail=f"kind must be one of: {', '.join(sorted(allowed))}")
    suffix = Path(filename or "").suffix or ".json"
    if suffix.lower() != ".json":
        raise HTTPException(status_code=400, detail="only .json uploads are supported")
    target = WEBSERVER_UPLOAD_ROOT / "instance_json" / run_id / f"{kind}{suffix}"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    manifest_path = target.parent / "manifest.json"
    existing = storage.read_json(manifest_path, {}) or {}
    existing.setdefault("run_id", run_id)
    existing.setdefault("files", {})
    existing["files"][kind] = str(target.relative_to(WEBSERVER_ROOT))
    existing["updated_at"] = storage.now_iso()
    storage.write_json(manifest_path, existing)
    return {
        "stored": True,
        "run_id": run_id,
        "kind": kind,
        "path": str(target.relative_to(WEBSERVER_ROOT)),
        "size": len(data),
        "updated_at": storage.now_iso(),
    }


def upload_artifact(*, relative_path: str, data: bytes) -> dict[str, Any]:
    try:
        target = resolve_server_relative_path(relative_path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    return {"stored": True, "path": relative_path, "size": len(data), "updated_at": storage.now_iso()}


def delete_artifact(*, relative_path: str) -> dict[str, Any]:
    try:
        target = resolve_server_relative_path(relative_path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if target.exists() and not target.is_file():
        raise HTTPException(status_code=400, detail="relative_path must point to a file")
    deleted = False
    if target.exists():
        target.unlink()
        deleted = True
        storage.cleanup_empty_parents(target, stop_at=WEBSERVER_ROOT)
    return {"deleted": deleted, "path": relative_path, "updated_at": storage.now_iso()}
