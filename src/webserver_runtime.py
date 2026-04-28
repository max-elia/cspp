from __future__ import annotations

import hashlib
import mimetypes
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WEBSERVER_ROOT = (PROJECT_ROOT / "var" / "webserver").resolve()
WEBSERVER_EXPORT_ROOT = WEBSERVER_ROOT / "exports"
WEBSERVER_INSTANCE_ROOT = WEBSERVER_EXPORT_ROOT / "instances"
WEBSERVER_RUN_ROOT = WEBSERVER_EXPORT_ROOT / "runs"
WEBSERVER_UPLOAD_ROOT = WEBSERVER_ROOT / "uploads"
WEBSERVER_STATE_ROOT = WEBSERVER_ROOT / "state"


def ensure_webserver_dirs() -> dict[str, Path]:
    for path in (
        WEBSERVER_ROOT,
        WEBSERVER_EXPORT_ROOT,
        WEBSERVER_INSTANCE_ROOT,
        WEBSERVER_RUN_ROOT,
        WEBSERVER_UPLOAD_ROOT,
        WEBSERVER_STATE_ROOT,
    ):
        path.mkdir(parents=True, exist_ok=True)
    return {
        "root": WEBSERVER_ROOT,
        "exports": WEBSERVER_EXPORT_ROOT,
        "instances": WEBSERVER_INSTANCE_ROOT,
        "runs": WEBSERVER_RUN_ROOT,
        "uploads": WEBSERVER_UPLOAD_ROOT,
        "state": WEBSERVER_STATE_ROOT,
    }


def webserver_env() -> dict[str, str]:
    ensure_webserver_dirs()
    return {
        "THESIS_EXPORT_ROOT": str(WEBSERVER_EXPORT_ROOT),
    }


def resolve_server_relative_path(relative_path: str) -> Path:
    normalized = relative_path.strip().lstrip("/")
    if not normalized:
        raise ValueError("relative_path must not be empty")
    candidate = (WEBSERVER_ROOT / normalized).resolve()
    if not str(candidate).startswith(str(WEBSERVER_ROOT)):
        raise ValueError("relative_path escapes webserver root")
    return candidate


def file_metadata(path: Path, *, root: Path | None = None) -> dict[str, Any] | None:
    base = root or WEBSERVER_ROOT
    try:
        stat = path.stat()
        rel = str(path.resolve().relative_to(base.resolve()))
        sha = hashlib.sha256(path.read_bytes()).hexdigest()
    except FileNotFoundError:
        return None
    mime_type, _ = mimetypes.guess_type(path.name)
    return {
        "path": rel,
        "size": stat.st_size,
        "mtime_ms": int(stat.st_mtime * 1000),
        "sha256": sha,
        "content_type": mime_type or "application/octet-stream",
    }


def collect_file_index() -> list[dict[str, Any]]:
    ensure_webserver_dirs()
    rows: list[dict[str, Any]] = []
    for root in (WEBSERVER_EXPORT_ROOT, WEBSERVER_UPLOAD_ROOT):
        if not root.exists():
            continue
        for path in sorted((item for item in root.rglob("*") if item.is_file())):
            metadata = file_metadata(path)
            if metadata is not None:
                rows.append(metadata)
    return rows
