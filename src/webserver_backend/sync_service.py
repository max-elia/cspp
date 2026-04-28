from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from webserver_runtime import WEBSERVER_ROOT
from webserver_runtime import resolve_server_relative_path

from . import storage


def health() -> dict[str, str]:
    return {"status": "ok", "updated_at": storage.now_iso()}


def file_index() -> dict[str, Any]:
    return {
        "updated_at": storage.now_iso(),
        "root": str(WEBSERVER_ROOT),
        "files": storage.dashboard_file_index(),
    }


def file_content(relative_path: str) -> tuple[bytes, str]:
    try:
        target = resolve_server_relative_path(relative_path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="file not found")
    return target.read_bytes(), storage.artifact_content_type(target)
