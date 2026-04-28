from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = 1


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _jsonify(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonify(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonify(item) for item in value]
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return _jsonify(item())
        except Exception:
            pass
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        try:
            return isoformat()
        except Exception:
            pass
    return str(value)


def read_json(path: str | Path, default: Any = None) -> Any:
    target = Path(path)
    if not target.exists():
        return default
    try:
        return json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return default


def write_json(path: str | Path, payload: Any) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(_jsonify(payload), indent=2, ensure_ascii=True), encoding="utf-8")
    return target


def table_document(
    columns: Iterable[str],
    rows: Iterable[dict[str, Any]],
    *,
    kind: str = "table",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "kind": kind,
        "updated_at": now_iso(),
        "columns": [str(column) for column in columns],
        "rows": list(rows),
    }
    if extra:
        payload.update(extra)
    return payload


def write_table(
    path: str | Path,
    columns: Iterable[str],
    rows: Iterable[dict[str, Any]],
    *,
    kind: str = "table",
    extra: dict[str, Any] | None = None,
) -> Path:
    return write_json(path, table_document(columns, rows, kind=kind, extra=extra))


def read_table_rows(path: str | Path) -> list[dict[str, Any]]:
    payload = read_json(path, {}) or {}
    rows = payload.get("rows")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    return []


def event_log_document(events: Iterable[dict[str, Any]], *, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "kind": "event_log",
        "updated_at": now_iso(),
        "events": list(events),
    }
    if extra:
        payload.update(extra)
    return payload


def write_event_log(path: str | Path, events: Iterable[dict[str, Any]], *, extra: dict[str, Any] | None = None) -> Path:
    return write_json(path, event_log_document(events, extra=extra))


def read_event_log(path: str | Path) -> list[dict[str, Any]]:
    payload = read_json(path, {}) or {}
    events = payload.get("events")
    if isinstance(events, list):
        return [event for event in events if isinstance(event, dict)]
    return []
