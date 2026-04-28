from __future__ import annotations

import os
import signal
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from runtime_manager import get_runtime
from runtime_manager import list_runtimes
from runtime_manager import load_queue_state
from runtime_manager import probe_runtime
from runtime_manager import save_queue_state
from runtime_manager import stop_remote_run
from webserver_runtime import WEBSERVER_ROOT

from . import storage


ACTIVE_STATES = {"queued", "preparing", "syncing_to_runtime", "running", "syncing_from_runtime"}
TERMINAL_STATES = {"idle", "completed", "failed", "stopped"}


def queue_info(runtime_id: str) -> dict[str, Any]:
    queue_state = load_queue_state(runtime_id)
    queued_run_ids = [str(item) for item in (queue_state.get("queued_run_ids") or []) if str(item).strip()]
    return {
        "runtime_id": runtime_id,
        "active_run_id": queue_state.get("active_run_id"),
        "queue_depth": len(queued_run_ids),
        "queued_run_ids": queued_run_ids,
        "updated_at": queue_state.get("updated_at"),
    }


def list_runtime_rows() -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for runtime in list_runtimes():
        runtime_id = str(runtime.get("id") or "").strip()
        if not runtime_id:
            continue
        probe = storage.read_json(WEBSERVER_ROOT / "state" / "runtime_queues" / f"{runtime_id}.probe.json", {}) or {}
        rows.append(
            {
                "id": runtime_id,
                "label": runtime.get("label"),
                "kind": runtime.get("kind"),
                "poll_interval_sec": runtime.get("poll_interval_sec"),
                "tags": runtime.get("tags") or [],
                "queue": queue_info(runtime_id),
                "probe": probe,
            }
        )
    return {"updated_at": storage.now_iso(), "runtimes": rows}


def probe_runtime_row(runtime_id: str, *, repair: bool = False) -> dict[str, Any]:
    try:
        probe = probe_runtime(runtime_id, repair=repair)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    storage.write_json(WEBSERVER_ROOT / "state" / "runtime_queues" / f"{runtime_id}.probe.json", probe)
    return {"updated_at": storage.now_iso(), "runtime_id": runtime_id, "probe": probe, "queue": queue_info(runtime_id)}


def enqueue_run(run_id: str, runtime_id: str) -> int:
    queue_state = load_queue_state(runtime_id)
    active_run_id = str(queue_state.get("active_run_id") or "").strip() or None
    queued_run_ids = [str(item) for item in (queue_state.get("queued_run_ids") or []) if str(item).strip()]
    if run_id not in queued_run_ids:
        queued_run_ids.append(run_id)
    save_queue_state(runtime_id, active_run_id=active_run_id, queued_run_ids=queued_run_ids)
    return queued_run_ids.index(run_id) + 1


def dequeue_run(run_id: str, runtime_id: str) -> None:
    queue_state = load_queue_state(runtime_id)
    active_run_id = str(queue_state.get("active_run_id") or "").strip() or None
    queued_run_ids = [str(item) for item in (queue_state.get("queued_run_ids") or []) if str(item).strip()]
    queued_run_ids = [item for item in queued_run_ids if item != run_id]
    if active_run_id == run_id:
        active_run_id = None
    save_queue_state(runtime_id, active_run_id=active_run_id, queued_run_ids=queued_run_ids)


def queue_position(runtime_id: str, run_id: str) -> int | None:
    queue_state = load_queue_state(runtime_id)
    queued_run_ids = [str(item) for item in (queue_state.get("queued_run_ids") or []) if str(item).strip()]
    if run_id not in queued_run_ids:
        return None
    return queued_run_ids.index(run_id) + 1


def stop_run(run_id: str) -> dict[str, Any]:
    job = storage.read_pipeline_job(run_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"run not found: {run_id}")
    state = str(job.get("status") or "").strip().lower()
    if state not in ACTIVE_STATES:
        raise HTTPException(status_code=400, detail=f"run is not active: {state or 'unknown'}")
    runtime_id = str(job.get("runtime_id") or "local").strip() or "local"
    runtime_kind = str(job.get("runtime_kind") or "local").strip().lower()
    reason = "Pipeline stopped by user."
    if runtime_kind == "ssh":
        remote_pid_path = str(job.get("remote_pid_path") or "").strip()
        if remote_pid_path:
            stop_remote_run(runtime_id, remote_pid_path=remote_pid_path)
    else:
        pid = job.get("pid")
        if isinstance(pid, int) and pid > 0:
            try:
                os.killpg(pid, signal.SIGTERM)
            except Exception:
                try:
                    os.kill(pid, signal.SIGTERM)
                except Exception:
                    pass
    job.update(
        {
            "status": "stopped",
            "current_stage_key": None,
            "current_stage_label": None,
            "current_step_key": None,
            "current_step_label": None,
            "finished_at": storage.now_iso(),
            "error": reason,
            "updated_at": storage.now_iso(),
        }
    )
    storage.write_json(storage.pipeline_job_path(run_id), job)
    storage.write_sync_manifest(
        run_id,
        {
            "runtime_id": runtime_id,
            "runtime_kind": runtime_kind,
            "sync_status": job.get("sync_status") or "stopped",
            "last_sync_at": job.get("last_sync_at"),
            "stop_requested": True,
        },
    )
    dequeue_run(run_id, runtime_id)
    storage.rebuild_run_artifact_manifest(run_id)
    return job


def ensure_runtime_ready(runtime_id: str, *, repair: bool = True) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        runtime = get_runtime(runtime_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    probe_payload = probe_runtime_row(runtime_id, repair=repair)
    probe = probe_payload["probe"]
    if not probe.get("ready"):
        raise HTTPException(status_code=400, detail=str(probe.get("error") or f"runtime {runtime_id} is not ready"))
    return runtime, probe
