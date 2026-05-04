from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from frontend_exports import export_instance_frontend_contract
from webserver_runtime import WEBSERVER_ROOT

from . import pipeline_jobs
from . import runtime
from . import storage
from .run_service import delete_run


def _payload_stores(payload: dict[str, object]) -> list[object]:
    stores = payload.get("stores")
    if isinstance(stores, list):
        return stores
    customers = payload.get("customers")
    return customers if isinstance(customers, list) else []


def _internal_customer_rows(stores: list[object]) -> list[object]:
    rows: list[object] = []
    for row in stores:
        if not isinstance(row, dict):
            rows.append(row)
            continue
        copy = dict(row)
        if copy.get("customer_id") is None and copy.get("store_id") is not None:
            copy["customer_id"] = copy.get("store_id")
        if copy.get("customer_name") is None and copy.get("store_name") is not None:
            copy["customer_name"] = copy.get("store_name")
        rows.append(copy)
    return rows


def _cluster_assignments_from_stores(stores: list[object]) -> list[dict[str, object]]:
    clustered = [row for row in stores if isinstance(row, dict) and row.get("cluster_id") is not None]
    if not clustered:
        return []
    if len(clustered) != len(stores):
        raise HTTPException(status_code=400, detail="cluster_id must be provided for every store or for none")
    assignments: list[dict[str, object]] = []
    for row in clustered:
        client_num = pipeline_jobs.safe_int(row.get("client_num"))
        cluster_id = pipeline_jobs.safe_int(row.get("cluster_id"))
        if client_num is None or cluster_id is None:
            raise HTTPException(status_code=400, detail="cluster_id assignments need valid client_num and cluster_id")
        assignments.append(
            {
                "client_num": client_num,
                "store_id": row.get("store_id") or row.get("customer_id"),
                "cluster_id": cluster_id,
            }
        )
    return assignments


def _validated_assignments(stores: list[object], raw_assignments: object) -> list[dict[str, object]]:
    if isinstance(raw_assignments, list) and raw_assignments:
        assignments = [row for row in raw_assignments if isinstance(row, dict)]
        for row in assignments:
            if pipeline_jobs.safe_int(row.get("client_num")) is None or pipeline_jobs.safe_int(row.get("cluster_id")) is None:
                raise HTTPException(status_code=400, detail="assignments need valid client_num and cluster_id")
        store_client_nums = {
            client_num
            for row in stores
            if isinstance(row, dict)
            for client_num in [pipeline_jobs.safe_int(row.get("client_num"))]
            if client_num is not None
        }
        assignment_client_nums = {
            client_num
            for row in assignments
            for client_num in [pipeline_jobs.safe_int(row.get("client_num"))]
            if client_num is not None
        }
        if assignment_client_nums != store_client_nums:
            raise HTTPException(status_code=400, detail="assignments must cover every store or be omitted")
        return assignments
    return _cluster_assignments_from_stores(stores)


def _instance_runs(instance_id: str) -> list[dict[str, Any]]:
    for row in storage.list_instances_from_manifests()["instances"]:
        if str(row.get("instance_id") or "") == instance_id:
            return [item for item in (row.get("runs") or []) if isinstance(item, dict)]
    return []


def _persist_instance_payload(
    *,
    instance_id: str,
    source_instance_id: str | None,
    payload: dict[str, object],
    assignments: list[dict[str, object]],
) -> Path:
    instance_root = storage.instance_dir(instance_id)
    instance_root.mkdir(parents=True, exist_ok=True)
    customers = _payload_stores(payload)
    internal_customers = _internal_customer_rows(customers)
    demand_rows = payload.get("demand_rows") if isinstance(payload.get("demand_rows"), list) else []
    manifest = {
        "instance_id": instance_id,
        "label": instance_id,
        "created_at": storage.now_iso(),
        "updated_at": storage.now_iso(),
        "source_instance_id": source_instance_id,
        "clustering_method": payload.get("clustering_method"),
        "max_distance_from_warehouse_km": payload.get("max_distance_from_warehouse_km"),
        "customer_count": len(customers),
        "demand_row_count": len(demand_rows),
    }
    storage.write_json(instance_root / "manifest.json", manifest)
    storage.write_json(instance_root / "prep" / "instance" / "manifest.json", manifest)
    storage.write_json(instance_root / "prep" / "instance" / "payload.json", payload)
    storage.write_json(instance_root / "prep" / "instance" / "customers.json", {"updated_at": storage.now_iso(), "customers": internal_customers})
    if assignments:
        storage.write_json(
            instance_root / "prep" / "clustering" / "assignments.json",
            {"updated_at": storage.now_iso(), "instance_id": instance_id, "assignments": assignments},
        )
        normalized = []
        for row in assignments:
            client_num = pipeline_jobs.safe_int(row.get("client_num"))
            cluster_id = pipeline_jobs.safe_int(row.get("cluster_id"))
            if client_num is None or cluster_id is None:
                continue
            normalized.append({"client_num": client_num, "cluster_id": cluster_id})
        storage.write_json(
            instance_root / "prep" / "clustering" / "cluster_assignments.json",
            {"updated_at": storage.now_iso(), "instance_id": instance_id, "rows": normalized},
        )
    export_instance_frontend_contract(instance_root)
    storage.rebuild_instance_artifact_manifest(instance_id)
    storage.list_instances_from_manifests()
    return instance_root


def create_instance(payload: dict[str, object]) -> dict[str, Any]:
    requested_value = payload.get("instance_id")
    if requested_value:
        requested_id = storage.slugify_identifier(requested_value, fallback="instance")
    else:
        requested_id = storage.compact_identifier(
            payload.get("clustering_method") or payload.get("source_instance_id") or "instance",
            fallback="instance",
        )
        requested_id = f"{requested_id}-{storage.timestamp_id(with_seconds=False)}"
    instance_id = requested_id
    counter = 2
    while storage.instance_dir(instance_id).exists():
        instance_id = f"{requested_id}_{counter}"
        counter += 1
    customers = _payload_stores(payload)
    demand_rows = payload.get("demand_rows")
    if not isinstance(customers, list) or not customers:
        raise HTTPException(status_code=400, detail="stores must be a non-empty list")
    if not isinstance(demand_rows, list):
        raise HTTPException(status_code=400, detail="demand_rows must be a list")
    instance_payload = {
        "schema_version": 1,
        "instance_id": instance_id,
        "source_instance_id": payload.get("source_instance_id"),
        "generated_at": storage.now_iso(),
        "max_distance_from_warehouse_km": payload.get("max_distance_from_warehouse_km"),
        "clustering_method": payload.get("clustering_method"),
        "warehouse": payload.get("warehouse"),
        "stores": customers,
        "demand_rows": demand_rows,
    }
    assignments = _validated_assignments(customers, payload.get("assignments"))
    instance_root = _persist_instance_payload(
        instance_id=instance_id,
        source_instance_id=str(payload.get("source_instance_id") or "").strip() or None,
        payload=instance_payload,
        assignments=[row for row in assignments if isinstance(row, dict)],
    )
    return {
        "updated_at": storage.now_iso(),
        "instance_id": instance_id,
        "path": str(instance_root.relative_to(WEBSERVER_ROOT)),
        "manifest": storage.read_instance_manifest(instance_id),
    }


def list_instances() -> dict[str, Any]:
    return storage.list_instances_from_manifests()


def delete_instance(instance_id: str) -> dict[str, Any]:
    instance_root = storage.instance_dir(instance_id)
    if not instance_root.exists():
        raise HTTPException(status_code=404, detail=f"instance not found: {instance_id}")
    attached_runs = _instance_runs(instance_id)
    blocking_runs = [
        row.get("run_id")
        for row in attached_runs
        if str(row.get("status") or "").strip().lower() not in runtime.TERMINAL_STATES
    ]
    if blocking_runs:
        raise HTTPException(status_code=409, detail=f"stop attached runs before delete: {', '.join(str(item) for item in blocking_runs)}")
    for row in attached_runs:
        run_id = str(row.get("run_id") or "").strip()
        if run_id:
            delete_run(run_id)
    shutil.rmtree(instance_root)
    storage.remove_file(storage.instance_artifact_manifest_path(instance_id))
    storage.list_instances_from_manifests()
    return {"updated_at": storage.now_iso(), "deleted": True, "instance_id": instance_id}
