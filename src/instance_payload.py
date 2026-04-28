from __future__ import annotations

import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any

from json_artifacts import write_json, write_table
from lieferdaten.runtime import ensure_run_subdirs, merge_run_config
from frontend_exports import export_frontend_contract


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except Exception:
        return None


def _safe_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed if math.isfinite(parsed) else None


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = math.sin(d_lat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(d_lon / 2) ** 2
    return round(radius_km * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)), 4)


def load_instance_payload(path: str | Path) -> dict[str, Any]:
    payload_path = Path(path).expanduser().resolve()
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("instance payload must be a JSON object")
    customers = payload.get("customers")
    demand_rows = payload.get("demand_rows")
    warehouse = payload.get("warehouse")
    if not isinstance(customers, list) or not customers:
        raise ValueError("customers must be a non-empty list")
    if not isinstance(demand_rows, list):
        raise ValueError("demand_rows must be a list")
    if not isinstance(warehouse, dict) or _safe_float(warehouse.get("latitude")) is None or _safe_float(warehouse.get("longitude")) is None:
        raise ValueError("warehouse.latitude and warehouse.longitude are required")
    seen: set[int] = set()
    for row in customers:
        if not isinstance(row, dict):
            raise ValueError("each customer must be an object")
        client_num = _safe_int(row.get("client_num"))
        if client_num is None or client_num <= 0:
            raise ValueError("each customer needs a positive integer client_num")
        if client_num in seen:
            raise ValueError(f"duplicate client_num: {client_num}")
        seen.add(client_num)
        if _safe_float(row.get("latitude")) is None or _safe_float(row.get("longitude")) is None:
            raise ValueError(f"customer {client_num} needs latitude and longitude")
    for row in demand_rows:
        if not isinstance(row, dict):
            raise ValueError("each demand row must be an object")
        client_num = _safe_int(row.get("client_num"))
        demand_kg = _safe_float(row.get("demand_kg"))
        if client_num not in seen:
            raise ValueError(f"demand row references unknown client_num: {client_num}")
        if demand_kg is None or demand_kg < 0:
            raise ValueError("demand_kg must be non-negative")
    clustered = [row for row in customers if isinstance(row, dict) and row.get("cluster_id") is not None]
    if clustered and len(clustered) != len(customers):
        raise ValueError("cluster_id must be provided for all customers or none")
    return payload


def import_instance_payload(payload_path: str | Path, run_dir: str | Path) -> Path:
    payload = load_instance_payload(payload_path)
    run_root = Path(run_dir).expanduser().resolve()
    layout = ensure_run_subdirs(run_root)
    customers = sorted(payload["customers"], key=lambda row: int(row["client_num"]))
    demand_rows = payload["demand_rows"]
    warehouse = payload["warehouse"]
    warehouse_lat = float(warehouse["latitude"])
    warehouse_lon = float(warehouse["longitude"])

    write_json(run_root / "prep" / "instance" / "payload.json", payload)
    write_json(run_root / "prep" / "instance" / "customers.json", {"customers": customers})
    write_json(run_root / "prep" / "instance" / "manifest.json", {
        "schema_version": 1,
        "instance_id": payload.get("instance_id") or run_root.name,
        "customer_count": len(customers),
        "demand_row_count": len(demand_rows),
    })
    write_table(run_root / "prep" / "instance" / "demand_long.json", ["delivery_date", "client_num", "customer_id", "demand_kg"], demand_rows)

    features = []
    for row in customers:
        props = dict(row)
        props["is_warehouse"] = False
        features.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": [row["longitude"], row["latitude"]]}, "properties": props})
    write_json(run_root / "prep" / "map" / "customers.json", {"type": "FeatureCollection", "features": features})
    write_json(run_root / "prep" / "map" / "customers_summary.json", {
        "customer_count": len(customers),
        "warehouse": {"latitude": warehouse_lat, "longitude": warehouse_lon},
        "available_demand_dates": sorted({str(row.get("delivery_date") or "") for row in demand_rows if row.get("delivery_date")}),
    })

    coordinates_rows = [{"node_index": 0, "latitude": warehouse_lat, "longitude": warehouse_lon}]
    mapping_rows = []
    positions = {0: (warehouse_lat, warehouse_lon)}
    for row in customers:
        client_num = int(row["client_num"])
        lat = float(row["latitude"])
        lon = float(row["longitude"])
        positions[client_num] = (lat, lon)
        coordinates_rows.append({"node_index": client_num, "latitude": lat, "longitude": lon})
        mapping_rows.append({"customer_id": str(row.get("customer_id") or client_num), "client_num": client_num})

    demand_by_date: dict[str, dict[int, float]] = defaultdict(lambda: defaultdict(float))
    for row in demand_rows:
        delivery_date = str(row.get("delivery_date") or "")
        if not delivery_date:
            continue
        demand_by_date[delivery_date][int(row["client_num"])] += float(row.get("demand_kg") or 0.0)
    customer_ids = [int(row["client_num"]) for row in customers]
    demand_table = []
    for delivery_date in sorted(demand_by_date):
        out = {"delivery_date": delivery_date}
        for client_num in customer_ids:
            out[str(client_num)] = round(demand_by_date[delivery_date].get(client_num, 0.0), 4)
        demand_table.append(out)

    nodes = [0, *customer_ids]
    distance_rows = []
    for i in nodes:
        row = {"node_index": i}
        lat1, lon1 = positions[i]
        for j in nodes:
            lat2, lon2 = positions[j]
            row[str(j)] = 0.0 if i == j else _haversine_km(lat1, lon1, lat2, lon2)
        distance_rows.append(row)

    write_table(layout["cspp_data"] / "coordinates.json", ["node_index", "latitude", "longitude"], coordinates_rows)
    write_table(layout["cspp_data"] / "customer_id_mapping.json", ["customer_id", "client_num"], mapping_rows)
    write_table(layout["cspp_data"] / "demand_matrix.json", ["delivery_date", *[str(i) for i in customer_ids]], demand_table)
    write_table(layout["cspp_data"] / "distances_matrix.json", ["node_index", *[str(i) for i in nodes]], distance_rows)

    assignments = []
    for row in customers:
        if row.get("cluster_id") is None:
            continue
        assignments.append({"client_num": int(row["client_num"]), "customer_id": row.get("customer_id"), "cluster_id": int(row["cluster_id"])})
    if assignments:
        write_json(run_root / "prep" / "clustering" / "assignments.json", {"assignments": assignments})
        write_table(run_root / "prep" / "clustering" / "cluster_assignments.json", ["client_num", "customer_id", "cluster_id"], assignments)

    merge_run_config(run_root, {
        "instance_id": payload.get("instance_id") or run_root.name,
        "clustering_method": payload.get("clustering_method") or ("manual" if assignments else "geographic"),
        "last_stage": "import_instance",
    })
    try:
        export_frontend_contract(run_root)
    except Exception:
        pass
    return run_root


def payload_has_cluster_assignments(payload: dict[str, Any]) -> bool:
    customers = payload.get("customers") if isinstance(payload, dict) else []
    return bool(customers) and all(isinstance(row, dict) and row.get("cluster_id") is not None for row in customers)
