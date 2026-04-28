from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from json_artifacts import read_json
from json_artifacts import read_table_rows
from json_artifacts import write_json
from json_artifacts import write_table
from lieferdaten.runtime import get_run_layout
from lieferdaten.runtime import resolve_run_root


PREP_SCHEMA_VERSION = 1


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path, default: Any = None) -> Any:
    return read_json(path, default)


def _read_table_rows(path: Path) -> list[dict[str, Any]]:
    return read_table_rows(path)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.lower() in {"none", "null", "nan", "n/a"}:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _safe_int(value: Any) -> int | None:
    parsed = _safe_float(value)
    if parsed is None:
        return None
    return int(parsed)


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _write_json(path: Path, payload: Any) -> None:
    write_json(path, payload)


def _write_table(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    write_table(path, fieldnames, rows)


def _prep_root(run_root: Path) -> Path:
    return run_root / "prep"


def _join_distinct(values: list[str]) -> str:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return " | ".join(ordered)


def _load_base_customer_bundle(layout: dict[str, Path]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    mapping_rows = _read_table_rows(layout["cspp_data"] / "customer_id_mapping.json")
    demand_rows = _read_table_rows(layout["cspp_data"] / "demand_matrix.json")
    tour_rows = _read_table_rows(layout["process_data"] / "tour_stops_clean.json")
    coords_rows = _read_table_rows(layout["cspp_data"] / "coordinates.json")

    customer_id_by_client_num: dict[int, str] = {}
    client_num_by_customer_id: dict[str, int] = {}
    for row in mapping_rows:
        customer_id = _safe_text(row.get("customer_id"))
        client_num = _safe_int(row.get("client_num"))
        if customer_id and client_num is not None:
            customer_id_by_client_num[client_num] = customer_id
            client_num_by_customer_id[customer_id] = client_num

    coords_by_node: dict[int, tuple[float, float]] = {}
    for row in coords_rows:
        node = _safe_int(row.get("node_index"))
        lat = _safe_float(row.get("latitude"))
        lon = _safe_float(row.get("longitude"))
        if node is not None and lat is not None and lon is not None:
            coords_by_node[node] = (lat, lon)

    meta_by_customer_id: dict[str, dict[str, Any]] = {}
    for row in tour_rows:
        customer_id = _safe_text(row.get("customer_id"))
        if not customer_id:
            continue
        current = meta_by_customer_id.get(customer_id, {})
        weight = _safe_float(row.get("weight")) or 0.0
        if current and weight < current.get("_max_weight", -1):
            continue
        meta_by_customer_id[customer_id] = {
            "_max_weight": weight,
            "customer_name": _safe_text(row.get("customer_name")) or current.get("customer_name"),
            "street": _safe_text(row.get("street")) or current.get("street"),
            "postal_code": _safe_text(row.get("postal_code")) or current.get("postal_code"),
            "city": _safe_text(row.get("city")) or current.get("city"),
        }

    raw_demand_by_pair: list[dict[str, Any]] = []
    delivery_dates: list[str] = []
    for row in demand_rows:
        delivery_date = _safe_text(row.get("delivery_date"))
        if delivery_date:
            delivery_dates.append(delivery_date)
        for key, value in row.items():
            if key == "delivery_date":
                continue
            client_num = _safe_int(key)
            demand = _safe_float(value)
            if client_num is None or demand is None:
                continue
            customer_id = customer_id_by_client_num.get(client_num)
            raw_demand_by_pair.append(
                {
                    "delivery_date": delivery_date,
                    "client_num": client_num,
                    "customer_id": customer_id,
                    "demand_kg": demand,
                }
            )

    customers: list[dict[str, Any]] = []
    demand_by_pair: list[dict[str, Any]] = []
    features: list[dict[str, Any]] = []
    latitudes: list[float] = []
    longitudes: list[float] = []
    customers_by_coord: dict[tuple[float, float], list[dict[str, Any]]] = defaultdict(list)
    merged_client_num_by_original: dict[int, int] = {}
    for client_num, customer_id in sorted(customer_id_by_client_num.items()):
        coord = coords_by_node.get(client_num)
        if coord is None:
            continue
        lat, lon = coord
        meta = meta_by_customer_id.get(customer_id, {})
        customers_by_coord[(lat, lon)].append(
            {
                "client_num": client_num,
                "customer_id": customer_id,
                "meta": meta,
            }
        )

    merged_customer_specs: list[dict[str, Any]] = []
    merged_customer_id_by_client_num: dict[int, str] = {}
    for (lat, lon), members in sorted(customers_by_coord.items(), key=lambda item: min(entry["client_num"] for entry in item[1])):
        representative = min(members, key=lambda entry: entry["client_num"])
        representative_customer_id = str(representative["customer_id"] or "")
        representative_meta = representative["meta"]
        source_client_nums = [int(entry["client_num"]) for entry in members]
        source_customer_ids = [str(entry["customer_id"] or "") for entry in members]
        record = {
            "client_num": int(representative["client_num"]),
            "customer_id": representative_customer_id,
            "customer_name": representative_meta.get("customer_name"),
            "street": representative_meta.get("street"),
            "postal_code": representative_meta.get("postal_code"),
            "city": representative_meta.get("city"),
            "latitude": lat,
            "longitude": lon,
            "cluster_id": None,
            "merged_customer_count": len(members),
            "source_client_nums": ",".join(str(value) for value in source_client_nums),
            "source_customer_ids": _join_distinct(source_customer_ids),
        }
        merged_customer_specs.append(record)
        merged_customer_id_by_client_num[record["client_num"]] = representative_customer_id
        for original_client_num in source_client_nums:
            merged_client_num_by_original[original_client_num] = record["client_num"]

    merged_demand_totals: dict[int, float] = defaultdict(float)
    merged_max_demands: dict[int, float] = defaultdict(float)
    merged_active_days: dict[int, int] = defaultdict(int)
    merged_latest_demand: dict[int, float] = {}
    merged_demand_by_pair: dict[tuple[str, int], float] = defaultdict(float)
    for row in raw_demand_by_pair:
        original_client_num = int(row["client_num"])
        merged_client_num = merged_client_num_by_original.get(original_client_num)
        if merged_client_num is None:
            continue
        delivery_date = str(row["delivery_date"] or "")
        key = (delivery_date, merged_client_num)
        merged_demand_by_pair[key] += float(row["demand_kg"] or 0.0)

    for (delivery_date, merged_client_num), demand in sorted(merged_demand_by_pair.items()):
        merged_customer_id = merged_customer_id_by_client_num.get(merged_client_num, "")
        merged_demand_totals[merged_client_num] += demand
        merged_max_demands[merged_client_num] = max(merged_max_demands.get(merged_client_num, 0.0), demand)
        merged_latest_demand[merged_client_num] = demand
        if demand > 0:
            merged_active_days[merged_client_num] += 1
        demand_by_pair.append(
            {
                "delivery_date": delivery_date,
                "client_num": merged_client_num,
                "customer_id": merged_customer_id,
                "demand_kg": round(demand, 4),
            }
        )

    for record in merged_customer_specs:
        client_num = int(record["client_num"])
        latitudes.append(record["latitude"])
        longitudes.append(record["longitude"])
        record["total_demand_kg"] = round(merged_demand_totals.get(client_num, 0.0), 4)
        record["max_demand_kg"] = round(merged_max_demands.get(client_num, 0.0), 4)
        record["active_days"] = merged_active_days.get(client_num, 0)
        record["latest_demand_kg"] = round(merged_latest_demand.get(client_num, 0.0), 4)
        customers.append(record)
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [record["longitude"], record["latitude"]]},
                "properties": {
                    **record,
                    "is_warehouse": False,
                },
            }
        )

    warehouse = None
    if 0 in coords_by_node:
        warehouse = {
            "node_index": 0,
            "latitude": coords_by_node[0][0],
            "longitude": coords_by_node[0][1],
        }
        latitudes.append(coords_by_node[0][0])
        longitudes.append(coords_by_node[0][1])

    summary = {
        "updated_at": _now_iso(),
        "customer_count": len(customers),
        "delivery_dates": sorted(set(date for date in delivery_dates if date)),
        "warehouse": warehouse,
        "bounds": {
            "min_latitude": min(latitudes) if latitudes else None,
            "max_latitude": max(latitudes) if latitudes else None,
            "min_longitude": min(longitudes) if longitudes else None,
            "max_longitude": max(longitudes) if longitudes else None,
        },
    }
    return customers, demand_by_pair, {"type": "FeatureCollection", "features": features, "summary": summary}


def _write_manifest(run_root: Path, *, instance_ready: bool, clustering_ready: bool) -> None:
    prep_root = _prep_root(run_root)
    payload = {
        "schema_version": PREP_SCHEMA_VERSION,
        "run_id": run_root.name,
        "updated_at": _now_iso(),
        "instance_ready": instance_ready,
        "clustering_ready": clustering_ready,
        "files": {
            "instance_manifest": "prep/instance/manifest.json",
            "instance_payload_json": "prep/instance/payload.json",
            "customers_csv": "prep/instance/customers.json",
            "customers_json": "prep/instance/customers.json",
            "customers_geojson": "prep/map/customers.json",
            "customers_summary_json": "prep/map/customers_summary.json",
            "demand_long_csv": "prep/instance/demand_long.json",
            "instance_import_payload_json": "prep/instance/instance_import_payload.json",
            "clustering_manifest": "prep/clustering/manifest.json",
            "cluster_assignments_json": "prep/clustering/assignments.json",
            "cluster_assignments_csv": "prep/clustering/cluster_assignments.json",
            "clusters_json": "prep/clustering/clusters.json",
        },
    }
    _write_json(prep_root / "manifest.json", payload)


def export_prepared_instance(run_dir: str | Path) -> None:
    run_root = resolve_run_root(run_dir)
    layout = get_run_layout(run_root)
    prep_root = _prep_root(run_root)
    customers, demand_rows, geojson_bundle = _load_base_customer_bundle(layout)
    warehouse = geojson_bundle.get("summary", {}).get("warehouse")
    warehouse_payload = None
    if isinstance(warehouse, dict):
        warehouse_payload = {
            "latitude": _safe_float(warehouse.get("latitude")),
            "longitude": _safe_float(warehouse.get("longitude")),
        }
        if warehouse_payload["latitude"] is None or warehouse_payload["longitude"] is None:
            warehouse_payload = None

    customer_fields = [
        "client_num",
        "customer_id",
        "customer_name",
        "street",
        "postal_code",
        "city",
        "latitude",
        "longitude",
        "total_demand_kg",
        "max_demand_kg",
        "active_days",
        "latest_demand_kg",
        "cluster_id",
        "merged_customer_count",
        "source_client_nums",
        "source_customer_ids",
    ]
    _write_table(prep_root / "instance" / "customers.json", customer_fields, customers)
    _write_json(prep_root / "instance" / "customers.json", {"updated_at": _now_iso(), "customers": customers})
    _write_json(
        prep_root / "instance" / "payload.json",
        {
            "schema_version": PREP_SCHEMA_VERSION,
            "run_id": run_root.name,
            "generated_at": _now_iso(),
            "warehouse": warehouse_payload,
            "customers": customers,
            "demand_rows": demand_rows,
        },
    )
    _write_table(
        prep_root / "instance" / "demand_long.json",
        ["delivery_date", "client_num", "customer_id", "demand_kg"],
        demand_rows,
    )
    _write_json(
        prep_root / "instance" / "manifest.json",
        {
            "schema_version": PREP_SCHEMA_VERSION,
            "run_id": run_root.name,
            "updated_at": _now_iso(),
            "customer_count": len(customers),
            "demand_rows": len(demand_rows),
        },
    )
    _write_json(prep_root / "map" / "customers.json", {"type": "FeatureCollection", "features": geojson_bundle["features"]})
    _write_json(prep_root / "map" / "customers_summary.json", geojson_bundle["summary"])
    _write_manifest(run_root, instance_ready=True, clustering_ready=(prep_root / "clustering" / "manifest.json").exists())


def export_instance_import_payload(run_dir: str | Path, *, include_clustering: bool = False) -> Path:
    run_root = resolve_run_root(run_dir)
    prep_root = _prep_root(run_root)
    layout = get_run_layout(run_root)

    export_prepared_instance(run_root)
    if include_clustering and list(layout["clustering_data"].glob("cluster_assignments*.json")):
        export_prepared_clustering(run_root)

    customers_json = _read_json(prep_root / "instance" / "customers.json", {}) or {}
    customers = list(customers_json.get("customers") or [])
    if not include_clustering:
        normalized_customers: list[dict[str, Any]] = []
        for customer in customers:
            copy = dict(customer)
            copy.pop("cluster_id", None)
            normalized_customers.append(copy)
        customers = normalized_customers
    demand_rows = _read_table_rows(prep_root / "instance" / "demand_long.json")
    map_summary = _read_json(prep_root / "map" / "customers_summary.json", {}) or {}

    warehouse = map_summary.get("warehouse") or {}
    warehouse_payload = {
        "latitude": _safe_float(warehouse.get("latitude")),
        "longitude": _safe_float(warehouse.get("longitude")),
    }
    if warehouse_payload["latitude"] is None or warehouse_payload["longitude"] is None:
        warehouse_payload = None

    bundle_path = prep_root / "instance" / "instance_import_payload.json"
    payload = {
        "schema_version": PREP_SCHEMA_VERSION,
        "instance_id": run_root.name,
        "source_instance_id": run_root.name,
        "generated_at": _now_iso(),
        "includes_clustering": include_clustering,
        "warehouse": warehouse_payload,
        "customers": customers,
        "demand_rows": [
            {
                "delivery_date": _safe_text(row.get("delivery_date")),
                "client_num": _safe_int(row.get("client_num")),
                "customer_id": _safe_text(row.get("customer_id")) or None,
                "demand_kg": _safe_float(row.get("demand_kg")) or 0.0,
            }
            for row in demand_rows
            if _safe_int(row.get("client_num")) is not None
        ],
    }
    _write_json(bundle_path, payload)

    instance_manifest = _read_json(prep_root / "instance" / "manifest.json", {}) or {}
    instance_manifest["instance_import_payload_path"] = "prep/instance/instance_import_payload.json"
    instance_manifest["instance_import_payload_includes_clustering"] = include_clustering
    instance_manifest["instance_import_payload_customer_count"] = len(customers)
    instance_manifest["instance_import_payload_demand_rows"] = len(payload["demand_rows"])
    instance_manifest["updated_at"] = _now_iso()
    _write_json(prep_root / "instance" / "manifest.json", instance_manifest)
    _write_manifest(run_root, instance_ready=True, clustering_ready=(prep_root / "clustering" / "manifest.json").exists())
    return bundle_path


def export_web_app_instance_bundle(run_dir: str | Path, *, include_clustering: bool = False) -> Path:
    return export_instance_import_payload(run_dir, include_clustering=include_clustering)


def export_prepared_clustering(run_dir: str | Path) -> None:
    run_root = resolve_run_root(run_dir)
    layout = get_run_layout(run_root)
    prep_root = _prep_root(run_root)

    customers_json = _read_json(prep_root / "instance" / "customers.json", {}) or {}
    customers = list(customers_json.get("customers") or [])
    if not customers:
        export_prepared_instance(run_root)
        customers_json = _read_json(prep_root / "instance" / "customers.json", {}) or {}
        customers = list(customers_json.get("customers") or [])

    cluster_path_candidates = [
        layout["clustering_data"] / "cluster_assignments.json",
        layout["clustering_data"] / "cluster_assignments_tour_containment.json",
    ]
    cluster_rows: list[dict[str, str]] = []
    selected_path = None
    for path in cluster_path_candidates:
        rows = _read_table_rows(path)
        if rows:
            cluster_rows = rows
            selected_path = path
            break

    if not cluster_rows:
        _write_manifest(run_root, instance_ready=True, clustering_ready=False)
        return

    assignment_by_client: dict[int, dict[str, Any]] = {}
    for row in cluster_rows:
        client_num = _safe_int(row.get("customer_id"))
        if client_num is None:
            continue
        assignment_by_client[client_num] = {
            "client_num": client_num,
            "cluster_id": _safe_int(row.get("cluster")),
        }

    normalized_rows: list[dict[str, Any]] = []
    clusters: dict[str, dict[str, Any]] = defaultdict(lambda: {"customer_count": 0, "total_demand_kg": 0.0, "client_nums": [], "customer_ids": []})
    customer_by_client = {int(customer["client_num"]): customer for customer in customers if customer.get("client_num") is not None}
    for client_num, assignment in sorted(assignment_by_client.items()):
        customer = customer_by_client.get(client_num)
        if not customer:
            continue
        cluster_id = assignment.get("cluster_id")
        customer["cluster_id"] = cluster_id
        normalized_rows.append(
            {
                "client_num": client_num,
                "customer_id": customer.get("customer_id"),
                "cluster_id": cluster_id,
            }
        )
        if cluster_id is not None:
            entry = clusters[str(cluster_id)]
            entry["cluster_id"] = cluster_id
            entry["customer_count"] += 1
            entry["total_demand_kg"] += float(customer.get("total_demand_kg") or 0.0)
            entry["client_nums"].append(client_num)
            entry["customer_ids"].append(customer.get("customer_id"))

    features: list[dict[str, Any]] = []
    for customer in customers:
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [customer["longitude"], customer["latitude"]]},
                "properties": {**customer, "is_warehouse": False},
            }
        )

    _write_table(
        prep_root / "clustering" / "cluster_assignments.json",
        ["client_num", "customer_id", "cluster_id"],
        normalized_rows,
    )
    _write_json(
        prep_root / "clustering" / "clusters.json",
        {
            "updated_at": _now_iso(),
            "source_file": selected_path.name if selected_path else None,
            "clusters": [value for _, value in sorted(clusters.items(), key=lambda item: int(item[0]))],
        },
    )
    _write_json(
        prep_root / "clustering" / "assignments.json",
        {
            "updated_at": _now_iso(),
            "run_id": run_root.name,
            "source_file": selected_path.name if selected_path else None,
            "assignments": normalized_rows,
        },
    )
    _write_json(
        prep_root / "clustering" / "manifest.json",
        {
            "schema_version": PREP_SCHEMA_VERSION,
            "run_id": run_root.name,
            "updated_at": _now_iso(),
            "cluster_count": len(clusters),
            "assignment_count": len(normalized_rows),
            "source_file": selected_path.name if selected_path else None,
        },
    )
    _write_json(prep_root / "instance" / "customers.json", {"updated_at": _now_iso(), "customers": customers})
    _write_table(
        prep_root / "instance" / "customers.json",
        [
            "client_num",
            "customer_id",
            "customer_name",
            "street",
            "postal_code",
            "city",
            "latitude",
            "longitude",
            "total_demand_kg",
            "max_demand_kg",
            "active_days",
            "latest_demand_kg",
            "cluster_id",
            "merged_customer_count",
            "source_client_nums",
            "source_customer_ids",
        ],
        customers,
    )
    _write_json(prep_root / "map" / "customers.json", {"type": "FeatureCollection", "features": features})
    _write_manifest(run_root, instance_ready=True, clustering_ready=True)
