from __future__ import annotations

import ast
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from json_artifacts import read_event_log
from json_artifacts import read_json
from json_artifacts import read_table_rows
from json_artifacts import write_json
from lieferdaten.runtime import get_run_layout
from lieferdaten.runtime import resolve_run_root
from pipeline_progress import build_pipeline_progress
from pipeline_progress import pipeline_progress_path


FRONTEND_SCHEMA_VERSION = 1


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path, default: Any = None) -> Any:
    return read_json(path, default)


def _read_text(path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return None


def _read_table_rows(path: Path) -> list[dict[str, Any]]:
    return read_table_rows(path)


def _tail_jsonl(path: Path, limit: int) -> list[dict[str, Any]]:
    return read_event_log(path)[-limit:]


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.lower() in {"none", "null", "nan", "n/a"}:
        return None
    try:
        parsed = float(text)
    except Exception:
        return None
    if math.isnan(parsed):
        return None
    return parsed


def _solver_metric(value: Any) -> float | None:
    parsed = _safe_float(value)
    if parsed is None:
        return None
    if abs(parsed) >= 1e99:
        return None
    return parsed


def _safe_int(value: Any) -> int | None:
    parsed = _safe_float(value)
    if parsed is None:
        return None
    return int(parsed)


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    return value


def _write_json(path: Path, payload: Any) -> None:
    write_json(path, _json_safe(payload))


def _list_stage3_scope_ids(live_dir: Path) -> list[str]:
    if not live_dir.exists():
        return []
    scope_ids: set[str] = set()
    for path in live_dir.glob("*_live.json"):
        scope_ids.add(path.name[:-10])
    for path in live_dir.glob("*_events.json"):
        scope_ids.add(path.name[:-13])
    return sorted(scope_ids)


def _load_node_coordinates(layout: dict[str, Path]) -> dict[int, tuple[float, float]]:
    rows = _read_table_rows(layout["cspp_data"] / "coordinates.json")
    node_coords: dict[int, tuple[float, float]] = {}
    for row in rows:
        node = _safe_int(row.get("node_index"))
        latitude = _safe_float(row.get("latitude"))
        longitude = _safe_float(row.get("longitude"))
        if node is None or latitude is None or longitude is None:
            continue
        node_coords[node] = (latitude, longitude)
    return node_coords


def _customer_feature_lookup(map_geojson: dict[str, Any]) -> dict[int, dict[str, Any]]:
    lookup: dict[int, dict[str, Any]] = {}
    for feature in map_geojson.get("features") or []:
        if not isinstance(feature, dict):
            continue
        geometry = feature.get("geometry") if isinstance(feature.get("geometry"), dict) else {}
        properties = feature.get("properties") if isinstance(feature.get("properties"), dict) else {}
        coordinates = geometry.get("coordinates") if isinstance(geometry.get("coordinates"), list) else []
        if len(coordinates) < 2:
            continue
        client_num = _safe_int(properties.get("client_num"))
        if client_num is None:
            continue
        lookup[client_num] = {
            "client_num": client_num,
            "customer_id": properties.get("customer_id"),
            "customer_name": properties.get("customer_name"),
            "cluster_id": _safe_int(properties.get("cluster_id")),
            "latitude": _safe_float(coordinates[1]),
            "longitude": _safe_float(coordinates[0]),
        }
    return lookup


def _parse_tuple_key(value: Any) -> tuple[Any, ...] | None:
    try:
        parsed = ast.literal_eval(str(value))
    except Exception:
        return None
    return parsed if isinstance(parsed, tuple) else None


def _customer_marker(customer_lookup: dict[int, dict[str, Any]], customer: int, **extra: Any) -> dict[str, Any] | None:
    meta = customer_lookup.get(customer)
    if not meta:
        return None
    latitude = _safe_float(meta.get("latitude"))
    longitude = _safe_float(meta.get("longitude"))
    if latitude is None or longitude is None:
        return None
    return {
        "customer": customer,
        "client_num": customer,
        "customer_id": meta.get("customer_id"),
        "customer_name": meta.get("customer_name"),
        "cluster_id": meta.get("cluster_id"),
        "latitude": latitude,
        "longitude": longitude,
        **extra,
    }


def _route_segment(node_coords: dict[int, tuple[float, float]], route: dict[str, Any]) -> dict[str, Any] | None:
    start_node = _safe_int(route.get("from"))
    end_node = _safe_int(route.get("to"))
    if start_node is None or end_node is None:
        return None
    if start_node not in node_coords or end_node not in node_coords:
        return None
    start_latitude, start_longitude = node_coords[start_node]
    end_latitude, end_longitude = node_coords[end_node]
    return {
        "from": start_node,
        "to": end_node,
        "truck": _safe_int(route.get("truck")),
        "tour": _safe_int(route.get("tour")),
        "from_latitude": start_latitude,
        "from_longitude": start_longitude,
        "to_latitude": end_latitude,
        "to_longitude": end_longitude,
    }


def _build_route_bundle(
    *,
    node_coords: dict[int, tuple[float, float]],
    customer_lookup: dict[int, dict[str, Any]],
    routes: list[dict[str, Any]],
    customer_charging: list[dict[str, Any]],
    warehouse_charging: list[dict[str, Any]] | None = None,
    overnight_charging: list[dict[str, Any]] | None = None,
    total_cost: Any = None,
    scenario_id: Any = None,
    status: Any = None,
) -> dict[str, Any] | None:
    route_segments = [segment for segment in (_route_segment(node_coords, row) for row in routes) if segment]
    customer_markers = [
        marker
        for marker in (
            _customer_marker(
                customer_lookup,
                _safe_int(row.get("customer")) or -1,
                truck=_safe_int(row.get("truck")),
                tour=_safe_int(row.get("tour")),
                energy_kwh=_safe_float(row.get("energy_kwh")),
                status="installed",
            )
            for row in customer_charging
        )
        if marker
    ]
    if not route_segments and not customer_markers:
        return None
    truck_count = len({(segment.get("truck"), segment.get("tour")) for segment in route_segments if segment.get("truck") is not None})
    tour_count = len({(segment.get("truck"), segment.get("tour")) for segment in route_segments if segment.get("tour") is not None})
    return {
        "scenario_id": _safe_int(scenario_id),
        "status": status,
        "summary": {
            "total_cost": _safe_float(total_cost),
            "route_count": len(route_segments),
            "truck_count": truck_count,
            "tour_count": tour_count,
            "customer_charge_total_kwh": round(sum(_safe_float(row.get("energy_kwh")) or 0.0 for row in customer_charging), 2),
            "warehouse_charge_total_kwh": round(sum(_safe_float(row.get("energy_kwh")) or 0.0 for row in (warehouse_charging or [])), 2),
            "overnight_charge_total_kwh": round(sum(_safe_float(row.get("energy_kwh")) or 0.0 for row in (overnight_charging or [])), 2),
        },
        "routes": route_segments,
        "customer_chargers": customer_markers,
        "warehouse_charging": warehouse_charging or [],
        "overnight_charging": overnight_charging or [],
    }


def _build_stage1_combined_bundle(layout: dict[str, Path], map_geojson: dict[str, Any]) -> dict[str, Any] | None:
    customer_lookup = _customer_feature_lookup(map_geojson)
    markers: list[dict[str, Any]] = []
    warehouse_chargers: list[dict[str, Any]] = []
    for path in sorted(layout["cspp_first_stage_json"].glob("cluster_*_first_stage.json")):
        payload = _read_json(path, {}) or {}
        cluster_id = _safe_int(payload.get("cluster_id"))
        for row in payload.get("a") or []:
            if _safe_float((row or {}).get("val")) is not None and (_safe_float((row or {}).get("val")) or 0.0) < 0.5:
                continue
            customer = _safe_int((row or {}).get("j"))
            if customer is None:
                continue
            marker = _customer_marker(
                customer_lookup,
                customer,
                tau=_safe_int((row or {}).get("tau")),
                val=_safe_float((row or {}).get("val")),
                status="installed",
                source_cluster_id=cluster_id,
            )
            if marker:
                markers.append(marker)
        for row in payload.get("a_wh") or []:
            if (_safe_float((row or {}).get("val")) or 0.0) < 0.5:
                continue
            warehouse_chargers.append(
                {
                    "tau": _safe_int((row or {}).get("tau")),
                    "val": _safe_float((row or {}).get("val")),
                    "source_cluster_id": cluster_id,
                }
            )
    if not markers and not warehouse_chargers:
        return None
    return {
        "summary": {
            "installed_customer_chargers": len(markers),
            "installed_warehouse_chargers": len(warehouse_chargers),
            "cluster_count": len({marker.get("source_cluster_id") for marker in markers if marker.get("source_cluster_id") is not None}),
        },
        "customer_chargers": markers,
        "warehouse_chargers": warehouse_chargers,
    }


def _build_stage3_charger_delta_bundle(layout: dict[str, Path], map_geojson: dict[str, Any]) -> dict[str, Any] | None:
    customer_lookup = _customer_feature_lookup(map_geojson)

    def _collect_customer_keys(paths: list[Path]) -> set[tuple[int, int | None]]:
        collected: set[tuple[int, int | None]] = set()
        for path in paths:
            payload = _read_json(path, {}) or {}
            for row in payload.get("a") or []:
                if (_safe_float((row or {}).get("val")) or 0.0) < 0.5:
                    continue
                customer = _safe_int((row or {}).get("j"))
                if customer is None:
                    continue
                collected.add((customer, _safe_int((row or {}).get("tau"))))
        return collected

    baseline_paths = sorted(layout["cspp_first_stage_json"].glob("cluster_*_first_stage.json"))
    reopt_paths = sorted((layout["cspp_reopt"] / "cluster_live").glob("cluster_*/best_first_stage_current.json"))
    baseline = _collect_customer_keys(baseline_paths)
    reoptimized = _collect_customer_keys(reopt_paths)
    all_keys = sorted(baseline | reoptimized)
    markers: list[dict[str, Any]] = []
    for customer, tau in all_keys:
        if (customer, tau) in baseline and (customer, tau) in reoptimized:
            status = "unchanged"
        elif (customer, tau) in reoptimized:
            status = "added"
        else:
            status = "removed"
        marker = _customer_marker(customer_lookup, customer, tau=tau, status=status)
        if marker:
            markers.append(marker)
    if not markers:
        return None
    return {
        "summary": {
            "baseline_customer_chargers": len(baseline),
            "best_customer_chargers": len(reoptimized),
            "added_customer_chargers": sum(1 for marker in markers if marker.get("status") == "added"),
            "removed_customer_chargers": sum(1 for marker in markers if marker.get("status") == "removed"),
            "unchanged_customer_chargers": sum(1 for marker in markers if marker.get("status") == "unchanged"),
        },
        "customer_chargers": markers,
    }


def _parse_json_csv_column(value: Any) -> Any:
    if value is None:
        return []
    if isinstance(value, (list, dict)):
        return value
    text = str(value).strip()
    if not text:
        return []
    try:
        return json.loads(text)
    except Exception:
        return value


def _relative_export_path(path: Path, run_root: Path) -> str:
    try:
        return str(path.resolve().relative_to(run_root.resolve()))
    except Exception:
        return str(path)


def _resolve_export_root(run_root: Path) -> Path:
    run_root = run_root.resolve()
    if run_root.parent.name == "runs":
        return run_root.parent.parent
    return run_root.parents[1]


def _load_latest_run_id(export_root: Path) -> str | None:
    latest_run = _read_json(export_root / "state" / "latest_run.json", {}) or {}
    content = str(latest_run.get("run_dir") or "").strip()
    if not content:
        return None
    return Path(content.strip()).name or None


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


def _build_stage1_live_snapshot(layout: dict[str, Path]) -> dict[str, Any]:
    live_root = layout["cspp_first_stage"] / "cluster_live"
    if not live_root.exists():
        return {}

    clusters: list[dict[str, Any]] = []
    for cluster_dir in sorted(live_root.glob("cluster_*"), key=lambda path: path.name):
        live_state = _read_json(cluster_dir / "live_state.json", {}) or {}
        cluster_id = _safe_int(live_state.get("cluster_id"))
        if cluster_id is None:
            continue
        master_callback = live_state.get("master_callback") if isinstance(live_state.get("master_callback"), dict) else {}
        best_first_stage = live_state.get("best_first_stage") if isinstance(live_state.get("best_first_stage"), dict) else {}
        installed = best_first_stage.get("installed_customer_chargers") if isinstance(best_first_stage, dict) else None
        clusters.append(
            {
                "cluster_id": cluster_id,
                "status": live_state.get("status", "running"),
                "runtime_sec": _safe_float(live_state.get("elapsed_sec")),
                "customers": _safe_int(live_state.get("customers")),
                "split_customers": _safe_int(live_state.get("split_customers")),
                "scenarios": _safe_int(live_state.get("scenarios_total")),
                "iterations": _safe_int(live_state.get("current_iteration")),
                "installed_customer_chargers": _safe_int(installed),
                "objective": _solver_metric(master_callback.get("best_obj")),
                "reached_gap": _solver_metric(live_state.get("reached_gap") or master_callback.get("gap")),
                "timeout": live_state.get("status") == "timeout",
                "infeasible": live_state.get("status") == "failed",
                "error": live_state.get("error"),
            }
        )

    if not clusters:
        return {}

    completed_statuses = {"completed", "timeout", "failed"}
    return {
        "stage": "solve_clusters_first_stage",
        "status": "running",
        "completed_clusters": sum(1 for row in clusters if str(row.get("status") or "") in completed_statuses),
        "total_clusters": len(clusters),
        "solved_clusters": sum(1 for row in clusters if row.get("status") == "completed"),
        "failed_clusters": sum(1 for row in clusters if row.get("status") == "failed"),
        "timeout_clusters": sum(1 for row in clusters if row.get("status") == "timeout"),
        "elapsed_sec": max((_safe_float(row.get("runtime_sec")) or 0.0 for row in clusters), default=0.0),
        "cluster_live_dir": str(live_root),
        "clusters": sorted(clusters, key=lambda row: int(row["cluster_id"])),
    }


def _derive_stage_status(
    *,
    explicit_status: Any = None,
    active_count: int = 0,
    current_phase: Any = None,
    total: Any = None,
    completed: Any = None,
) -> str:
    explicit = str(explicit_status or "").strip().lower()
    if active_count > 0 or current_phase:
        return "running"
    if explicit and explicit not in {"missing", "unknown", "none", "null"}:
        return explicit
    total_int = _safe_int(total)
    completed_int = _safe_int(completed)
    if total_int is not None and total_int > 0:
        if completed_int is not None and completed_int >= total_int:
            return "completed"
        if completed_int is not None and completed_int > 0:
            return "running"
        return "pending"
    return "idle"


def _build_map_bundle(layout: dict[str, Path]) -> tuple[dict[str, Any], dict[str, Any]]:
    prep_geojson = _read_json(layout["run"] / "prep" / "map" / "customers.json", None)
    prep_summary = _read_json(layout["run"] / "prep" / "map" / "customers_summary.json", None)
    if prep_geojson and prep_summary:
        return prep_geojson, prep_summary

    coords_rows = _read_table_rows(layout["cspp_data"] / "coordinates.json")
    mapping_rows = _read_table_rows(layout["cspp_data"] / "customer_id_mapping.json")
    demand_rows = _read_table_rows(layout["cspp_data"] / "demand_matrix.json")
    cluster_rows = _read_table_rows(layout["clustering_data"] / "cluster_assignments.json")
    tour_rows = _read_table_rows(layout["process_data"] / "tour_stops_clean.json")

    coords_by_node: dict[int, tuple[float, float]] = {}
    for row in coords_rows:
        node_index = _safe_int(row.get("node_index"))
        lat = _safe_float(row.get("latitude"))
        lon = _safe_float(row.get("longitude"))
        if node_index is None or lat is None or lon is None:
            continue
        coords_by_node[node_index] = (lat, lon)

    mapping_by_client_num: dict[int, str] = {}
    client_num_by_customer_id: dict[str, int] = {}
    for row in mapping_rows:
        customer_id = _safe_text(row.get("customer_id"))
        client_num = _safe_int(row.get("client_num"))
        if not customer_id or client_num is None:
            continue
        mapping_by_client_num[client_num] = customer_id
        client_num_by_customer_id[customer_id] = client_num

    cluster_by_client_num: dict[int, int | None] = {}
    for row in cluster_rows:
        client_num = _safe_int(row.get("customer_id"))
        if client_num is None:
            continue
        cluster_by_client_num[client_num] = _safe_int(row.get("cluster"))

    meta_by_customer_id: dict[str, dict[str, Any]] = {}
    for row in tour_rows:
        customer_id = _safe_text(row.get("customer_id"))
        if not customer_id:
            continue
        current = meta_by_customer_id.get(customer_id, {})
        weight = _safe_float(row.get("weight")) or 0.0
        prev_weight = current.get("_max_weight", -1.0)
        if current and weight < prev_weight:
            continue
        meta_by_customer_id[customer_id] = {
            "_max_weight": weight,
            "customer_name": _safe_text(row.get("customer_name")) or current.get("customer_name"),
            "address": _safe_text(row.get("street")) or current.get("address"),
            "postal_code": _safe_text(row.get("postal_code")) or current.get("postal_code"),
            "city": _safe_text(row.get("city")) or current.get("city"),
            "gpslat": _safe_float(row.get("gpslat")) or current.get("gpslat"),
            "gpslon": _safe_float(row.get("gpslon")) or current.get("gpslon"),
        }

    demand_dates: list[str] = []

    customers_by_coord: dict[tuple[float, float], list[dict[str, Any]]] = defaultdict(list)
    for client_num, customer_id in sorted(mapping_by_client_num.items()):
        meta = meta_by_customer_id.get(customer_id, {})
        coord = coords_by_node.get(client_num)
        if coord is None:
            lat = _safe_float(meta.get("gpslat"))
            lon = _safe_float(meta.get("gpslon"))
            if lat is not None and lon is not None:
                coord = (lat, lon)
        if coord is None:
            continue
        customers_by_coord[coord].append(
            {
                "client_num": client_num,
                "customer_id": customer_id,
                "meta": meta,
            }
        )

    merged_totals_by_client_num: dict[int, float] = defaultdict(float)
    merged_max_by_client_num: dict[int, float] = defaultdict(float)
    merged_latest_by_client_num: dict[int, float] = {}
    merged_nonzero_days_by_client_num: dict[int, int] = defaultdict(int)
    merged_client_num_by_original: dict[int, int] = {}
    merged_customer_id_by_client_num: dict[int, str] = {}
    merged_records: list[dict[str, Any]] = []
    for coord, members in sorted(customers_by_coord.items(), key=lambda item: min(entry["client_num"] for entry in item[1])):
        representative = min(members, key=lambda entry: entry["client_num"])
        merged_client_num = int(representative["client_num"])
        merged_customer_id = str(representative["customer_id"] or "")
        source_client_nums = [int(entry["client_num"]) for entry in members]
        source_customer_ids = [str(entry["customer_id"] or "") for entry in members]
        merged_customer_id_by_client_num[merged_client_num] = merged_customer_id
        for original_client_num in source_client_nums:
            merged_client_num_by_original[original_client_num] = merged_client_num
        merged_records.append(
            {
                "client_num": merged_client_num,
                "customer_id": merged_customer_id,
                "meta": representative["meta"],
                "latitude": coord[0],
                "longitude": coord[1],
                "merged_customer_count": len(members),
                "source_client_nums": ",".join(str(value) for value in source_client_nums),
                "source_customer_ids": _join_distinct(source_customer_ids),
            }
        )

    merged_demands_by_date_and_client: dict[tuple[str, int], float] = defaultdict(float)
    for row in demand_rows:
        date = _safe_text(row.get("delivery_date"))
        if date:
            demand_dates.append(date)
        for key, value in row.items():
            if key == "delivery_date":
                continue
            client_num = _safe_int(key)
            amount = _safe_float(value)
            if client_num is None or amount is None:
                continue
            merged_client_num = merged_client_num_by_original.get(client_num)
            if merged_client_num is None:
                continue
            merged_demands_by_date_and_client[(date, merged_client_num)] += amount

    for (date, merged_client_num), amount in sorted(merged_demands_by_date_and_client.items()):
        merged_totals_by_client_num[merged_client_num] += amount
        merged_max_by_client_num[merged_client_num] = max(merged_max_by_client_num.get(merged_client_num, 0.0), amount)
        merged_latest_by_client_num[merged_client_num] = amount
        if amount > 0:
            merged_nonzero_days_by_client_num[merged_client_num] += 1

    features: list[dict[str, Any]] = []
    latitudes: list[float] = []
    longitudes: list[float] = []

    warehouse = None
    if 0 in coords_by_node:
        lat, lon = coords_by_node[0]
        warehouse = {"latitude": lat, "longitude": lon}
        latitudes.append(lat)
        longitudes.append(lon)

    for record in merged_records:
        client_num = record["client_num"]
        customer_id = record["customer_id"]
        meta = record["meta"]
        lat = record["latitude"]
        lon = record["longitude"]
        latitudes.append(lat)
        longitudes.append(lon)
        total_demand = merged_totals_by_client_num.get(client_num, 0.0)
        nonzero_days = merged_nonzero_days_by_client_num.get(client_num, 0)
        feature = {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "client_num": client_num,
                "customer_id": customer_id,
                "customer_name": meta.get("customer_name"),
                "cluster_id": cluster_by_client_num.get(client_num),
                "max_demand": merged_max_by_client_num.get(client_num, 0.0),
                "total_demand": total_demand,
                "nonzero_days": nonzero_days,
                "latest_demand": merged_latest_by_client_num.get(client_num, 0.0),
                "avg_demand": (total_demand / nonzero_days) if nonzero_days else 0.0,
                "address": meta.get("address"),
                "postal_code": meta.get("postal_code"),
                "city": meta.get("city"),
                "merged_customer_count": record["merged_customer_count"],
                "source_client_nums": record["source_client_nums"],
                "source_customer_ids": record["source_customer_ids"],
                "is_warehouse": False,
            },
        }
        features.append(feature)

    bounds = None
    if latitudes and longitudes:
        bounds = {
            "min_latitude": min(latitudes),
            "max_latitude": max(latitudes),
            "min_longitude": min(longitudes),
            "max_longitude": max(longitudes),
        }

    cluster_counts: dict[str, int] = defaultdict(int)
    demand_values = [f["properties"]["total_demand"] for f in features]
    for feature in features:
        cluster_id = feature["properties"].get("cluster_id")
        cluster_counts[str(cluster_id) if cluster_id is not None else "unassigned"] += 1

    geojson = {"type": "FeatureCollection", "features": features}
    summary = {
        "updated_at": _now_iso(),
        "customer_count": len(features),
        "warehouse": warehouse,
        "bounds": bounds,
        "available_demand_dates": sorted(d for d in demand_dates if d),
        "cluster_counts": dict(sorted(cluster_counts.items())),
        "demand_min": min(demand_values) if demand_values else None,
        "demand_max": max(demand_values) if demand_values else None,
    }
    return geojson, summary


def _build_stage1(layout: dict[str, Path], map_geojson: dict[str, Any]) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    snapshot = _read_json(layout["cspp_first_stage"] / "cluster_progress_snapshot.json", {}) or {}
    if not (snapshot.get("clusters") or []):
        snapshot = _build_stage1_live_snapshot(layout)
    current_state = _read_json(layout["cspp_first_stage"] / "current_state.json", {}) or {}
    first_stage_rows = _read_table_rows(layout["cspp_first_stage"] / "cluster_first_stage.json")
    live_root = layout["cspp_first_stage"] / "cluster_live"

    installed_by_cluster: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in first_stage_rows:
        cluster_id = _safe_int(row.get("cluster_id"))
        if cluster_id is None:
            continue
        installed_by_cluster[cluster_id].append(
            {
                "customer_id": _safe_int(row.get("customer_id")),
                "charger_type": _safe_int(row.get("charger_type")),
                "value": _safe_float(row.get("value")),
            }
        )

    clusters = snapshot.get("clusters") or []
    cluster_details: dict[str, dict[str, Any]] = {}
    customer_features = map_geojson.get("features") or []
    by_cluster_feature: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for feature in customer_features:
        cluster_id = feature.get("properties", {}).get("cluster_id")
        if cluster_id is not None:
            by_cluster_feature[str(cluster_id)].append(feature)

    rows: list[dict[str, Any]] = []
    for row in clusters:
        cluster_id = _safe_int(row.get("cluster_id"))
        if cluster_id is None:
            continue
        cluster_key = str(cluster_id)
        live_state = _read_json(live_root / f"cluster_{cluster_id}" / "live_state.json", {}) or {}
        best_first_stage = _read_json(live_root / f"cluster_{cluster_id}" / "best_first_stage_current.json", {}) or {}
        events = _tail_jsonl(live_root / f"cluster_{cluster_id}" / "event_log.json", 200)
        record = {
            "cluster_id": cluster_id,
            "status": row.get("status"),
            "runtime_sec": _safe_float(row.get("runtime_sec")),
            "customers": _safe_int(row.get("customers")),
            "split_customers": _safe_int(row.get("split_customers")),
            "scenarios": _safe_int(row.get("scenarios")),
            "iterations": _safe_int(row.get("iterations")),
            "installed_customer_chargers": _safe_int(row.get("installed_customer_chargers")),
            "objective": _solver_metric(row.get("objective")),
            "reached_gap": _solver_metric(row.get("reached_gap")),
            "timeout": str(row.get("timeout")).lower() == "true",
            "infeasible": str(row.get("infeasible")).lower() == "true",
            "error": row.get("error"),
            "current_phase": live_state.get("current_phase"),
            "current_iteration": live_state.get("current_iteration"),
            "is_active": (live_state.get("status") in {"running", "initialized"}) if live_state else False,
            "updated_at": _now_iso(),
        }
        rows.append(record)
        cluster_details[cluster_key] = {
            "updated_at": _now_iso(),
            "cluster": record,
            "live_state": live_state,
            "best_first_stage": best_first_stage,
            "events": events,
            "installed_rows": installed_by_cluster.get(cluster_id, []),
            "map": {
                "type": "FeatureCollection",
                "features": by_cluster_feature.get(cluster_key, []),
            },
        }

    overview = {
        "updated_at": _now_iso(),
        "status": _derive_stage_status(
            explicit_status=current_state.get("status") or snapshot.get("status"),
            active_count=sum(1 for row in rows if row.get("is_active")),
            current_phase=next((row.get("current_phase") for row in rows if row.get("current_phase")), None),
            total=current_state.get("total_clusters") or snapshot.get("total_clusters"),
            completed=current_state.get("completed_clusters") or snapshot.get("completed_clusters"),
        ),
        "current_state": current_state,
        "summary": {
            "completed_clusters": current_state.get("completed_clusters") or snapshot.get("completed_clusters"),
            "total_clusters": current_state.get("total_clusters") or snapshot.get("total_clusters"),
            "solved_clusters": snapshot.get("solved_clusters"),
            "failed_clusters": snapshot.get("failed_clusters"),
            "timeout_clusters": snapshot.get("timeout_clusters"),
            "elapsed_sec": current_state.get("elapsed_sec") or snapshot.get("elapsed_sec"),
        },
        "clusters": rows,
        "combined_solution": _build_stage1_combined_bundle(layout, map_geojson),
    }
    return overview, cluster_details


def _build_stage2(layout: dict[str, Path]) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    node_coords = _load_node_coordinates(layout)
    map_geojson, _ = _build_map_bundle(layout)
    customer_lookup = _customer_feature_lookup(map_geojson)
    snapshot = _read_json(layout["cspp_scenario_evaluation"] / "scenario_progress_snapshot.json", {}) or {}
    total_costs = snapshot.get("scenario_total_costs") or {}
    runtime_rows = snapshot.get("cluster_runtime_rows") or []
    current_states = _read_json(layout["cspp_scenario_evaluation"] / "solver_live" / "current_solver_states.json", {"states": {}}) or {"states": {}}
    solver_events = _tail_jsonl(layout["cspp_scenario_evaluation"] / "solver_live" / "solver_events.json", 400)

    rows_by_scenario: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in runtime_rows:
        scenario = _safe_int(row.get("scenario"))
        if scenario is None:
            continue
        rows_by_scenario[str(scenario)].append(row)

    states_by_scenario: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for payload in (current_states.get("states") or {}).values():
        scenario = _safe_int(payload.get("scenario"))
        if scenario is not None:
            states_by_scenario[str(scenario)].append(payload)

    events_by_scenario: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in solver_events:
        scenario = _safe_int(event.get("scenario"))
        if scenario is not None:
            events_by_scenario[str(scenario)].append(event)

    scenario_rows: list[dict[str, Any]] = []
    details: dict[str, dict[str, Any]] = {}
    live_scenario_keys = {
        str(_safe_int(payload.get("scenario")))
        for payload in (current_states.get("states") or {}).values()
        if _safe_int(payload.get("scenario")) is not None
    }
    scenario_keys = sorted(
        {
            *[str(key) for key in total_costs.keys()],
            *[str(key) for key in (snapshot.get("scenario_statuses") or {}).keys()],
            *[str(_safe_int(row.get("scenario"))) for row in runtime_rows if _safe_int(row.get("scenario")) is not None],
            *live_scenario_keys,
        },
        key=lambda value: int(value),
    )
    for key in scenario_keys:
        scenario = _safe_int(key)
        if scenario is None:
            continue
        warmstart = _read_json(layout["cspp_scenario_warmstarts"] / f"scenario_{scenario}_warmstart.json", {}) or {}
        routes: list[dict[str, Any]] = []
        customer_charging: list[dict[str, Any]] = []
        warehouse_charging: list[dict[str, Any]] = []
        overnight_charging: list[dict[str, Any]] = []
        for tuple_key, value in (warmstart.get("r") or {}).items():
            parsed = _parse_tuple_key(tuple_key)
            if not parsed or len(parsed) != 4 or (_safe_float(value) or 0.0) <= 0.5:
                continue
            routes.append({"from": parsed[0], "to": parsed[1], "truck": parsed[2], "tour": parsed[3]})
        for tuple_key, value in (warmstart.get("p") or {}).items():
            parsed = _parse_tuple_key(tuple_key)
            if not parsed or len(parsed) != 3 or (_safe_float(value) or 0.0) <= 0.01:
                continue
            customer_charging.append({"customer": parsed[0], "truck": parsed[1], "tour": parsed[2], "energy_kwh": round(float(value), 2)})
        for tuple_key, value in (warmstart.get("p_wh") or {}).items():
            parsed = _parse_tuple_key(tuple_key)
            if not parsed or len(parsed) != 2 or (_safe_float(value) or 0.0) <= 0.01:
                continue
            warehouse_charging.append({"truck": parsed[0], "tour": parsed[1], "energy_kwh": round(float(value), 2)})
        for truck, value in (warmstart.get("p_overnight") or {}).items():
            if (_safe_float(value) or 0.0) <= 0.01:
                continue
            overnight_charging.append({"truck": _safe_int(truck), "energy_kwh": round(float(value), 2)})
        record = {
            "scenario_id": scenario,
            "total_cost": _safe_float(total_costs.get(key)),
            "status": (snapshot.get("scenario_statuses") or {}).get(key),
            "cluster_solves": len(rows_by_scenario.get(key, [])),
            "live_solver_count": len(states_by_scenario.get(key, [])),
        }
        scenario_rows.append(record)
        details[key] = {
            "updated_at": _now_iso(),
            "scenario": record,
            "cluster_runtime_rows": rows_by_scenario.get(key, []),
            "cluster_runtime_summary_rows": [],
            "live_solver_states": states_by_scenario.get(key, []),
            "recent_events": events_by_scenario.get(key, [])[-200:],
            "warmstart_available": (layout["cspp_scenario_warmstarts"] / f"scenario_{scenario}_warmstart.json").exists(),
            "route_bundle": _build_route_bundle(
                node_coords=node_coords,
                customer_lookup=customer_lookup,
                routes=routes,
                customer_charging=customer_charging,
                warehouse_charging=warehouse_charging,
                overnight_charging=overnight_charging,
                total_cost=record.get("total_cost"),
                scenario_id=scenario,
                status=record.get("status"),
            ),
        }

    overview = {
        "updated_at": _now_iso(),
        "status": _derive_stage_status(
            explicit_status=snapshot.get("status"),
            active_count=len((current_states.get("states") or {}).values()),
            current_phase="cluster_second_stage_solve" if (current_states.get("states") or {}) else None,
            total=snapshot.get("total_scenarios"),
            completed=snapshot.get("completed_scenarios"),
        ),
        "summary": {
            "elapsed_sec": snapshot.get("elapsed_sec"),
            "total_scenarios": snapshot.get("total_scenarios"),
            "completed_scenarios": snapshot.get("completed_scenarios"),
            "total_clusters": snapshot.get("total_clusters"),
            "total_cluster_solves": snapshot.get("total_cluster_solves"),
            "completed_cluster_solves": snapshot.get("completed_cluster_solves"),
        },
        "scenarios": scenario_rows,
        "live_solver_states": list((current_states.get("states") or {}).values()),
        "recent_events": solver_events[-200:],
    }
    return overview, details


def _build_stage3_reoptimization(layout: dict[str, Path], map_geojson: dict[str, Any]) -> tuple[dict[str, Any], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    current_state = _read_json(layout["cspp_reopt"] / "current_state.json", {}) or {}
    baseline_snapshot = next(
        (
            snapshot.get("totals")
            for snapshot in (current_state.get("iteration_totals_snapshots") or [])
            if str(snapshot.get("phase") or "").strip().lower() == "before"
        ),
        {},
    )
    best_iteration = _safe_int(current_state.get("best_iteration"))
    best_totals = next(
        (
            snapshot.get("totals")
            for snapshot in (current_state.get("iteration_totals_snapshots") or [])
            if _safe_int(snapshot.get("iteration")) == best_iteration and str(snapshot.get("phase") or "").strip().lower() in {"applied", "after"}
        ),
        current_state.get("current_totals") or {},
    )
    def _normalized_totals(raw: Any) -> dict[int, float]:
        if not isinstance(raw, dict):
            return {}
        result: dict[int, float] = {}
        for key, value in raw.items():
            scenario = _safe_int(key)
            cost = _safe_float(value)
            if scenario is None or cost is None:
                continue
            result[scenario] = cost
        return result
    baseline_totals = _normalized_totals(baseline_snapshot)
    best_iteration_totals = _normalized_totals(best_totals)
    event_log = _tail_jsonl(layout["cspp_reopt"] / "event_log.json", 300)
    iteration_rows = _read_table_rows(layout["cspp_reopt"] / "cluster_reopt_iterations.json")
    totals_rows = _read_table_rows(layout["cspp_reopt"] / "cluster_reopt_iteration_totals.json")
    mip_state_rows = _read_table_rows(layout["cspp_reopt"] / "cluster_reopt_mip_states.json")

    scope_live_dir = layout["cspp_reopt"] / "solver_live"
    scope_ids = _list_stage3_scope_ids(scope_live_dir)
    scopes: dict[str, dict[str, Any]] = {}
    for scope_id in scope_ids:
        scopes[scope_id] = {
            "updated_at": _now_iso(),
            "scope_id": scope_id,
            "live_state": _read_json(scope_live_dir / f"{scope_id}_live.json", {}) or {},
            "events": _tail_jsonl(scope_live_dir / f"{scope_id}_events.json", 200),
            "mip_state_rows": [row for row in mip_state_rows if (row.get("entity_id") or "") == scope_id or (row.get("scope") or "") == scope_id],
        }

    cluster_live_root = layout["cspp_reopt"] / "cluster_live"
    customer_features = map_geojson.get("features") or []
    by_cluster_feature: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for feature in customer_features:
        cluster_id = feature.get("properties", {}).get("cluster_id")
        if cluster_id is not None:
            by_cluster_feature[str(cluster_id)].append(feature)

    clusters: dict[str, dict[str, Any]] = {}
    if cluster_live_root.exists():
        for cluster_dir in sorted(cluster_live_root.glob("cluster_*")):
            cluster_id = cluster_dir.name.split("_")[-1]
            clusters[cluster_id] = {
                "updated_at": _now_iso(),
                "cluster_id": _safe_int(cluster_id),
                "live_state": _read_json(cluster_dir / "live_state.json", {}) or {},
                "best_first_stage": _read_json(cluster_dir / "best_first_stage_current.json", {}) or {},
                "events": _tail_jsonl(cluster_dir / "event_log.json", 200),
                "iteration_rows": [
                    row
                    for row in iteration_rows
                    if str(cluster_id) in json.dumps(row, ensure_ascii=True)
                ],
                "map": {
                    "type": "FeatureCollection",
                    "features": by_cluster_feature.get(cluster_id, []),
                },
            }

    overview = {
        "updated_at": _now_iso(),
        "status": _derive_stage_status(
            explicit_status=current_state.get("status"),
            active_count=len(scope_ids),
            current_phase=current_state.get("current_phase"),
            total=1 if current_state else None,
            completed=1 if current_state.get("stop_reason") else 0 if current_state else None,
        ),
        "current_state": current_state,
        "event_log": event_log,
        "iteration_rows": [
            {
                **row,
                "D_set": _parse_json_csv_column(row.get("D_set")),
                "candidate_fs_changed_cluster_ids": _parse_json_csv_column(row.get("candidate_fs_changed_cluster_ids")),
                "applied_fs_changed_cluster_ids": _parse_json_csv_column(row.get("applied_fs_changed_cluster_ids")),
                "accepted_clusters": _parse_json_csv_column(row.get("accepted_clusters")),
                "rejected_clusters": _parse_json_csv_column(row.get("rejected_clusters")),
            }
            for row in iteration_rows
        ],
        "iteration_totals": totals_rows,
        "mip_state_rows": mip_state_rows,
        "active_scope_ids": scope_ids,
        "dashboard": {
            "trend_series": [
                {
                    "iteration": 0,
                    "label": "Baseline",
                    "worst_cost": max(baseline_totals.values()) if baseline_totals else None,
                    "best_worst_cost": max(baseline_totals.values()) if baseline_totals else None,
                    "accepted": True,
                },
                *[
                    {
                        "iteration": _safe_int(row.get("iteration")),
                        "label": f"Iter {_safe_int(row.get('iteration'))}",
                        "worst_cost": _safe_float(row.get("worst_cost")),
                        "best_worst_cost": _safe_float(row.get("best_worst_cost")),
                        "accepted": _safe_text(row.get("iteration_accepted")).lower() == "true",
                    }
                    for row in iteration_rows
                ],
            ],
            "scenario_comparison": [
                {
                    "scenario_id": scenario,
                    "baseline_total": baseline_totals.get(scenario),
                    "best_total": best_iteration_totals.get(scenario),
                    "improvement": (
                        baseline_totals.get(scenario) - best_iteration_totals.get(scenario)
                        if baseline_totals.get(scenario) is not None and best_iteration_totals.get(scenario) is not None
                        else None
                    ),
                }
                for scenario in sorted({*baseline_totals.keys(), *best_iteration_totals.keys()})
            ],
            "charger_delta_bundle": _build_stage3_charger_delta_bundle(layout, map_geojson),
        },
    }
    return overview, clusters, scopes


def _build_recent_events(stage1: dict[str, Any], stage2: dict[str, Any], stage3: dict[str, Any], reoptimization: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for cluster in stage1.get("clusters", [])[:50]:
        if cluster.get("current_phase"):
            items.append({
                "source": "stage_1",
                "entity_id": cluster.get("cluster_id"),
                "kind": cluster.get("current_phase"),
                "status": cluster.get("status"),
            })
    for event in stage2.get("recent_events", [])[-80:]:
        items.append({"source": "stage_2", "entity_id": event.get("scenario"), "kind": event.get("event"), "status": event.get("event")})
    for scenario in stage3.get("scenarios", [])[:80]:
        if scenario.get("is_active"):
            items.append({"source": "stage_3", "entity_id": scenario.get("scenario_id"), "kind": "live", "status": scenario.get("status")})
    for event in reoptimization.get("event_log", [])[-80:]:
        items.append({"source": "stage_3", "entity_id": event.get("iteration"), "kind": event.get("event"), "status": event.get("phase")})
    return items[-200:]


def _build_alerts(stage1: dict[str, Any], stage2: dict[str, Any], stage3: dict[str, Any], reoptimization: dict[str, Any]) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    for cluster in stage1.get("clusters", []):
        if cluster.get("timeout"):
            alerts.append({"severity": "warning", "source": "stage_1", "message": f"Cluster {cluster['cluster_id']} timed out."})
        if cluster.get("error"):
            alerts.append({"severity": "error", "source": "stage_1", "message": f"Cluster {cluster['cluster_id']} error: {cluster['error']}"})
    for scenario in stage3.get("scenarios", []):
        if scenario.get("status") and str(scenario.get("status")).lower() not in {"optimal", "completed", "feasible"} and scenario.get("cost") is None:
            alerts.append({"severity": "warning", "source": "stage_3", "message": f"Scenario {scenario['scenario_id']} has status {scenario.get('status')}."})
    stop_reason = (stage3.get("current_state") or {}).get("stop_reason")
    if stop_reason:
        alerts.append({"severity": "info", "source": "stage_3", "message": f"Reoptimization stop reason: {stop_reason}"})
    return alerts


def _write_runs_index(export_root: Path) -> None:
    latest_run_id = _load_latest_run_id(export_root)
    runs: list[dict[str, Any]] = []
    runs_dir = export_root / "runs"
    if runs_dir.exists():
        for run_dir in sorted((path for path in runs_dir.iterdir() if path.is_dir()), key=lambda p: p.stat().st_mtime, reverse=True):
            summary = _read_json(run_dir / "summary.json", {}) or {}
            manifest = _read_json(run_dir / "manifest.json", {}) or {}
            run_section = summary.get("run") or {}
            stage_status = summary.get("stage_status") or {}
            timestamp = None
            try:
                timestamp = datetime.fromtimestamp(run_dir.stat().st_mtime, tz=timezone.utc).isoformat()
            except Exception:
                pass
            runs.append(
                {
                    "run_id": run_dir.name,
                    "label": run_dir.name,
                    "latest": run_dir.name == latest_run_id,
                    "run_last_modified_at": timestamp,
                    "last_stage_recorded": run_section.get("last_stage_recorded") or manifest.get("last_stage"),
                    "clustering_method": run_section.get("clustering_method") or manifest.get("clustering_method"),
                    "max_distance_from_warehouse_km": run_section.get("max_distance_from_warehouse_km") or manifest.get("max_distance_from_warehouse_km"),
                    "stage_status": stage_status,
                    "frontend_manifest_available": (run_dir / "frontend" / "manifest.json").exists(),
                }
            )
    payload = {
        "schema_version": FRONTEND_SCHEMA_VERSION,
        "updated_at": _now_iso(),
        "latest_run_id": latest_run_id,
        "runs": runs,
    }
    _write_json(export_root / "state" / "web_app_runs_index.json", payload)


def _read_instance_payload_dir(instance_dir: Path) -> dict[str, Any]:
    payload = _read_json(instance_dir / "prep" / "instance" / "payload.json", {}) or {}
    return payload if isinstance(payload, dict) else {}


def _build_instance_map_bundle(instance_dir: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    prep_geojson = _read_json(instance_dir / "prep" / "map" / "customers.json", None)
    prep_summary = _read_json(instance_dir / "prep" / "map" / "customers_summary.json", None)
    if prep_geojson and prep_summary:
        return prep_geojson, prep_summary

    payload = _read_instance_payload_dir(instance_dir)
    customers = payload.get("customers") if isinstance(payload.get("customers"), list) else []
    demand_rows = payload.get("demand_rows") if isinstance(payload.get("demand_rows"), list) else []
    warehouse = payload.get("warehouse") if isinstance(payload.get("warehouse"), dict) else None

    features: list[dict[str, Any]] = []
    demand_totals_by_client: dict[int, float] = defaultdict(float)
    demand_max_by_client: dict[int, float] = defaultdict(float)
    demand_dates: set[str] = set()
    for row in demand_rows:
        if not isinstance(row, dict):
            continue
        client_num = _safe_int(row.get("client_num"))
        demand_kg = _safe_float(row.get("demand_kg")) or 0.0
        delivery_date = _safe_text(row.get("delivery_date"))
        if client_num is None:
            continue
        demand_totals_by_client[client_num] += demand_kg
        demand_max_by_client[client_num] = max(demand_max_by_client.get(client_num, 0.0), demand_kg)
        if delivery_date:
            demand_dates.add(delivery_date)

    latitudes: list[float] = []
    longitudes: list[float] = []
    cluster_counts: dict[str, int] = defaultdict(int)
    for customer in customers:
        if not isinstance(customer, dict):
            continue
        client_num = _safe_int(customer.get("client_num"))
        latitude = _safe_float(customer.get("latitude"))
        longitude = _safe_float(customer.get("longitude"))
        if client_num is None or latitude is None or longitude is None:
            continue
        cluster_id = _safe_int(customer.get("cluster_id"))
        if cluster_id is not None:
            cluster_counts[str(cluster_id)] += 1
        latitudes.append(latitude)
        longitudes.append(longitude)
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [longitude, latitude]},
                "properties": {
                    "client_num": client_num,
                    "customer_id": customer.get("customer_id"),
                    "customer_name": customer.get("customer_name"),
                    "address": customer.get("street"),
                    "postal_code": customer.get("postal_code"),
                    "city": customer.get("city"),
                    "total_demand": demand_totals_by_client.get(client_num),
                    "max_demand": demand_max_by_client.get(client_num),
                    "nonzero_days": None,
                    "latest_demand": customer.get("latest_demand_kg"),
                    "cluster_id": cluster_id,
                },
            }
        )

    if isinstance(warehouse, dict):
        warehouse_lat = _safe_float(warehouse.get("latitude"))
        warehouse_lon = _safe_float(warehouse.get("longitude"))
        if warehouse_lat is not None and warehouse_lon is not None:
            latitudes.append(warehouse_lat)
            longitudes.append(warehouse_lon)

    summary = {
        "updated_at": _now_iso(),
        "customer_count": len(features),
        "warehouse": warehouse if isinstance(warehouse, dict) else None,
        "bounds": {
            "min_latitude": min(latitudes) if latitudes else None,
            "max_latitude": max(latitudes) if latitudes else None,
            "min_longitude": min(longitudes) if longitudes else None,
            "max_longitude": max(longitudes) if longitudes else None,
        },
        "available_demand_dates": sorted(demand_dates),
        "cluster_counts": dict(cluster_counts),
        "demand_min": min(demand_totals_by_client.values()) if demand_totals_by_client else None,
        "demand_max": max(demand_totals_by_client.values()) if demand_totals_by_client else None,
    }
    return {"type": "FeatureCollection", "features": features}, summary


def export_instance_frontend_contract(instance_dir: str | Path) -> None:
    instance_root = Path(instance_dir).expanduser().resolve()
    payload = _read_instance_payload_dir(instance_root)
    manifest = _read_json(instance_root / "manifest.json", {}) or {}
    frontend_root = instance_root / "frontend"
    map_geojson, map_summary = _build_instance_map_bundle(instance_root)
    clustering_assignments = _read_json(instance_root / "prep" / "clustering" / "assignments.json", {}) or {}
    assignments = clustering_assignments.get("assignments") if isinstance(clustering_assignments.get("assignments"), list) else []
    cluster_ids = sorted(
        {
            int(value)
            for value in (
                _safe_int((row or {}).get("cluster_id")) for row in assignments if isinstance(row, dict)
            )
            if value is not None
        }
    )
    overview = {
        "schema_version": FRONTEND_SCHEMA_VERSION,
        "updated_at": _now_iso(),
        "instance": manifest,
        "map_summary": map_summary,
        "instance_setup": {
            "customer_count": len(payload.get("customers") or []),
            "demand_row_count": len(payload.get("demand_rows") or []),
            "cluster_count": len(cluster_ids),
            "clustering_method": manifest.get("clustering_method") or payload.get("clustering_method"),
            "max_distance_from_warehouse_km": manifest.get("max_distance_from_warehouse_km"),
        },
        "recent_events": [],
        "alerts": [],
    }
    frontend_manifest = {
        "schema_version": FRONTEND_SCHEMA_VERSION,
        "instance_id": manifest.get("instance_id") or instance_root.name,
        "generated_at": _now_iso(),
        "available_routes": {"map": True},
        "counts": {"map_customers": len(map_geojson.get("features") or []), "clusters": len(cluster_ids)},
        "files": {
            "overview": "frontend/overview.json",
            "map_geojson": "frontend/map/customers.json",
            "map_summary": "frontend/map/customers_summary.json",
        },
    }
    _write_json(frontend_root / "overview.json", overview)
    _write_json(frontend_root / "manifest.json", frontend_manifest)
    _write_json(frontend_root / "map" / "customers.json", map_geojson)
    _write_json(frontend_root / "map" / "customers.geojson", map_geojson)
    _write_json(frontend_root / "map" / "customers_summary.json", map_summary)


def write_web_app_catalog(export_root: str | Path) -> None:
    export_root = Path(export_root).expanduser().resolve()
    instances_root = export_root / "instances"
    runs_root = export_root / "runs"
    instance_rows: list[dict[str, Any]] = []
    run_rows: list[dict[str, Any]] = []

    if runs_root.exists():
        for run_dir in sorted((path for path in runs_root.iterdir() if path.is_dir()), key=lambda path: path.stat().st_mtime, reverse=True):
            run_manifest = _read_json(run_dir / "manifest.json", {}) or {}
            pipeline_job = _read_json(run_dir / "state" / "pipeline_job.json", {}) or {}
            run_summary = _read_json(run_dir / "summary.json", {}) or {}
            run_section = run_summary.get("run") if isinstance(run_summary.get("run"), dict) else {}
            if not isinstance(run_section, dict):
                run_section = {}
            instance_id = str(run_manifest.get("instance_id") or run_section.get("instance_id") or "").strip()
            if not instance_id:
                continue
            run_rows.append(
                {
                    "run_id": run_dir.name,
                    "instance_id": instance_id,
                    "label": run_dir.name,
                    "created_at": run_manifest.get("created_at"),
                    "run_last_modified_at": datetime.fromtimestamp(run_dir.stat().st_mtime, tz=timezone.utc).isoformat(),
                    "latest": False,
                    "status": pipeline_job.get("status") or "idle",
                    "runtime_id": pipeline_job.get("runtime_id") or run_manifest.get("runtime_id"),
                    "runtime_label": pipeline_job.get("runtime_label") or run_manifest.get("runtime_label"),
                    "runtime_kind": pipeline_job.get("runtime_kind") or run_manifest.get("runtime_kind"),
                    "started_at": pipeline_job.get("started_at"),
                    "finished_at": pipeline_job.get("finished_at"),
                    "last_stage_recorded": run_section.get("last_stage_recorded"),
                    "clustering_method": run_section.get("clustering_method") or run_manifest.get("clustering_method"),
                    "max_distance_from_warehouse_km": run_section.get("max_distance_from_warehouse_km") or run_manifest.get("max_distance_from_warehouse_km"),
                    "stage_status": run_summary.get("stage_status") or {},
                    "frontend_manifest_available": (run_dir / "frontend" / "manifest.json").exists(),
                }
            )

    runs_by_instance: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in run_rows:
        runs_by_instance[str(row["instance_id"])].append(row)

    latest_instance_id: str | None = None
    latest_run_id: str | None = run_rows[0]["run_id"] if run_rows else None
    if instances_root.exists():
        for instance_dir in sorted((path for path in instances_root.iterdir() if path.is_dir()), key=lambda path: path.stat().st_mtime, reverse=True):
            manifest = _read_json(instance_dir / "manifest.json", {}) or {}
            payload = _read_instance_payload_dir(instance_dir)
            instance_id = str(manifest.get("instance_id") or instance_dir.name).strip() or instance_dir.name
            instance_runs = sorted(runs_by_instance.get(instance_id, []), key=lambda item: str(item.get("run_last_modified_at") or ""), reverse=True)
            if latest_instance_id is None:
                latest_instance_id = instance_id
            if instance_runs:
                instance_runs[0]["latest"] = True
            instance_rows.append(
                {
                    "instance_id": instance_id,
                    "label": manifest.get("label") or instance_id,
                    "created_at": manifest.get("created_at"),
                    "updated_at": manifest.get("updated_at") or datetime.fromtimestamp(instance_dir.stat().st_mtime, tz=timezone.utc).isoformat(),
                    "latest": instance_id == latest_instance_id,
                    "source_instance_id": manifest.get("source_instance_id"),
                    "clustering_method": manifest.get("clustering_method") or payload.get("clustering_method"),
                    "max_distance_from_warehouse_km": manifest.get("max_distance_from_warehouse_km") or payload.get("max_distance_from_warehouse_km"),
                    "customer_count": manifest.get("customer_count") or len(payload.get("customers") or []),
                    "demand_row_count": manifest.get("demand_row_count") or len(payload.get("demand_rows") or []),
                    "latest_run_id": instance_runs[0]["run_id"] if instance_runs else None,
                    "run_count": len(instance_runs),
                    "runs": instance_runs,
                }
            )

    catalog = {
        "schema_version": FRONTEND_SCHEMA_VERSION,
        "updated_at": _now_iso(),
        "latest_instance_id": latest_instance_id,
        "latest_run_id": latest_run_id,
        "instances": instance_rows,
    }
    _write_json(export_root / "state" / "web_app_catalog.json", catalog)
    _write_json(
        export_root / "state" / "web_app_runs_index.json",
        {
            "schema_version": FRONTEND_SCHEMA_VERSION,
            "updated_at": catalog["updated_at"],
            "latest_run_id": latest_run_id,
            "runs": run_rows,
        },
    )


def export_frontend_contract(run_dir: str | Path, *, summary: dict[str, Any] | None = None) -> None:
    run_root = resolve_run_root(run_dir)
    layout = get_run_layout(run_root)
    frontend_root = run_root / "frontend"
    export_root = _resolve_export_root(run_root)

    if summary is None:
        summary = _read_json(run_root / "summary.json", {}) or {}
        if not summary:
            try:
                from run_summary import build_run_summary

                summary = build_run_summary(run_root)
            except Exception:
                summary = {}

    map_geojson, map_summary = _build_map_bundle(layout)
    stage1, stage1_details = _build_stage1(layout, map_geojson)
    stage2, stage2_details = _build_stage2(layout)
    stage3, stage3_cluster_details, stage3_scope_details = _build_stage3_reoptimization(layout, map_geojson)
    pipeline_progress = build_pipeline_progress(run_root, summary=summary)
    recent_events = _build_recent_events(stage1, stage2, {}, stage3)
    alerts = _build_alerts(stage1, stage2, {}, stage3)

    overview = {
        "schema_version": FRONTEND_SCHEMA_VERSION,
        "updated_at": _now_iso(),
        "run": summary.get("run") or {},
        "stage_status": summary.get("stage_status") or {},
        "summary": summary,
        "map_summary": map_summary,
        "stage_cards": {
            "stage_1": stage1.get("summary"),
            "stage_2": stage2.get("summary"),
            "stage_3": {
                "current_phase": (stage3.get("current_state") or {}).get("current_phase"),
                "current_iteration": (stage3.get("current_state") or {}).get("current_iteration"),
                "best_iteration": (stage3.get("current_state") or {}).get("best_iteration"),
                "best_worst_cost": (stage3.get("current_state") or {}).get("best_worst_cost"),
            },
        },
        "pipeline": pipeline_progress,
        "recent_events": pipeline_progress.get("recent_events") or recent_events[-50:],
        "alerts": pipeline_progress.get("alerts") or alerts,
    }

    manifest = {
        "schema_version": FRONTEND_SCHEMA_VERSION,
        "run_id": run_root.name,
        "generated_at": _now_iso(),
        "latest_stage": (summary.get("run") or {}).get("last_stage_recorded"),
        "available_routes": {
            "map": True,
            "stage_1_clusters": sorted(stage1_details.keys(), key=int) if stage1_details else [],
            "stage_2_scenarios": sorted(stage2_details.keys(), key=int) if stage2_details else [],
            "stage_3_clusters": sorted(stage3_cluster_details.keys(), key=int) if stage3_cluster_details else [],
            "stage_3_scopes": sorted(stage3_scope_details.keys()),
        },
        "counts": {
            "map_customers": len(map_geojson.get("features") or []),
            "stage_1_clusters": len(stage1_details),
            "stage_2_scenarios": len(stage2_details),
            "stage_3_clusters": len(stage3_cluster_details),
            "stage_3_scopes": len(stage3_scope_details),
        },
        "files": {
            "overview": _relative_export_path(frontend_root / "overview.json", run_root),
            "map_geojson": _relative_export_path(frontend_root / "map" / "customers.json", run_root),
            "map_summary": _relative_export_path(frontend_root / "map" / "customers_summary.json", run_root),
            "pipeline_progress": _relative_export_path(pipeline_progress_path(run_root), run_root),
        },
    }

    _write_json(frontend_root / "manifest.json", manifest)
    _write_json(frontend_root / "overview.json", overview)
    _write_json(frontend_root / "map" / "customers.json", map_geojson)
    _write_json(frontend_root / "map" / "customers.geojson", map_geojson)
    _write_json(frontend_root / "map" / "customers_summary.json", map_summary)
    _write_json(frontend_root / "stage_1" / "clusters.json", stage1)
    _write_json(frontend_root / "stage_2" / "scenarios.json", stage2)
    _write_json(frontend_root / "stage_3" / "overview.json", stage3)
    _write_json(pipeline_progress_path(run_root), pipeline_progress)
    _write_json(
        frontend_root / "activity" / "recent_events.json",
        {"updated_at": _now_iso(), "events": pipeline_progress.get("recent_events") or recent_events},
    )
    _write_json(
        frontend_root / "activity" / "alerts.json",
        {"updated_at": _now_iso(), "alerts": pipeline_progress.get("alerts") or alerts},
    )

    for cluster_id, payload in stage1_details.items():
        _write_json(frontend_root / "stage_1" / "clusters" / f"{cluster_id}.json", payload)
    for scenario_id, payload in stage2_details.items():
        _write_json(frontend_root / "stage_2" / "scenarios" / f"{scenario_id}.json", payload)
    for cluster_id, payload in stage3_cluster_details.items():
        _write_json(frontend_root / "stage_3" / "clusters" / f"{cluster_id}.json", payload)
    for scope_id, payload in stage3_scope_details.items():
        _write_json(frontend_root / "stage_3" / "scopes" / f"{scope_id}.json", payload)

    write_web_app_catalog(export_root)
