#!/usr/bin/env python3
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from arc_set_builder import build_cluster_arc_set
from arc_set_builder import build_global_arc_set
from json_artifacts import read_json
from json_artifacts import read_table_rows
from json_artifacts import write_json
from json_artifacts import write_table
from lieferdaten.runtime import get_run_layout
from prep_exports import export_prepared_clustering


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[1]

import os

_env_run_dir = os.environ.get("RUN_DIR")
if _env_run_dir:
    RUN_DIR = Path(_env_run_dir)
else:
    raise FileNotFoundError("RUN_DIR env var not set. Run the pipeline with RUN_DIR or via src/run.py.")

RUN_LAYOUT = get_run_layout(RUN_DIR)
CSPP_DATA_DIR = RUN_LAYOUT["cspp_data"]
CLUSTER_DATA_DIR = RUN_LAYOUT["clustering_data"]
REPORTS_DIR = RUN_LAYOUT["clustering_reports"]
FIGURES_DIR = RUN_LAYOUT["clustering_figures"]
MANUAL_ASSIGNMENTS_FILE = RUN_LAYOUT["run"] / "prep" / "clustering" / "manual_assignments.json"
ASSIGNMENTS_JSON_FILE = RUN_LAYOUT["run"] / "prep" / "clustering" / "assignments.json"
INSTANCE_PAYLOAD_FILE = RUN_LAYOUT["run"] / "prep" / "instance" / "payload.json"
CUSTOMERS_JSON_FILE = RUN_LAYOUT["run"] / "prep" / "instance" / "customers.json"

for directory in (CLUSTER_DATA_DIR, REPORTS_DIR, FIGURES_DIR):
    directory.mkdir(parents=True, exist_ok=True)


def _load_json(path: Path) -> dict[str, object]:
    payload = read_json(path, {})
    return payload if isinstance(payload, dict) else {}


def _load_customers_df() -> pd.DataFrame:
    payload = _load_json(INSTANCE_PAYLOAD_FILE)
    customers = payload.get("customers")
    if isinstance(customers, list) and customers:
        return pd.DataFrame(customers)

    customers_json = _load_json(CUSTOMERS_JSON_FILE)
    customers = customers_json.get("customers")
    if isinstance(customers, list) and customers:
        return pd.DataFrame(customers)

    customers_json = RUN_LAYOUT["run"] / "prep" / "instance" / "customers.json"
    if customers_json.exists():
        return pd.DataFrame((read_json(customers_json, {}) or {}).get("customers") or [])
    raise FileNotFoundError(f"Instance customers not found for manual clustering: {INSTANCE_PAYLOAD_FILE}")


def _load_manual_assignments_df() -> tuple[pd.DataFrame, str]:
    assignments_payload = _load_json(ASSIGNMENTS_JSON_FILE)
    assignments = assignments_payload.get("assignments")
    if isinstance(assignments, list) and assignments:
        return pd.DataFrame(assignments), str(ASSIGNMENTS_JSON_FILE)

    payload = _load_json(INSTANCE_PAYLOAD_FILE)
    customers = payload.get("customers")
    if isinstance(customers, list) and customers:
        derived = [
            {
                "client_num": customer.get("client_num"),
                "customer_id": customer.get("customer_id"),
                "cluster_id": customer.get("cluster_id"),
            }
            for customer in customers
            if isinstance(customer, dict) and customer.get("cluster_id") is not None
        ]
        if derived and len(derived) == len(customers):
            return pd.DataFrame(derived), f"{INSTANCE_PAYLOAD_FILE}#customers"

    if MANUAL_ASSIGNMENTS_FILE.exists():
        return pd.DataFrame(read_table_rows(MANUAL_ASSIGNMENTS_FILE)), str(MANUAL_ASSIGNMENTS_FILE)
    raise FileNotFoundError(f"Manual assignments not found: {ASSIGNMENTS_JSON_FILE}")


def main() -> None:
    customers_df = _load_customers_df()
    manual_df, assignments_source = _load_manual_assignments_df()

    required = {"client_num", "cluster_id"}
    missing = required - set(manual_df.columns)
    if missing:
        raise ValueError(f"Manual clustering file missing columns: {', '.join(sorted(missing))}")

    customers_df["client_num"] = pd.to_numeric(customers_df["client_num"], errors="coerce").astype("Int64")
    manual_df["client_num"] = pd.to_numeric(manual_df["client_num"], errors="coerce").astype("Int64")
    manual_df["cluster_id"] = pd.to_numeric(manual_df["cluster_id"], errors="coerce").astype("Int64")

    customers_df = customers_df.drop(columns=[col for col in ("cluster_id",) if col in customers_df.columns])
    joined = customers_df.merge(
        manual_df[["client_num", "cluster_id"]],
        on="client_num",
        how="left",
    )
    if joined["cluster_id"].isna().any():
        missing_clients = joined.loc[joined["cluster_id"].isna(), "client_num"].dropna().astype(int).tolist()
        raise ValueError(f"Manual clustering does not cover all customers. Missing client_num values: {missing_clients[:20]}")

    final_clusters = {int(row.client_num): int(row.cluster_id) for row in joined.itertuples()}
    customer_ids = sorted(final_clusters.keys())

    arcs, _, arc_stats = build_cluster_arc_set(customer_ids=customer_ids, final_clusters=final_clusters, depot=0)
    global_arcs = build_global_arc_set(customer_ids=customer_ids, depot=0)

    arc_rows = pd.DataFrame(sorted(arcs), columns=["from_node", "to_node"])
    write_table(CLUSTER_DATA_DIR / "arc_set.json", arc_rows.columns.tolist(), arc_rows.to_dict(orient="records"))
    global_arc_rows = pd.DataFrame(sorted(global_arcs), columns=["from_node", "to_node"])
    write_table(CLUSTER_DATA_DIR / "arc_set_global.json", global_arc_rows.columns.tolist(), global_arc_rows.to_dict(orient="records"))
    assignments_df = pd.DataFrame(
        [
            {"customer_id": cid, "cluster": final_clusters[cid]}
            for cid in customer_ids
        ]
    )
    write_table(CLUSTER_DATA_DIR / "cluster_assignments.json", assignments_df.columns.tolist(), assignments_df.to_dict(orient="records"))

    report = {
        "generated_at": datetime.now().isoformat(),
        "mode": "manual",
        "cluster_count": len(set(final_clusters.values())),
        "customer_count": len(customer_ids),
        "arc_stats": arc_stats,
        "manual_assignments_file": assignments_source,
    }
    write_json(REPORTS_DIR / "arc_set_report.json", report)
    export_prepared_clustering(RUN_DIR)
    print(f"Manual clustering completed for {RUN_DIR}")


if __name__ == "__main__":
    main()
