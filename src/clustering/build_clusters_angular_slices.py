#!/usr/bin/env python3
"""
Generate restricted arc sets for CSPP using demand-aware angular slices clustering.

Method:
1. Compute customer polar angles around the warehouse.
2. Rotate at the largest angular gap (sector cut in sparse direction).
3. Partition the rotated angular order into contiguous slices with a
   demand-balancing dynamic program under min/max cluster-size bounds.
4. Export cluster assignments and arc sets.
"""

from __future__ import annotations

import math
import os
from datetime import datetime
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from arc_set_builder import build_cluster_arc_set
from arc_set_builder import build_global_arc_set
from config import choose_cluster_count
from config import cluster_generation_params
from json_artifacts import read_table_rows
from json_artifacts import write_json
from json_artifacts import write_table
from lieferdaten.runtime import get_run_layout
from prep_exports import export_prepared_clustering

matplotlib.use("Agg")

TARGET_CUSTOMERS_PER_CLUSTER, MIN_CLUSTER_SIZE, MAX_CLUSTER_SIZE = cluster_generation_params()

SCRIPT_DIR = Path(__file__).resolve().parent

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
for directory in (CLUSTER_DATA_DIR, REPORTS_DIR, FIGURES_DIR):
    directory.mkdir(parents=True, exist_ok=True)


def resolve_cspp_data_file(filename: str) -> Path:
    primary = CSPP_DATA_DIR / filename
    if primary.exists():
        return primary
    raise FileNotFoundError(f"Required CSPP data file not found: {primary}")


def _compute_angles(customers_df: pd.DataFrame, warehouse_row: pd.Series) -> pd.DataFrame:
    rel_lon = customers_df["longitude"].to_numpy(dtype=float) - float(warehouse_row["longitude"])
    rel_lat = customers_df["latitude"].to_numpy(dtype=float) - float(warehouse_row["latitude"])
    angles = np.mod(np.arctan2(rel_lat, rel_lon), 2.0 * np.pi)
    result = customers_df.copy()
    result["angle"] = angles
    return result


def _largest_gap_rotation(sorted_angles: np.ndarray) -> int:
    if len(sorted_angles) <= 1:
        return 0
    shifted = np.roll(sorted_angles, -1)
    gaps = shifted - sorted_angles
    gaps[-1] = (sorted_angles[0] + 2.0 * np.pi) - sorted_angles[-1]
    largest_gap_index = int(np.argmax(gaps))
    return (largest_gap_index + 1) % len(sorted_angles)


def _env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() not in {"0", "false", "no", "off"}


def _partition_demand_balanced(
    ordered_ids: list[int],
    demand_by_id: dict[int, float],
    n_clusters: int,
    min_size: int,
    max_size: int,
) -> list[list[int]]:
    n = len(ordered_ids)
    if n == 0:
        return []

    min_size = max(1, min_size)
    max_size = max(min_size, max_size)
    if n < n_clusters * min_size:
        min_size = max(1, n // max(n_clusters, 1))
    if n_clusters * max_size < n:
        max_size = max(max_size, math.ceil(n / max(n_clusters, 1)))

    demands = [float(demand_by_id.get(cid, 0.0)) for cid in ordered_ids]
    prefix = [0.0]
    for d in demands:
        prefix.append(prefix[-1] + d)

    total_demand = prefix[-1]
    target = total_demand / float(n_clusters) if n_clusters > 0 else total_demand

    inf = float("inf")
    dp = [[inf] * (n + 1) for _ in range(n_clusters + 1)]
    prev = [[-1] * (n + 1) for _ in range(n_clusters + 1)]
    dp[0][0] = 0.0

    for k in range(1, n_clusters + 1):
        for i in range(1, n + 1):
            lo = max((k - 1) * min_size, i - max_size)
            hi = min(i - min_size, (k - 1) * max_size)
            if lo > hi:
                continue
            best_cost = inf
            best_j = -1
            for j in range(lo, hi + 1):
                if dp[k - 1][j] == inf:
                    continue
                seg_demand = prefix[i] - prefix[j]
                seg_cost = (seg_demand - target) ** 2
                cost = dp[k - 1][j] + seg_cost
                if cost < best_cost:
                    best_cost = cost
                    best_j = j
            dp[k][i] = best_cost
            prev[k][i] = best_j

    if prev[n_clusters][n] == -1:
        base = n // n_clusters
        rem = n % n_clusters
        groups: list[list[int]] = []
        start = 0
        for k in range(n_clusters):
            size = base + (1 if k < rem else 0)
            end = start + size
            groups.append(ordered_ids[start:end])
            start = end
        return groups

    cuts: list[tuple[int, int]] = []
    i = n
    for k in range(n_clusters, 0, -1):
        j = prev[k][i]
        if j < 0:
            break
        cuts.append((j, i))
        i = j
    cuts.reverse()

    return [ordered_ids[start:end] for start, end in cuts]


def _partition_equal_size(
    ordered_ids: list[int],
    n_clusters: int,
) -> list[list[int]]:
    n = len(ordered_ids)
    if n == 0:
        return []
    if n_clusters <= 0:
        return [ordered_ids]

    base = n // n_clusters
    rem = n % n_clusters
    groups: list[list[int]] = []
    start = 0
    for cluster_index in range(n_clusters):
        size = base + (1 if cluster_index < rem else 0)
        end = start + size
        if end > start:
            groups.append(ordered_ids[start:end])
        start = end
    return groups


def _plot_clusters(
    customers_df: pd.DataFrame,
    warehouse_row: pd.Series,
    assignments: dict[int, int],
    title: str,
    output_path: Path,
) -> None:
    fig, ax = plt.subplots(1, 1, figsize=(10.5, 8.0))
    unique_ids = sorted(set(assignments.values()))
    cmap = plt.cm.get_cmap("tab20", max(len(unique_ids), 1))
    color_map = {cluster_id: cmap(idx) for idx, cluster_id in enumerate(unique_ids)}

    for cluster_id in unique_ids:
        members = [cid for cid, assigned in assignments.items() if assigned == cluster_id]
        coords = customers_df[customers_df["node_index"].isin(members)]
        ax.scatter(
            coords["longitude"],
            coords["latitude"],
            s=55,
            c=[color_map[cluster_id]],
            edgecolors="black",
            linewidth=0.5,
            alpha=0.9,
            label=f"Cluster {cluster_id} ({len(members)})",
        )

    ax.scatter(
        [warehouse_row["longitude"]],
        [warehouse_row["latitude"]],
        s=220,
        c="#1D4ED8",
        marker="s",
        edgecolors="black",
        linewidth=1.2,
        label="Warehouse",
        zorder=6,
    )
    ax.set_title(title, fontsize=13, fontweight="bold")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.grid(True, alpha=0.25, linestyle="--")
    ax.legend(loc="upper left", bbox_to_anchor=(1.02, 1.0), fontsize=8, borderaxespad=0)
    fig.tight_layout(rect=[0, 0, 0.82, 1])
    fig.savefig(output_path, dpi=220, bbox_inches="tight")
    plt.close(fig)


DEMAND_AWARE = _env_flag("ANGULAR_SLICES_DEMAND_AWARE", True)
MODE_LABEL = "Demand-Aware" if DEMAND_AWARE else "Non-Demand-Aware"
MODE_TOKEN = "angular_slices_demand_aware" if DEMAND_AWARE else "angular_slices_non_demand_aware"

print("=" * 80)
print(f"ARC SET GENERATOR (Angular Slices, {MODE_LABEL})")
print("=" * 80)

coords_df = pd.DataFrame(read_table_rows(resolve_cspp_data_file("coordinates.json")))
warehouse = coords_df[coords_df["node_index"] == 0].iloc[0]
customers_df = coords_df[coords_df["node_index"] > 0].copy()

demand_df = pd.DataFrame(read_table_rows(resolve_cspp_data_file("demand_matrix.json"))).set_index("delivery_date")
demand_df.columns = demand_df.columns.astype(int)
max_demand = {int(cid): float(v) for cid, v in demand_df.max().to_dict().items()}

customer_ids = [int(cid) for cid in customers_df["node_index"].tolist()]
n_customers = len(customer_ids)
n_clusters = choose_cluster_count(
    n_customers=n_customers,
    target_customers_per_cluster=TARGET_CUSTOMERS_PER_CLUSTER,
    max_cluster_size=MAX_CLUSTER_SIZE,
)

print(f"Customers: {n_customers}")
print(f"Demand matrix shape: {demand_df.shape}")
print(f"Target clusters: {n_clusters} (target={TARGET_CUSTOMERS_PER_CLUSTER}, min={MIN_CLUSTER_SIZE}, max={MAX_CLUSTER_SIZE})")

angular_df = _compute_angles(customers_df, warehouse).sort_values("angle").reset_index(drop=True)
angles = angular_df["angle"].to_numpy()
rotation_start = _largest_gap_rotation(angles)
if rotation_start > 0:
    angular_df = pd.concat([angular_df.iloc[rotation_start:], angular_df.iloc[:rotation_start]], ignore_index=True)

ordered_ids = [int(cid) for cid in angular_df["node_index"].tolist()]
if DEMAND_AWARE:
    groups = _partition_demand_balanced(
        ordered_ids=ordered_ids,
        demand_by_id=max_demand,
        n_clusters=n_clusters,
        min_size=MIN_CLUSTER_SIZE,
        max_size=MAX_CLUSTER_SIZE,
    )
else:
    groups = _partition_equal_size(
        ordered_ids=ordered_ids,
        n_clusters=n_clusters,
    )

final_clusters: dict[int, int] = {}
for cluster_id, members in enumerate(groups):
    for cid in members:
        final_clusters[int(cid)] = int(cluster_id)

for cid in customer_ids:
    final_clusters.setdefault(int(cid), 0)

cluster_members: dict[int, list[int]] = {}
for cid, cluster_id in final_clusters.items():
    cluster_members.setdefault(cluster_id, []).append(cid)

for cluster_id in sorted(cluster_members):
    members = cluster_members[cluster_id]
    total_demand = sum(max_demand.get(cid, 0.0) for cid in members)
    print(f"  Cluster {cluster_id}: {len(members)} stores, total max demand {total_demand:.0f} kg")

arcs, _, arc_stats = build_cluster_arc_set(customer_ids=customer_ids, final_clusters=final_clusters, depot=0)
global_arcs = build_global_arc_set(customer_ids=customer_ids, depot=0)

arc_df = pd.DataFrame(sorted(arcs), columns=["from_node", "to_node"])
write_table(CLUSTER_DATA_DIR / "arc_set.json", arc_df.columns.tolist(), arc_df.to_dict(orient="records"))

arc_global_df = pd.DataFrame(sorted(global_arcs), columns=["from_node", "to_node"])
write_table(CLUSTER_DATA_DIR / "arc_set_global.json", arc_global_df.columns.tolist(), arc_global_df.to_dict(orient="records"))

cluster_df = pd.DataFrame(
    [{"customer_id": int(cid), "cluster": int(cluster_id)} for cid, cluster_id in sorted(final_clusters.items())]
)
write_table(CLUSTER_DATA_DIR / "cluster_assignments.json", cluster_df.columns.tolist(), cluster_df.to_dict(orient="records"))

cluster_demands = [sum(max_demand.get(cid, 0.0) for cid in members) for _, members in sorted(cluster_members.items())]
demand_mean = float(np.mean(cluster_demands)) if cluster_demands else 0.0
demand_std = float(np.std(cluster_demands)) if cluster_demands else 0.0

write_json(
    REPORTS_DIR / "arc_set_report.json",
    {
        "generated_at": datetime.now().isoformat(),
        "mode": MODE_TOKEN,
        "cluster_count": int(len(cluster_members)),
        "customer_count": int(n_customers),
        "arc_count": int(len(arcs)),
        "global_arc_count": int(len(global_arcs)),
        "target_customers_per_cluster": int(TARGET_CUSTOMERS_PER_CLUSTER),
        "cluster_size_min_allowed": int(MIN_CLUSTER_SIZE),
        "cluster_size_max_allowed": int(MAX_CLUSTER_SIZE),
        "demand_balance": {
            "cluster_demands_kg": cluster_demands,
            "mean_kg": demand_mean,
            "std_kg": demand_std,
            "cv": (demand_std / demand_mean) if demand_mean > 1e-9 else None,
        },
        "rotation_start_index": int(rotation_start),
        "arc_stats": arc_stats,
    },
)

_plot_clusters(
    customers_df=customers_df,
    warehouse_row=warehouse,
    assignments=final_clusters,
    title=f"Angular Slices ({MODE_LABEL}), {len(cluster_members)} clusters",
    output_path=FIGURES_DIR / "arc_set_clusters.png",
)
_plot_clusters(
    customers_df=customers_df,
    warehouse_row=warehouse,
    assignments=final_clusters,
    title=f"Angular Slices Cluster Structure ({MODE_LABEL}), {len(cluster_members)} clusters",
    output_path=FIGURES_DIR / "cluster_structure_map.png",
)
_plot_clusters(
    customers_df=customers_df,
    warehouse_row=warehouse,
    assignments=final_clusters,
    title=f"Angular Slices Map ({MODE_LABEL}), {len(cluster_members)} clusters",
    output_path=FIGURES_DIR / "sub_cluster_map.png",
)

print(f"Saved cluster outputs under: {CLUSTER_DATA_DIR}")

try:
    export_prepared_clustering(RUN_DIR)
    print(f"Prepared clustering export updated for {RUN_DIR}")
except Exception as exc:
    print(f"Warning: failed to export prepared clustering artifacts: {exc}")
