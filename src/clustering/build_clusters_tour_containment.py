#!/usr/bin/env python3
"""
Generate restricted arc set for CSPP using Tour-Containment clustering.

This script builds clustering outputs and a restricted arc set while clustering customers to MAXIMIZE
the number of fully intact historical tours, rather than minimizing pairwise cuts.

Key difference from tour_adjacency:
- tour_adjacency: Minimizes adjacent tour cuts (edges between consecutive customers)
- tour_containment: Maximizes number of complete historical tours preserved

Mode:
- "tour_containment": Pure tour containment maximization

Output:
- arc_set_<mode>.json: Cluster-complete allowed arcs (from_node, to_node)
- arc_set_global.json: Complete global directed arc set
- cluster_assignments_<mode>.json: Customer-to-cluster mapping
- arc_set_<mode>_report.json: Clustering summary and statistics
- arc_set_clusters_<mode>.png: Visualization
"""

import pandas as pd
import numpy as np
from pathlib import Path
import gurobipy as gp
from gurobipy import GRB
from datetime import datetime
from config import (
    choose_cluster_count,
    cluster_generation_params,
    cluster_mip_params,
)
from arc_set_builder import build_cluster_arc_set
from arc_set_builder import build_global_arc_set
from json_artifacts import read_json
from json_artifacts import read_table_rows
from json_artifacts import write_json
from json_artifacts import write_table
from lieferdaten.runtime import get_run_layout
from prep_exports import export_prepared_clustering

# ============================================================================
# CONFIGURATION
# ============================================================================


# Clustering parameters
TARGET_CUSTOMERS_PER_CLUSTER, MIN_CLUSTER_SIZE, MAX_CLUSTER_SIZE = cluster_generation_params()

# Default tour weights (if not provided externally)
DEFAULT_TOUR_WEIGHT = 1.0

# MIP settings
MIP_TIME_LIMIT, MIP_GAP = cluster_mip_params(default_timelimit=60 * 60)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[1]

import os

GUROBI_THREADS = max(1, int(os.environ.get("GUROBI_THREADS", "16")))

_env_run_dir = os.environ.get("RUN_DIR")
if _env_run_dir:
    RUN_DIR = Path(_env_run_dir)
else:
    raise FileNotFoundError("RUN_DIR env var not set. Run the pipeline with RUN_DIR or via src/run.py.")

RUN_LAYOUT = get_run_layout(RUN_DIR)

CSPP_DATA_DIR = RUN_LAYOUT["cspp_data"]
LIEFERDATEN_DATA_DIR = RUN_LAYOUT["process_data"]
CLUSTER_DATA_DIR = RUN_LAYOUT["clustering_data"]
REPORTS_DIR = RUN_LAYOUT["clustering_reports"]
FIGURES_DIR = RUN_LAYOUT["clustering_figures"]
for d in (CLUSTER_DATA_DIR, REPORTS_DIR, FIGURES_DIR):
    d.mkdir(parents=True, exist_ok=True)


def resolve_cspp_data_file(filename: str) -> Path:
    primary = CSPP_DATA_DIR / filename
    if primary.exists():
        return primary
    raise FileNotFoundError(f"Required CSPP data file not found: {primary}")

print("=" * 80)
print("ARC SET GENERATOR (Tour-Containment Clustering)")
print("=" * 80)

# ============================================================================
# LOAD DATA
# ============================================================================

print("\n" + "=" * 80)
print("LOADING DATA")
print("=" * 80)

# Load coordinates
coords_file = resolve_cspp_data_file("coordinates.json")
coords_df = pd.DataFrame(read_table_rows(coords_file))
print(f"Loaded {len(coords_df)} node coordinates")

# Extract warehouse and customers
warehouse = coords_df[coords_df['node_index'] == 0].iloc[0]
customers_df = coords_df[coords_df['node_index'] > 0].copy()
print(f"Warehouse: ({warehouse['latitude']:.4f}, {warehouse['longitude']:.4f})")
print(f"Customers: {len(customers_df)}")

# Load customer ID mapping
mapping_file = resolve_cspp_data_file("customer_id_mapping.json")
if not mapping_file.exists():
    print(f"\nERROR: Customer ID mapping not found: {mapping_file}")
    print("Run generate_instance_data.py first to create the mapping.")
    exit(1)

mapping_df = pd.DataFrame(read_table_rows(mapping_file))
customer_id_to_client_num = dict(zip(mapping_df['customer_id'], mapping_df['client_num']))
client_num_to_customer_id = {v: k for k, v in customer_id_to_client_num.items()}
print(f"Loaded customer ID mapping: {len(customer_id_to_client_num)} entries")

# Load tour data
tours_file = LIEFERDATEN_DATA_DIR / "tour_stops_clean.json"
if not tours_file.exists():
    print(f"\nERROR: Tour data not found: {tours_file}")
    print("Run process_tour_data.py first to generate tour data.")
    exit(1)

tours_df = pd.DataFrame(read_table_rows(tours_file))
print(f"Loaded {len(tours_df)} tour stop records")
print(f"Unique tours: {tours_df['tour_name'].nunique()}")

# Load demand data (for reports)
demand_file = resolve_cspp_data_file("demand_matrix.json")
demand_rows = read_table_rows(demand_file)
demand_df = pd.DataFrame(demand_rows)
index_column = "delivery_date" if "delivery_date" in demand_df.columns else "scenario"
demand_df = demand_df.set_index(index_column)
demand_df.columns = demand_df.columns.astype(int)

# Calculate max demand per customer across all scenarios
max_demand = demand_df.max().to_dict()

customer_ids = customers_df['node_index'].tolist()  # These are client_nums (1-based)
n_customers = len(customer_ids)
customer_set = set(customer_ids)

# ============================================================================
# BUILD HISTORICAL TOURS LIST
# ============================================================================

print("\n" + "=" * 80)
print("BUILDING HISTORICAL TOURS")
print("=" * 80)

# Group tours by tour_name to get customers per tour
tour_groups = tours_df.groupby('tour_name')['customer_id'].apply(list).to_dict()
print(f"Processing {len(tour_groups)} historical tours...")

# Build list of historical tours with mapped client_nums
# Each tour is: (tour_index, original_tour_name, [list of client_nums])
historical_tours = []
tour_weights = {}
raw_tours_with_customers = 0

for tour_name, customers in tour_groups.items():
    # Map original customer_ids to client_nums
    mapped = [customer_id_to_client_num.get(c) for c in customers]
    mapped = [c for c in mapped if c is not None and c in customer_set]  # Filter unmapped
    if mapped:
        mapped = list(dict.fromkeys(mapped))  # Remove duplicates, preserve order
    
    if len(mapped) >= 2:  # Only consider tours with 2+ customers
        raw_tours_with_customers += 1
        tour_idx = len(historical_tours)
        historical_tours.append((tour_idx, tour_name, mapped))
        tour_weights[tour_idx] = DEFAULT_TOUR_WEIGHT  # Default weight

# Aggregate identical tours (same customer set) to reduce model size
aggregated = {}
for _, tour_name, customers in historical_tours:
    key = tuple(sorted(customers))
    if key not in aggregated:
        aggregated[key] = {
            "customers": list(key),
            "count": 1,
            "example": tour_name
        }
    else:
        aggregated[key]["count"] += 1

historical_tours = []
tour_weights = {}
for idx, data in enumerate(aggregated.values()):
    historical_tours.append((idx, data["example"], data["customers"]))
    tour_weights[idx] = data["count"] * DEFAULT_TOUR_WEIGHT

print(f"Historical tours with 2+ mapped customers: {raw_tours_with_customers}")
print(f"Unique tours after aggregation: {len(historical_tours)}")

# Statistics on tour sizes
tour_sizes = [len(t[2]) for t in historical_tours]
print(f"Tour size range: {min(tour_sizes)} - {max(tour_sizes)}")
print(f"Average tour size: {np.mean(tour_sizes):.1f}")

# Show distribution
size_dist = {}
for size in tour_sizes:
    size_dist[size] = size_dist.get(size, 0) + 1
print(f"\nTour size distribution:")
for size in sorted(size_dist.keys()):
    print(f"  Size {size}: {size_dist[size]} tours")


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def solve_tour_containment_mip(customer_ids, n_clusters, historical_tours, tour_weights):
    """
    Solve Tour-Containment clustering MIP.

    MAXIMIZES the number of fully intact historical tours (weighted sum).

    Decision Variables:
    - x[j, c]: Binary, customer j assigned to cluster c
    - v[t, c]: Binary, tour t is fully contained in cluster c

    Constraints:
    - Each customer assigned to exactly one cluster
    - Cluster size bounds (min/max)
    - Linking: v[t, c] <= x[j, c] for all j in tour t
              (tour can only be "in" a cluster if ALL its customers are)

    Objective:
    - Maximize sum(tour_weights[t] * v[t, c] for t, c)
    Args:
        customer_ids: List of client_nums
        n_clusters: Number of clusters (K)
        historical_tours: List of (tour_idx, tour_name, [customer_list])
        tour_weights: Dict of tour_idx -> weight

    Returns:
        Dict of client_num -> cluster assignment
    """
    print(f"\n  Building Tour-Containment MIP model (threads: {GUROBI_THREADS})...")
    
    model = gp.Model("tour_containment_clustering")
    model.Params.OutputFlag = 1
    model.Params.Threads = GUROBI_THREADS
    model.Params.TimeLimit = MIP_TIME_LIMIT
    model.Params.MIPGap = MIP_GAP
    model.Params.LogToConsole = 1
    model.Params.DisplayInterval = 5
    
    K = n_clusters  # Number of clusters
    
    # -------------------------------------------------------------------------
    # Step 1: Pre-filter tours that are too large to fit in any cluster
    # -------------------------------------------------------------------------
    valid_tours = []
    skipped_tours = 0
    for tour_idx, tour_name, customers in historical_tours:
        if len(customers) <= MAX_CLUSTER_SIZE:
            valid_tours.append((tour_idx, tour_name, customers))
        else:
            skipped_tours += 1
    
    print(f"  Valid tours (size <= {MAX_CLUSTER_SIZE}): {len(valid_tours)}")
    if skipped_tours > 0:
        print(f"  Skipped tours (too large): {skipped_tours}")
    
    # -------------------------------------------------------------------------
    # Step 2: Decision Variables
    # -------------------------------------------------------------------------
    
    # x[j, c] = 1 if customer j is assigned to cluster c
    x = model.addVars(customer_ids, range(K), vtype=GRB.BINARY, name="x")
    
    # v[t, c] = 1 if tour t is fully contained in cluster c
    # Use enumeration index i (not original tour_idx) for Gurobi indexing
    v = model.addVars(len(valid_tours), K, vtype=GRB.BINARY, name="v")
    
    print(f"  Created {len(customer_ids) * K} customer assignment variables (x)")
    print(f"  Created {len(valid_tours) * K} tour containment variables (v)")
    
    # -------------------------------------------------------------------------
    # Step 3: Constraints
    # -------------------------------------------------------------------------
    
    # Each customer assigned to exactly one cluster
    for j in customer_ids:
        model.addConstr(gp.quicksum(x[j, c] for c in range(K)) == 1, name=f"assign_{j}")

    
    # Cluster size constraints
    for c in range(K):
        model.addConstr(
            gp.quicksum(x[j, c] for j in customer_ids) >= MIN_CLUSTER_SIZE,
            name=f"min_size_{c}"
        )
        model.addConstr(
            gp.quicksum(x[j, c] for j in customer_ids) <= MAX_CLUSTER_SIZE,
            name=f"max_size_{c}"
        )
    
    # Linking constraints (aggregated):
    # v[t,c] <= (1/|t|) * sum_{j in t} x[j,c]
    # This is equivalent for binary x and v, but uses far fewer constraints.
    linking_count = 0
    for i, (original_t_idx, tour_name, customers) in enumerate(valid_tours):
        tour_size = len(customers)
        if tour_size == 0:
            continue
        for c in range(K):
            expr = gp.quicksum(x[j, c] for j in customers)
            model.addConstr(v[i, c] <= (1.0 / tour_size) * expr, name=f"link_t{i}_c{c}")
            linking_count += 1

    print(f"  Added {linking_count} linking constraints (aggregated)")
    
    # -------------------------------------------------------------------------
    # Step 4: Objective Function
    # -------------------------------------------------------------------------
    
    # Tour reward: weighted sum of contained tours
    tour_reward = gp.quicksum(
        tour_weights.get(original_t_idx, DEFAULT_TOUR_WEIGHT) * v[i, c]
        for i, (original_t_idx, tour_name, customers) in enumerate(valid_tours)
        for c in range(K)
    )

    # Pure tour maximization
    obj = tour_reward
    print("  Objective: Maximize tour_reward (pure)")
    
    model.setObjective(obj, GRB.MAXIMIZE)
    
    # -------------------------------------------------------------------------
    # Step 5: Solve
    # -------------------------------------------------------------------------
    
    print(f"  Solving MIP (time limit: {MIP_TIME_LIMIT}s, gap: {MIP_GAP:.1%})...")
    interrupted = False
    try:
        model.optimize()
    except KeyboardInterrupt:
        interrupted = True
        print("  KeyboardInterrupt received: stopping optimization and using best incumbent if available...")
        model.terminate()

    if model.Status == GRB.TIME_LIMIT:
        if model.SolCount > 0:
            print("  Time limit reached: using best incumbent and exporting results.")
        else:
            print("  Time limit reached before first incumbent: nothing to export.")
    
    # -------------------------------------------------------------------------
    # Step 6: Extract Solution
    # -------------------------------------------------------------------------
    
    if model.SolCount > 0:
        if model.Status == GRB.OPTIMAL:
            print(f"  MIP solved to optimality (obj={model.ObjVal:.2f})")
        elif model.Status == GRB.TIME_LIMIT:
            print(f"  MIP reached time limit (obj={model.ObjVal:.2f}, gap={model.MIPGap:.2%})")
        elif model.Status == GRB.INTERRUPTED or interrupted:
            print(f"  MIP interrupted, using best incumbent (obj={model.ObjVal:.2f}, gap={model.MIPGap:.2%})")
        else:
            print(f"  MIP status {model.Status}, best obj={model.ObjVal:.2f}")
    else:
        print(f"  WARNING: MIP status {model.Status}, no solution found")
        return None, None
    
    # Extract cluster assignments
    final_clusters = {}
    for j in customer_ids:
        for c in range(K):
            if x[j, c].X > 0.5:
                final_clusters[j] = c
                break
    
    # Extract which tours are fully contained
    contained_tours = []
    for i, (original_t_idx, tour_name, customers) in enumerate(valid_tours):
        for c in range(K):
            if v[i, c].X > 0.5:
                contained_tours.append({
                    'tour_idx': original_t_idx,
                    'tour_name': tour_name,
                    'cluster': c,
                    'size': len(customers),
                    'weight': tour_weights.get(original_t_idx, DEFAULT_TOUR_WEIGHT)
                })
    
    print(f"  Tours fully contained: {len(contained_tours)} / {len(valid_tours)}")
    
    return final_clusters, contained_tours


def compute_tour_coverage_adjacent(final_clusters, tours_df, customer_id_to_client_num):
    """Compute adjacency-based tour coverage metrics (tour-adjacency)."""
    total_tours = 0
    covered_tours = 0
    total_violations = 0

    for tour_name, group in tours_df.groupby('tour_name'):
        group = group.copy()
        sort_cols = [c for c in ["arrival_time", "departure_time"] if c in group.columns]
        if sort_cols:
            group = group.sort_values(sort_cols)

        customers = group['customer_id'].tolist()
        mapped = [customer_id_to_client_num.get(c) for c in customers]
        mapped = [c for c in mapped if c is not None and c in final_clusters]

        if len(mapped) < 2:
            continue

        total_tours += 1

        tour_violations = 0
        prev = mapped[0]
        for curr in mapped[1:]:
            if curr != prev and final_clusters[prev] != final_clusters[curr]:
                tour_violations += 1
            prev = curr

        if tour_violations == 0:
            covered_tours += 1
        total_violations += tour_violations

    coverage_rate = covered_tours / total_tours if total_tours > 0 else 0
    return {
        'coverage_rate': coverage_rate,
        'covered_tours': covered_tours,
        'total_tours': total_tours,
        'total_violations': total_violations
    }


def compute_tour_coverage_explicit(final_clusters, tours_df, customer_id_to_client_num):
    """Compute full-containment tour coverage metrics (tour-containment).

    coverage_rate: average across tours of the fraction of customer pairs
    that share a cluster.  A fully preserved tour contributes 1.0, a tour
    where no two customers share a cluster contributes 0.0.
    """
    total_tours = 0
    total_violations = 0
    pair_coverage_sum = 0.0

    for tour_name, group in tours_df.groupby('tour_name'):
        customers = group['customer_id'].tolist()

        mapped = [customer_id_to_client_num.get(c) for c in customers]
        mapped = [c for c in mapped if c is not None and c in final_clusters]

        if len(mapped) < 2:
            continue

        total_tours += 1
        total_pairs = len(mapped) * (len(mapped) - 1) // 2
        violations = 0

        for i in range(len(mapped)):
            for j in range(i + 1, len(mapped)):
                if final_clusters[mapped[i]] != final_clusters[mapped[j]]:
                    violations += 1

        total_violations += violations
        pair_coverage_sum += (total_pairs - violations) / total_pairs

    coverage_rate = pair_coverage_sum / total_tours if total_tours > 0 else 0
    return {
        'coverage_rate': coverage_rate,
        'total_tours': total_tours,
        'total_violations': total_violations
    }


def generate_arc_set(final_clusters, customer_ids, depot=0):
    """Generate depot + complete intra-cluster arc set."""
    return build_cluster_arc_set(
        customer_ids=customer_ids,
        final_clusters=final_clusters,
        depot=depot,
    )


def save_outputs(arcs, final_clusters, cluster_members, arc_stats,
                 tour_metrics_adjacent, tour_metrics_explicit, contained_tours, n_customers, coords_df,
                 warehouse, max_demand, historical_tours, raw_tour_count):
    """Save all output files for tour-explicit clustering."""

    mode_suffix = "tour_containment"
    
    # Save arc set
    arc_list = sorted(list(arcs))
    arc_file = CLUSTER_DATA_DIR / f"arc_set_{mode_suffix}.json"
    write_table(arc_file, ["from_node", "to_node"], [{"from_node": i, "to_node": j} for i, j in arc_list])
    print(f"  Arc set saved: {arc_file}")

    global_arcs = build_global_arc_set(customer_ids=final_clusters.keys())
    arc_global_file = CLUSTER_DATA_DIR / "arc_set_global.json"
    write_table(
        arc_global_file,
        ["from_node", "to_node"],
        [{"from_node": i, "to_node": j} for i, j in sorted(global_arcs)],
    )
    print(f"  Global arc set saved: {arc_global_file}")
    
    # Save cluster assignments
    cluster_file = CLUSTER_DATA_DIR / f"cluster_assignments_{mode_suffix}.json"
    write_table(
        cluster_file,
        ["customer_id", "cluster"],
        [{"customer_id": cid, "cluster": c} for cid, c in final_clusters.items()],
    )
    print(f"  Cluster assignments saved: {cluster_file}")
    
    # Calculate arc statistics
    total_arcs = arc_stats["total_arcs"]
    full_arcs = arc_stats["full_arcs"]
    reduction = arc_stats["reduction_pct"]
    customer_customer_arcs = arc_stats["customer_customer_arcs"]
    intra_cluster_arcs = arc_stats["intra_cluster_arcs"]
    
    # Save report
    report_file = REPORTS_DIR / f"arc_set_{mode_suffix}_report.json"
    tours_by_cluster = {}
    for t in contained_tours:
        tours_by_cluster.setdefault(t["cluster"], []).append(t)
    write_json(
        report_file,
        {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "mode": "tour_containment",
            "configuration": {
                "target_customers_per_cluster": TARGET_CUSTOMERS_PER_CLUSTER,
                "min_cluster_size": MIN_CLUSTER_SIZE,
                "max_cluster_size": MAX_CLUSTER_SIZE,
                "default_tour_weight": DEFAULT_TOUR_WEIGHT,
                "mip_time_limit_sec": MIP_TIME_LIMIT,
                "mip_gap": MIP_GAP,
            },
            "historical_tours": {
                "raw_tour_count": raw_tour_count,
                "aggregated_tour_count": len(historical_tours),
                "contained_tour_count": len(contained_tours),
                "preservation_rate": (
                    len(contained_tours) / len(historical_tours) if historical_tours else 0.0
                ),
            },
            "tour_metrics_adjacent": tour_metrics_adjacent,
            "tour_metrics_explicit": tour_metrics_explicit,
            "preserved_tours_by_cluster": {str(c): tours for c, tours in sorted(tours_by_cluster.items())},
            "clusters": [
                {
                    "cluster": c,
                    "members": cluster_members[c],
                    "size": len(cluster_members[c]),
                    "total_max_demand": float(sum(max_demand.get(cid, 0) for cid in cluster_members[c])),
                    "preserved_tours": len([t for t in contained_tours if t["cluster"] == c]),
                }
                for c in sorted(cluster_members.keys())
            ],
            "arc_statistics": {
                "depot_arcs": n_customers * 2,
                "customer_customer_arcs": customer_customer_arcs,
                "intra_cluster_arcs": intra_cluster_arcs,
                "total_arcs": total_arcs,
                "full_arcs": full_arcs,
                "reduction_pct": reduction,
            },
        },
    )
    
    print(f"  Report saved: {report_file}")
    
    # Generate visualization
    import matplotlib.pyplot as plt
    import matplotlib
    matplotlib.use('Agg')
    
    fig, ax = plt.subplots(1, 1, figsize=(12, 10))
    
    # Color palette for clusters
    n_clusters = len(cluster_members)
    colors = plt.cm.tab20(np.linspace(0, 1, max(n_clusters, 2)))
    n_colors = len(colors)
    
    # Plot clusters
    for c in sorted(cluster_members.keys()):
        members = cluster_members[c]
        member_coords = coords_df[coords_df['node_index'].isin(members)]
        preserved = len([t for t in contained_tours if t['cluster'] == c])
        ax.scatter(
            member_coords['longitude'], member_coords['latitude'],
            s=150, c=[colors[c % n_colors]], 
            label=f'Cluster {c} ({len(members)} cust, {preserved} tours)',
            edgecolors='black', linewidth=1, alpha=0.8
        )
        
        # Add customer labels
        for _, row in member_coords.iterrows():
            ax.annotate(str(int(row['node_index'])), (row['longitude'], row['latitude']),
                       fontsize=7, ha='center', va='bottom', xytext=(0, 5),
                       textcoords='offset points')
    
    # Warehouse
    ax.scatter(warehouse['longitude'], warehouse['latitude'], s=300, c='blue',
              marker='s', edgecolors='darkblue', linewidth=2, label='Warehouse', zorder=5)
    
    ax.set_xlabel('Longitude', fontsize=12, fontweight='bold')
    ax.set_ylabel('Latitude', fontsize=12, fontweight='bold')
    title_mode = "Tour-Containment"
    title_lines = [f'Customer Clusters ({title_mode} Clustering)']
    title_lines.append(
        f'Adjacent: {tour_metrics_adjacent["covered_tours"]}/{tour_metrics_adjacent["total_tours"]} '
        f'({tour_metrics_adjacent["coverage_rate"]:.1%})'
    )
    title_lines.append(
        f'Containment: {tour_metrics_explicit["coverage_rate"]:.1%} avg pair coverage'
    )
    ax.set_title("\n".join(title_lines), fontsize=14, fontweight='bold')
    ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1), fontsize=8, ncol=2, borderaxespad=0)
    ax.grid(True, alpha=0.3, linestyle='--')
    
    plt.tight_layout(rect=[0, 0, 0.82, 1])
    viz_file = FIGURES_DIR / f"arc_set_clusters_{mode_suffix}.png"
    plt.savefig(viz_file, dpi=200, bbox_inches='tight')
    plt.close()
    print(f"  Visualization saved: {viz_file}")
    
    return arc_file, cluster_file, report_file, viz_file


# ============================================================================
# MAIN: RUN TOUR-CONTAINMENT CLUSTERING
# ============================================================================

# Determine number of clusters
n_clusters = choose_cluster_count(
    n_customers=n_customers,
    target_customers_per_cluster=TARGET_CUSTOMERS_PER_CLUSTER,
    max_cluster_size=MAX_CLUSTER_SIZE,
)
print(f"\nTarget clusters: {n_clusters} (based on {TARGET_CUSTOMERS_PER_CLUSTER} customers/cluster)")

# ============================================================================
# SOLVE TOUR-CONTAINMENT MIP
# ============================================================================

print("\n" + "=" * 80)
print("RUNNING: TOUR-CONTAINMENT CLUSTERING")
print("Objective: Maximize number of fully preserved historical tours")
print("=" * 80)

final_clusters, contained_tours = solve_tour_containment_mip(
    customer_ids, n_clusters, historical_tours, tour_weights
)

if final_clusters:
    # Generate arc set
    arcs, cluster_members, arc_stats = generate_arc_set(final_clusters, customer_ids)

    # Compute metrics (for validation/comparison)
    tour_metrics_adjacent = compute_tour_coverage_adjacent(
        final_clusters, tours_df, customer_id_to_client_num
    )
    tour_metrics_explicit = compute_tour_coverage_explicit(
        final_clusters, tours_df, customer_id_to_client_num
    )

    print(f"\nCluster sizes: {[len(m) for m in cluster_members.values()]}")
    print(f"Tours preserved (unique): {len(contained_tours)} / {len(historical_tours)}")
    print(f"Tour coverage (adjacent): {tour_metrics_adjacent['coverage_rate']:.1%}")
    print(f"Total edge violations: {tour_metrics_adjacent['total_violations']}")
    print(f"Tour coverage (containment): {tour_metrics_explicit['coverage_rate']:.1%}")
    print(f"Total pair violations: {tour_metrics_explicit['total_violations']}")
    print(f"Total arcs: {len(arcs)}")
    print(f"  Intra-cluster arcs: {arc_stats['intra_cluster_arcs']}")

    # Save outputs
    print("\nSaving outputs...")
    output_files = save_outputs(
        arcs, final_clusters, cluster_members, arc_stats,
        tour_metrics_adjacent, tour_metrics_explicit, contained_tours, n_customers, coords_df,
        warehouse, max_demand, historical_tours, raw_tours_with_customers
    )

    # Print summary
    print("\n" + "=" * 80)
    print("RESULTS SUMMARY")
    print("=" * 80)
    print("Mode: tour_containment")
    print(f"Tours preserved (unique): {len(contained_tours)} / {len(historical_tours)} "
          f"({len(contained_tours)/len(historical_tours)*100:.1f}%)")
    print(f"Tour coverage (adjacent): {tour_metrics_adjacent['coverage_rate']:.1%}")
    print(f"Total edge violations: {tour_metrics_adjacent['total_violations']}")
    print(f"Tour coverage (containment): {tour_metrics_explicit['coverage_rate']:.1%}")
    print(f"Total pair violations: {tour_metrics_explicit['total_violations']}")
    print(f"Total arcs: {len(arcs)}")
else:
    print("\nERROR: MIP solver did not return a solution")
    exit(1)

print("\n" + "=" * 80)
print("COMPLETED SUCCESSFULLY")
print("=" * 80)

print(f"\nOutput files saved to:")
print(f"  Data: {CLUSTER_DATA_DIR}")
print(f"  Reports/Visualizations: {RUN_DIR}")
try:
    export_prepared_clustering(RUN_DIR)
    print(f"  Prep: {RUN_DIR / 'prep' / 'clustering'}")
except Exception as exc:
    print(f"WARNING: failed to write prep clustering exports: {exc}")
