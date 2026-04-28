#!/usr/bin/env python3
"""
Generate restricted arc set for CSPP using demand-aware clustering.

This script builds clustering outputs and a restricted arc set by:
1. Clustering customers geographically (k-means)
2. Adjusting clusters to respect capacity constraints (MIP reassignment)
3. Generating clustering/global arc exports:
   - clustering: depot links + full intra-cluster arcs
   - global: complete directed graph over all customers

Output:
- arc_set.json: Cluster-complete allowed arcs (from_node, to_node)
- arc_set_global.json: Complete global directed arc set
- arc_set_report.json: Clustering summary and statistics
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.cluster import KMeans
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

# MIP settings
MIP_TIME_LIMIT, MIP_GAP = cluster_mip_params(default_timelimit=10 * 60 * 60)

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
print("ARC SET GENERATOR (Demand-Aware Clustering)")
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

# Load demand data
demand_file = resolve_cspp_data_file("demand_matrix.json")
demand_df = pd.DataFrame(read_table_rows(demand_file)).set_index("delivery_date")
demand_df.columns = demand_df.columns.astype(int)
print(f"Demand matrix: {demand_df.shape}")

# Calculate max demand per customer across all scenarios
max_demand = demand_df.max().to_dict()
avg_demand = demand_df[demand_df > 0].mean().fillna(0).to_dict()

customer_ids = customers_df['node_index'].tolist()
n_customers = len(customer_ids)

print(f"\nDemand statistics:")
print(f"  Max demand range: {min(max_demand.values()):.0f} - {max(max_demand.values()):.0f} kg")
print(f"  Avg demand range: {min(avg_demand.values()):.0f} - {max(avg_demand.values()):.0f} kg")

# Load customer ID mapping
mapping_file = resolve_cspp_data_file("customer_id_mapping.json")
if mapping_file.exists():
    mapping_df = pd.DataFrame(read_table_rows(mapping_file))
    customer_id_to_client_num = dict(zip(mapping_df['customer_id'], mapping_df['client_num']))
    client_num_to_customer_id = {v: k for k, v in customer_id_to_client_num.items()}
    print(f"Loaded customer ID mapping: {len(customer_id_to_client_num)} entries")
else:
    print(f"WARNING: Customer ID mapping not found: {mapping_file}")
    customer_id_to_client_num = {}
    client_num_to_customer_id = {}

# Load tour data for coverage analysis
tours_file = LIEFERDATEN_DATA_DIR / "tour_stops_clean.json"
if tours_file.exists():
    tours_df = pd.DataFrame(read_table_rows(tours_file))
    print(f"Loaded {len(tours_df)} tour stop records")
    print(f"Unique tours: {tours_df['tour_name'].nunique()}")
    HAS_TOUR_DATA = True
else:
    print(f"WARNING: Tour data not found: {tours_file}")
    print("Tour coverage metrics will not be computed.")
    tours_df = None
    HAS_TOUR_DATA = False

# ============================================================================
# HELPER FUNCTIONS: TOUR COVERAGE METRICS
# ============================================================================

def compute_tour_coverage_adjacent(final_clusters, tours_df, customer_id_to_client_num, customer_set):
    """Compute metrics on how well clusters cover historical tours (adjacent edges)."""
    if tours_df is None:
        return None

    total_tours = 0
    covered_tours = 0
    total_violations = 0
    covered_tour_names = []

    for tour_name, group in tours_df.groupby('tour_name'):
        group = group.copy()
        sort_cols = [c for c in ["arrival_time", "departure_time"] if c in group.columns]
        if sort_cols:
            group = group.sort_values(sort_cols)

        customers = group['customer_id'].tolist()

        # Map to client_nums
        mapped = [customer_id_to_client_num.get(c) for c in customers]
        mapped = [c for c in mapped if c is not None and c in customer_set and c in final_clusters]

        if len(mapped) < 2:
            continue

        total_tours += 1

        # Count adjacent edge violations
        tour_violations = 0
        prev = mapped[0]
        for curr in mapped[1:]:
            if curr != prev and final_clusters[prev] != final_clusters[curr]:
                tour_violations += 1
            prev = curr

        if tour_violations == 0:
            covered_tours += 1
            covered_tour_names.append(tour_name)
        total_violations += tour_violations

    coverage_rate = covered_tours / total_tours if total_tours > 0 else 0
    return {
        'coverage_rate': coverage_rate,
        'covered_tours': covered_tours,
        'total_tours': total_tours,
        'total_violations': total_violations,
        'covered_tour_names': covered_tour_names
    }


def compute_tour_coverage_explicit(final_clusters, tours_df, customer_id_to_client_num, customer_set):
    """Compute metrics on how well clusters preserve full tours (all customers in same cluster)."""
    if tours_df is None:
        return None

    total_tours = 0
    covered_tours = 0
    total_violations = 0
    covered_tour_names = []

    for tour_name, group in tours_df.groupby('tour_name'):
        customers = group['customer_id'].tolist()

        # Map to client_nums
        mapped = [customer_id_to_client_num.get(c) for c in customers]
        mapped = [c for c in mapped if c is not None and c in customer_set and c in final_clusters]

        if len(mapped) < 2:
            continue

        total_tours += 1

        # Get clusters for these customers
        clusters_in_tour = set(final_clusters[c] for c in mapped)

        if len(clusters_in_tour) == 1:
            covered_tours += 1
            covered_tour_names.append(tour_name)
        else:
            # Count violations as customer pairs split across clusters
            for i in range(len(mapped)):
                for j in range(i+1, len(mapped)):
                    if final_clusters[mapped[i]] != final_clusters[mapped[j]]:
                        total_violations += 1

    coverage_rate = covered_tours / total_tours if total_tours > 0 else 0
    return {
        'coverage_rate': coverage_rate,
        'covered_tours': covered_tours,
        'total_tours': total_tours,
        'total_violations': total_violations,
        'covered_tour_names': covered_tour_names
    }

# ============================================================================
# STEP 1: INITIAL GEOGRAPHIC CLUSTERING (k-means)
# ============================================================================

print("\n" + "=" * 80)
print("STEP 1: GEOGRAPHIC CLUSTERING (k-means)")
print("=" * 80)

# Determine number of clusters
n_clusters = choose_cluster_count(
    n_customers=n_customers,
    target_customers_per_cluster=TARGET_CUSTOMERS_PER_CLUSTER,
    max_cluster_size=MAX_CLUSTER_SIZE,
)
print(f"Target clusters: {n_clusters} (based on {TARGET_CUSTOMERS_PER_CLUSTER} customers/cluster)")

# Prepare coordinates for clustering (include warehouse so centroids are depot-aware)
customer_coords = customers_df[['latitude', 'longitude']].values
warehouse_coords = np.array([[warehouse['latitude'], warehouse['longitude']]])
clustering_coords = np.vstack([warehouse_coords, customer_coords])  # Depot + customers

# Run k-means on all points (depot influences centroid positions)
kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
all_labels = kmeans.fit_predict(clustering_coords)
initial_labels = all_labels[1:]  # Exclude depot label, keep only customer labels

# Map customer IDs to initial clusters
initial_clusters = {cid: int(label) for cid, label in zip(customer_ids, initial_labels)}

# Print initial cluster statistics
print(f"\nInitial cluster sizes:")
for c in range(n_clusters):
    members = [cid for cid, label in initial_clusters.items() if label == c]
    total_demand = sum(max_demand.get(cid, 0) for cid in members)
    print(f"  Cluster {c}: {len(members)} customers, max demand {total_demand:.0f} kg")

# ============================================================================
# STEP 2: BALANCED CLUSTER REFINEMENT (MIP)
# ============================================================================

print("\n" + "=" * 80)
print("STEP 2: BALANCED CLUSTER REFINEMENT (MIP)")
print("=" * 80)

# MIP to reassign customers to clusters while:
# - Minimizing total distance from cluster centroids (geographic compactness)
# - Keeping cluster sizes balanced (avoid tiny/huge clusters)
# - Ensuring all clusters are populated

model = gp.Model("cluster_assignment")
model.Params.OutputFlag = 0
model.Params.Threads = GUROBI_THREADS
model.Params.TimeLimit = MIP_TIME_LIMIT
model.Params.MIPGap = MIP_GAP

# Decision variables: x[i,c] = 1 if customer i assigned to cluster c
x = model.addVars(customer_ids, range(n_clusters), vtype=GRB.BINARY, name="x")

# Each customer assigned to exactly one cluster
for i in customer_ids:
    model.addConstr(gp.quicksum(x[i, c] for c in range(n_clusters)) == 1, name=f"assign_{i}")

# Cluster size constraints (ensures balanced zones)
for c in range(n_clusters):
    model.addConstr(
        gp.quicksum(x[i, c] for i in customer_ids) >= MIN_CLUSTER_SIZE,
        name=f"min_size_{c}"
    )
    model.addConstr(
        gp.quicksum(x[i, c] for i in customer_ids) <= MAX_CLUSTER_SIZE,
        name=f"max_size_{c}"
    )

# Objective: minimize distance to cluster centroids
# Centroids already include depot influence from k-means
centroids = kmeans.cluster_centers_

# Calculate distance from each customer to each centroid
dist_to_centroid = {}
for i, cid in enumerate(customer_ids):
    lat, lon = customer_coords[i]
    for c in range(n_clusters):
        clat, clon = centroids[c]
        # Euclidean distance in coordinate space (proxy for geographic distance)
        dist_to_centroid[cid, c] = np.sqrt((lat - clat)**2 + (lon - clon)**2)

model.setObjective(
    gp.quicksum(dist_to_centroid[i, c] * x[i, c] for i in customer_ids for c in range(n_clusters)),
    GRB.MINIMIZE
)

# Solve
print(f"Solving MIP (time limit: {MIP_TIME_LIMIT}s, gap: {MIP_GAP:.1%}, threads: {GUROBI_THREADS})...")
while True:
    model.optimize()
    if model.Status == GRB.TIME_LIMIT and model.SolCount > 0 and model.MIPGap > MIP_GAP:
        print(f"Gap {model.MIPGap:.2%} > {MIP_GAP:.1%}, extending time limit...")
        continue
    break

if model.Status == GRB.OPTIMAL:
    print("MIP solved to optimality")
    # Extract final cluster assignments
    final_clusters = {}
    for i in customer_ids:
        for c in range(n_clusters):
            if x[i, c].X > 0.5:
                final_clusters[i] = c
                break
elif model.Status == GRB.TIME_LIMIT and model.SolCount > 0:
    print("MIP reached time limit, using best solution found")
    # Extract best solution found
    final_clusters = {}
    for i in customer_ids:
        for c in range(n_clusters):
            if x[i, c].X > 0.5:
                final_clusters[i] = c
                break
else:
    print(f"WARNING: MIP did not find solution (status {model.Status})")
    print("Using initial k-means clusters instead")
    final_clusters = initial_clusters

# Print final cluster statistics
print(f"\nFinal cluster assignments:")
cluster_members = {c: [] for c in range(n_clusters)}
for cid, c in final_clusters.items():
    cluster_members[c].append(cid)

for c in range(n_clusters):
    members = cluster_members[c]
    total_demand = sum(max_demand.get(cid, 0) for cid in members)
    print(f"  Cluster {c}: {len(members)} customers, total max demand {total_demand:.0f} kg")

# Compute tour coverage metrics
customer_set = set(customer_ids)
tour_metrics_adjacent = None
tour_metrics_explicit = None
if HAS_TOUR_DATA:
    print("\nComputing tour coverage metrics...")
    tour_metrics_adjacent = compute_tour_coverage_adjacent(final_clusters, tours_df, customer_id_to_client_num, customer_set)
    tour_metrics_explicit = compute_tour_coverage_explicit(final_clusters, tours_df, customer_id_to_client_num, customer_set)
    if tour_metrics_adjacent:
        print(f"  Tour coverage (adjacent edges): {tour_metrics_adjacent['coverage_rate']:.1%}")
        print(f"    Fully covered tours: {tour_metrics_adjacent['covered_tours']} / {tour_metrics_adjacent['total_tours']}")
        print(f"    Total edge violations: {tour_metrics_adjacent['total_violations']}")
    if tour_metrics_explicit:
        print(f"  Tour coverage (full containment): {tour_metrics_explicit['coverage_rate']:.1%}")
        print(f"    Fully covered tours: {tour_metrics_explicit['covered_tours']} / {tour_metrics_explicit['total_tours']}")
        print(f"    Total pair violations: {tour_metrics_explicit['total_violations']}")

# ============================================================================
# STEP 3: GENERATE FULL ARC SET
# ============================================================================

print("\n" + "=" * 80)
print("STEP 3: GENERATING ARC SETS")
print("=" * 80)

arcs, _, arc_stats = build_cluster_arc_set(
    customer_ids=customer_ids,
    final_clusters=final_clusters,
    depot=0,
)
global_arcs = build_global_arc_set(customer_ids=customer_ids, depot=0)
depot_arcs = arc_stats["depot_arcs"]
customer_customer_arcs = arc_stats["customer_customer_arcs"]
intra_cluster_arcs = arc_stats["intra_cluster_arcs"]
total_arcs = arc_stats["total_arcs"]
full_arcs = arc_stats["full_arcs"]
reduction = arc_stats["reduction_pct"]

print(f"Depot arcs: {depot_arcs}")
print(f"Customer-customer arcs: {customer_customer_arcs}")
print(f"  Intra-cluster arcs: {intra_cluster_arcs}")

print(f"\nArc set summary:")
print(f"  Total arcs: {total_arcs}")
print(f"  Full graph would have: {full_arcs}")
print(f"  Reduction: {reduction:.1f}%")
print(f"Global arc set arcs: {len(global_arcs)}")

# ============================================================================
# STEP 4: VALIDATE FEASIBILITY
# ============================================================================

print("\n" + "=" * 80)
print("STEP 4: VALIDATING BASIC ARC COVERAGE")
print("=" * 80)

missing_depot_links = []
for cid in customer_ids:
    if (0, cid) not in arcs or (cid, 0) not in arcs:
        missing_depot_links.append(cid)
if missing_depot_links:
    print(f"WARNING: Missing depot links for {len(missing_depot_links)} customers")
else:
    print("Depot-link validation passed")

# ============================================================================
# STEP 5: SAVE OUTPUT
# ============================================================================

print("\n" + "=" * 80)
print("STEP 5: SAVING OUTPUT")
print("=" * 80)

# Save arc set
arc_list = sorted(list(arcs))
arc_df = pd.DataFrame(arc_list, columns=['from_node', 'to_node'])
arc_file = CLUSTER_DATA_DIR / "arc_set.json"
write_table(arc_file, arc_df.columns.tolist(), arc_df.to_dict(orient="records"))
print(f"Arc set saved: {arc_file}")

arc_global_df = pd.DataFrame(sorted(global_arcs), columns=['from_node', 'to_node'])
arc_global_file = CLUSTER_DATA_DIR / "arc_set_global.json"
write_table(arc_global_file, arc_global_df.columns.tolist(), arc_global_df.to_dict(orient="records"))
print(f"Global arc set saved: {arc_global_file}")

# Save cluster assignments
cluster_df = pd.DataFrame([
    {'customer_id': cid, 'cluster': c}
    for cid, c in final_clusters.items()
])
cluster_file = CLUSTER_DATA_DIR / "cluster_assignments.json"
write_table(cluster_file, cluster_df.columns.tolist(), cluster_df.to_dict(orient="records"))
print(f"Cluster assignments saved: {cluster_file}")

# Save report
report_file = REPORTS_DIR / "arc_set_report.json"
write_json(report_file, {
    "generated_at": datetime.now().isoformat(),
    "cluster_count": int(len(set(final_clusters.values()))),
    "customer_count": int(len(customer_ids)),
    "arc_count": int(len(arcs)),
    "global_arc_count": int(len(global_arcs)),
    "target_customers_per_cluster": int(TARGET_CUSTOMERS_PER_CLUSTER),
    "cluster_size_min_allowed": int(MIN_CLUSTER_SIZE),
    "cluster_size_max_allowed": int(MAX_CLUSTER_SIZE),
})

print(f"Report saved: {report_file}")

# ============================================================================
# STEP 6: GENERATE VISUALIZATION
# ============================================================================

print("\n" + "=" * 80)
print("STEP 6: GENERATING VISUALIZATION")
print("=" * 80)

import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')

fig, ax1 = plt.subplots(1, 1, figsize=(10, 10))

# Color palette for clusters
colors = plt.cm.tab10(np.linspace(0, 1, n_clusters))

# Count tours per cluster for legend
tours_per_cluster = {c: 0 for c in range(n_clusters)}
if tour_metrics_adjacent and tour_metrics_adjacent['covered_tour_names']:
    for tour_name in tour_metrics_adjacent['covered_tour_names']:
        group = tours_df[tours_df['tour_name'] == tour_name]
        customers = group['customer_id'].tolist()
        mapped = [customer_id_to_client_num.get(cust) for cust in customers]
        mapped = [cust for cust in mapped if cust is not None and cust in customer_set and cust in final_clusters]
        if mapped:
            cluster = final_clusters[mapped[0]]
            tours_per_cluster[cluster] += 1

# Plot: Clusters
for c in range(n_clusters):
    members = cluster_members[c]
    member_coords = customers_df[customers_df['node_index'].isin(members)]
    n_tours = tours_per_cluster.get(c, 0)
    if tour_metrics_adjacent:
        label = f'Cluster {c} ({len(members)} cust, {n_tours} tours)'
    else:
        label = f'Cluster {c} ({len(members)})'
    ax1.scatter(
        member_coords['longitude'], member_coords['latitude'],
        s=150, c=[colors[c]], label=label,
        edgecolors='black', linewidth=1, alpha=0.8
    )

    # Add customer labels
    for _, row in member_coords.iterrows():
        ax1.annotate(str(int(row['node_index'])), (row['longitude'], row['latitude']),
                    fontsize=7, ha='center', va='bottom', xytext=(0, 5),
                    textcoords='offset points')

# Warehouse
ax1.scatter(warehouse['longitude'], warehouse['latitude'], s=300, c='blue',
           marker='s', edgecolors='darkblue', linewidth=2, label='Warehouse', zorder=5)

ax1.set_xlabel('Longitude', fontsize=12, fontweight='bold')
ax1.set_ylabel('Latitude', fontsize=12, fontweight='bold')
if tour_metrics_adjacent or tour_metrics_explicit:
    title_lines = ["Customer Clusters (Geographic Clustering)"]
    if tour_metrics_adjacent:
        title_lines.append(
            f'Adjacent: {tour_metrics_adjacent["covered_tours"]}/{tour_metrics_adjacent["total_tours"]} '
            f'({tour_metrics_adjacent["coverage_rate"]*100:.1f}%)'
        )
    if tour_metrics_explicit:
        title_lines.append(
            f'Containment: {tour_metrics_explicit["covered_tours"]}/{tour_metrics_explicit["total_tours"]} '
            f'({tour_metrics_explicit["coverage_rate"]*100:.1f}%)'
        )
    ax1.set_title("\n".join(title_lines), fontsize=14, fontweight='bold')
else:
    ax1.set_title(f'Customer Clusters ({n_clusters} clusters)', fontsize=14, fontweight='bold')
ax1.legend(loc='upper left', bbox_to_anchor=(1.02, 1), fontsize=9, borderaxespad=0)
ax1.grid(True, alpha=0.3, linestyle='--')

plt.tight_layout(rect=[0, 0, 0.82, 1])
viz_file = FIGURES_DIR / "arc_set_clusters.png"
plt.savefig(viz_file, dpi=200, bbox_inches='tight')
plt.close()
print(f"Visualization saved: {viz_file}")

# ============================================================================
# DONE
# ============================================================================

print("\n" + "=" * 80)
print("COMPLETED SUCCESSFULLY")
print("=" * 80)
print(f"\nOutput files:")
print(f"  {arc_file}")
print(f"  {cluster_file}")
print(f"  {report_file}")
print(f"  {viz_file}")
try:
    export_prepared_clustering(RUN_DIR)
    print(f"  {RUN_DIR / 'prep' / 'clustering'}")
except Exception as exc:
    print(f"WARNING: failed to write prep clustering exports: {exc}")
