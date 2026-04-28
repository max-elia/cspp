"""
Example script to run the CSPP (Charging Station Placement Problem) model.

This script demonstrates how to use the CSPP implementation with the
Mercedes-Benz eActros or Volvo FM Electric vehicle configurations.
"""

import argparse
import os

import helper
helper.VerbosityManager.global_verbosity = 1

from algorithm_types import AlgorithmType
from classes import AlgorithmParams, TimeoutException
from algorithm import ourAlgorithm, toenAlgorithm, rodrAlgorithm

from applications.cspp.instance import create_mercedes_instance, create_volvo_instance
import applications.cspp.model as model
from applications import Application
from cspp.fixed_costs import default_fixed_truck_cost

parser = argparse.ArgumentParser(description="Run CSPP base model.")
parser.add_argument("--vehicle-type", choices=["mercedes", "volvo"], default="mercedes")
parser.add_argument("--scenarios-to-use", type=int, default=3)
parser.add_argument("--customers-to-use", type=int, default=50)
parser.add_argument("--gap", type=float, default=0.05)
parser.add_argument("--alg", choices=["our", "toen", "rodr"], default="our")
parser.add_argument("--K-max", type=int, default=3)
parser.add_argument("--M-max", type=int, default=2)
parser.add_argument("--d-cost", type=float, default=0.30)
parser.add_argument("--h", type=float, default=50.0)
parser.add_argument("--F", type=float, default=None)
parser.add_argument("--gurobi-threads", type=int, default=int(os.environ.get("GUROBI_THREADS", "16")))
args = parser.parse_args()

# Configuration
vehicle_type = args.vehicle_type
scenarios_to_use = args.scenarios_to_use
customers_to_use = args.customers_to_use
gap = args.gap
timelimit = 2 * 60 * 60
alg = args.alg
gurobi_threads = max(1, args.gurobi_threads)

# Instance parameters
initial_soc_fraction = 0.8
distance_multiplier = 1.0
demand_multiplier = 0.3

# Fleet parameters
K_max = args.K_max
M_max = args.M_max

# Cost parameters
d_cost = args.d_cost
h = args.h
F = args.F if args.F is not None else default_fixed_truck_cost(vehicle_type)
charger_lifespan_years = 10
operating_days_per_year = 290

# Create instance based on vehicle type
if vehicle_type == "mercedes":
    inst = create_mercedes_instance(
        initial_soc_fraction=initial_soc_fraction,
        distance_multiplier=distance_multiplier,
        demand_multiplier=demand_multiplier,
        d_cost=d_cost,
        h=h,
        F=F,
        K_max=K_max,
        M_max=M_max,
        charger_lifespan_years=charger_lifespan_years,
        operating_days_per_year=operating_days_per_year
    )
    print(f"Created Mercedes-Benz eActros 300 instance")
else:
    inst = create_volvo_instance(
        initial_soc_fraction=initial_soc_fraction,
        distance_multiplier=distance_multiplier,
        demand_multiplier=demand_multiplier,
        d_cost=d_cost,
        h=h,
        F=F,
        K_max=K_max,
        M_max=M_max,
        charger_lifespan_years=charger_lifespan_years,
        operating_days_per_year=operating_days_per_year
    )
    print(f"Created Volvo FM Electric instance")

# Display configuration
print(f"Initial SoC: {inst.c0:.1f} kWh ({initial_soc_fraction*100:.0f}%)")
print(f"Distance multiplier: {distance_multiplier}x")
print(f"Demand multiplier: {demand_multiplier}x")
print(f"\nFleet Configuration:")
print(f"  Max trucks (K_max): {K_max}")
print(f"  Max tours per truck (M_max): {M_max}")

print(f"\nCost Configuration:")
print(f"  Electricity cost: {d_cost:.2f} EUR/kWh")
print(f"  Waiting time cost: {h:.2f} EUR/hour")
print(f"  Truck fixed cost: {F:.2f} EUR/truck-day")
print(f"  Charger lifespan: {charger_lifespan_years} years")
print(f"  Operating days: {operating_days_per_year} days/year")

# Limit to first N scenarios and customers for testing
# NOTE: Model uses multi-truck/multi-tour structure with K_max × M_max × |J| × |V| binary variables
# Memory usage scales as O(K_max × M_max × |J|² × |S|), so keep instances small for testing
inst.S = inst.S[:scenarios_to_use]
if customers_to_use is not None:
    inst.J = inst.J[:customers_to_use]
    inst.V = [inst.i0] + inst.J  # Update V after modifying J

# Update strings after modifying instance
inst.strings.ALG_INTRO_TEXT = f"CSPP Algorithm for {inst.name} ({len(inst.J)} customers, {len(inst.T)} types, {len(inst.S)} scenarios)\n"
inst.strings.UNIQUE_IDENTIFIER = f"{inst.name}-{len(inst.J)}-{len(inst.T)}-{len(inst.S)}"

print(f"Using {len(inst.S)} scenarios, {len(inst.J)} customers, {len(inst.T)} charger types")
print(f"Gurobi threads: {gurobi_threads}")

# Report arc set status
print(f"Full arc set: {len(inst.l)} arcs")
print(f"Model size: ~{inst.K_max * inst.M_max * len(inst.J) * len(inst.V):.0f} route binary variables per scenario")

# === DEBUG: Print instance details ===
print(f"\n{'='*60}")
print(f"INSTANCE DETAILS (DEBUG)")
print(f"{'='*60}")

# Vehicle parameters
print(f"\nVehicle Parameters:")
print(f"  Battery capacity (C): {inst.C} kWh")
print(f"  Initial SoC (c0): {inst.c0} kWh ({inst.c0/inst.C*100:.0f}%)")
print(f"  Min SoC: {0.2*inst.C} kWh (20%)")
print(f"  Usable energy: {inst.c0 - 0.2*inst.C:.1f} kWh")
print(f"  Load capacity (L): {inst.L} kg")
print(f"  Energy consumption: {inst.P_min} - {inst.P_max} kWh/km")

# Estimate max range
avg_consumption = (inst.P_min + inst.P_max) / 2
max_range = (inst.c0 - 0.2 * inst.C) / avg_consumption
print(f"  Estimated max range (avg load): {max_range:.1f} km")

# Distance statistics
distances = [d for d in inst.l.values() if d > 0]
print(f"\nDistance Statistics:")
print(f"  Min distance: {min(distances):.2f} km")
print(f"  Max distance: {max(distances):.2f} km")
print(f"  Avg distance: {sum(distances)/len(distances):.2f} km")

# Distances from depot
depot_distances = [(j, inst.l.get((inst.i0, j), 0)) for j in inst.J]
print(f"\nDistances from depot (node {inst.i0}):")
for j, d in sorted(depot_distances, key=lambda x: x[1]):
    print(f"  Customer {j}: {d:.2f} km")

# Charger types
print(f"\nCharger Types (available for {vehicle_type}):")
for tau in inst.T:
    daily_cost = inst.e.get((inst.J[0], tau), 0)
    print(f"  Type {tau}: {inst.kappa[tau]} kW, daily cost: {daily_cost:.2f} EUR")

# Scenario demands
print(f"\nScenario Demands (kg per customer):")
print(f"{'Scenario':<10}", end="")
for j in inst.J:
    print(f"{j:>8}", end="")
print(f"{'Total':>10}")
print("-" * (10 + 8*len(inst.J) + 10))
for s in inst.S:
    print(f"{s:<10}", end="")
    total = 0
    for j in inst.J:
        demand = inst.beta.get((s, j), 0)
        total += demand
        print(f"{demand:>8.0f}", end="")
    print(f"{total:>10.0f}")

print(f"\n{'='*60}")
print(f"RUNNING ALGORITHM")
print(f"{'='*60}\n")

# Create application
type = AlgorithmType()
appl = Application(
    inst=inst,
    MasterModel=model.MasterModel,
    SecondStageModel=model.SecondStageModel
)

start_sc = []

params = AlgorithmParams(
    app=appl,
    start_sc=start_sc,
    desired_gap=gap,
    MASTER_P=gap,
    HEURTIMELIMIT=0.1,
    total_timelimit=timelimit,
    n_threads=gurobi_threads
)

try:
    if alg == "our":
        s = ourAlgorithm(params=params, type=type)
    elif alg == "toen":
        s = toenAlgorithm(params=params)
    elif alg == "rodr":
        s = rodrAlgorithm(params=params)
    else:
        raise Exception("Wrong alg name given.")
    timeout_reached = False
except TimeoutException as tex:
    s = tex.stats
    s.TIME_TOT = timelimit
    s.reached_gap = tex.reached_gap
    timeout_reached = True

print(f"\n{'='*60}")
print(f"RESULTS")
print(f"{'='*60}")
print(f"Time total: {s.TIME_TOT_PROC:.3f}s")
print(f"Time Master: {s.TIME_MASTER_PROC:.3f}s")
print(f"Time Second Stage: {s.TIME_SS_PROC:.3f}s")
print(f"Reached gap: {s.reached_gap:.5f}")
print(f"Iterations: {s.ITERATIONS}")
print(f"Timeout reached: {timeout_reached}")

# Print installed chargers
print(f"\n{'='*60}")
print(f"INSTALLED CHARGERS")
print(f"{'='*60}")

# Charger type names for display
charger_names = {
    1: "22kW AC",
    2: "43kW AC",
    3: "40kW DC",
    4: "50kW DC",
    5: "90kW DC",
    6: "120kW DC",
    7: "150kW DC",
    8: "250kW DC"
}

# Get first-stage solution from the final master model
# first_stage is a dict of customer chargers; warehouse charger is fixed (22 kW AC)
first_stage = s.first_stage
if first_stage:
    # Customer chargers only; warehouse charger is fixed (22 kW AC)
    a_dict = first_stage[0] if isinstance(first_stage, tuple) else first_stage
    a_wh_dict = {model.WAREHOUSE_CHARGER_TYPE: 1}

    # Customer chargers
    installed = []
    total_charger_cost = 0
    for (j, tau), val in a_dict.items():
        if val >= 0.5:  # Binary variable is 1
            charger_name = charger_names.get(tau, f"Type {tau}")
            cost = inst.e.get((j, tau), 0)
            installed.append((j, tau, charger_name, cost))
            total_charger_cost += cost

    # Warehouse chargers
    wh_installed = []
    for tau, val in a_wh_dict.items():
        if val >= 0.5:
            charger_name = charger_names.get(tau, f"Type {tau}")
            cost = inst.e_wh.get(tau, 0)
            wh_installed.append((tau, charger_name, cost))
            total_charger_cost += cost

    # Print warehouse chargers
    if wh_installed:
        print(f"\nWarehouse Chargers:")
        print("-" * 40)
        for tau, name, cost in wh_installed:
            print(f"{'Warehouse':<10} {name:<15} {cost:>10,.0f}")

    # Print customer chargers
    if installed:
        installed.sort(key=lambda x: x[0])  # Sort by customer
        print(f"\nCustomer Chargers:")
        print(f"{'Customer':<10} {'Charger Type':<15} {'Cost (EUR)':<12}")
        print("-" * 40)
        for j, tau, name, cost in installed:
            print(f"{j:<10} {name:<15} {cost:>10,.0f}")
        print("-" * 40)
        print(f"{'TOTAL':<10} {'':<15} {total_charger_cost:>10,.0f}")
        print(f"\nTotal chargers installed: {len(wh_installed)} warehouse + {len(installed)} customer = {len(wh_installed) + len(installed)}")
    else:
        print("No customer chargers were installed.")
        if wh_installed:
            print(f"Total chargers: {len(wh_installed)} (warehouse only)")
else:
    print("First-stage solution not available.")

# === ROUTE EXTRACTION AND VISUALIZATION ===
# Use the algorithm's first-stage solution to extract routes for each scenario
# and generate visualizations in one pass (no duplicate model solving)
print(f"\n{'='*60}")
print(f"ROUTE EXTRACTION & VISUALIZATION")
print(f"{'='*60}")

# Get the first-stage solution from algorithm
if not first_stage:
    print("\nNo first-stage solution available - cannot extract routes.")
    print("This typically happens when the optimization times out without finding a feasible solution.")
    exit(0)

a_sol = first_stage[0] if isinstance(first_stage, tuple) else first_stage

import gurobipy as gp
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import pandas as pd
import numpy as np
from pathlib import Path
from route_map_export import plot_unused_customers, plot_warehouse
from json_artifacts import read_table_rows

project_root = Path(__file__).resolve().parents[2]
coords_file = project_root / "exports" / "cspp" / "data" / "coordinates.json"
if not coords_file.exists():
    coords_file = Path(__file__).parent / "core" / "applications" / "cspp" / "instances" / "coordinates.json"

if not coords_file.exists():
    print(f"\nWARNING: Coordinates file not found: {coords_file}")
    print("Run generate_instance_data.py first to generate coordinates.")
else:
    node_coords = {}
    for row in read_table_rows(coords_file):
        try:
            node_idx = int(row["node_index"])
            node_coords[node_idx] = (float(row["latitude"]), float(row["longitude"]))
        except (KeyError, TypeError, ValueError):
            continue
    
    # Output directory
    output_dir = Path(__file__).resolve().parents[2] / "exports" / "cspp" / "figures" / "route_maps"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\nFirst-stage charger installations:")
    installed_chargers = [(j, tau) for j in inst.J for tau in inst.T if a_sol.get((j, tau), 0) >= 0.5]
    if installed_chargers:
        for j, tau in installed_chargers:
            print(f"  Node {j}: {charger_names.get(tau, f'Type {tau}')} ({inst.kappa[tau]} kW)")
    else:
        print("  None")
    
    print(f"\nOutput directory: {output_dir}")
    print(f"\nProcessing scenarios: {inst.S}")
    
    for scenario in inst.S:
        print(f"\n--- Scenario {scenario} ---")
        
        # Check demand
        total_scenario_demand = sum(inst.beta.get((scenario, j), 0) for j in inst.J)
        if total_scenario_demand == 0:
            print(f"  Skipping - no demand")
            continue
        
        # Solve second-stage model (only time we solve it)
        ss_model = model.SecondStageModel(inst, scenario, first_stage, name=f"Route_{scenario}")
        ss_model.Params.OutputFlag = 0
        ss_model.Params.Threads = gurobi_threads
        ss_model.Params.MIPGap = 0.05
        ss_model.Params.TimeLimit = 60  # 60 second limit per scenario for visualization
        ss_model.optimize()
        
        # Report solve status
        status_names = {
            gp.GRB.OPTIMAL: "OPTIMAL",
            gp.GRB.SUBOPTIMAL: "SUBOPTIMAL",
            gp.GRB.TIME_LIMIT: "TIME_LIMIT",
            gp.GRB.INFEASIBLE: "INFEASIBLE",
            gp.GRB.INF_OR_UNBD: "INF_OR_UNBD",
            gp.GRB.UNBOUNDED: "UNBOUNDED"
        }
        status_str = status_names.get(ss_model.Status, f"STATUS_{ss_model.Status}")

        if ss_model.Status not in [gp.GRB.OPTIMAL, gp.GRB.SUBOPTIMAL, gp.GRB.TIME_LIMIT]:
            print(f"  Model {status_str}")
            continue

        # Check if we have a feasible solution (even if time limit hit)
        if ss_model.SolCount == 0:
            print(f"  {status_str} - No feasible solution found")
            continue

        # Report solution quality
        obj_val = ss_model.ObjVal
        obj_bound = ss_model.ObjBound
        gap = abs(obj_val - obj_bound) / max(abs(obj_val), 1e-10) * 100
        solve_time = ss_model.Runtime
        print(f"  {status_str} in {solve_time:.1f}s | Obj: {obj_val:.2f} | Bound: {obj_bound:.2f} | Gap: {gap:.1f}%")
        
        # Get solution (supports both 10- and 11-element return signatures).
        ss_sol = ss_model.get_second_stage_solution()
        if len(ss_sol) < 10:
            print(f"  Unexpected second-stage solution size: {len(ss_sol)}")
            continue
        y_sol, u_sol, t_sol, r_sol, c_arr_sol, p_sol, omega_sol, c_dep_sol, c_ret_sol, p_wh_sol = ss_sol[:10]

        depot = inst.i0
        K_range = range(1, inst.K_max + 1)
        M_range = range(1, inst.M_max + 1)

        # Extract routes per truck and tour
        routes_data = []
        for k in K_range:
            if y_sol.get(k, 0) < 0.5:
                continue  # Truck not used

            for m in M_range:
                if u_sol.get((k, m), 0) < 0.5:
                    continue  # Tour not used

                # Find arcs used by this truck/tour
                tour_arcs = [(v1, v2) for (v1, v2, tk, tm), val in r_sol.items()
                            if tk == k and tm == m and val >= 0.5]

                if not tour_arcs:
                    continue

                # Build route starting from depot
                route = [depot]
                current = depot
                visited = {depot}

                # Find first customer from depot
                for v1, v2 in tour_arcs:
                    if v1 == depot:
                        current = v2
                        route.append(current)
                        visited.add(current)
                        break

                # Follow the tour
                while current != depot:
                    next_node = None
                    for v1, v2 in tour_arcs:
                        if v1 == current and (v2 not in visited or v2 == depot):
                            next_node = v2
                            break

                    if next_node is None:
                        break

                    if next_node == depot:
                        route.append(depot)
                        break

                    route.append(next_node)
                    visited.add(next_node)
                    current = next_node

                routes_data.append({
                    'truck': k,
                    'tour': m,
                    'route': route,
                    'c_dep': c_dep_sol.get((k, m), 0),
                    'c_ret': c_ret_sol.get((k, m), 0),
                    'p_wh': p_wh_sol.get((k, m), 0)
                })

        # Calculate totals (summing over all trucks and tours)
        total_charged = sum(p_sol.get((j, k, m), 0) for k in K_range for m in M_range for j in inst.J)
        total_wait = sum(omega_sol.get((j, k, m), 0) for k in K_range for m in M_range for j in inst.J)
        total_warehouse_recharge = sum(p_wh_sol.get((k, m), 0) for k in K_range for m in M_range)
        active_customers = [j for j in inst.J if inst.beta.get((scenario, j), 0) > 0]
        
        # Print operational summary
        print(f"    Routes: {len(routes_data)} | Charged: {total_charged:.1f}kWh | Wait: {total_wait:.2f}h | WH recharge: {total_warehouse_recharge:.1f}kWh")
        
        # === Generate PNG visualization ===
        fig, ax = plt.subplots(1, 1, figsize=(14, 12))
        plot_unused_customers(ax, node_coords, inst.J, active_customers, size=50, alpha=0.35)
        plot_warehouse(ax, node_coords, depot, annotate=True)
        
        # Plot active customers
        for j in active_customers:
            if j not in node_coords:
                continue
            lat, lon = node_coords[j]
            
            charger_power = None
            for tau in inst.T:
                if a_sol.get((j, tau), 0) >= 0.5:
                    charger_power = inst.kappa[tau]
                    break
            
            extra_wait = omega_sol.get(j, 0)
            color = 'green' if charger_power else 'orange'
            
            ax.scatter(lon, lat, s=300, c=color, edgecolors='black', linewidth=1.5, zorder=4)
            
            label_parts = [str(j)]
            if charger_power:
                label_parts.append(f"{int(charger_power)}kW")
            if extra_wait > 0.01:
                label_parts.append(f"+{extra_wait:.1f}h")
            
            ax.annotate('\n'.join(label_parts), (lon, lat), textcoords="offset points",
                       xytext=(0, -18), ha='center', fontsize=9, 
                       fontweight='bold' if charger_power else 'normal',
                       color='darkgreen' if charger_power else 'black')
        
        # Draw route arrows
        route_colors = ['red', 'purple', 'brown', 'teal', 'magenta']
        for route_idx, route_data in enumerate(routes_data):
            route = route_data['route']
            route_color = route_colors[route_idx % len(route_colors)]
            for i in range(len(route) - 1):
                v1, v2 = route[i], route[i + 1]
                if v1 in node_coords and v2 in node_coords:
                    lat1, lon1 = node_coords[v1]
                    lat2, lon2 = node_coords[v2]
                    ax.annotate('', xy=(lon2, lat2), xytext=(lon1, lat1),
                               arrowprops=dict(arrowstyle='->', color=route_color, lw=2, mutation_scale=15), zorder=3)
        
        ax.set_xlabel('Longitude', fontsize=12, fontweight='bold')
        ax.set_ylabel('Latitude', fontsize=12, fontweight='bold')
        ax.set_title(f'Scenario {scenario} | Green=charger, Orange=no charger', fontsize=14, fontweight='bold', pad=20)
        ax.grid(True, alpha=0.3, linestyle='--', zorder=1)
        
        textstr = f'Demand: {total_scenario_demand:,.0f}kg\nRoutes: {len(routes_data)}\nCharged: {total_charged:.1f}kWh\nWait: {total_wait:.2f}h'
        ax.text(0.02, 0.98, textstr, transform=ax.transAxes, fontsize=10, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8), family='monospace')
        
        from matplotlib.lines import Line2D
        ax.legend(handles=[
            Line2D([0], [0], marker='s', color='w', markerfacecolor='blue', markersize=12, label='Warehouse'),
            Line2D([0], [0], marker='o', color='w', markerfacecolor='green', markersize=10, label='Charger'),
            Line2D([0], [0], marker='o', color='w', markerfacecolor='orange', markersize=10, label='No charger'),
        ], loc='upper right')
        
        plt.tight_layout()
        plt.savefig(output_dir / f"scenario_{scenario}.png", dpi=300, bbox_inches='tight')
        plt.close(fig)
        
        # === Generate TXT report ===
        with open(output_dir / f"scenario_{scenario}.txt", 'w', encoding='utf-8') as f:
            f.write(f"SCENARIO {scenario} | Demand: {total_scenario_demand:,.0f}kg | Routes: {len(routes_data)} | Charged: {total_charged:.1f}kWh | Wait: {total_wait:.2f}h | WH_recharge: {total_warehouse_recharge:.1f}kWh\n")
            f.write("=" * 100 + "\n\n")

            for route_idx, route_data in enumerate(routes_data, 1):
                route = route_data['route']
                k = route_data['truck']
                m = route_data['tour']
                c_dep = route_data['c_dep']
                c_ret = route_data['c_ret']
                p_wh = route_data['p_wh']

                route_distance = sum(inst.l.get((route[i], route[i+1]), 0) for i in range(len(route)-1))
                route_demand = sum(inst.beta.get((scenario, n), 0) for n in route if n != depot)
                route_charged = sum(p_sol.get((n, k, m), 0) for n in route if n != depot)
                route_wait = sum(omega_sol.get((n, k, m), 0) for n in route if n != depot)

                f.write(f"TRUCK {k}, TOUR {m}: {' -> '.join(str(n) for n in route)}\n")
                f.write(f"Distance: {route_distance:.1f}km | Demand: {route_demand:,.0f}kg | Charged: {route_charged:.1f}kWh | Wait: {route_wait:.2f}h\n")
                f.write(f"SoC_dep: {c_dep:.1f}kWh | SoC_ret: {c_ret:.1f}kWh | WH_recharge: {p_wh:.1f}kWh\n")
                f.write("-" * 100 + "\n")
                f.write(f"{'Stop':<5} {'Node':<6} {'Dist':<8} {'SoC_arr':<10} {'Demand':<10} {'Charged':<10} {'Wait':<8} {'Charger':<10}\n")
                f.write("-" * 100 + "\n")

                for stop_idx, node in enumerate(route):
                    if node == depot:
                        if stop_idx == 0:
                            f.write(f"{stop_idx:<5} {node:<6} {'-':<8} {c_dep:<10.1f} {'-':<10} {'-':<10} {'-':<8} {'-':<10}\n")
                        else:
                            dist_to_depot = inst.l.get((route[stop_idx - 1], depot), 0)
                            wh_str = f"{p_wh:.1f}" if p_wh > 0.01 else "-"
                            f.write(f"{stop_idx:<5} {node:<6} {dist_to_depot:<8.1f} {c_ret:<10.1f} {'-':<10} {wh_str:<10} {'-':<8} {'-':<10}\n")
                    else:
                        soc_arr = c_arr_sol.get((node, k, m), 0)
                        charged = p_sol.get((node, k, m), 0)
                        wait = omega_sol.get((node, k, m), 0)
                        demand = inst.beta.get((scenario, node), 0)
                        prev_node = route[stop_idx - 1]
                        dist_from_prev = inst.l.get((prev_node, node), 0)

                        charger_str = "-"
                        for tau in inst.T:
                            if a_sol.get((node, tau), 0) >= 0.5:
                                charger_str = f"{int(inst.kappa[tau])}kW"
                                break

                        charged_str = f"{charged:.1f}" if charged > 0.01 else "-"
                        wait_str = f"{wait:.2f}" if wait > 0.01 else "-"

                        f.write(f"{stop_idx:<5} {node:<6} {dist_from_prev:<8.1f} {soc_arr:<10.1f} {demand:<10.0f} {charged_str:<10} {wait_str:<8} {charger_str:<10}\n")

                f.write("\n")
        
        print(f"  Exports: scenario_{scenario}.png, scenario_{scenario}.txt")
    
    print(f"\nAll exports saved to: {output_dir}")
