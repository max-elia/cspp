import json
import re
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from json_artifacts import read_table_rows
from lieferdaten.runtime import get_run_layout
from lieferdaten.runtime import resolve_run_root


def plot_unused_customers(
    ax,
    node_coords,
    all_customer_nodes,
    used_customer_nodes,
    *,
    size=50,
    alpha=0.35,
    zorder=2,
):
    """Plot customers without service in muted gray for consistent map exports."""
    unused_nodes = sorted(set(all_customer_nodes) - set(used_customer_nodes))
    for node in unused_nodes:
        if node not in node_coords:
            continue
        lat, lon = node_coords[node]
        ax.scatter(
            lon,
            lat,
            s=size,
            c="lightgray",
            edgecolors="gray",
            linewidth=0.5,
            alpha=alpha,
            zorder=zorder,
        )


def plot_warehouse(ax, node_coords, warehouse_node=0, *, annotate=False):
    """Plot the warehouse marker with shared styling."""
    if warehouse_node not in node_coords:
        return

    lat, lon = node_coords[warehouse_node]
    ax.scatter(
        lon,
        lat,
        s=400,
        c="blue",
        marker="s",
        edgecolors="darkblue",
        linewidth=2,
        zorder=5,
        label="Warehouse",
    )

    if annotate:
        ax.annotate(
            str(warehouse_node),
            (lon, lat),
            textcoords="offset points",
            xytext=(0, 12),
            ha="center",
            fontsize=10,
            fontweight="bold",
            color="darkblue",
        )


def load_node_coordinates(results_dir):
    """Load node coordinates from run data with fallback to default instances."""
    results_dir = Path(results_dir)
    project_root = Path(__file__).resolve().parents[2]
    run_dir = resolve_run_root(results_dir)
    run_layout = get_run_layout(run_dir)

    candidates = [
        run_layout["cspp_data"] / "coordinates.json",
        project_root / "src" / "cspp" / "core" / "applications" / "cspp" / "instances" / "coordinates.json",
    ]

    coords_file = None
    for candidate in candidates:
        if candidate.exists():
            coords_file = candidate
            break

    if coords_file is None:
        return {}

    node_coords = {}
    for row in read_table_rows(coords_file):
        try:
            node_idx = int(row["node_index"])
            lat = float(row["latitude"])
            lon = float(row["longitude"])
        except Exception:
            continue
        node_coords[node_idx] = (lat, lon)
    return node_coords


def _scenario_from_path(path):
    match = re.search(r"scenario_(\d+)_solution\.json$", path.name)
    return int(match.group(1)) if match else None


def export_route_maps_from_global_solutions(
    results_dir,
    solution_dir,
    maps_dir,
    file_prefix,
    title_prefix,
    warehouse_node=0,
):
    """
    Export route-map figures from global solution JSON files.

    Returns number of exported PNG files.
    """
    solution_dir = Path(solution_dir)
    maps_dir = Path(maps_dir)
    maps_dir.mkdir(parents=True, exist_ok=True)

    if not solution_dir.exists():
        return 0

    node_coords = load_node_coordinates(results_dir)
    if not node_coords:
        return 0

    scenario_files = sorted(
        solution_dir.glob("scenario_*_solution.json"),
        key=lambda p: (_scenario_from_path(p) is None, _scenario_from_path(p) or 0),
    )
    if not scenario_files:
        return 0

    exported = 0
    cmap = plt.colormaps.get_cmap("tab20")

    for path in scenario_files:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        scenario = data.get("scenario")
        if scenario is None:
            scenario = _scenario_from_path(path)
        routes = data.get("routes", [])
        customer_charging = data.get("customer_charging", [])
        charged_customers = {int(item.get("customer")) for item in customer_charging if item.get("customer") is not None}

        fig, ax = plt.subplots(1, 1, figsize=(16, 14))
        all_customer_nodes = {node for node in node_coords if node != warehouse_node}
        plot_warehouse(ax, node_coords, warehouse_node)

        route_nodes = set()
        color_lookup = {}
        next_color_idx = 0

        for route in routes:
            try:
                u = int(route.get("from"))
                v = int(route.get("to"))
            except Exception:
                continue
            route_nodes.add(u)
            route_nodes.add(v)
            if u not in node_coords or v not in node_coords:
                continue
            route_key = (int(route.get("truck", 0)), int(route.get("tour", 0)))
            if route_key not in color_lookup:
                color_lookup[route_key] = cmap(next_color_idx % 20)
                next_color_idx += 1
            color = color_lookup[route_key]

            lat1, lon1 = node_coords[u]
            lat2, lon2 = node_coords[v]
            ax.plot([lon1, lon2], [lat1, lat2], c=color, alpha=0.65, linewidth=1.6, zorder=3)

            mid_lon = (lon1 + lon2) / 2.0
            mid_lat = (lat1 + lat2) / 2.0
            ax.annotate(
                "",
                xy=(mid_lon, mid_lat),
                xytext=(lon1, lat1),
                arrowprops=dict(arrowstyle="->", color=color, lw=1.4),
                zorder=3,
            )

        customer_nodes = [n for n in route_nodes if n != warehouse_node]
        plot_unused_customers(ax, node_coords, all_customer_nodes, customer_nodes)

        for node in customer_nodes:
            if node not in node_coords:
                continue
            lat, lon = node_coords[node]
            has_charging = node in charged_customers
            ax.scatter(
                lon, lat, s=130,
                c=("green" if has_charging else "orange"),
                edgecolors="black", linewidth=1, zorder=4,
            )
            ax.annotate(str(node), (lon, lat), xytext=(0, -10), textcoords="offset points", ha="center", fontsize=7, zorder=5)

        objective = data.get("objective")
        objective_str = f"{objective:.2f} EUR" if isinstance(objective, (int, float)) else "N/A"
        ax.set_title(f"{title_prefix} - Scenario {scenario}\nCost: {objective_str} | Arcs: {len(routes)}", fontsize=16)
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")

        map_path = maps_dir / f"{file_prefix}_scenario_{scenario}.png"
        plt.savefig(map_path, dpi=300, bbox_inches="tight")
        plt.close(fig)
        exported += 1

    return exported
