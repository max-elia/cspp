"""
Data loaders for CSPP instances.

Loads instance data from JSON table artifacts.
"""

from pathlib import Path
from typing import Dict, List, Optional, Tuple

from json_artifacts import read_table_rows

from ..charger_config import CHARGER_TYPES


def load_distance_matrix(filepath: str, distance_multiplier: float = 1.0) -> Dict[Tuple[int, int], float]:
    """
    Load distance matrix from JSON table file.

    Args:
        filepath: Path to distances_matrix.json
        distance_multiplier: Factor to multiply all distances by (default: 1.0)

    Returns:
        Dictionary mapping (from_node, to_node) to distance in km
    """
    distances = {}
    for row in read_table_rows(filepath):
        from_node = int(row["node_index"])
        for key, value in row.items():
            if key == "node_index" or value in {None, ""}:
                continue
            distances[(from_node, int(key))] = float(value) * distance_multiplier
    return distances


def load_charger_types(filepath: Optional[str] = None, vehicle_type: str = "mercedes") -> Tuple[List[int], Dict[int, float], Dict[int, float]]:
    """
    Load charger types from config (default) or JSON table file (optional override).
    """
    charger_types: list[int] = []
    power_by_type: dict[int, float] = {}
    costs_by_type: dict[int, float] = {}

    rows = read_table_rows(filepath) if filepath is not None and Path(filepath).exists() else CHARGER_TYPES

    for row in rows:
        type_id = int(row["type_id"])
        volvo_available = str(row["volvo_available"]).lower() == "true"
        mercedes_available = str(row["mercedes_available"]).lower() == "true"

        if vehicle_type == "mercedes" and not mercedes_available:
            continue
        if vehicle_type == "volvo" and not volvo_available:
            continue

        charger_types.append(type_id)
        power_by_type[type_id] = float(row["power_kw"])
        costs_by_type[type_id] = float(row["total_cost_eur"])

    return charger_types, power_by_type, costs_by_type


def load_arc_set(filepath: str) -> set:
    """
    Load sparse arc set from JSON table file.
    """
    arcs = set()
    target = Path(filepath)
    if not target.exists():
        return None
    for row in read_table_rows(target):
        arcs.add((int(row["from_node"]), int(row["to_node"])))
    return arcs


def load_demand_scenarios(filepath: str, demand_multiplier: float = 1.0) -> Tuple[List[int], Dict[Tuple[int, int], float], Dict[Tuple[int, int], float], List[int]]:
    """
    Load demand scenarios from JSON table file.
    """
    scenarios: list[int] = []
    beta: dict[tuple[int, int], float] = {}
    delta: dict[tuple[int, int], float] = {}
    rows = read_table_rows(filepath)
    customers = [int(key) for key in rows[0].keys() if key != "delivery_date"] if rows else []

    scenario_id = 1
    for row in rows:
        if not row or not row.get("delivery_date"):
            continue

        scenarios.append(scenario_id)
        for customer in customers:
            raw_value = row.get(str(customer))
            demand_kg = float(raw_value) * demand_multiplier if raw_value not in {None, ""} else 0.0
            beta[(scenario_id, customer)] = demand_kg
            delta[(scenario_id, customer)] = 0.25 + 0.00005 * demand_kg if demand_kg > 0 else 0.0
        scenario_id += 1

    return scenarios, beta, delta, customers
