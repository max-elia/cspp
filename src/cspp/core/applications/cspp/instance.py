from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional
import math
from pathlib import Path
from json_artifacts import read_json
from .. import InstanceStrings
from lieferdaten.unloading_model import load_unloading_time_model
from lieferdaten.unloading_model import unloading_time_hours
from lieferdaten.runtime import get_run_layout
from cspp.fixed_costs import default_fixed_truck_cost


def annualize_charger_cost(total_cost: float, lifespan_years: int = 10, operating_days_per_year: int = 290) -> float:
    """
    Convert charger installation cost to daily equivalent cost (simple annualization).

    Args:
        total_cost: Total installation cost in EUR
        lifespan_years: Expected charger lifespan in years (default: 10)
        operating_days_per_year: Operating days per year (default: 290)

    Returns:
        Daily equivalent cost in EUR
    """
    return total_cost / (lifespan_years * operating_days_per_year)


def calculate_unloading_time(demand_kg: float) -> float:
    """
    Calculate unloading time in hours using the shared global unloading model.

    Args:
        demand_kg: Demand in kilograms

    Returns:
        Unloading time in hours
    """
    return unloading_time_hours(demand_kg)


@dataclass
class Instance:
    """
    Instance data for the Charging Station Placement Problem (CSPP).
    """

    # === Network Structure ===
    i0: int = 0  # Warehouse (depot) index
    J: List[int] = field(default_factory=list)  # Set of customers
    J_base: List[int] = field(default_factory=list)  # Set of base customers (locations)
    T: List[int] = field(default_factory=list)  # Set of charger types

    # V = {i0} ∪ J: All nodes (computed in __post_init__)
    V: List[int] = field(default_factory=list)

    # Arc lengths: l[v1, v2] = length of arc (v1, v2)
    l: Dict[Tuple[int, int], float] = field(default_factory=dict)

    # Allowed arcs (sparse): A = set of (v1, v2) tuples. If None, all arcs allowed.
    A: Optional[set] = None

    # === Warehouse Parameters ===
    W: float = 0.0  # Warehouse capacity (tons)
    warehouse_overnight_hours: float = 10.0  # Hours available for overnight depot charging
    warehouse_loading_time_factor: float = 0.6  # Share of modeled loading time usable for charging

    # === Fleet Parameters ===
    K_max: int = 3  # Maximum number of available trucks
    M_max: int = 2  # Maximum number of tours per truck
    L: float = 0.0  # Vehicle load capacity (tons)
    C: float = 0.0  # Vehicle battery capacity (kWh)
    c0: float = 0.0  # SoC at departure from warehouse and target SoC after final return (kWh)
    P_min: float = 0.0  # kWh consumption per km for empty truck
    P_max: float = 0.0  # kWh consumption per km for fully loaded truck
    F: float = 0.0  # Fixed cost per used truck

    # === Charging Station Parameters ===
    # e[j, tau] = fixed cost for installing charger type tau at customer j
    e: Dict[Tuple[int, int], float] = field(default_factory=dict)
    # e_wh[tau] = fixed cost for installing charger type tau at warehouse
    e_wh: Dict[int, float] = field(default_factory=dict)
    # kappa[tau] = charging speed of charger type tau (kW)
    kappa: Dict[int, float] = field(default_factory=dict)
    d_cost: float = 0.0  # Cost per kWh (renamed from 'd' to avoid conflict with dict)
    h: float = 0.0  # Cost per hour of extra waiting time
    M_big: float = 0.0  # Big-M constant (computed in __post_init__ if not set)

    # === Scenario Parameters ===
    S: List[int] = field(default_factory=list)  # Set of scenarios
    # beta[s, j] = demand of customer j in scenario s
    beta: Dict[Tuple[int, int], float] = field(default_factory=dict)
    # delta[s, j] = unloading time at customer j in scenario s (hours), proportional to demand
    delta: Dict[Tuple[int, int], float] = field(default_factory=dict)

    # Optional metadata
    name: str = ""
    # Split-delivery mappings (pseudo-customer -> base customer)
    pseudo_to_base: Dict[int, int] = field(default_factory=dict)
    base_to_pseudo: Dict[int, List[int]] = field(default_factory=dict)

    # Application interface
    strings: InstanceStrings = field(default_factory=InstanceStrings)
    
    def __post_init__(self):
        """Compute derived sets after initialization."""
        if not self.J_base:
            self.J_base = list(self.J)
        if not self.pseudo_to_base or not self.base_to_pseudo:
            self.pseudo_to_base = {j: j for j in self.J}
            self.base_to_pseudo = {j: [j] for j in self.J}
        self.V = [self.i0] + self.J

        # Compute Big-M if not set (sufficient for SoC constraints)
        if self.M_big == 0.0 and self.C > 0:
            self.M_big = 2 * self.C

        # Initialize strings
        n_customers = len(self.J)
        n_types = len(self.T)
        n_scenarios = len(self.S)
        self.strings.ALG_INTRO_TEXT = f"CSPP Algorithm for {self.name} ({n_customers} customers, {n_types} types, {n_scenarios} scenarios)\n"
        self.strings.UNIQUE_IDENTIFIER = f"{self.name}-{n_customers}-{n_types}-{n_scenarios}"

    @property
    def scenarios(self) -> List[int]:
        """Convenience accessor for the scenario index set."""
        return self.S

    @staticmethod
    def createInstance(
        vehicle_type: str = "volvo",
        instances_dir: Optional[str] = None,
        arc_set_path: Optional[str] = None,
        d_cost: float = 0.30,  # EUR per kWh
        h: float = 50.0,  # EUR per hour waiting
        F: Optional[float] = None,  # EUR per truck/day (if None, uses vehicle-specific daily default)
        K_max: int = 3,  # Maximum number of available trucks
        M_max: int = 2,  # Maximum number of tours per truck
        charger_lifespan_years: int = 10,  # Charger lifespan for cost annualization
        operating_days_per_year: int = 290,  # Operating days per year
        charger_cost_multiplier: float = 1.0,  # Multiplier on charger installation costs
        distance_multiplier: float = 1.0,  # Multiply all distances by this factor (for testing)
        demand_multiplier: float = 1.0,  # Multiply all demands by this factor (for testing)
        initial_soc_fraction: float = 0.8,  # Initial SoC as fraction of battery capacity (0.0-1.0)
        warehouse_overnight_hours: float = 10.0,  # Hours available for overnight depot charging
        warehouse_loading_time_factor: float = 0.6,  # Share of warehouse loading time usable for charging
        name: Optional[str] = None,
        split_deliveries: bool = True  # Pre-split demands into pseudo-customers (Option C)
    ) -> "Instance":
        """
        Create a CSPP instance for the specified vehicle type.

        Args:
            vehicle_type: "mercedes" for Mercedes-Benz eActros 300,
                          "volvo" for Volvo FM Electric
            instances_dir: Directory containing CSV files (default: exports/cspp/data)
            arc_set_path: Ignored. CSPP always uses the full arc set.
            d_cost: Cost per kWh of electricity
            h: Cost per hour of extra waiting time
            F: Fixed cost per used truck-day (default: vehicle-specific daily value if None)
            K_max: Maximum number of available trucks (default: 3)
            M_max: Maximum number of tours per truck (default: 2)
            charger_lifespan_years: Expected lifespan of chargers
            operating_days_per_year: Operating days per year
            charger_cost_multiplier: Multiplier applied to charger installation costs
            distance_multiplier: Multiply all distances by this factor (for testing harder instances)
            demand_multiplier: Multiply all demands by this factor (for testing, e.g., 0.5 = half demand)
            initial_soc_fraction: Initial state of charge as fraction of battery capacity (default: 0.8)
            warehouse_overnight_hours: Hours available for overnight charging at the depot
            warehouse_loading_time_factor: Share of modeled loading time assumed usable for charging
            name: Optional instance name
            split_deliveries: Pre-split customer demands into pseudo-customers (Option C)

        Returns:
            Configured Instance object
        """
        # Import loaders here to avoid circular imports
        from .instances import load_distance_matrix, load_charger_types, load_demand_scenarios

        import os

        if instances_dir is None:
            env_run_dir = os.environ.get("RUN_DIR")
            if env_run_dir:
                instances_dir = get_run_layout(Path(env_run_dir))["cspp_data"]
            else:
                raise FileNotFoundError("instances_dir was not provided and RUN_DIR is not set.")
        else:
            instances_dir = Path(instances_dir)

        def resolve_required_table(filename: str) -> Path:
            candidate = instances_dir / filename
            if candidate.exists():
                return candidate
            raise FileNotFoundError(f"Missing required CSPP input '{filename}': {candidate}")

        def resolve_optional_table(filename: str) -> Optional[Path]:
            candidate = instances_dir / filename
            if candidate.exists():
                return candidate
            return None

        # Load instance tables (JSON)
        l = load_distance_matrix(
            resolve_required_table("distances_matrix.json"),
            distance_multiplier=distance_multiplier
        )
        charger_types_path = resolve_optional_table("charger_types.json")
        T, kappa, base_costs = load_charger_types(
            str(charger_types_path) if charger_types_path is not None else None,
            vehicle_type
        )
        S, beta, _, customers = load_demand_scenarios(
            resolve_required_table("demand_matrix.json"),
            demand_multiplier=demand_multiplier
        )

        # Always solve on the full graph. Keep arc_set_path in the signature so
        # older callers do not break, but ignore any sparse arc-set files.
        arc_set = None

        # Vehicle parameters (using kg everywhere)
        if vehicle_type == "mercedes":
            L = 17700.0  # 17.7 tons in kg
            C = 336.0  # kWh
            P_min = 0.90  # kWh/km
            P_max = 1.40  # kWh/km
            vehicle_name = "Mercedes-Benz eActros 300"
        elif vehicle_type == "volvo":
            L = 16200.0  # 16.2 tons in kg
            C = 360.0  # kWh
            P_min = 0.95  # kWh/km
            P_max = 1.45  # kWh/km
            vehicle_name = "Volvo FM Electric"
        else:
            raise ValueError(f"Unknown vehicle type: {vehicle_type}")

        # Network structure
        i0 = 0  # Warehouse
        J_base = customers  # Original customers from demand data

        # Option C: split demands into pseudo-customers (allow multiple trips)
        if split_deliveries:
            base_to_pseudo: Dict[int, List[int]] = {}
            pseudo_to_base: Dict[int, int] = {}
            J_split: List[int] = []
            next_id = (max(J_base) if J_base else 0) + 1

            for j in J_base:
                max_demand = max(beta.get((s, j), 0) for s in S) if S else 0
                n_splits = max(1, int(math.ceil(max_demand / L))) if max_demand > 0 else 1
                pseudo_list = [j]
                for _ in range(1, n_splits):
                    pseudo_list.append(next_id)
                    next_id += 1
                base_to_pseudo[j] = pseudo_list
                for pid in pseudo_list:
                    pseudo_to_base[pid] = j
                J_split.extend(pseudo_list)

            beta_split: Dict[Tuple[int, int], float] = {}
            for s in S:
                for j in J_base:
                    remaining = beta.get((s, j), 0)
                    for pid in base_to_pseudo[j]:
                        if remaining > 0:
                            qty = min(L, remaining)
                            beta_split[(s, pid)] = qty
                            remaining -= qty
                        else:
                            beta_split[(s, pid)] = 0.0

            def base_of(node: int) -> int:
                return i0 if node == i0 else pseudo_to_base.get(node, node)

            if arc_set is not None:
                expanded_arcs = set()
                for (u, v) in arc_set:
                    u_list = [i0] if u == i0 else base_to_pseudo.get(u, [u])
                    v_list = [i0] if v == i0 else base_to_pseudo.get(v, [v])
                    for u2 in u_list:
                        for v2 in v_list:
                            if u2 != v2:
                                expanded_arcs.add((u2, v2))
                arc_set = list(expanded_arcs)

                l_split: Dict[Tuple[int, int], float] = dict(l)
                for (u, v) in arc_set:
                    if (u, v) not in l_split:
                        base_u = base_of(u)
                        base_v = base_of(v)
                        if (base_u, base_v) in l:
                            l_split[(u, v)] = l[(base_u, base_v)]
                l = l_split
            else:
                V_split = [i0] + J_split
                l_split: Dict[Tuple[int, int], float] = {}
                for u in V_split:
                    base_u = base_of(u)
                    for v in V_split:
                        if u == v:
                            continue
                        base_v = base_of(v)
                        if (base_u, base_v) in l:
                            l_split[(u, v)] = l[(base_u, base_v)]
                l = l_split

            J = J_split
            beta = beta_split
        else:
            J = J_base
            base_to_pseudo = {j: [j] for j in J_base}
            pseudo_to_base = {j: j for j in J_base}

        unloading_model = load_unloading_time_model()

        # Calculate unloading times using the shared global linear model.
        delta = {}
        for s in S:
            for j in J:
                demand_kg = beta.get((s, j), 0)
                delta[(s, j)] = unloading_time_hours(demand_kg, model=unloading_model)
        
        # Charger installation costs: annualized to daily cost
        # Same cost for all customers (could be customized per location)
        e = {}
        for j in J_base:
            for tau in T:
                daily_cost = annualize_charger_cost(
                    base_costs[tau] * charger_cost_multiplier,
                    lifespan_years=charger_lifespan_years,
                    operating_days_per_year=operating_days_per_year
                )
                e[(j, tau)] = daily_cost

        # Warehouse charger costs (same types, same annualized costs)
        e_wh = {}
        for tau in T:
            daily_cost = annualize_charger_cost(
                base_costs[tau] * charger_cost_multiplier,
                lifespan_years=charger_lifespan_years,
                operating_days_per_year=operating_days_per_year
            )
            e_wh[tau] = daily_cost

        # Use provided F or the vehicle-specific daily default.
        F_value = F if F is not None else default_fixed_truck_cost(
            vehicle_type,
            operating_days_per_year=operating_days_per_year,
        )

        # Initial SoC based on specified fraction
        c0 = C * initial_soc_fraction
        
        # Warehouse capacity: 1000 tons = 1,000,000 kg
        W = 1000000.0
        
        instance_name = name or f"CSPP_{vehicle_name}"
        
        return Instance(
            i0=i0,
            J=J,
            J_base=J_base,
            T=T,
            l=l,
            A=arc_set,
            W=W,
            warehouse_overnight_hours=warehouse_overnight_hours,
            warehouse_loading_time_factor=warehouse_loading_time_factor,
            K_max=K_max,
            M_max=M_max,
            L=L,
            C=C,
            c0=c0,
            P_min=P_min,
            P_max=P_max,
            F=F_value,
            e=e,
            e_wh=e_wh,
            kappa=kappa,
            d_cost=d_cost,
            h=h,
            S=S,
            beta=beta,
            delta=delta,
            name=instance_name,
            pseudo_to_base=pseudo_to_base,
            base_to_pseudo=base_to_pseudo
        )


# Convenience factory functions
def create_instance(**kwargs) -> Instance:
    """Create a CSPP instance. Alias for Instance.createInstance()."""
    return Instance.createInstance(**kwargs)


def create_mercedes_instance(**kwargs) -> Instance:
    """Create instance for Mercedes-Benz eActros 300."""
    return Instance.createInstance(vehicle_type="mercedes", **kwargs)


def create_volvo_instance(**kwargs) -> Instance:
    """Create instance for Volvo FM Electric."""
    return Instance.createInstance(vehicle_type="volvo", **kwargs)


# Vehicle type constants
MERCEDES_EACTROS = "mercedes"
VOLVO_FM_ELECTRIC = "volvo"
