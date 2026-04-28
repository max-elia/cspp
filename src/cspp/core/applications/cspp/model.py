import gurobipy as gp
from gurobipy import GRB
from gurobipy import quicksum
import numpy as np

from ..optimization_model import OptimizationModel
from .instance import Instance
from .. import SecondStageModelType
from helper import getValueDict, vprint
from lieferdaten.unloading_model import load_unloading_time_model


# Warehouse charger is fixed at 22kW (type_id=1)
WAREHOUSE_CHARGER_TYPE = 1
WAREHOUSE_CHARGER_POWER_KW = 22.0


def P(t_flow: gp.Var, inst: Instance) -> gp.LinExpr:
    """
    Compute energy consumption per km based on cargo load.

    P(t) = P_min + (t/L) * (P_max - P_min)

    Args:
        t_flow: Cargo flow on the arc (continuous variable or constant)
        inst: Problem instance with vehicle parameters

    Returns:
        Linear expression for energy consumption rate (kWh/km)
    """
    return inst.P_min + (t_flow / inst.L) * (inst.P_max - inst.P_min)


def get_arcs(inst: Instance) -> list:
    """Get list of valid arcs, using sparse arc set if available and filtering by inst.V."""
    V_set = set(inst.V)
    if inst.A is not None:
        # Use sparse arc set - only include arcs that have distances defined AND are within V
        return [arc for arc in inst.A if arc[0] in V_set and arc[1] in V_set and arc in inst.l]
    else:
        # Use full arc set from distance matrix - filter by V
        return [arc for arc in inst.l.keys() if arc[0] in V_set and arc[1] in V_set]


def compute_tight_big_M(inst: Instance) -> float:
    """
    Compute a tight Big-M value for SoC constraints.

    M = C is sufficient since SoC is always bounded by [0, C].
    """
    return inst.C


def arc_energy_max(inst: Instance, v1: int, v2: int) -> float:
    """Upper bound on energy consumption for arc (v1, v2)."""
    return inst.l.get((v1, v2), 0.0) * inst.P_max


def get_customer_wait_upper_bounds(inst: Instance) -> dict:
    """
    Return charger-specific upper bounds for customer waiting time in hours.

    Waiting longer than required to fill the full battery can never improve the
    solution, so C / kappa[tau] is a valid and much tighter bound than a
    generic Big-M.
    """
    bounds = {}
    for tau, power_kw in inst.kappa.items():
        bounds[tau] = (inst.C / power_kw) if power_kw > 0 else 0.0
    return bounds


def get_warehouse_wait_upper_bound(inst: Instance) -> float:
    """Maximum useful extra waiting time at the warehouse in hours."""
    return (inst.C / WAREHOUSE_CHARGER_POWER_KW) if WAREHOUSE_CHARGER_POWER_KW > 0 else 0.0


def getFirstStageObjective(inst: Instance, a: dict) -> gp.LinExpr:
    """
    First-stage objective: cost of installing chargers at customers only.

    Warehouse charger is fixed (22kW) and not a decision variable.

    f(a) = Σ_{j∈J} Σ_{τ∈T} e[j,τ] * a[j,τ]
    """
    J_base = inst.J_base if inst.J_base else inst.J
    return sum(inst.e[j, tau] * a[j, tau] for j in J_base for tau in inst.T)


def getSecondStageObjective(inst: Instance, s: int, J: list, y: dict, p: dict, omega: dict,
                             p_wh: dict, omega_wh: dict, p_overnight: dict, K_max: int, M_max: int) -> gp.LinExpr:
    """
    Second-stage objective for scenario s.

    g^s = Σ_{k,m,j} d * p[j,k,m]           - charging costs at customers
        + Σ_{k,m,j} h * ω[j,k,m]           - waiting time costs
        + Σ_{k,m} d * p_wh[k,m]            - warehouse charging cost (during loading)
        + Σ_{k,m} h * ω_wh[k,m]            - warehouse waiting time costs
        + Σ_k d * p_overnight[k]           - overnight charging cost to reach c0
        + Σ_k F * y[k]                     - fixed cost per truck used
    """
    K_range = range(1, K_max + 1)
    M_range = range(1, M_max + 1)

    # Charging costs at customers
    charging_cost = quicksum(
        inst.d_cost * p[j, k, m]
        for k in K_range
        for m in M_range
        for j in J
    )

    # Waiting time costs (customers)
    waiting_cost = quicksum(
        inst.h * omega[j, k, m]
        for k in K_range
        for m in M_range
        for j in J
    )

    # Warehouse recharge cost (during loading between tours)
    warehouse_charging_cost = quicksum(
        inst.d_cost * p_wh[k, m]
        for k in K_range
        for m in M_range
    )

    # Warehouse waiting time costs
    warehouse_waiting_cost = quicksum(
        inst.h * omega_wh[k, m]
        for k in K_range
        for m in M_range
    )

    # Overnight charging cost (to reach c0 for next day)
    overnight_charging_cost = quicksum(
        inst.d_cost * p_overnight[k]
        for k in K_range
    )

    # Fixed cost per truck used
    truck_cost = quicksum(inst.F * y[k] for k in K_range)

    return charging_cost + waiting_cost + warehouse_charging_cost + warehouse_waiting_cost + overnight_charging_cost + truck_cost


def get_active_customers(inst: Instance, s: int, tol: float = 1e-3) -> list:
    """Return customers with positive demand in scenario s."""
    return [j for j in inst.J if inst.beta.get((s, j), 0) > tol]


class MasterModel(OptimizationModel):
    """
    Master (first-stage) model for the CSPP.

    Decides charger type installations at customer locations.
    Warehouse has a fixed 22kW charger (not a decision variable).
    """

    def __init__(self, instance: Instance, scenarios: list, *argc, **argv):
        super().__init__(*argc, **argv)

        inst = instance
        self._instance = inst
        self._scenarios = scenarios
        K_max = inst.K_max
        M_max = inst.M_max

        # Use tighter Big-M value
        M_big = compute_tight_big_M(inst)

        # === First-stage variables ===
        # a[j, tau] = 1 if charger type tau is installed at customer j (base customers)
        J_base = inst.J_base if inst.J_base else inst.J
        a = self.addVars(J_base, inst.T, vtype=GRB.BINARY, name="a")

        # z = second-stage cost bound (for robust optimization)
        z = self.addVar(lb=0, vtype=GRB.CONTINUOUS, name="z")

        # === First-stage constraints ===
        # At most one charger type per customer
        for j in J_base:
            self.addConstr(quicksum(a[j, tau] for tau in inst.T) <= 1, name=f"one_charger_{j}")

        # === Symmetry breaking for trucks ===
        # Will be added per scenario in second-stage constraints

        # === Second-stage subproblems ===
        for s in scenarios:
            # Second-stage variables for scenario s
            ss_vars = self._create_second_stage_vars(inst, s)
            y_s, u_s, t_s, r_s, c_arr_s, p_s, omega_s, c_dep_s, c_ret_s, p_wh_s, omega_wh_s, p_overnight_s = ss_vars

            # Add second-stage constraints
            self._add_second_stage_constraints(inst, s, a,
                                               y_s, u_s, t_s, r_s, c_arr_s, p_s, omega_s,
                                               c_dep_s, c_ret_s, p_wh_s, omega_wh_s, p_overnight_s, M_big)

            # z >= g^s (second-stage objective bound)
            ss_obj = getSecondStageObjective(inst, s, inst.J, y_s, p_s, omega_s, p_wh_s, omega_wh_s, p_overnight_s, K_max, M_max)
            self.addConstr(z >= ss_obj, name=f"ss_bound_{s}")

        # === Objective ===
        # min f(a) + z (warehouse charger cost is fixed, not included)
        self.setObjective(getFirstStageObjective(inst, a) + z, GRB.MINIMIZE)

    def _create_second_stage_vars(self, inst: Instance, s: int):
        """Create second-stage variables for scenario s."""
        J = inst.J
        K_max = inst.K_max
        M_max = inst.M_max
        K_range = range(1, K_max + 1)
        M_range = range(1, M_max + 1)

        # Get allowed arcs (sparse or full)
        V_s = [inst.i0] + J
        if inst.A is not None:
            arcs = [arc for arc in inst.A if arc[0] in V_s and arc[1] in V_s and arc in inst.l]
        else:
            arcs = [arc for arc in inst.l.keys() if arc[0] in V_s and arc[1] in V_s]

        # y[k] = 1 if truck k is used
        y = self.addVars(K_range, vtype=GRB.BINARY, name=f"y_{s}")

        # u[k, m] = 1 if tour m of truck k is used
        u = self.addVars(K_range, M_range, vtype=GRB.BINARY, name=f"u_{s}")

        # Arc-indexed variables (only for allowed arcs)
        arc_keys = [(v1, v2, k, m) for (v1, v2) in arcs for k in K_range for m in M_range]

        # t[v1, v2, k, m] = cargo transported on arc (v1, v2) for truck k, tour m
        t = self.addVars(arc_keys, lb=0, name=f"t_{s}")

        # r[v1, v2, k, m] = 1 if arc (v1, v2) is used by truck k, tour m
        r = self.addVars(arc_keys, vtype=GRB.BINARY, name=f"r_{s}")

        # c_arr[j, k, m] = SoC upon arrival at customer j for truck k, tour m
        c_arr = self.addVars(J, K_range, M_range, lb=0, ub=inst.C, name=f"c_arr_{s}")

        # p[j, k, m] = energy charged at customer j for truck k, tour m
        p = self.addVars(J, K_range, M_range, lb=0, name=f"p_{s}")

        # omega[j, k, m] = extra waiting time at customer j for truck k, tour m
        omega = self.addVars(J, K_range, M_range, lb=0, name=f"omega_{s}")

        # c_dep[k, m] = SoC when departing warehouse on tour (k, m)
        c_dep = self.addVars(K_range, M_range, lb=0, ub=inst.C, name=f"c_dep_{s}")

        # c_ret[k, m] = SoC when returning to warehouse after tour (k, m)
        c_ret = self.addVars(K_range, M_range, lb=0, ub=inst.C, name=f"c_ret_{s}")

        # p_wh[k, m] = energy charged at warehouse during loading before tour (k, m)
        # This happens BEFORE departure, during loading time
        p_wh = self.addVars(K_range, M_range, lb=0, name=f"p_wh_{s}")

        # omega_wh[k, m] = extra waiting time at warehouse before tour (k, m)
        omega_wh = self.addVars(K_range, M_range, lb=0, name=f"omega_wh_{s}")

        # p_overnight[k] = energy charged overnight to reach c0 for next day
        p_overnight = self.addVars(K_range, lb=0, name=f"p_overnight_{s}")

        return y, u, t, r, c_arr, p, omega, c_dep, c_ret, p_wh, omega_wh, p_overnight

    def _add_second_stage_constraints(self, inst: Instance, s: int, a: dict,
                                       y: dict, u: dict, t: dict, r: dict, c_arr: dict,
                                       p: dict, omega: dict, c_dep: dict, c_ret: dict,
                                       p_wh: dict, omega_wh: dict, p_overnight: dict, M_big: float):
        """Add second-stage constraints for scenario s."""
        J = inst.J
        i0 = inst.i0
        K_max = inst.K_max
        M_max = inst.M_max
        K_range = range(1, K_max + 1)
        M_range = range(1, M_max + 1)
        customer_wait_ub = get_customer_wait_upper_bounds(inst)
        warehouse_wait_ub = get_warehouse_wait_upper_bound(inst)

        # Get allowed arcs over the full scenario node set
        V_s = [i0] + J
        if inst.A is not None:
            arcs = [arc for arc in inst.A if arc[0] in V_s and arc[1] in V_s and arc in inst.l]
        else:
            arcs = [arc for arc in inst.l.keys() if arc[0] in V_s and arc[1] in V_s]
        arcs_set = set(arcs)

        # Precompute incoming/outgoing arcs for each node (simplified)
        arcs_into = {v: [(v1, v2) for (v1, v2) in arcs if v2 == v] for v in V_s}
        arcs_outof = {v: [(v1, v2) for (v1, v2) in arcs if v1 == v] for v in V_s}

        # eta[j,k,m,tau] linearizes omega[j,k,m] * a[base_j,tau] in the master.
        eta = self.addVars(J, K_range, M_range, inst.T, lb=0, name=f"eta_{s}")

        # === Truck symmetry breaking ===
        # Force trucks to be used in order: y[k+1] <= y[k]
        for k in range(1, K_max):
            self.addConstr(y[k + 1] <= y[k], name=f"sym_break_truck_{s}_{k}")

        # === Truck and tour activation constraints ===
        # u[k,m] <= y[k]
        for k in K_range:
            for m in M_range:
                self.addConstr(u[k, m] <= y[k], name=f"truckuse_{s}_{k}_{m}")

        # Tour sequencing: u[k,m+1] <= u[k,m]
        for k in K_range:
            for m in range(1, M_max):
                self.addConstr(u[k, m + 1] <= u[k, m], name=f"tourseq_{s}_{k}_{m}")

        # === Tour structure constraints ===
        # Departure from depot: Σ_j r[i0,j,k,m] = u[k,m]
        for k in K_range:
            for m in M_range:
                self.addConstr(
                    quicksum(r[i0, j, k, m] for (_, j) in arcs_outof[i0] if j in J) == u[k, m],
                    name=f"tourdep_{s}_{k}_{m}"
                )

        # Return to depot: Σ_j r[j,i0,k,m] = u[k,m]
        for k in K_range:
            for m in M_range:
                self.addConstr(
                    quicksum(r[j, i0, k, m] for (j, _) in arcs_into[i0] if j in J) == u[k, m],
                    name=f"tourret_{s}_{k}_{m}"
                )

        # === Flow constraints ===
        # Each customer with positive demand visited exactly once
        for j in J:
            demand = inst.beta.get((s, j), 0)
            incoming = arcs_into[j]
            if demand > 0.001:
                self.addConstr(
                    quicksum(r[v, j, k, m] for k in K_range for m in M_range for (v, _) in incoming) == 1,
                    name=f"visit_{s}_{j}"
                )
            else:
                # No visit if demand = 0
                self.addConstr(
                    quicksum(r[v, j, k, m] for k in K_range for m in M_range for (v, _) in incoming) == 0,
                    name=f"no_visit_{s}_{j}"
                )

        # Flow conservation at customers
        for j in J:
            incoming = arcs_into[j]
            outgoing = arcs_outof[j]
            for k in K_range:
                for m in M_range:
                    self.addConstr(
                        quicksum(r[v, j, k, m] for (v, _) in incoming) ==
                        quicksum(r[j, v, k, m] for (_, v) in outgoing),
                        name=f"custflow_{s}_{j}_{k}_{m}"
                    )

        # Cargo balance at each customer
        for j in J:
            demand = inst.beta.get((s, j), 0)
            incoming = arcs_into[j]
            outgoing = arcs_outof[j]
            for k in K_range:
                for m in M_range:
                    self.addConstr(
                        quicksum(t[v, j, k, m] for (v, _) in incoming) ==
                        quicksum(t[j, v, k, m] for (_, v) in outgoing) +
                        demand * quicksum(r[v, j, k, m] for (v, _) in incoming),
                        name=f"cargo_balance_{s}_{j}_{k}_{m}"
                    )

        # Arc capacity
        for (v1, v2) in arcs:
            for k in K_range:
                for m in M_range:
                    self.addConstr(
                        t[v1, v2, k, m] <= inst.L * r[v1, v2, k, m],
                        name=f"arc_cap_{s}_{v1}_{v2}_{k}_{m}"
                    )

        # === Energy constraints ===
        # Loading time at the warehouse follows the shared unloading-time proxy:
        # base time plus a term proportional to the cargo loaded onto the truck.
        unloading_model = load_unloading_time_model()
        loading_time_per_kg = unloading_model.slope_minutes_per_kg / 60.0
        base_loading_time = unloading_model.intercept_minutes / 60.0

        # First tour: departure SoC is fixed at c0.
        for k in K_range:
            self.addConstr(
                c_dep[k, 1] == inst.c0 * u[k, 1],
                name=f"soc_first_dep_{s}_{k}"
            )
            self.addConstr(
                p_wh[k, 1] == 0,
                name=f"wh_charge_first_tour_zero_{s}_{k}"
            )
            self.addConstr(
                omega_wh[k, 1] == 0,
                name=f"wh_wait_first_tour_zero_{s}_{k}"
            )

        # SoC from depot to first customer (two-sided constraints for tightness)
        for j in J:
            if (i0, j) in arcs_set:
                for k in K_range:
                    for m in M_range:
                        energy_consumed = inst.l[i0, j] * P(t[i0, j, k, m], inst)
                        energy_max = arc_energy_max(inst, i0, j)
                        M_ub = inst.C + energy_max
                        M_lb = max(0.0, 2 * inst.C - energy_max)
                        # Upper bound
                        self.addConstr(
                            c_arr[j, k, m] <= c_dep[k, m] - energy_consumed + M_ub * (1 - r[i0, j, k, m]),
                            name=f"soc_from_depot_ub_{s}_{j}_{k}_{m}"
                        )
                        # Lower bound (new - for tightness)
                        self.addConstr(
                            c_arr[j, k, m] >= c_dep[k, m] - energy_consumed - M_lb * (1 - r[i0, j, k, m]),
                            name=f"soc_from_depot_lb_{s}_{j}_{k}_{m}"
                        )

        # SoC propagation between customers (two-sided)
        for (v1, v2) in arcs:
            if v1 in J and v2 in J:
                for k in K_range:
                    for m in M_range:
                        energy_consumed = inst.l[v1, v2] * P(t[v1, v2, k, m], inst)
                        energy_max = arc_energy_max(inst, v1, v2)
                        M_ub = inst.C + energy_max
                        M_lb = max(0.0, 2 * inst.C - energy_max)
                        # Upper bound
                        self.addConstr(
                            c_arr[v2, k, m] <= c_arr[v1, k, m] + p[v1, k, m] - energy_consumed + M_ub * (1 - r[v1, v2, k, m]),
                            name=f"soc_prop_ub_{s}_{v1}_{v2}_{k}_{m}"
                        )
                        # Lower bound (new - for tightness)
                        self.addConstr(
                            c_arr[v2, k, m] >= c_arr[v1, k, m] + p[v1, k, m] - energy_consumed - M_lb * (1 - r[v1, v2, k, m]),
                            name=f"soc_prop_lb_{s}_{v1}_{v2}_{k}_{m}"
                        )

        # SoC when returning to warehouse (two-sided)
        for j in J:
            if (j, i0) in arcs_set:
                for k in K_range:
                    for m in M_range:
                        energy_consumed = inst.l[j, i0] * P(t[j, i0, k, m], inst)
                        energy_max = arc_energy_max(inst, j, i0)
                        M_ub = inst.C + energy_max
                        M_lb = max(0.0, 2 * inst.C - energy_max)
                        # Upper bound
                        self.addConstr(
                            c_ret[k, m] <= c_arr[j, k, m] + p[j, k, m] - energy_consumed + M_ub * (1 - r[j, i0, k, m]),
                            name=f"soc_return_ub_{s}_{j}_{k}_{m}"
                        )
                        # Lower bound (new - for tightness)
                        self.addConstr(
                            c_ret[k, m] >= c_arr[j, k, m] + p[j, k, m] - energy_consumed - M_lb * (1 - r[j, i0, k, m]),
                            name=f"soc_return_lb_{s}_{j}_{k}_{m}"
                        )

        # SoC at departure of next tour = return SoC + warehouse charging during loading
        for k in K_range:
            for m in range(1, M_max):
                # If u[k, m+1] = 1, then c_dep[k, m+1] == c_ret[k, m] + p_wh[k, m+1]
                # Note: p_wh[k, m+1] is charging during loading BEFORE tour m+1
                self.addConstr(
                    c_dep[k, m + 1] - (c_ret[k, m] + p_wh[k, m + 1]) <= M_big * (1 - u[k, m + 1]),
                    name=f"soc_next_tour_ub_{s}_{k}_{m}"
                )
                self.addConstr(
                    (c_ret[k, m] + p_wh[k, m + 1]) - c_dep[k, m + 1] <= M_big * (1 - u[k, m + 1]),
                    name=f"soc_next_tour_lb_{s}_{k}_{m}"
                )

        # === Overnight charging constraint ===
        # Overnight charging is based on the return SoC of the last used tour.
        for k in K_range:
            for m in range(1, M_max):
                self.addConstr(
                    p_overnight[k] >= inst.c0 * (u[k, m] - u[k, m + 1]) - c_ret[k, m],
                    name=f"overnight_charge_last_{s}_{k}_{m}"
                )
            self.addConstr(
                p_overnight[k] >= inst.c0 * u[k, M_max] - c_ret[k, M_max],
                name=f"overnight_charge_last_{s}_{k}_{M_max}"
            )
            # p_overnight is 0 if truck not used
            self.addConstr(
                p_overnight[k] <= inst.C * y[k],
                name=f"overnight_charge_bound_{s}_{k}"
            )

        # === Charging constraints ===
        # Customer charging limited by charger speed and available time
        for j in J:
            for k in K_range:
                for m in M_range:
                    unload_time = inst.delta.get((s, j), 0)
                    base_j = inst.pseudo_to_base.get(j, j)
                    wait_upper = quicksum(customer_wait_ub[tau] * a[base_j, tau] for tau in inst.T)
                    for tau in inst.T:
                        ub_tau = customer_wait_ub[tau]
                        self.addConstr(
                            eta[j, k, m, tau] <= omega[j, k, m],
                            name=f"eta_ub_omega_{s}_{j}_{k}_{m}_{tau}"
                        )
                        self.addConstr(
                            eta[j, k, m, tau] <= ub_tau * a[base_j, tau],
                            name=f"eta_ub_binary_{s}_{j}_{k}_{m}_{tau}"
                        )
                        self.addConstr(
                            eta[j, k, m, tau] >= omega[j, k, m] - ub_tau * (1 - a[base_j, tau]),
                            name=f"eta_lb_{s}_{j}_{k}_{m}_{tau}"
                        )
                    self.addConstr(
                        p[j, k, m] <= quicksum(
                            inst.kappa[tau] * (unload_time * a[base_j, tau] + eta[j, k, m, tau])
                            for tau in inst.T
                        ),
                        name=f"charging_limit_{s}_{j}_{k}_{m}"
                    )
                    # If no charger installed at base node, force zero charging/waiting
                    charger_on = quicksum(a[base_j, tau] for tau in inst.T)
                    self.addConstr(
                        p[j, k, m] <= inst.C * charger_on,
                        name=f"charge_if_charger_{s}_{j}_{k}_{m}"
                    )
                    self.addConstr(
                        omega[j, k, m] <= wait_upper,
                        name=f"wait_if_charger_{s}_{j}_{k}_{m}"
                    )

        # Warehouse charging during loading - fixed 22kW charger
        # Charging is limited by charger power and the loading time implied by the
        # cargo loaded from the depot, plus optional extra warehouse waiting.
        for k in K_range:
            for m in range(2, M_max + 1):
                loaded_cargo = quicksum(t[i0, j, k, m] for (_, j) in arcs_outof[i0] if j in J)
                loading_time = inst.warehouse_loading_time_factor * (
                    base_loading_time * u[k, m] + loading_time_per_kg * loaded_cargo
                )
                self.addConstr(
                    p_wh[k, m] <= WAREHOUSE_CHARGER_POWER_KW * (loading_time + omega_wh[k, m]),
                    name=f"wh_charging_limit_{s}_{k}_{m}"
                )

                # Waiting time at warehouse only if tour is used
                self.addConstr(
                    omega_wh[k, m] <= warehouse_wait_ub * u[k, m],
                    name=f"wh_waiting_limit_{s}_{k}_{m}"
                )

        # Overnight charging capacity intentionally not constrained.
        # Max explicitly wants no shared overnight-capacity limit in this model.

        # === SoC bounds ===
        # Minimum SoC at arrival at customers (20% of capacity, only when visited)
        for j in J:
            incoming = arcs_into[j]
            for k in K_range:
                for m in M_range:
                    self.addConstr(
                        c_arr[j, k, m] >= 0.2 * inst.C * quicksum(r[v, j, k, m] for (v, _) in incoming),
                        name=f"min_soc_arr_{s}_{j}_{k}_{m}"
                    )

        # Minimum SoC at return to warehouse (20% of capacity, only when tour is used)
        for k in K_range:
            for m in M_range:
                self.addConstr(
                    c_ret[k, m] >= 0.2 * inst.C * u[k, m],
                    name=f"min_soc_ret_{s}_{k}_{m}"
                )

        # Maximum SoC after charging at customer
        for j in J:
            for k in K_range:
                for m in M_range:
                    self.addConstr(
                        c_arr[j, k, m] + p[j, k, m] <= inst.C,
                        name=f"max_soc_{s}_{j}_{k}_{m}"
                    )

        # SoC at departure bounded by capacity
        for k in K_range:
            for m in M_range:
                self.addConstr(
                    c_dep[k, m] <= inst.C * u[k, m],
                    name=f"max_soc_dep_{s}_{k}_{m}"
                )

    def get_first_stage_solution(self) -> tuple:
        """Return the first-stage solution: charger installations at customers."""
        J_base = self._instance.J_base if self._instance.J_base else self._instance.J
        a = {}
        for j in J_base:
            for tau in self._instance.T:
                try:
                    val = self._vars["a"][j, tau].X
                except AttributeError:
                    val = 0.0
                a[(j, tau)] = round(val)
        # Warehouse charger is fixed at 22kW
        a_wh = {WAREHOUSE_CHARGER_TYPE: 1}
        return a, a_wh

    def get_first_stage_objective(self) -> float:
        """Return the objective value of the first stage solution."""
        a, _ = self.get_first_stage_solution()
        return sum(
            self._instance.e[j, tau] * a[j, tau]
            for j in (self._instance.J_base if self._instance.J_base else self._instance.J)
            for tau in self._instance.T
        )

    def get_second_stage_bound(self) -> float:
        """Return the second-stage objective bound."""
        z_var = self._vars["z"]
        try:
            return z_var.X
        except AttributeError:
            # No incumbent value available yet; fall back to model bound if present.
            try:
                return float(self.ObjBound)
            except Exception:
                return float("inf")

    def get_second_stage_solution_for_scenario(self, s: int) -> tuple:
        """Return the second-stage solution variables for scenario s."""
        if s not in self._scenarios:
            return None

        inst = self._instance
        K_range = range(1, inst.K_max + 1)
        M_range = range(1, inst.M_max + 1)

        y_s = getValueDict(self._vars[f"y_{s}"], roundBinaries=True)
        u_s = getValueDict(self._vars[f"u_{s}"], roundBinaries=True)
        t_s = getValueDict(self._vars[f"t_{s}"])
        r_s = getValueDict(self._vars[f"r_{s}"], roundBinaries=True)
        c_arr_s = getValueDict(self._vars[f"c_arr_{s}"])
        p_s = getValueDict(self._vars[f"p_{s}"])
        omega_s = getValueDict(self._vars[f"omega_{s}"])
        c_dep_s = getValueDict(self._vars[f"c_dep_{s}"])
        c_ret_s = getValueDict(self._vars[f"c_ret_{s}"])
        p_wh_s = getValueDict(self._vars[f"p_wh_{s}"])
        p_overnight_s = getValueDict(self._vars[f"p_overnight_{s}"])

        return y_s, u_s, t_s, r_s, c_arr_s, p_s, omega_s, c_dep_s, c_ret_s, p_wh_s, p_overnight_s

    def get_routes_for_scenario(self, s: int) -> list:
        """
        Extract routes for a given scenario from the solution.
        Returns a list of routes, where each route is a dict with truck, tour, and stops info.
        """
        inst = self._instance
        i0 = inst.i0
        K_range = range(1, inst.K_max + 1)
        M_range = range(1, inst.M_max + 1)

        sol = self.get_second_stage_solution_for_scenario(s)
        if sol is None:
            return []

        y_s, u_s, t_s, r_s, c_arr_s, p_s, omega_s, c_dep_s, c_ret_s, p_wh_s, p_overnight_s = sol
        routes = []

        for k in K_range:
            if y_s.get(k, 0) < 0.5:
                continue  # Truck not used

            last_used_tour = max(
                (m for m in M_range if u_s.get((k, m), 0) >= 0.5),
                default=None
            )

            for m in M_range:
                if u_s.get((k, m), 0) < 0.5:
                    continue  # Tour not used

                # Find used arcs for this truck/tour
                used_arcs = [
                    (v1, v2) for v1 in inst.V for v2 in inst.V
                    if r_s.get((v1, v2, k, m), 0) >= 0.5
                ]

                if not used_arcs:
                    continue

                # Build route starting from depot
                route_nodes = [i0]
                current = None
                for v1, v2 in used_arcs:
                    if v1 == i0:
                        current = v2
                        break

                visited = {i0}
                while current is not None and current not in visited:
                    route_nodes.append(current)
                    visited.add(current)
                    next_node = None
                    for v1, v2 in used_arcs:
                        if v1 == current and (v2 not in visited or v2 == i0):
                            next_node = v2
                            break
                    if next_node == i0:
                        route_nodes.append(i0)
                        break
                    current = next_node

                # Build route details
                route_details = {
                    'truck': k,
                    'tour': m,
                    'c_dep': c_dep_s.get((k, m), 0),
                    'c_ret': c_ret_s.get((k, m), 0),
                    'p_wh': p_wh_s.get((k, m), 0),
                    'p_overnight': p_overnight_s.get(k, 0) if m == last_used_tour else 0,
                    'stops': []
                }

                for j in route_nodes:
                    if j == i0:
                        route_details['stops'].append({'node': j, 'is_depot': True})
                    else:
                        route_details['stops'].append({
                            'node': j,
                            'is_depot': False,
                            'demand': inst.beta.get((s, j), 0),
                            'soc_arr': c_arr_s.get((j, k, m), 0),
                            'charged': p_s.get((j, k, m), 0),
                            'wait': omega_s.get((j, k, m), 0)
                        })

                routes.append(route_details)

        return routes


class SecondStageModel(SecondStageModelType):
    """
    Second-stage model for a single scenario.

    Given first-stage charger installation decisions, optimizes routing,
    charging, and waiting time decisions for a specific scenario.
    """

    def __init__(self, instance: Instance, k: int, first_stage_solution: tuple,
                 warmstart=None, *argc, **argv):
        super().__init__(*argc, **argv)

        inst = instance
        self._instance = inst
        self._k = k  # scenario index
        self._first_stage_solution = first_stage_solution

        J = inst.J
        i0 = inst.i0
        K_max = inst.K_max
        M_max = inst.M_max
        K_range = range(1, K_max + 1)
        M_range = range(1, M_max + 1)
        customer_wait_ub = get_customer_wait_upper_bounds(inst)
        warehouse_wait_ub = get_warehouse_wait_upper_bound(inst)

        # Use tighter Big-M
        M_big = compute_tight_big_M(inst)

        # Fixed first-stage values (warehouse charger is fixed 22kW AC)
        if isinstance(first_stage_solution, tuple):
            a = first_stage_solution[0]
        else:
            a = first_stage_solution
        warehouse_charger_count = 1

        # Get allowed arcs (sparse or full) over the full scenario node set
        V_s = [i0] + J
        if inst.A is not None:
            arcs = [arc for arc in inst.A if arc[0] in V_s and arc[1] in V_s and arc in inst.l]
        else:
            arcs = [arc for arc in inst.l.keys() if arc[0] in V_s and arc[1] in V_s]
        arcs_set = set(arcs)

        # Precompute incoming/outgoing arcs for each node
        arcs_into = {v: [(v1, v2) for (v1, v2) in arcs if v2 == v] for v in V_s}
        arcs_outof = {v: [(v1, v2) for (v1, v2) in arcs if v1 == v] for v in V_s}

        # Arc-indexed variable keys
        arc_keys = [(v1, v2, truck, tour) for (v1, v2) in arcs for truck in K_range for tour in M_range]

        # Loading time parameters follow the shared unloading-time proxy.
        unloading_model = load_unloading_time_model()
        loading_time_per_kg = unloading_model.slope_minutes_per_kg / 60.0
        base_loading_time = unloading_model.intercept_minutes / 60.0

        # === Second-stage variables ===
        y = self.addVars(K_range, vtype=GRB.BINARY, name="y")
        u = self.addVars(K_range, M_range, vtype=GRB.BINARY, name="u")
        t = self.addVars(arc_keys, lb=0, name="t")
        r = self.addVars(arc_keys, vtype=GRB.BINARY, name="r")
        c_arr = self.addVars(J, K_range, M_range, lb=0, ub=inst.C, name="c_arr")
        p = self.addVars(J, K_range, M_range, lb=0, name="p")
        omega = self.addVars(J, K_range, M_range, lb=0, name="omega")
        c_dep = self.addVars(K_range, M_range, lb=0, ub=inst.C, name="c_dep")
        c_ret = self.addVars(K_range, M_range, lb=0, ub=inst.C, name="c_ret")
        p_wh = self.addVars(K_range, M_range, lb=0, name="p_wh")
        omega_wh = self.addVars(K_range, M_range, lb=0, name="omega_wh")
        p_overnight = self.addVars(K_range, lb=0, name="p_overnight")

        # === Truck symmetry breaking ===
        for truck in range(1, K_max):
            self.addConstr(y[truck + 1] <= y[truck], name=f"sym_break_{truck}")

        # === Truck and tour activation constraints ===
        for truck in K_range:
            for tour in M_range:
                self.addConstr(u[truck, tour] <= y[truck])
        for truck in K_range:
            for tour in range(1, M_max):
                self.addConstr(u[truck, tour + 1] <= u[truck, tour])

        # === Tour structure constraints ===
        for truck in K_range:
            for tour in M_range:
                self.addConstr(
                    quicksum(r[i0, j, truck, tour] for (_, j) in arcs_outof[i0] if j in J) == u[truck, tour]
                )
                self.addConstr(
                    quicksum(r[j, i0, truck, tour] for (j, _) in arcs_into[i0] if j in J) == u[truck, tour]
                )

        # === Flow constraints ===
        for j in J:
            demand = inst.beta.get((k, j), 0)
            incoming = arcs_into[j]
            if demand > 0.001:
                self.addConstr(
                    quicksum(r[v, j, truck, tour] for truck in K_range for tour in M_range for (v, _) in incoming) == 1
                )
            else:
                self.addConstr(
                    quicksum(r[v, j, truck, tour] for truck in K_range for tour in M_range for (v, _) in incoming) == 0
                )

        for j in J:
            incoming = arcs_into[j]
            outgoing = arcs_outof[j]
            for truck in K_range:
                for tour in M_range:
                    self.addConstr(
                        quicksum(r[v, j, truck, tour] for (v, _) in incoming) ==
                        quicksum(r[j, v, truck, tour] for (_, v) in outgoing)
                    )

        for j in J:
            demand = inst.beta.get((k, j), 0)
            incoming = arcs_into[j]
            outgoing = arcs_outof[j]
            for truck in K_range:
                for tour in M_range:
                    self.addConstr(
                        quicksum(t[v, j, truck, tour] for (v, _) in incoming) ==
                        quicksum(t[j, v, truck, tour] for (_, v) in outgoing) +
                        demand * quicksum(r[v, j, truck, tour] for (v, _) in incoming)
                    )

        # Arc capacity
        for (v1, v2) in arcs:
            for truck in K_range:
                for tour in M_range:
                    self.addConstr(t[v1, v2, truck, tour] <= inst.L * r[v1, v2, truck, tour])

        # === Energy constraints ===
        # First tour departure starts at c0.
        for truck in K_range:
            self.addConstr(c_dep[truck, 1] == inst.c0 * u[truck, 1])
            self.addConstr(p_wh[truck, 1] == 0)
            self.addConstr(omega_wh[truck, 1] == 0)

        # SoC from depot to first customer (two-sided)
        for j in J:
            if (i0, j) in arcs_set:
                for truck in K_range:
                    for tour in M_range:
                        energy_consumed = inst.l[i0, j] * P(t[i0, j, truck, tour], inst)
                        energy_max = arc_energy_max(inst, i0, j)
                        M_ub = inst.C + energy_max
                        M_lb = max(0.0, 2 * inst.C - energy_max)
                        self.addConstr(
                            c_arr[j, truck, tour] <= c_dep[truck, tour] - energy_consumed + M_ub * (1 - r[i0, j, truck, tour])
                        )
                        self.addConstr(
                            c_arr[j, truck, tour] >= c_dep[truck, tour] - energy_consumed - M_lb * (1 - r[i0, j, truck, tour])
                        )

        # SoC propagation between customers (two-sided)
        for (v1, v2) in arcs:
            if v1 in J and v2 in J:
                for truck in K_range:
                    for tour in M_range:
                        energy_consumed = inst.l[v1, v2] * P(t[v1, v2, truck, tour], inst)
                        energy_max = arc_energy_max(inst, v1, v2)
                        M_ub = inst.C + energy_max
                        M_lb = max(0.0, 2 * inst.C - energy_max)
                        self.addConstr(
                            c_arr[v2, truck, tour] <= c_arr[v1, truck, tour] + p[v1, truck, tour] - energy_consumed + M_ub * (1 - r[v1, v2, truck, tour])
                        )
                        self.addConstr(
                            c_arr[v2, truck, tour] >= c_arr[v1, truck, tour] + p[v1, truck, tour] - energy_consumed - M_lb * (1 - r[v1, v2, truck, tour])
                        )

        # SoC when returning to warehouse (two-sided)
        for j in J:
            if (j, i0) in arcs_set:
                for truck in K_range:
                    for tour in M_range:
                        energy_consumed = inst.l[j, i0] * P(t[j, i0, truck, tour], inst)
                        energy_max = arc_energy_max(inst, j, i0)
                        M_ub = inst.C + energy_max
                        M_lb = max(0.0, 2 * inst.C - energy_max)
                        self.addConstr(
                            c_ret[truck, tour] <= c_arr[j, truck, tour] + p[j, truck, tour] - energy_consumed + M_ub * (1 - r[j, i0, truck, tour])
                        )
                        self.addConstr(
                            c_ret[truck, tour] >= c_arr[j, truck, tour] + p[j, truck, tour] - energy_consumed - M_lb * (1 - r[j, i0, truck, tour])
                        )

        # Next tour departure = return SoC + warehouse charging during loading
        for truck in K_range:
            for tour in range(1, M_max):
                self.addConstr(
                    c_dep[truck, tour + 1] - (c_ret[truck, tour] + p_wh[truck, tour + 1]) <= M_big * (1 - u[truck, tour + 1])
                )
                self.addConstr(
                    (c_ret[truck, tour] + p_wh[truck, tour + 1]) - c_dep[truck, tour + 1] <= M_big * (1 - u[truck, tour + 1])
                )

        # === Overnight charging constraint ===
        for truck in K_range:
            for tour in range(1, M_max):
                self.addConstr(
                    p_overnight[truck] >= inst.c0 * (u[truck, tour] - u[truck, tour + 1]) - c_ret[truck, tour],
                    name=f"overnight_last_{truck}_{tour}"
                )
            self.addConstr(
                p_overnight[truck] >= inst.c0 * u[truck, M_max] - c_ret[truck, M_max],
                name=f"overnight_last_{truck}_{M_max}"
            )
            self.addConstr(
                p_overnight[truck] <= inst.C * y[truck],
                name=f"overnight_bound_{truck}"
            )

        # === Charging constraints ===
        for j in J:
            for truck in K_range:
                for tour in M_range:
                    unload_time = inst.delta.get((k, j), 0)
                    base_j = inst.pseudo_to_base.get(j, j)
                    charger_on = quicksum(a.get((base_j, tau), 0) for tau in inst.T)
                    wait_upper = sum(customer_wait_ub[tau] * a.get((base_j, tau), 0) for tau in inst.T)
                    self.addConstr(
                        p[j, truck, tour] <= quicksum(
                            inst.kappa[tau] * (unload_time + omega[j, truck, tour]) * a.get((base_j, tau), 0)
                            for tau in inst.T
                        )
                    )
                    # If no charger installed at base node, force zero charging/waiting
                    self.addConstr(p[j, truck, tour] <= inst.C * charger_on)
                    self.addConstr(omega[j, truck, tour] <= wait_upper)

        # Warehouse charging with fixed 22kW charger and waiting time
        for truck in K_range:
            for tour in range(2, M_max + 1):
                loaded_cargo = quicksum(t[i0, j, truck, tour] for (_, j) in arcs_outof[i0] if j in J)
                loading_time = inst.warehouse_loading_time_factor * (
                    base_loading_time * u[truck, tour] + loading_time_per_kg * loaded_cargo
                )
                self.addConstr(
                    p_wh[truck, tour] <= WAREHOUSE_CHARGER_POWER_KW * (loading_time + omega_wh[truck, tour])
                )
                self.addConstr(
                    omega_wh[truck, tour] <= warehouse_wait_ub * u[truck, tour]
                )

        # Overnight charging capacity intentionally not constrained.
        # Max explicitly wants no shared overnight-capacity limit in this model.

        # === SoC bounds ===
        # Minimum SoC at arrival at customers (20% of capacity, only when visited)
        for j in J:
            incoming = arcs_into[j]
            for truck in K_range:
                for tour in M_range:
                    self.addConstr(
                        c_arr[j, truck, tour] >= 0.2 * inst.C * quicksum(r[v, j, truck, tour] for (v, _) in incoming)
                    )

        # Minimum SoC at return to warehouse (20% of capacity, only when tour is used)
        for truck in K_range:
            for tour in M_range:
                self.addConstr(
                    c_ret[truck, tour] >= 0.2 * inst.C * u[truck, tour]
                )

        # Maximum SoC after charging at customer
        for j in J:
            for truck in K_range:
                for tour in M_range:
                    self.addConstr(c_arr[j, truck, tour] + p[j, truck, tour] <= inst.C)

        for truck in K_range:
            for tour in M_range:
                self.addConstr(c_dep[truck, tour] <= inst.C * u[truck, tour])

        # === Objective ===
        self.setObjective(
            getSecondStageObjective(inst, k, J, y, p, omega, p_wh, omega_wh, p_overnight, K_max, M_max),
            GRB.MINIMIZE
        )

        # Store variables for later access
        self._vars = {
            "y": y, "u": u, "t": t, "r": r,
            "c_arr": c_arr, "p": p, "omega": omega,
            "c_dep": c_dep, "c_ret": c_ret,
            "p_wh": p_wh, "omega_wh": omega_wh,
            "p_overnight": p_overnight
        }

        # Apply warmstart if provided
        if warmstart:
            self._apply_warmstart(warmstart, y, u, t, r, c_arr, p, omega, c_dep, c_ret, p_wh, omega_wh, p_overnight)

    def _apply_warmstart(self, warmstart, y, u, t, r, c_arr, p, omega, c_dep, c_ret, p_wh, omega_wh, p_overnight):
        """Apply warmstart values to variables.

        For a complete MIP start, we need to set ALL variables:
        - Variables in the warmstart get their warmstart values
        - Variables not in the warmstart get default values (0 for unused trucks/tours)

        Important: The warmstart may include routes to customers that don't have demand
        in this scenario. We only apply warmstart values for arcs/nodes that exist in
        this model (i.e., customers with demand in this scenario).
        """
        try:
            self._apply_warmstart_impl(warmstart, y, u, t, r, c_arr, p, omega, c_dep, c_ret, p_wh, omega_wh, p_overnight)
        except Exception as e:
            import traceback
            print(f"  Warmstart error: {e}")
            traceback.print_exc()
            # Skip warmstart on error - model will start from scratch

    def _apply_warmstart_impl(self, warmstart, y, u, t, r, c_arr, p, omega, c_dep, c_ret, p_wh, omega_wh, p_overnight):
        """Internal implementation of warmstart application."""
        inst = self._instance
        K_range = range(1, inst.K_max + 1)
        M_range = range(1, inst.M_max + 1)

        if len(warmstart) >= 11:
            y_ws, u_ws, t_ws, r_ws, c_arr_ws, p_ws, omega_ws, c_dep_ws, c_ret_ws, p_wh_ws, omega_wh_ws = warmstart[:11]
            p_overnight_ws = warmstart[11] if len(warmstart) > 11 else {}

            # Identify which trucks/tours from warmstart are valid for this scenario
            # A truck/tour is valid if its arcs exist in this model
            model_arcs = set(r.keys())  # All arcs in this model

            # Get active customers for this scenario
            k = self._k
            active_customers = set(j for j in inst.J if inst.beta.get((k, j), 0) > 0.001)

            # Count warmstart arcs and check which exist in model
            ws_arcs_total = 0
            ws_arcs_in_model = 0
            ws_customers_served = set()
            valid_truck_tours = set()

            for (v1, v2, truck, tour), val in r_ws.items():
                if val > 0.5:
                    ws_arcs_total += 1
                    if v2 != inst.i0 and v2 in inst.J:
                        ws_customers_served.add(v2)
                    if (v1, v2, truck, tour) in model_arcs:
                        ws_arcs_in_model += 1
                        valid_truck_tours.add((truck, tour))

            # Check customer coverage
            missing_customers = active_customers - ws_customers_served
            extra_customers = ws_customers_served - active_customers

            if ws_arcs_total != ws_arcs_in_model or missing_customers:
                print(f"  Warmstart coverage: {ws_arcs_in_model}/{ws_arcs_total} arcs valid, "
                      f"{len(ws_customers_served)}/{len(active_customers)} customers covered")
                if missing_customers:
                    print(f"  WARNING: Missing {len(missing_customers)} customers - warmstart will be rejected!")
                    print(f"    Missing: {list(missing_customers)[:10]}...")
            else:
                print(f"  Warmstart coverage: {ws_arcs_in_model}/{ws_arcs_total} arcs, "
                      f"{len(ws_customers_served)}/{len(active_customers)} customers - OK")

            # Check if there are customers with demand not covered by warmstart
            if extra_customers:
                print(f"  Note: {len(extra_customers)} customers in warmstart have no demand in this scenario")

            # Only include trucks that have at least one valid tour
            valid_trucks = set(truck for truck, tour in valid_truck_tours)

            # Check if warmstart trucks fit within model's K_max
            max_ws_truck = max((truck for truck, tour in valid_truck_tours), default=0)
            if max_ws_truck > inst.K_max:
                print(f"  WARNING: Warmstart uses truck {max_ws_truck} but K_max={inst.K_max}!")
                print(f"    Trucks {inst.K_max+1} to {max_ws_truck} will be ignored - warmstart may be incomplete")

            vars_set = 0
            vars_from_warmstart = 0

            # First pass: set tour variables and track the c_ret value we SET for each tour.
            c_ret_set = {}  # (truck, tour) -> c_ret value we set

            for truck in K_range:
                for tour in M_range:
                    if (truck, tour) in valid_truck_tours and (truck, tour) in u_ws:
                        # Active tour from warmstart
                        u[truck, tour].Start = 1
                        c_dep[truck, tour].Start = c_dep_ws.get((truck, tour), 0)
                        c_ret_val = c_ret_ws.get((truck, tour), 0)
                        c_ret[truck, tour].Start = c_ret_val
                        c_ret_set[(truck, tour)] = c_ret_val
                        if tour == 1:
                            p_wh[truck, tour].Start = 0
                            omega_wh[truck, tour].Start = 0
                        else:
                            p_wh[truck, tour].Start = p_wh_ws.get((truck, tour), 0)
                            omega_wh[truck, tour].Start = omega_wh_ws.get((truck, tour), 0)
                        vars_from_warmstart += 1
                    else:
                        # Inactive tour
                        u[truck, tour].Start = 0
                        c_dep[truck, tour].Start = 0
                        p_wh[truck, tour].Start = 0
                        omega_wh[truck, tour].Start = 0
                        c_ret[truck, tour].Start = 0
                        c_ret_set[(truck, tour)] = 0
                    vars_set += 5

            # Second pass: set truck variables with correct p_overnight based on the
            # return SoC of the last used tour in this scenario.
            for truck in K_range:
                last_used_tour = max((tour for t, tour in valid_truck_tours if t == truck), default=0)
                c_ret_at_last_used = c_ret_set.get((truck, last_used_tour), 0)
                min_p_overnight = max(0, inst.c0 - c_ret_at_last_used) if last_used_tour else 0

                # Only mark truck as used if it has valid tours in this scenario
                if truck in valid_trucks and truck in y_ws:
                    y[truck].Start = 1
                    vars_from_warmstart += 1
                    p_overnight[truck].Start = max(p_overnight_ws.get(truck, 0), min_p_overnight)
                else:
                    y[truck].Start = 0
                    p_overnight[truck].Start = 0
                vars_set += 2

            # Set arc variables - only for arcs that exist in model
            for key in t:
                t[key].Start = t_ws.get(key, 0)
                vars_set += 1
            for key in r:
                v1, v2, truck, tour = key
                # Only set arc if it's part of a valid truck/tour
                if (truck, tour) in valid_truck_tours:
                    val = r_ws.get(key, 0)
                    r[key].Start = val
                    if val > 0.5:
                        vars_from_warmstart += 1
                else:
                    r[key].Start = 0
                vars_set += 1

            # Set node variables - only for nodes that exist in model
            for key in c_arr:
                j, truck, tour = key
                if (truck, tour) in valid_truck_tours:
                    c_arr[key].Start = c_arr_ws.get(key, 0)
                else:
                    c_arr[key].Start = 0
                vars_set += 1
            for key in p:
                j, truck, tour = key
                if (truck, tour) in valid_truck_tours:
                    p[key].Start = p_ws.get(key, 0)
                else:
                    p[key].Start = 0
                vars_set += 1
            for key in omega:
                j, truck, tour = key
                if (truck, tour) in valid_truck_tours:
                    omega[key].Start = omega_ws.get(key, 0)
                else:
                    omega[key].Start = 0
                vars_set += 1

            # Count statistics
            r_ws_active = sum(1 for v in r_ws.values() if v > 0.5)
            print(f"  Warmstart applied: {vars_set} vars set, {vars_from_warmstart} from warmstart")
            print(f"  Valid trucks: {len(valid_trucks)}, valid tours: {len(valid_truck_tours)}")

    def get_second_stage_solution(self) -> tuple:
        """Return the second-stage solution variables."""
        inst = self._instance
        K_range = range(1, inst.K_max + 1)
        M_range = range(1, inst.M_max + 1)

        y_sol = {truck: round(self._vars["y"][truck].X) for truck in K_range}
        u_sol = {(truck, tour): round(self._vars["u"][truck, tour].X) for truck in K_range for tour in M_range}
        t_sol = {key: self._vars["t"][key].X for key in self._vars["t"]}
        r_sol = {key: round(self._vars["r"][key].X) for key in self._vars["r"]}
        c_arr_sol = {key: self._vars["c_arr"][key].X for key in self._vars["c_arr"]}
        p_sol = {key: self._vars["p"][key].X for key in self._vars["p"]}
        omega_sol = {key: self._vars["omega"][key].X for key in self._vars["omega"]}
        c_dep_sol = {(truck, tour): self._vars["c_dep"][truck, tour].X for truck in K_range for tour in M_range}
        c_ret_sol = {(truck, tour): self._vars["c_ret"][truck, tour].X for truck in K_range for tour in M_range}
        p_wh_sol = {(truck, tour): self._vars["p_wh"][truck, tour].X for truck in K_range for tour in M_range}
        omega_wh_sol = {(truck, tour): self._vars["omega_wh"][truck, tour].X for truck in K_range for tour in M_range}
        p_overnight_sol = {truck: self._vars["p_overnight"][truck].X for truck in K_range}

        return y_sol, u_sol, t_sol, r_sol, c_arr_sol, p_sol, omega_sol, c_dep_sol, c_ret_sol, p_wh_sol, omega_wh_sol, p_overnight_sol

    @property
    def mipgap(self):
        try:
            return self.getAttr("MIPGap")
        except:
            return np.inf

    @property
    def objbound(self):
        try:
            return self.getAttr("objbound")
        except:
            return -np.inf

    @property
    def objval(self):
        try:
            obj = self.getAttr("objval")
            if obj < -1e-6:
                print(f"WARNING: Negative objective value detected: {obj:.2f}")
            return obj
        except:
            return np.inf
