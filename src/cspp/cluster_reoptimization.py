"""
Cluster reoptimization (Stage 3): iterative worst-scenario-driven reoptimization.

Algorithm:
    1. Load baseline first-stage solutions from stages 1+2.
    2. Evaluate all scenarios to find the globally worst scenario.
    3. If the worst scenario is already in A: stop (converged).
    4. Otherwise add the worst scenario to A.
    5. Re-solve each cluster's first-stage with D=A (MasterModel with fixed D).
    6. Re-evaluate all scenarios for the candidate first-stage solution.
    7. Accept the iteration only if the global worst-case objective does not
       worsen (robust acceptance check).
    8. Repeat until convergence or max iterations.
    9. Return the best iteration (lowest worst-case objective).

Evaluation uses cluster-sum evaluation: per-cluster second-stage costs are solved for all scenarios and summed.
"""

import argparse
import ast
import contextlib
import copy
import hashlib
import json
import math
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
from gurobipy import GRB

from applications.cspp.instance import create_mercedes_instance, create_volvo_instance
import applications.cspp.model as model
from json_artifacts import read_event_log
from json_artifacts import read_json
from json_artifacts import read_table_rows
from json_artifacts import write_event_log
from json_artifacts import write_json
from json_artifacts import write_table
import solve_clusters_first_stage as step1

from logging_utils import (
    format_cost,
    format_duration,
    format_status,
    print_config,
    print_output_paths,
    print_cluster_start,
    print_cluster_result,
    print_scenario_start,
    print_step_header,
    print_subheader,
)
from lieferdaten.runtime import get_run_layout
from lieferdaten.runtime import merge_results_json
from lieferdaten.runtime import merge_run_config
from lieferdaten.runtime import resolve_run_root
from cspp.fixed_costs import default_fixed_truck_cost
from frontend_exports import export_frontend_contract


# ---------------------------------------------------------------------------
# Config system (inlined from solve_clusters_first_stage)
# ---------------------------------------------------------------------------

DEFAULT_CONFIG = {
    "vehicle_type": "mercedes",
    "scenarios_to_use": None,
    "gap": 0.05,
    "timelimit_master_iter": 600,
    "initial_master_scenario_mode": "empty",
    "alg": "our",
    "clustering_method": "geographic",
    "initial_soc_fraction": 0.8,
    "distance_multiplier": 1.25,
    "demand_multiplier": 1.0,
    "split_deliveries": True,
    "d_cost": 0.25,
    "h": 50.0,
    "F": None,
    "charger_lifespan_years": 10,
    "operating_days_per_year": 290,
    "charger_cost_multiplier": 1.0,
    "max_tours_per_truck": int(os.environ.get("MAX_TOURS_PER_TRUCK", "3")),
    "reuse_first_stage_dir": None,
    "export_first_stage": True,
    "export_first_stage_only_installed": True,
    # Empirically backed by the nested-family second-stage replay runs:
    # 300s captures the plateau for most 16-customer iterations while keeping
    # the evaluation budget materially below the old 600s replay horizon.
    "second_stage_eval_timelimit": 300,
    "second_stage_eval_mipgap": 0.05,
    "reopt_unit": "inner_cluster",
    "reopt_loop": "iterative_active_set",
    "reopt_parallel_clusters": True,
    "reopt_eval_mipgap": 0.05,
    # Stage 3 should optimize each cluster only on the active worst-case set A
    # (i.e., S = D = A).
    "reopt_scenario_mode": "S_D_A",
    "reopt_max_iterations": None,
    "gurobi_threads": int(os.environ.get("GUROBI_THREADS", "16")),
    "parallel_total_threads": int(
        os.environ.get("CSPP_PARALLEL_TOTAL_THREADS", str(os.cpu_count() or 1))
    ),
    "parallel_max_workers": None,
}

# Runtime globals (set by apply_config)
vehicle_type = None
scenarios_to_use = None
gap = None
timelimit_master_iter = None
initial_master_scenario_mode = None
alg = None
clustering_method = None
initial_soc_fraction = None
distance_multiplier = None
demand_multiplier = None
split_deliveries = None
d_cost = None
h = None
F = None
charger_lifespan_years = None
operating_days_per_year = None
charger_cost_multiplier = None
max_tours_per_truck = None
reuse_first_stage_dir = None
export_first_stage = None
export_first_stage_only_installed = None
second_stage_eval_timelimit = None
second_stage_eval_mipgap = None
reopt_unit = None
reopt_loop = None
reopt_parallel_clusters = None
reopt_eval_mipgap = None
reopt_scenario_mode = None
reopt_max_iterations = None
gurobi_threads = None
parallel_total_threads = None
parallel_max_workers = None
explicit_config_overrides = {}

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MAX_TOTAL_MIP_THREADS = 32
MIP_THREADS_PER_SOLVE = 4


def add_bool_optional_arg(parser: argparse.ArgumentParser, name: str, default=None):
    if hasattr(argparse, "BooleanOptionalAction"):
        parser.add_argument(name, action=argparse.BooleanOptionalAction, default=default)
        return
    dest = name.lstrip("-").replace("-", "_")
    parser.add_argument(name, dest=dest, action="store_true", default=default)
    parser.add_argument(f"--no-{name.lstrip('-')}", dest=dest, action="store_false")


def add_common_args(parser: argparse.ArgumentParser):
    parser.add_argument("--vehicle-type", choices=["mercedes", "volvo"], default=None)
    parser.add_argument("--scenarios-to-use", type=int, default=None)
    parser.add_argument("--gap", type=float, default=None)
    parser.add_argument(
        "--initial-master-scenario-mode",
        choices=["empty", "max_total_demand"],
        default=None,
    )
    parser.add_argument("--alg", choices=["our", "toen", "rodr"], default=None)
    parser.add_argument(
        "--clustering-method",
        choices=["geographic", "angular_slices", "angular_slices_store_count", "tour_containment"],
        default=None,
    )
    parser.add_argument("--d-cost", type=float, default=None)
    parser.add_argument("--h", type=float, default=None)
    parser.add_argument("--F", type=float, default=None)
    parser.add_argument("--charger-cost-multiplier", type=float, default=None)
    parser.add_argument("--max-tours-per-truck", type=int, default=None)
    parser.add_argument("--reuse-first-stage-dir", type=str, default=None)
    add_bool_optional_arg(parser, "--export-first-stage", default=None)
    add_bool_optional_arg(parser, "--export-first-stage-only-installed", default=None)
    parser.add_argument("--second-stage-eval-timelimit", type=float, default=None)
    parser.add_argument("--second-stage-eval-mipgap", type=float, default=0.05)
    parser.add_argument(
        "--reopt-unit",
        choices=["inner_cluster"],
        default=None,
    )
    parser.add_argument(
        "--reopt-loop",
        choices=["iterative_active_set"],
        default=None,
    )
    add_bool_optional_arg(parser, "--reopt-parallel-clusters", default=None)
    parser.add_argument("--reopt-eval-mipgap", type=float, default=0.05)
    parser.add_argument("--reopt-max-iterations", type=int, default=None)
    parser.add_argument("--gurobi-threads", type=int, default=None)
    parser.add_argument("--parallel-total-threads", type=int, default=None)
    parser.add_argument("--parallel-max-workers", type=int, default=None)


def config_from_args(args):
    return {
        "vehicle_type": args.vehicle_type,
        "scenarios_to_use": args.scenarios_to_use,
        "gap": args.gap,
        "initial_master_scenario_mode": args.initial_master_scenario_mode,
        "alg": args.alg,
        "clustering_method": args.clustering_method,
        "d_cost": args.d_cost,
        "h": args.h,
        "F": args.F,
        "charger_cost_multiplier": args.charger_cost_multiplier,
        "max_tours_per_truck": args.max_tours_per_truck,
        "reuse_first_stage_dir": args.reuse_first_stage_dir,
        "export_first_stage": args.export_first_stage,
        "export_first_stage_only_installed": args.export_first_stage_only_installed,
        "second_stage_eval_timelimit": args.second_stage_eval_timelimit,
        "second_stage_eval_mipgap": args.second_stage_eval_mipgap,
        "reopt_unit": args.reopt_unit,
        "reopt_loop": args.reopt_loop,
        "reopt_parallel_clusters": args.reopt_parallel_clusters,
        "reopt_eval_mipgap": args.reopt_eval_mipgap,
        "reopt_max_iterations": args.reopt_max_iterations,
        "gurobi_threads": args.gurobi_threads,
        "parallel_total_threads": args.parallel_total_threads,
        "parallel_max_workers": args.parallel_max_workers,
    }


def apply_config(config=None):
    cfg = dict(DEFAULT_CONFIG)
    overrides = {}
    if config:
        for key, value in config.items():
            if value is not None:
                cfg[key] = value
                overrides[key] = value

    global vehicle_type, scenarios_to_use, gap, timelimit_master_iter, initial_master_scenario_mode, alg
    global clustering_method, initial_soc_fraction, distance_multiplier, demand_multiplier, split_deliveries
    global d_cost, h, F, charger_lifespan_years, operating_days_per_year, charger_cost_multiplier, max_tours_per_truck
    global reuse_first_stage_dir, export_first_stage, export_first_stage_only_installed
    global second_stage_eval_timelimit, second_stage_eval_mipgap
    global reopt_unit, reopt_loop
    global reopt_parallel_clusters, reopt_eval_mipgap, reopt_scenario_mode, reopt_max_iterations, gurobi_threads
    global parallel_total_threads, parallel_max_workers
    global explicit_config_overrides

    vehicle_type = cfg["vehicle_type"]
    scenarios_to_use = cfg["scenarios_to_use"]
    gap = cfg["gap"]
    timelimit_master_iter = cfg["timelimit_master_iter"]
    initial_master_scenario_mode = cfg["initial_master_scenario_mode"]
    alg = cfg["alg"]
    clustering_method = cfg["clustering_method"]
    initial_soc_fraction = cfg["initial_soc_fraction"]
    distance_multiplier = cfg["distance_multiplier"]
    demand_multiplier = cfg["demand_multiplier"]
    split_deliveries = cfg["split_deliveries"]
    d_cost = cfg["d_cost"]
    h = cfg["h"]
    charger_lifespan_years = cfg["charger_lifespan_years"]
    operating_days_per_year = cfg["operating_days_per_year"]
    charger_cost_multiplier = float(cfg["charger_cost_multiplier"])
    F = cfg["F"] if cfg["F"] is not None else default_fixed_truck_cost(
        vehicle_type,
        operating_days_per_year=operating_days_per_year,
    )
    max_tours_per_truck = max(1, int(cfg["max_tours_per_truck"]))
    reuse_first_stage_dir = Path(cfg["reuse_first_stage_dir"]) if cfg["reuse_first_stage_dir"] else None
    export_first_stage = cfg["export_first_stage"]
    export_first_stage_only_installed = cfg["export_first_stage_only_installed"]
    second_stage_eval_timelimit = cfg["second_stage_eval_timelimit"]
    second_stage_eval_mipgap = cfg["second_stage_eval_mipgap"]
    reopt_unit = cfg["reopt_unit"]
    reopt_loop = cfg["reopt_loop"]
    reopt_parallel_clusters = cfg["reopt_parallel_clusters"]
    reopt_eval_mipgap = cfg["reopt_eval_mipgap"]
    reopt_scenario_mode = cfg["reopt_scenario_mode"]
    reopt_max_iterations = cfg["reopt_max_iterations"]
    gurobi_threads = max(1, int(cfg["gurobi_threads"]))
    parallel_total_threads = max(1, int(cfg["parallel_total_threads"]))
    parallel_max_workers = (
        None if cfg["parallel_max_workers"] is None else max(1, int(cfg["parallel_max_workers"]))
    )
    explicit_config_overrides = overrides
    return cfg


apply_config()


# ---------------------------------------------------------------------------
# Path helpers (inlined from solve_clusters_first_stage)
# ---------------------------------------------------------------------------

def get_base_export_dir():
    return PROJECT_ROOT / "exports"


def _resolve_run_dir():
    """Resolve the current run directory from RUN_DIR."""
    env_run_dir = os.environ.get("RUN_DIR")
    if env_run_dir:
        return Path(env_run_dir)
    raise RuntimeError("RUN_DIR must be set for cluster_reoptimization.")


# ---------------------------------------------------------------------------
# Cluster / instance builders (inlined from solve_clusters_first_stage)
# ---------------------------------------------------------------------------

def load_clusters():
    global clustering_method
    run_dir = _resolve_run_dir()

    suffix_map = {
        "geographic": "cluster_assignments.json",
        "angular_slices": "cluster_assignments.json",
        "angular_slices_store_count": "cluster_assignments.json",
        "tour_containment": "cluster_assignments_tour_containment.json",
    }

    if clustering_method not in suffix_map:
        raise ValueError(f"Unknown clustering method: {clustering_method}")

    # If clustering method was not explicitly provided, prefer run config when available.
    run_config_method = None
    if run_dir:
        run_config_path = run_dir / "run_config.json"
        if run_config_path.exists():
            try:
                run_cfg = json.loads(run_config_path.read_text(encoding="utf-8"))
                candidate_method = run_cfg.get("clustering_method")
                if candidate_method in suffix_map:
                    run_config_method = candidate_method
            except Exception:
                run_config_method = None

    explicit_method = explicit_config_overrides.get("clustering_method")
    method_priority = []
    if run_config_method and explicit_method is None:
        method_priority.append(run_config_method)
    method_priority.append(clustering_method)
    for method in suffix_map:
        if method not in method_priority:
            method_priority.append(method)

    candidates = []
    for method in method_priority:
        filename = suffix_map[method]
        candidates.append((method, get_run_layout(run_dir)["clustering_data"] / filename))

    cluster_file = None
    resolved_method = None
    for method, candidate in candidates:
        if candidate.exists():
            cluster_file = candidate
            resolved_method = method
            break

    if cluster_file is None:
        if run_dir is not None:
            prep_dir = get_run_layout(run_dir)["run"] / "prep" / "clustering"
            assignments_json = prep_dir / "assignments.json"
            assignments_json_table = prep_dir / "cluster_assignments.json"
            if assignments_json.exists():
                try:
                    payload = json.loads(assignments_json.read_text(encoding="utf-8"))
                except Exception:
                    payload = {}
                assignments = payload.get("assignments") if isinstance(payload, dict) else None
                if isinstance(assignments, list):
                    clusters: dict[int, list[int]] = {}
                    for row in assignments:
                        if not isinstance(row, dict):
                            continue
                        cluster_id = _to_int_or_none(row.get("cluster_id"))
                        client_num = _to_int_or_none(row.get("client_num"))
                        if cluster_id is None or client_num is None:
                            continue
                        clusters.setdefault(cluster_id, []).append(client_num)
                    if clusters:
                        return {cluster_id: sorted(values) for cluster_id, values in sorted(clusters.items())}
            if assignments_json_table.exists():
                df = pd.DataFrame(read_table_rows(assignments_json_table))
                if "cluster_id" in df.columns and "client_num" in df.columns:
                    return df.groupby("cluster_id")["client_num"].apply(list).to_dict()
        raise FileNotFoundError(
            f"Cluster assignments not found. Checked: "
            + ", ".join(str(c) for _, c in candidates)
        )

    if resolved_method and resolved_method != clustering_method:
        print(
            f"Info: Using clustering method '{resolved_method}' based on available cluster assignments "
            f"at {cluster_file}"
        )
        clustering_method = resolved_method

    cluster_df = pd.DataFrame(read_table_rows(cluster_file))
    return cluster_df.groupby('cluster')['customer_id'].apply(list).to_dict()
def compute_fleet_bounds(inst, customer_subset=None):
    import math

    customers = customer_subset if customer_subset is not None else inst.J
    n_customers = len(customers)

    M_max = max_tours_per_truck

    # Demand-based fleet bound with safety factor
    alpha = 1.5  # safety factor for energy/routing infeasibility margin
    customer_set = set(customers)
    max_total_demand = 0.0
    for s in inst.S:
        scenario_demand = sum(inst.beta.get((s, j), 0) for j in customer_set)
        max_total_demand = max(max_total_demand, scenario_demand)

    if max_total_demand > 0 and inst.L > 0:
        K_demand = math.ceil(alpha * max_total_demand / (inst.L * M_max))
    else:
        K_demand = 1

    K_max = max(1, min(K_demand, n_customers))

    return K_max, M_max


def apply_demand_based_limits(inst, customer_subset=None):
    inst.K_max, inst.M_max = compute_fleet_bounds(inst, customer_subset)


def create_base_instance(apply_fleet_limits=True, arc_set_path=None):
    instance_kwargs = {
        "initial_soc_fraction": initial_soc_fraction,
        "distance_multiplier": distance_multiplier,
        "demand_multiplier": demand_multiplier,
        "split_deliveries": split_deliveries,
        "d_cost": d_cost,
        "h": h,
        "F": F,
        "K_max": 1,
        "M_max": 1,
        "charger_lifespan_years": charger_lifespan_years,
        "operating_days_per_year": operating_days_per_year,
        "charger_cost_multiplier": charger_cost_multiplier,
    }
    if arc_set_path is not None:
        instance_kwargs["arc_set_path"] = arc_set_path

    if vehicle_type == "mercedes":
        base_inst = create_mercedes_instance(**instance_kwargs)
    else:
        base_inst = create_volvo_instance(**instance_kwargs)

    if scenarios_to_use is not None:
        base_inst.S = base_inst.S[:scenarios_to_use]
    if apply_fleet_limits:
        apply_demand_based_limits(base_inst)
    return base_inst


def build_cluster_instance(base_inst, customer_list, cluster_id, check_feasibility=False):
    inst = copy.deepcopy(base_inst)
    inst.J_base = customer_list
    if hasattr(inst, "base_to_pseudo") and inst.base_to_pseudo:
        cluster_pseudos = []
        for base in customer_list:
            cluster_pseudos.extend(inst.base_to_pseudo.get(base, [base]))
        inst.J = cluster_pseudos
        inst.base_to_pseudo = {base: inst.base_to_pseudo.get(base, [base]) for base in customer_list}
        inst.pseudo_to_base = {pid: base for base, pids in inst.base_to_pseudo.items() for pid in pids}
    else:
        inst.J = customer_list
        inst.base_to_pseudo = {j: [j] for j in customer_list}
        inst.pseudo_to_base = {j: j for j in customer_list}
    inst.V = [inst.i0] + inst.J
    inst.A = None  # Complete graph within cluster
    inst.strings.ALG_INTRO_TEXT = f"CSPP Cluster {cluster_id} ({len(inst.J_base)} base, {len(inst.J)} split, {len(inst.S)} scenarios)\n"
    inst.strings.UNIQUE_IDENTIFIER = f"{inst.name}-cluster{cluster_id}"
    apply_demand_based_limits(inst)
    return inst


# ---------------------------------------------------------------------------
# First-stage I/O (inlined from solve_clusters_first_stage)
# ---------------------------------------------------------------------------

def export_first_stage_solution(path, cluster_id, first_stage, only_installed=True):
    if not first_stage:
        return
    a_dict, _ = first_stage if isinstance(first_stage, tuple) else (first_stage, {})
    a_records = []
    for (j, tau), val in a_dict.items():
        if only_installed and val < 0.5:
            continue
        a_records.append({"j": int(j), "tau": int(tau), "val": float(val)})
    data = {
        "cluster_id": int(cluster_id),
        "a": a_records,
        "a_wh": [{"tau": int(model.WAREHOUSE_CHARGER_TYPE), "val": 1.0}]
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2)


def save_first_stage_csv(output_dir, cluster_results):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    fs_path = output_dir / "cluster_first_stage.json"
    rows = []
    for cluster_id, result in cluster_results.items():
        first_stage = result.get("first_stage")
        if not first_stage:
            continue
        a_dict = first_stage[0] if isinstance(first_stage, tuple) else first_stage
        for (j, tau), val in a_dict.items():
            rows.append(
                {
                    "cluster_id": cluster_id,
                    "customer_id": j,
                    "charger_type": tau,
                    "value": float(val),
                }
            )
    write_table(fs_path, ["cluster_id", "customer_id", "charger_type", "value"], rows)


# ---------------------------------------------------------------------------
# From scenario_evaluation: load_first_stage_csv, load_warmstart_solution
# ---------------------------------------------------------------------------

def load_first_stage_csv(output_dir, base_inst, clusters):
    output_dir = Path(output_dir)
    fs_path = output_dir / "cluster_first_stage.json"
    if not fs_path.exists():
        raise FileNotFoundError(f"Missing first-stage JSON: {fs_path}")

    cluster_results = {}
    for cluster_id, customer_list in clusters.items():
        inst = build_cluster_instance(base_inst, customer_list, cluster_id, check_feasibility=False)
        cluster_results[cluster_id] = {
            "stats": None,
            "instance": inst,
            "timeout": False,
            "infeasible": False,
            "time": 0.0,
            "first_stage": ({}, {model.WAREHOUSE_CHARGER_TYPE: 1})
        }

    per_cluster = {}
    for row in read_table_rows(fs_path):
        cid = int(row["cluster_id"])
        j = int(row["customer_id"])
        tau = int(row["charger_type"])
        val = float(row["value"])
        per_cluster.setdefault(cid, {})[(j, tau)] = val

    for cid, a_dict in per_cluster.items():
        if cid in cluster_results:
            cluster_results[cid]["first_stage"] = (a_dict, {model.WAREHOUSE_CHARGER_TYPE: 1})

    return cluster_results


def load_stage1_active_sets(output_dir):
    """Load per-cluster active scenario sets saved by Stage 1.

    Returns a dict {cluster_id: [scenario_ids]} or {} if the file does not exist.
    """
    path = Path(output_dir) / "cluster_active_sets.json"
    if not path.exists():
        return {}
    result = {}
    for row in read_table_rows(path):
        cid = int(row["cluster_id"])
        scenario = int(row["scenario"])
        result.setdefault(cid, []).append(scenario)
    return result


def stage1_coverage_allows_skip(cluster_id, scenario, stage1_active_sets, current_first_stage, baseline_first_stage):
    """Skip Stage-3 re-solve when baseline Stage 1 already covered the current worst scenario."""
    if scenario is None:
        return False
    if current_first_stage != baseline_first_stage:
        return False
    covered = set(int(s) for s in (stage1_active_sets.get(cluster_id) or []))
    return int(scenario) in covered


def load_warmstart_solution(warmstart_dir, scenario):
    """
    Load warmstart solution for a scenario.

    Returns tuple matching SecondStageModel warmstart format, or None if not found.
    """
    warmstart_dir = Path(warmstart_dir)
    path = warmstart_dir / f"scenario_{scenario}_warmstart.json"

    if not path.exists():
        return None

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    def deserialize_dict(d, key_type="tuple"):
        result = {}
        for k, v in d.items():
            if key_type == "tuple":
                try:
                    parsed = ast.literal_eval(k)
                except (ValueError, SyntaxError) as exc:
                    raise ValueError(f"Invalid warmstart key '{k}' in {path}") from exc
                if not isinstance(parsed, tuple):
                    raise ValueError(f"Expected tuple key, got '{k}' in {path}")
                key = tuple(int(x) for x in parsed)
            else:
                key = int(k)
            result[key] = v
        return result

    y = deserialize_dict(data["y"], "int")
    u = deserialize_dict(data["u"], "tuple")
    t = deserialize_dict(data["t"], "tuple")
    r = deserialize_dict(data["r"], "tuple")
    c_arr = deserialize_dict(data["c_arr"], "tuple")
    p = deserialize_dict(data["p"], "tuple")
    omega = deserialize_dict(data["omega"], "tuple")
    c_dep = deserialize_dict(data["c_dep"], "tuple")
    c_ret = deserialize_dict(data["c_ret"], "tuple")
    p_wh = deserialize_dict(data["p_wh"], "tuple")
    omega_wh = deserialize_dict(data["omega_wh"], "tuple")
    p_overnight = deserialize_dict(data["p_overnight"], "int")

    return (y, u, t, r, c_arr, p, omega, c_dep, c_ret, p_wh, omega_wh, p_overnight)


def resolve_output_dir(output_dir):
    if output_dir is not None:
        output_dir = Path(output_dir).expanduser().resolve()
        try:
            run_dir = resolve_run_root(output_dir)
        except FileNotFoundError:
            return output_dir
        run_layout = get_run_layout(run_dir)
        if output_dir in {run_dir, run_layout["cspp_scenario_stage"]}:
            return run_layout["cspp_scenario_evaluation"]
        return output_dir
    raise FileNotFoundError("No output_dir provided. Pass --output_dir or set RUN_DIR to a valid run.")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_float_or_none(value):
    """Return finite float(value), else None."""
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _to_int_or_none(value):
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def extract_mip_state(grb_model):
    """Extract a compact MIP state snapshot from a solved Gurobi model."""
    status_code = None
    try:
        status_code = int(grb_model.Status)
    except Exception:
        status_code = None

    try:
        solcount = int(grb_model.SolCount)
    except Exception:
        solcount = 0

    runtime = _to_float_or_none(getattr(grb_model, "Runtime", None))
    nodecount = _to_float_or_none(getattr(grb_model, "NodeCount", None))

    obj_val = None
    mip_gap = None
    if solcount > 0:
        obj_val = _to_float_or_none(getattr(grb_model, "ObjVal", None))
        mip_gap = _to_float_or_none(getattr(grb_model, "MIPGap", None))

    obj_bound = _to_float_or_none(getattr(grb_model, "ObjBound", None))
    status_name = format_status(status_code) if status_code is not None else "UNKNOWN"

    return {
        "status_code": status_code,
        "status_name": status_name,
        "solcount": solcount,
        "runtime": runtime,
        "nodecount": nodecount,
        "obj_val": obj_val,
        "obj_bound": obj_bound,
        "mip_gap": mip_gap,
    }


def reopt_master_timelimit_for_instance(inst):
    """Spend more time in reoptimization than Stage 1, but only on selected clusters."""
    stage1_limit, _, _ = step1.effective_stage1_master_timelimit(inst)
    if stage1_limit <= 120.0:
        return 250.0
    if stage1_limit <= 250.0:
        return 300.0
    if stage1_limit <= 300.0:
        return 450.0
    return 600.0


def reopt_total_timelimit_for_instance(inst):
    """Cap total Stage-3 cluster re-solve time to avoid replaying full Stage-1 runtimes."""
    master_limit = reopt_master_timelimit_for_instance(inst)
    if master_limit <= 250.0:
        return 900.0
    if master_limit <= 300.0:
        return 1200.0
    if master_limit <= 450.0:
        return 1500.0
    return 1800.0


def focused_reopt_eval_timelimit(eval_timelimit):
    if eval_timelimit is None:
        return 600.0
    return min(600.0, max(float(eval_timelimit), 2.0 * float(eval_timelimit)))


def _compute_rel_gap(best_obj, best_bound):
    if best_obj is None or best_bound is None:
        return None
    denom = abs(best_obj)
    if denom <= 1e-12:
        return 0.0 if abs(best_bound - best_obj) <= 1e-9 else None
    return abs(best_obj - best_bound) / denom


def _reopt_solver_live_root():
    raw = os.environ.get("CSPP_LIVE_STAGE_ROOT")
    if not raw:
        return None
    return Path(raw) / "solver_live"


def _make_live_callback(scope, throttle_sec=15.0):
    root = _reopt_solver_live_root()
    if root is None:
        return None
    root.mkdir(parents=True, exist_ok=True)
    state_path = root / f"{scope}_live.json"
    events_path = root / f"{scope}_events.json"
    last_best_obj = None
    last_progress_runtime = None

    def emit(payload):
        state_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
        events = read_event_log(events_path)
        events.append(payload)
        write_event_log(events_path, events)
        try:
            export_frontend_contract(root.parents[1])
        except Exception:
            pass

    def callback(model, where):
        nonlocal last_best_obj, last_progress_runtime
        if where == GRB.Callback.MIPSOL:
            runtime = _to_float_or_none(model.cbGet(GRB.Callback.RUNTIME))
            best_obj = _to_float_or_none(model.cbGet(GRB.Callback.MIPSOL_OBJBST))
            best_bound = _to_float_or_none(model.cbGet(GRB.Callback.MIPSOL_OBJBND))
            nodecount = _to_float_or_none(model.cbGet(GRB.Callback.MIPSOL_NODCNT))
            if best_obj is None:
                return
            if last_best_obj is not None and abs(best_obj - last_best_obj) <= 1e-9:
                return
            emit({
                "event": "incumbent",
                "scope": scope,
                "runtime_sec": runtime,
                "best_obj": best_obj,
                "best_bound": best_bound,
                "gap": _compute_rel_gap(best_obj, best_bound),
                "nodecount": nodecount,
            })
            last_best_obj = best_obj
            last_progress_runtime = runtime
            return
        if where != GRB.Callback.MIP:
            return
        runtime = _to_float_or_none(model.cbGet(GRB.Callback.RUNTIME))
        if runtime is None:
            return
        if last_progress_runtime is not None and runtime - last_progress_runtime < throttle_sec:
            return
        best_obj = _to_float_or_none(model.cbGet(GRB.Callback.MIP_OBJBST))
        best_bound = _to_float_or_none(model.cbGet(GRB.Callback.MIP_OBJBND))
        nodecount = _to_float_or_none(model.cbGet(GRB.Callback.MIP_NODCNT))
        emit({
            "event": "progress",
            "scope": scope,
            "runtime_sec": runtime,
            "best_obj": best_obj,
            "best_bound": best_bound,
            "gap": _compute_rel_gap(best_obj, best_bound),
            "nodecount": nodecount,
        })
        last_progress_runtime = runtime

    return callback


def _format_gap_percent(value):
    if value is None:
        return "N/A"
    return f"{value*100:.2f}%"


def print_mip_state(label, mip_state, indent="  "):
    """Print one-line MIP diagnostics."""
    if not mip_state:
        print(f"{indent}MIP [{label}]: N/A")
        return
    runtime_str = format_duration(mip_state["runtime"]) if mip_state.get("runtime") is not None else "N/A"
    nodes = mip_state.get("nodecount")
    nodes_str = f"{nodes:.0f}" if nodes is not None else "N/A"
    print(
        f"{indent}MIP [{label}]: status={mip_state.get('status_name')} "
        f"(code={mip_state.get('status_code')}) | sols={mip_state.get('solcount', 0)} | "
        f"time={runtime_str} | nodes={nodes_str} | "
        f"obj={format_cost(mip_state.get('obj_val'))} | "
        f"bound={format_cost(mip_state.get('obj_bound'))} | "
        f"gap={_format_gap_percent(mip_state.get('mip_gap'))}"
    )


def _first_stage_signature(first_stages):
    """Build a deterministic hash for effective first-stage solutions.

    Cluster-sum evaluations only depend on installed chargers. Zero-valued
    entries in the raw first-stage dict are metadata noise and must not
    invalidate cache reuse or Stage 3 reuse.
    """
    canonical = []
    for cid in sorted(first_stages.keys()):
        active_arcs = sorted(_extract_active_arcs(first_stages.get(cid)))
        entries = []
        for j, tau in active_arcs:
            entries.append([int(j), int(tau), 1.0])
        canonical.append([int(cid), entries])
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _extract_active_arcs(first_stage):
    """Return installed arcs {(j, tau)} from a first-stage solution."""
    if not first_stage:
        return set()
    a_dict = first_stage[0] if isinstance(first_stage, tuple) else first_stage
    if not a_dict:
        return set()
    active = set()
    for (j, tau), val in a_dict.items():
        if float(val) >= 0.5:
            active.add((int(j), int(tau)))
    return active


def compute_first_stage_changes(old_first_stages, new_first_stages):
    """Compute per-cluster and aggregate first-stage arc changes."""
    per_cluster = {}
    total_added = 0
    total_removed = 0
    total_prev = 0
    total_new = 0

    all_clusters = sorted(set(old_first_stages.keys()) | set(new_first_stages.keys()))
    for cid in all_clusters:
        old_arcs = _extract_active_arcs(old_first_stages.get(cid))
        new_arcs = _extract_active_arcs(new_first_stages.get(cid))
        added = new_arcs - old_arcs
        removed = old_arcs - new_arcs

        old_count = len(old_arcs)
        new_count = len(new_arcs)
        added_count = len(added)
        removed_count = len(removed)
        changed = (added_count + removed_count) > 0

        per_cluster[cid] = {
            "prev_count": old_count,
            "new_count": new_count,
            "added_count": added_count,
            "removed_count": removed_count,
            "changed": changed,
        }

        total_prev += old_count
        total_new += new_count
        total_added += added_count
        total_removed += removed_count

    changed_clusters = sorted(cid for cid, d in per_cluster.items() if d["changed"])
    unchanged_clusters = sorted(cid for cid, d in per_cluster.items() if not d["changed"])
    summary = {
        "total_prev_arcs": total_prev,
        "total_new_arcs": total_new,
        "total_added_arcs": total_added,
        "total_removed_arcs": total_removed,
        "net_arc_change": total_new - total_prev,
        "changed_clusters": changed_clusters,
        "unchanged_clusters": unchanged_clusters,
        "num_changed_clusters": len(changed_clusters),
    }
    return summary, per_cluster


def _cluster_sum_eval_cache_key(first_stages, mipgap, timelimit, scenarios):
    """Cache key for cluster-sum evaluation results."""
    sig = _first_stage_signature(first_stages)
    gap_token = f"{float(mipgap):.5f}"
    time_token = "none" if timelimit is None else str(int(float(timelimit)))
    scen_payload = json.dumps(sorted(int(s) for s in scenarios), separators=(",", ":"))
    scen_hash = hashlib.sha1(scen_payload.encode("utf-8")).hexdigest()[:10]
    scen_token = str(len(scenarios))
    return f"{sig}__g{gap_token}__t{time_token}__n{scen_token}__sh{scen_hash}"


def _load_cluster_sum_eval_cache(cache_file):
    if not cache_file.exists():
        return None
    try:
        raw = json.loads(cache_file.read_text(encoding="utf-8"))
        totals = {int(k): v for k, v in raw.get("totals", {}).items()}
        per_cluster = {
            int(cid): {int(s): c for s, c in by_s.items()}
            for cid, by_s in raw.get("per_cluster", {}).items()
        }
        mip_states = {
            int(cid): {int(s): state for s, state in by_s.items()}
            for cid, by_s in raw.get("mip_states", {}).items()
        }
        worst_scenario = raw.get("worst_scenario")
        if worst_scenario is not None:
            worst_scenario = int(worst_scenario)
        worst_cost = raw.get("worst_cost")
        return worst_scenario, worst_cost, totals, per_cluster, mip_states
    except Exception:
        return None


def _save_cluster_sum_eval_cache(
    cache_dir,
    cache_key,
    worst_scenario,
    worst_cost,
    totals,
    per_cluster,
    mip_states,
):
    entry_dir = cache_dir / cache_key
    entry_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "worst_scenario": worst_scenario,
        "worst_cost": worst_cost,
        "totals": {str(k): v for k, v in totals.items()},
        "per_cluster": {
            str(cid): {str(s): c for s, c in by_s.items()}
            for cid, by_s in per_cluster.items()
        },
        "mip_states": {
            str(cid): {str(s): state for s, state in by_s.items()}
            for cid, by_s in mip_states.items()
        },
    }
    (entry_dir / "cluster_sum_eval.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    totals_csv = entry_dir / "cluster_sum_eval_totals.json"
    write_table(
        totals_csv,
        ["scenario", "total_cost"],
        [{"scenario": scenario, "total_cost": totals.get(scenario)} for scenario in sorted(totals.keys())],
    )

    details_csv = entry_dir / "cluster_sum_eval_details.json"
    rows = []
    for cid in sorted(per_cluster.keys()):
        by_s = per_cluster.get(cid, {})
        by_state = mip_states.get(cid, {})
        for scenario in sorted(by_s.keys()):
            state = by_state.get(scenario) or {}
            rows.append(
                {
                    "cluster_id": cid,
                    "scenario": scenario,
                    "cost": by_s.get(scenario),
                    "status": state.get("status_name"),
                    "runtime_sec": state.get("runtime"),
                    "mip_gap": state.get("mip_gap"),
                }
            )
    write_table(details_csv, ["cluster_id", "scenario", "cost", "status", "runtime_sec", "mip_gap"], rows)

    return entry_dir


def find_worst_scenario_cluster_sum_cached(
    first_stages,
    clusters,
    base_inst,
    eval_mipgap,
    eval_timelimit,
    cache_dir,
    phase_label,
):
    """Cache wrapper for cluster-sum evaluation."""
    scenarios = sorted(int(s) for s in base_inst.S)
    cache_key = _cluster_sum_eval_cache_key(first_stages, eval_mipgap, eval_timelimit, scenarios)
    cache_file = cache_dir / cache_key / "cluster_sum_eval.json"

    cached = _load_cluster_sum_eval_cache(cache_file)
    if cached is not None:
        worst_scenario, worst_cost, totals, per_cluster, mip_states = cached
        print(f"  Cluster-sum eval cache HIT [{phase_label}] key={cache_key}")
        return worst_scenario, worst_cost, totals, per_cluster, mip_states, True, cache_key

    print(
        f"  Cluster-sum eval cache MISS [{phase_label}] key={cache_key} "
        f"-> solving {len(scenarios)} scenarios"
    )
    cluster_results = _build_cluster_results_from_fs(first_stages, clusters, base_inst)
    worst_scenario, worst_cost, totals, per_cluster, mip_states = find_worst_scenario_cluster_sum(
        cluster_results, eval_mipgap, eval_timelimit
    )

    entry_dir = _save_cluster_sum_eval_cache(
        cache_dir, cache_key, worst_scenario, worst_cost, totals, per_cluster, mip_states
    )
    fs_dir = entry_dir / "first_stage_bundle"
    save_first_stage_csv_from_dict(fs_dir, first_stages, clusters, base_inst)
    save_first_stage_jsons(fs_dir, first_stages)
    print(f"  Cluster-sum eval cache SAVED [{phase_label}] -> {entry_dir}")

    return worst_scenario, worst_cost, totals, per_cluster, mip_states, False, cache_key


def append_mip_state_rows(rows, iteration, phase, entity_type, entity_id, scenario, mip_state):
    """Append a normalized row to the MIP-state export list."""
    if not mip_state:
        return
    runtime_sec = mip_state.get("runtime")
    if runtime_sec is None:
        runtime_sec = mip_state.get("runtime_sec")
    status_name = mip_state.get("status_name")
    if status_name is None and mip_state.get("status_code") is not None:
        status_name = format_status(mip_state.get("status_code"))
    rows.append({
        "iteration": iteration,
        "phase": phase,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "scenario": scenario,
        "status_code": mip_state.get("status_code"),
        "status_name": status_name,
        "solcount": mip_state.get("solcount"),
        "runtime_sec": runtime_sec,
        "nodecount": mip_state.get("nodecount"),
        "obj_val": mip_state.get("obj_val"),
        "obj_bound": mip_state.get("obj_bound"),
        "mip_gap": mip_state.get("mip_gap"),
    })


def save_mip_states_csv(output_dir, mip_rows):
    """Save collected per-solve MIP state snapshots."""
    path = Path(output_dir) / "cluster_reopt_mip_states.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    write_table(
        path,
        [
            "iteration",
            "phase",
            "entity_type",
            "entity_id",
            "scenario",
            "status_code",
            "status_name",
            "solcount",
            "runtime_sec",
            "nodecount",
            "obj_val",
            "obj_bound",
            "mip_gap",
        ],
        mip_rows,
    )
    return path


def solve_cluster_with_D(
    inst,
    cluster_id,
    D,
    cluster_num=None,
    total_clusters=None,
    master_timelimit_override=None,
    total_timelimit_override=None,
    log_path=None,
):
    """
    Solve a single cluster using the exact same Stage 1 solver path.

    Stage 3 uses a single fixed-D Stage-1 solve:
    - `inst.S` is already restricted upstream to the active set A
    - `start_sc` is forced to D
    - outer-loop scenario selection is disabled via `max_iterations=1`
    """
    def _run_solve():
        print(f"  D = {sorted(D)} | S (on instance) = {sorted(inst.S)}")
        if master_timelimit_override is None and total_timelimit_override is None:
            return step1.solve_cluster(
                inst,
                cluster_id,
                cluster_num,
                total_clusters,
                start_sc_override=sorted(D),
                max_iterations_override=1,
            )

        previous_cfg = step1.effective_config()
        overrides = {}
        if master_timelimit_override is not None:
            overrides["timelimit_master_iter"] = float(master_timelimit_override)
        step1.apply_config(overrides)
        try:
            return step1.solve_cluster(
                inst,
                cluster_id,
                cluster_num,
                total_clusters,
                total_timelimit_override=total_timelimit_override,
                start_sc_override=sorted(D),
                max_iterations_override=1,
            )
        finally:
            step1.apply_config(previous_cfg)

    if log_path is not None:
        log_path = Path(log_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as cluster_log:
            with contextlib.redirect_stdout(cluster_log), contextlib.redirect_stderr(cluster_log):
                result = _run_solve()
    else:
        result = _run_solve()

    stats = result.get('stats')
    mip_state = None
    if stats is not None:
        try:
            mip_state = extract_mip_state(stats.master_problem.model)
            print_mip_state(f"cluster-{cluster_id}/master", mip_state)
        except Exception:
            mip_state = None
    result['mip_state'] = mip_state
    return result


def _solve_cluster_worker(args):
    """
    Worker function for parallel cluster re-solves.

    Designed for use with multiprocessing: receives serializable inputs,
    returns serializable outputs (no Gurobi model objects).
    """
    (
        cid,
        customer_list,
        base_inst,
        A_set,
        scenario_mode,
        D,
        worker_config,
        master_timelimit_override,
        total_timelimit_override,
        stage1_D,
        log_path,
    ) = args
    apply_config(worker_config)
    step1.apply_config(worker_config)
    effective_D = sorted(set(D) | set(stage1_D or []))
    inst = prepare_cluster_instance(base_inst, customer_list, cid, A_set, scenario_mode, stage1_D=stage1_D)
    result = solve_cluster_with_D(
        inst,
        cid,
        effective_D,
        master_timelimit_override=master_timelimit_override,
        total_timelimit_override=total_timelimit_override,
        log_path=log_path,
    )
    # Strip non-serializable Gurobi objects before returning
    result.pop('stats', None)
    result.pop('instance', None)
    return cid, result


def solve_clusters_parallel(
    clusters,
    base_inst,
    A_set,
    scenario_mode,
    D,
    iteration=None,
    max_workers=None,
    master_timelimit_overrides=None,
    total_timelimit_overrides=None,
    stage1_active_sets=None,
):
    """
    Solve all clusters in parallel using multiprocessing.

    Falls back to sequential execution if max_workers <= 1 or on error.

    Args:
        stage1_active_sets: per-cluster active scenario sets from Stage 1,
            merged into each cluster's scenario set to preserve Stage 1 knowledge.

    Returns:
        dict mapping cluster_id -> result dict (with 'first_stage' and 'mip_state')
    """
    import multiprocessing as mp

    if stage1_active_sets is None:
        stage1_active_sets = {}
    reopt_logs_dir = get_run_layout(_resolve_run_dir())["cspp_reopt_stage"] / "logs"
    reopt_logs_dir.mkdir(parents=True, exist_ok=True)

    worker_count, threads_per_worker = step1.plan_fixed_mip_parallelism(
        len(clusters),
        max_workers=max_workers,
    )
    if worker_count <= 1:
        # Sequential fallback
        worker_config = step1.effective_config({"gurobi_threads": threads_per_worker})
        step1.apply_config(worker_config)
        results = {}
        for cid, customer_list in clusters.items():
            effective_D = sorted(set(D) | set(stage1_active_sets.get(cid) or []))
            inst = prepare_cluster_instance(base_inst, customer_list, cid, A_set, scenario_mode, stage1_D=stage1_active_sets.get(cid))
            log_path = reopt_logs_dir / (
                f"iter_{int(iteration or 0):02d}__cluster_{int(cid)}.log"
            )
            results[cid] = solve_cluster_with_D(
                inst,
                cid,
                effective_D,
                master_timelimit_override=None if master_timelimit_overrides is None else master_timelimit_overrides.get(cid),
                total_timelimit_override=None if total_timelimit_overrides is None else total_timelimit_overrides.get(cid),
                log_path=log_path,
            )
        return results

    worker_config = step1.effective_config({"gurobi_threads": threads_per_worker})
    worker_args = [
        (
            cid,
            customer_list,
            base_inst,
            A_set,
            scenario_mode,
            D,
            worker_config,
            None if master_timelimit_overrides is None else master_timelimit_overrides.get(cid),
            None if total_timelimit_overrides is None else total_timelimit_overrides.get(cid),
            stage1_active_sets.get(cid),
            str(reopt_logs_dir / f"iter_{int(iteration or 0):02d}__cluster_{int(cid)}.log"),
        )
        for cid, customer_list in clusters.items()
    ]

    print(
        f"  Launching {worker_count} parallel cluster solve worker(s) "
        f"with {threads_per_worker} Gurobi thread(s) each"
    )

    # Use fork context (Linux default) to inherit global config state
    ctx = mp.get_context("fork")
    results = {}
    with ctx.Pool(processes=worker_count) as pool:
        for cid, result in pool.imap_unordered(_solve_cluster_worker, worker_args):
            status = "OK" if result.get("first_stage") is not None else "FAILED"
            print(f"  Cluster {cid}: {status}")
            results[cid] = result

    return results


def prepare_cluster_instance(base_inst, customer_list, cluster_id, A_set, scenario_mode, stage1_D=None):
    """
    Build a cluster instance, optionally restricting inst.S to stage1_D ∪ A.

    Args:
        scenario_mode: 'S_D_A' restricts inst.S to the union set used for re-solving.
        stage1_D: per-cluster active scenarios from Stage 1.
    """
    inst = build_cluster_instance(base_inst, customer_list, cluster_id, check_feasibility=False)
    if scenario_mode == "S_D_A":
        active_set = set(A_set)
        active_set.update(stage1_D or [])
        inst.S = [s for s in inst.S if s in active_set]
    return inst


def _second_stage_tracking_callback(live_callback, trajectory, runtime_offset_sec):
    """Wrap a live callback to also record (cumulative_runtime, best_obj) points.

    Gurobi's RUNTIME resets each solve, so we add runtime_offset_sec (the total
    runtime accumulated in prior rounds) before appending, giving a single
    monotonic timeline across continuation rounds."""

    def wrapped(model, where):
        if live_callback is not None:
            live_callback(model, where)
        if where == GRB.Callback.MIPSOL:
            runtime = _to_float_or_none(model.cbGet(GRB.Callback.RUNTIME))
            best_obj = _to_float_or_none(model.cbGet(GRB.Callback.MIPSOL_OBJBST))
            if runtime is not None and best_obj is not None:
                trajectory.append((float(runtime) + float(runtime_offset_sec), best_obj))

    return wrapped


def _run_second_stage_with_continuation(build_model, base_timelimit, scope_label):
    """Run a second-stage solve with Stage-1-style continuation.

    Continues if no incumbent was found, OR the round timed out with a late
    incumbent improvement. Each extra round grows the cumulative wall-clock cap
    linearly (base * (n+1)), capped at CONTINUATION_MAX_EXTRA_ROUNDS extra
    rounds.

    build_model must return a ready-to-solve SecondStageModel. The same model is
    kept alive across continuation rounds so Gurobi can reuse its existing search
    state instead of restarting from a warmstart."""

    extension_rounds_used = 0
    ss_model = build_model()
    mip_state = None
    trajectory = []
    live_callback = _make_live_callback(scope_label)

    while True:
        round_target_sec = float(base_timelimit) * (extension_rounds_used + 1)
        round_start_sec = float(getattr(ss_model, "_accRuntime", 0.0) or 0.0)
        round_timelimit = max(0.0, round_target_sec - round_start_sec)
        ss_model.Params.TimeLimit = round_timelimit
        cb = _second_stage_tracking_callback(live_callback, trajectory, round_start_sec)
        ss_model.optimize(cb)
        mip_state = extract_mip_state(ss_model)
        total_runtime_sec = float(getattr(ss_model, "_accRuntime", 0.0) or 0.0)
        mip_state["runtime"] = total_runtime_sec

        status = ss_model.Status
        solcount = int(ss_model.SolCount)

        progress = step1.analyze_progress_points(trajectory)
        last_improvement_time_sec = progress.get("last_improvement_time_sec")

        round_duration_sec = total_runtime_sec - round_start_sec
        late_improvement_threshold = (
            round_start_sec
            + step1.CONTINUATION_LATE_IMPROVEMENT_FRACTION * round_duration_sec
        )
        missing_incumbent = solcount == 0
        late_improvement = (
            status == GRB.TIME_LIMIT
            and solcount > 0
            and last_improvement_time_sec is not None
            and last_improvement_time_sec >= late_improvement_threshold
        )
        should_continue = (
            (missing_incumbent or late_improvement)
            and status == GRB.TIME_LIMIT
            and extension_rounds_used < step1.CONTINUATION_MAX_EXTRA_ROUNDS
        )
        if not should_continue:
            break
        extension_rounds_used += 1

    if mip_state is not None:
        mip_state["extension_rounds_used"] = extension_rounds_used
        mip_state["effective_timelimit_per_round_sec"] = float(base_timelimit)
        mip_state["effective_timelimit_total_sec"] = float(getattr(ss_model, "_accRuntime", 0.0) or 0.0)
    return ss_model, mip_state


def _evaluate_cluster_scenario(args):
    cid, result, scenario, eval_mipgap, eval_timelimit, threads = args
    inst = result["instance"]
    first_stage = result.get("first_stage")

    if result.get("infeasible", False) or not first_stage:
        return cid, scenario, None, None

    cluster_demand = sum(inst.beta.get((scenario, j), 0) for j in inst.J)
    if cluster_demand <= 0:
        return cid, scenario, 0.0, None

    def build_model():
        ss_model = model.SecondStageModel(
            inst,
            scenario,
            first_stage,
            name=f"ReoptEval_{cid}_{scenario}",
        )
        ss_model.Params.OutputFlag = 0
        ss_model.Params.Threads = threads
        ss_model.Params.MIPGap = eval_mipgap
        return ss_model

    ss_model, mip_state = _run_second_stage_with_continuation(
        build_model, eval_timelimit, f"cluster_eval_c{cid}_s{scenario}"
    )

    if ss_model.SolCount == 0:
        return cid, scenario, None, mip_state

    return cid, scenario, float(ss_model.ObjVal), mip_state


def find_worst_scenario_cluster_sum(cluster_results, eval_mipgap, eval_timelimit):
    """
    Solve per-cluster second-stage for all scenarios, sum costs, and return
    the worst scenario.

    Returns:
        (worst_scenario, worst_cost, totals_dict, per_cluster_dict, mip_states_dict)
    """
    if not cluster_results:
        raise ValueError("No cluster results available for scenario evaluation.")

    first_cluster = next(iter(cluster_results.values()))
    scenarios = sorted(first_cluster["instance"].S)

    per_cluster = {cid: {} for cid in sorted(cluster_results.keys())}
    mip_states = {cid: {} for cid in sorted(cluster_results.keys())}
    totals = {scenario: 0.0 for scenario in scenarios}
    feasible_by_scenario = {scenario: True for scenario in scenarios}
    total_thread_budget = min(step1.get_parallel_total_threads(), MAX_TOTAL_MIP_THREADS)
    max_concurrent_mips = max(1, total_thread_budget // MIP_THREADS_PER_SOLVE)

    print(
        f"  Launching up to {max_concurrent_mips} concurrent cluster/scenario MIP(s) "
        f"with {MIP_THREADS_PER_SOLVE} Gurobi thread(s) each"
    )

    with ThreadPoolExecutor(max_workers=max_concurrent_mips) as executor:
        futures = {
            executor.submit(
                _evaluate_cluster_scenario,
                (
                    cid,
                    result,
                    scenario,
                    eval_mipgap,
                    step1.effective_evaluation_timelimit(result["instance"])[0],
                    MIP_THREADS_PER_SOLVE,
                ),
            ): (cid, scenario)
            for scenario in scenarios
            for cid, result in sorted(cluster_results.items())
        }
        for future in as_completed(futures):
            cid, scenario, cost, mip_state = future.result()
            per_cluster[cid][scenario] = cost
            if mip_state is not None:
                mip_states[cid][scenario] = mip_state
                print_mip_state(f"eval/cluster-{cid}/scenario-{scenario}", mip_state)
            if cost is None:
                feasible_by_scenario[scenario] = False
                continue
            if feasible_by_scenario[scenario]:
                totals[scenario] += cost

    totals = {
        scenario: totals[scenario] if feasible_by_scenario[scenario] else None
        for scenario in scenarios
    }

    feasible = {s: c for s, c in totals.items() if c is not None}
    if not feasible:
        return None, None, totals, per_cluster, mip_states

    worst = max(feasible, key=feasible.get)
    return worst, feasible[worst], totals, per_cluster, mip_states


def evaluate_cluster_subset_all_scenarios(
    first_stages,
    clusters,
    base_inst,
    cluster_ids,
    eval_mipgap,
    eval_timelimit,
):
    """Evaluate all scenarios, but only for the provided subset of clusters."""
    cluster_ids = [int(cid) for cid in cluster_ids if int(cid) in clusters]
    if not cluster_ids:
        return {}, {}

    subset_first_stages = {cid: first_stages[cid] for cid in cluster_ids}
    subset_clusters = {cid: clusters[cid] for cid in cluster_ids}
    cluster_results = _build_cluster_results_from_fs(subset_first_stages, subset_clusters, base_inst)
    _, _, _, per_cluster, mip_states = find_worst_scenario_cluster_sum(
        cluster_results,
        eval_mipgap,
        eval_timelimit,
    )
    return per_cluster, mip_states


def merge_cluster_sum_evaluation(previous_per_cluster, updated_per_cluster, scenarios):
    """Merge updated per-cluster scenario costs into the current cluster-sum snapshot."""
    merged_per_cluster = {
        int(cid): {int(s): cost for s, cost in by_s.items()}
        for cid, by_s in (previous_per_cluster or {}).items()
    }
    for cid, by_s in (updated_per_cluster or {}).items():
        merged_per_cluster[int(cid)] = {int(s): cost for s, cost in by_s.items()}

    scenario_ids = sorted(int(s) for s in scenarios)
    totals = {}
    for scenario in scenario_ids:
        total_cost = 0.0
        feasible = True
        for cid in sorted(merged_per_cluster):
            cost = merged_per_cluster.get(cid, {}).get(scenario)
            if cost is None:
                feasible = False
                break
            total_cost += cost
        totals[scenario] = total_cost if feasible else None
    return totals, merged_per_cluster


def iteration_output_dir(base_dir, iteration, d_set):
    """Return path like .../08_cluster_reoptimization/iterations/iter_01__D-3/results/"""
    d_label = "-".join(str(s) for s in sorted(d_set))
    return get_run_layout(base_dir)["cspp_reopt_iterations"] / f"iter_{iteration:02d}__D-{d_label}" / "results"


def save_iteration_csv(output_dir, iteration_rows):
    """Save per-iteration log as JSON table."""
    path = Path(output_dir) / "cluster_reopt_iterations.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "iteration", "D_set", "worst_scenario", "worst_cost",
        "candidate_worst_scenario", "candidate_worst_cost",
        "iteration_accepted",
        "target_cluster_ids", "target_cluster_cost_shares", "target_cluster_ranking_signal",
        "pressure_cluster_ids", "stage1_covered_cluster_ids", "focused_eval_cluster_ids", "focused_eval_timelimit",
        "focused_eval_costs", "focused_eval_master_gaps", "reopt_master_timelimit_by_cluster", "reopt_total_timelimit_by_cluster",
        "changed_eval_cluster_ids", "changed_eval_timelimit",
        "candidate_fs_changed_clusters", "candidate_fs_added_arcs", "candidate_fs_removed_arcs", "candidate_fs_net_arc_change",
        "candidate_fs_changed_cluster_ids",
        "applied_fs_changed_clusters", "applied_fs_added_arcs", "applied_fs_removed_arcs", "applied_fs_net_arc_change",
        "applied_fs_changed_cluster_ids",
        "cluster_solve_successful", "cluster_solve_failed", "cluster_solve_skipped",
        "accepted_clusters", "rejected_clusters", "best_iteration",
        "best_worst_cost", "eval_method",
        "eval_before_cache_hit", "eval_before_cache_key",
        "eval_after_cache_hit", "eval_after_cache_key",
    ]
    write_table(path, columns, iteration_rows)
    return path


def save_totals_comparison(output_dir, baseline_totals, final_totals):
    """Save per-scenario cost comparison (baseline vs final)."""
    path = Path(output_dir) / "cluster_reopt_totals_comparison.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    scenarios = sorted(set(baseline_totals.keys()) | set(final_totals.keys()))
    rows = []
    for s in scenarios:
        base = baseline_totals.get(s)
        final = final_totals.get(s)
        improvement = None
        if base is not None and final is not None:
            improvement = base - final
        rows.append({"scenario": s, "baseline_total": base, "final_total": final, "improvement": improvement})
    write_table(path, ["scenario", "baseline_total", "final_total", "improvement"], rows)
    return path


def save_summary_txt(output_dir, baseline_output_dir, iteration_rows, stop_reason,
                     baseline_totals, final_totals, best_iteration, best_worst_cost,
                     eval_method, A_set, totals_snapshots=None):
    """Save structured summary."""
    path = Path(output_dir) / "cluster_reopt_summary.json"
    path.parent.mkdir(parents=True, exist_ok=True)

    def _worst(costs):
        feasible = {s: c for s, c in costs.items() if c is not None}
        return max(feasible, key=feasible.get) if feasible else None

    worst_baseline = _worst(baseline_totals)
    worst_final = _worst(final_totals)
    baseline_worst_label = "Baseline worst (Stage 2 cluster-sum)"
    final_worst_label = "Final worst (Stage 3 cluster-sum eval)"

    phase_order = {"before": 0, "after": 1, "applied": 2}
    ordered_snapshots = sorted(
        totals_snapshots or [],
        key=lambda s: (s.get("iteration", 0), phase_order.get(s.get("phase"), 99)),
    )
    worst_case_improvement = None
    if worst_baseline is not None and worst_final is not None:
        base_cost = baseline_totals[worst_baseline]
        final_cost = final_totals.get(worst_final)
        if base_cost is not None and final_cost is not None:
            worst_case_improvement = base_cost - final_cost
    write_json(
        path,
        {
            "baseline_output_dir": str(baseline_output_dir),
            "eval_method": eval_method,
            "iterations": len(iteration_rows),
            "final_A_set": sorted(A_set),
            "stop_reason": stop_reason,
            "best_iteration": best_iteration,
            "best_worst_cost": best_worst_cost,
            "baseline_worst_label": baseline_worst_label,
            "baseline_worst_scenario": worst_baseline,
            "baseline_worst_cost": None if worst_baseline is None else baseline_totals.get(worst_baseline),
            "final_worst_label": final_worst_label,
            "final_worst_scenario": worst_final,
            "final_worst_cost": None if worst_final is None else final_totals.get(worst_final),
            "worst_case_improvement": worst_case_improvement,
            "iteration_rows": iteration_rows,
            "totals_snapshots": ordered_snapshots,
        },
    )
    return path


def save_first_stage_csv_from_dict(output_dir, first_stages, clusters, base_inst):
    """Save cluster_first_stage.json from a dict of {cid: first_stage}."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Build cluster_results-like dict expected by save_first_stage_csv
    cluster_results = {}
    for cid in clusters:
        fs = first_stages.get(cid)
        cluster_results[cid] = {"first_stage": fs}

    save_first_stage_csv(output_dir, cluster_results)


def save_first_stage_jsons(output_dir, first_stages):
    """Save per-cluster first-stage JSON files."""
    fs_dir = Path(output_dir) / "first_stage"
    fs_dir.mkdir(parents=True, exist_ok=True)
    for cid, fs in first_stages.items():
        if fs:
            path = fs_dir / f"cluster_{cid}_first_stage.json"
            export_first_stage_solution(path, cid, fs, only_installed=True)


def _load_baseline_totals(output_dir, scenarios):
    """Load scenario_total_costs.json from baseline run."""
    path = Path(output_dir) / "scenario_total_costs.json"
    if not path.exists():
        raise FileNotFoundError(
            f"Missing scenario_total_costs.json: {path}. "
            "Run scenario_evaluation (stage 2) first."
        )

    costs = {int(s): None for s in scenarios}
    for row in read_table_rows(path):
        s = int(row["scenario"])
        if s not in costs:
            continue
        raw = row.get("total_cost")
        if raw in {"", "n/a", "nan", "none", None}:
            costs[s] = None
        else:
            val = float(raw)
            costs[s] = val if math.isfinite(val) else None
    return costs


def _load_baseline_per_cluster_costs(output_dir, clusters, scenarios):
    """Load Stage-2 cluster_scenario_costs.json as {cluster_id: {scenario: cost}}."""
    path = Path(output_dir) / "cluster_scenario_costs.json"
    if not path.exists():
        raise FileNotFoundError(
            f"Missing cluster_scenario_costs.json: {path}. "
            "Run scenario_evaluation (stage 2) first."
        )

    scenario_ids = sorted(int(s) for s in scenarios)
    per_cluster = {
        int(cid): {int(s): None for s in scenario_ids}
        for cid in clusters
    }
    for row in read_table_rows(path):
        cid = int(row["cluster_id"])
        scenario = int(row["scenario"])
        if cid not in per_cluster or scenario not in per_cluster[cid]:
            continue
        raw = row.get("cost")
        if raw in {"", "n/a", "nan", "none", None}:
            per_cluster[cid][scenario] = None
        else:
            val = float(raw)
            per_cluster[cid][scenario] = val if math.isfinite(val) else None
    return per_cluster


def _worst_from_costs(costs):
    """Pick the worst (highest cost) feasible scenario from a costs dict.

    Returns:
        (worst_scenario, worst_cost) or (None, None) if no feasible scenario.
    """
    feasible = {s: c for s, c in costs.items() if c is not None}
    if not feasible:
        return None, None
    worst = max(feasible, key=feasible.get)
    return worst, feasible[worst]


def _format_totals_compact(totals):
    """Compact scenario->cost string for logs/summary."""
    if not totals:
        return "(none)"
    parts = []
    for scenario in sorted(totals.keys()):
        cost = totals.get(scenario)
        if cost is None:
            parts.append(f"s{scenario}=N/A")
        else:
            parts.append(f"s{scenario}={cost:.2f}")
    return ", ".join(parts)


def print_intermediate_totals(iteration, phase, source, totals):
    """Print per-scenario intermediate totals for Stage 3 diagnostics."""
    print(
        f"  Intermediate totals [iter {iteration} | {phase} | {source}]: "
        f"{_format_totals_compact(totals)}"
    )


def save_iteration_totals_csv(output_dir, totals_snapshots):
    """Export per-iteration intermediate scenario totals."""
    path = Path(output_dir) / "cluster_reopt_iteration_totals.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for snap in totals_snapshots:
        iteration = snap.get("iteration")
        phase = snap.get("phase")
        source = snap.get("source")
        totals = snap.get("totals", {})
        for scenario in sorted(totals.keys()):
            cost = totals.get(scenario)
            rows.append(
                {
                    "iteration": iteration,
                    "phase": phase,
                    "source": source,
                    "scenario": scenario,
                    "total_cost": cost,
                    "is_feasible": cost is not None,
                }
            )
    write_table(path, ["iteration", "phase", "source", "scenario", "total_cost", "is_feasible"], rows)
    return path


def _json_safe(value):
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def append_reopt_event(events_path, payload):
    events = read_event_log(events_path)
    events.append(_json_safe(payload))
    write_event_log(events_path, events)


def save_reopt_current_state(
    run_dir,
    output_dir,
    *,
    started_at,
    current_phase,
    iteration,
    A,
    best_iteration,
    best_worst_cost,
    stop_reason,
    current_totals,
    iteration_rows,
    iteration_totals_snapshots,
    mip_state_rows,
):
    output_dir = Path(output_dir)
    path = output_dir / "current_state.json"
    payload = {
        "stage": "cluster_reoptimization",
        "status": "completed" if stop_reason else ("running" if current_phase else "pending"),
        "elapsed_sec": max(0.0, time.time() - started_at),
        "current_phase": current_phase,
        "current_iteration": iteration,
        "A": sorted(A),
        "best_iteration": best_iteration,
        "best_worst_cost": best_worst_cost if best_worst_cost != float("inf") else None,
        "stop_reason": stop_reason,
        "current_totals": _json_safe(current_totals or {}),
        "iteration_rows": _json_safe(iteration_rows),
        "iteration_totals_snapshots": _json_safe(iteration_totals_snapshots),
        "mip_state_rows_count": len(mip_state_rows),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    merge_results_json(
        run_dir,
        "metrics.cluster_reoptimization_progress",
        {
            "elapsed_sec": payload["elapsed_sec"],
            "current_phase": current_phase,
            "current_iteration": iteration,
            "best_iteration": best_iteration,
            "best_worst_cost": payload["best_worst_cost"],
            "A": sorted(A),
            "mip_state_rows_count": len(mip_state_rows),
        },
    )
    try:
        export_frontend_contract(run_dir)
    except Exception:
        pass
    return path


def _build_cluster_results_from_fs(first_stages, clusters, base_inst):
    """Build cluster_results dict from first_stages dict (for Method A eval)."""
    results = {}
    for cid, customer_list in clusters.items():
        inst = build_cluster_instance(base_inst, customer_list, cid, check_feasibility=False)
        fs = first_stages.get(cid)
        results[cid] = {
            "stats": None,
            "instance": inst,
            "timeout": False,
            "infeasible": fs is None,
            "time": 0.0,
            "first_stage": fs,
        }
    return results
# ---------------------------------------------------------------------------
# Main run function
# ---------------------------------------------------------------------------

def run(output_dir=None, config=None):
    """
    Run cluster reoptimization (Stage 3).

    Args:
        output_dir: Path to baseline runs/<run_id>/06_scenario_evaluation/results.
        config: Config dict override.
    """
    run_start = time.time()
    apply_config(config)
    # Keep Stage 3 first-stage re-solves aligned with the exact Stage 1 solver path.
    step1.apply_config({
        "vehicle_type": vehicle_type,
        "scenarios_to_use": scenarios_to_use,
        "gap": gap,
        "timelimit_master_iter": timelimit_master_iter,
        "initial_master_scenario_mode": initial_master_scenario_mode,
        "alg": alg,
        "clustering_method": clustering_method,
        "initial_soc_fraction": initial_soc_fraction,
        "distance_multiplier": distance_multiplier,
        "demand_multiplier": demand_multiplier,
        "split_deliveries": split_deliveries,
        "d_cost": d_cost,
        "h": h,
        "F": F,
        "max_tours_per_truck": max_tours_per_truck,
        "reuse_first_stage_dir": str(reuse_first_stage_dir) if reuse_first_stage_dir else None,
        "export_first_stage": export_first_stage,
        "export_first_stage_only_installed": export_first_stage_only_installed,
        "second_stage_eval_timelimit": second_stage_eval_timelimit,
        "second_stage_eval_mipgap": second_stage_eval_mipgap,
        "reopt_unit": reopt_unit,
        "reopt_loop": reopt_loop,
        "reopt_eval_mipgap": reopt_eval_mipgap,
        "reopt_max_iterations": reopt_max_iterations,
        "gurobi_threads": gurobi_threads,
        "parallel_total_threads": parallel_total_threads,
        "parallel_max_workers": parallel_max_workers,
    })

    # Resolve baseline output dir (must contain stages 1+2 outputs)
    baseline_output_dir = resolve_output_dir(output_dir)
    baseline_run_dir = resolve_run_root(baseline_output_dir)
    run_layout = get_run_layout(baseline_run_dir)
    merge_run_config(
        baseline_run_dir,
        {
            "last_stage": "cluster_reoptimization",
            "max_tours_per_truck": max_tours_per_truck,
            "cluster_master_mip_timelimit": timelimit_master_iter,
            "second_stage_eval_timelimit": second_stage_eval_timelimit,
        },
    )

    # Load clusters and base instance
    clusters = load_clusters()

    base_inst = create_base_instance(apply_fleet_limits=False)

    # Load baseline first-stage solutions
    cluster_results = load_first_stage_csv(run_layout["cspp_first_stage"], base_inst, clusters)

    # Load per-cluster active scenario sets from Stage 1
    stage1_active_sets = load_stage1_active_sets(run_layout["cspp_first_stage"])

    # Extract first_stage dicts
    current_first_stages = {}
    for cid, result in cluster_results.items():
        current_first_stages[cid] = result.get("first_stage")
    baseline_first_stages = copy.deepcopy(current_first_stages)

    # Load baseline scenario totals for comparison
    baseline_totals = _load_baseline_totals(run_layout["cspp_scenario_evaluation"], base_inst.S)

    # Config parameters
    scenario_mode = reopt_scenario_mode
    if scenario_mode != "S_D_A":
        print(f"  NOTE: overriding scenario mode '{scenario_mode}' -> 'S_D_A' for Stage 3.")
        scenario_mode = "S_D_A"
    _eval_mipgap = second_stage_eval_mipgap
    _eval_timelimit, _, _ = step1.effective_evaluation_timelimit(base_inst)
    max_iterations = reopt_max_iterations
    resolved_eval_mode = "cluster_sum"
    eval_method = "A"
    merge_run_config(
        baseline_run_dir,
        {
            "second_stage_eval_timelimit": _eval_timelimit,
            "reoptimization_evaluation": resolved_eval_mode,
            "reopt_unit_effective": reopt_unit,
            "reopt_loop_effective": reopt_loop,
        },
    )
    cluster_sum_eval_cache_dir = run_layout["cspp_reopt_cache"] / "cluster_sum_eval"
    cluster_sum_eval_cache_dir.mkdir(parents=True, exist_ok=True)

    eval_scenarios = sorted(int(s) for s in base_inst.S)

    feasible_baseline = {s: c for s, c in baseline_totals.items() if c is not None}
    limit = max_iterations if max_iterations is not None else len(feasible_baseline)
    limit = max(1, int(limit))

    print_step_header(3, "CLUSTER REOPTIMIZATION", total_steps=3)
    print_config({
        "Baseline run": baseline_run_dir.name,
        "Total clusters": len(clusters),
        "Total customers": total_customers,
        "Eval method": "cluster-sum",
        "Evaluation mode": resolved_eval_mode,
        "Reopt unit": reopt_unit,
        "Reopt loop": reopt_loop,
        "Scenario mode": scenario_mode,
        "Cluster master MIP limit": format_duration(timelimit_master_iter),
        "Eval MIP gap": f"{_eval_mipgap*100:.0f}%",
        "Eval time limit": format_duration(_eval_timelimit),
        "Eval settings source": (
            "second_stage_eval_mipgap/second_stage_eval_timelimit override"
            if step1.is_explicit_config_override("second_stage_eval_timelimit")
            else "second_stage_eval_mipgap/complexity-aware evaluation bucket"
        ),
        "Cluster eval cache": cluster_sum_eval_cache_dir,
        "Stage 1 active sets": (
            f"{len(stage1_active_sets)} clusters, "
            f"{len(set(s for d in stage1_active_sets.values() for s in d))} unique scenarios"
            if stage1_active_sets
            else "not available (legacy run)"
        ),
        "Max iterations": limit,
        "Parallel cluster solves": reopt_parallel_clusters,
        "Parallel thread budget": step1.get_parallel_total_threads(),
        "Max Gurobi threads/solve": gurobi_threads,
    })

    # Initialize tracking
    A = []
    best_iteration = 0
    best_worst_cost = float('inf')
    best_first_stages = copy.deepcopy(current_first_stages)
    best_totals = None
    iteration_rows = []
    iteration_totals_snapshots = []
    mip_state_rows = []
    stop_reason = "Reached max iterations."
    current_totals = dict(baseline_totals)
    current_eval_per_cluster = None
    robust_accept_eps = 1e-4

    print_subheader("Reoptimization Loop")
    reopt_live_dir = run_layout["cspp_reopt"]
    reopt_live_dir.mkdir(parents=True, exist_ok=True)
    os.environ["CSPP_LIVE_STAGE_ROOT"] = str(reopt_live_dir)
    reopt_events_path = reopt_live_dir / "event_log.json"
    reopt_current_state_path = save_reopt_current_state(
        baseline_run_dir,
        reopt_live_dir,
        started_at=run_start,
        current_phase="initialized",
        iteration=0,
        A=A,
        best_iteration=best_iteration,
        best_worst_cost=best_worst_cost,
        stop_reason=stop_reason,
        current_totals=current_totals,
        iteration_rows=iteration_rows,
        iteration_totals_snapshots=iteration_totals_snapshots,
        mip_state_rows=mip_state_rows,
    )
    append_reopt_event(
        reopt_events_path,
        {
            "event": "initialized",
            "iteration": 0,
            "phase": "initialized",
            "eval_method": eval_method,
            "baseline_run_dir": baseline_run_dir,
        },
    )

    for iteration in range(1, limit + 1):
        iter_start = time.time()
        append_reopt_event(
            reopt_events_path,
            {
                "event": "iteration_start",
                "iteration": iteration,
                "phase": "eval_before",
                "A": sorted(A),
            },
        )
        save_reopt_current_state(
            baseline_run_dir,
            reopt_live_dir,
            started_at=run_start,
            current_phase="eval_before",
            iteration=iteration,
            A=A,
            best_iteration=best_iteration,
            best_worst_cost=best_worst_cost,
            stop_reason=stop_reason,
            current_totals=current_totals,
            iteration_rows=iteration_rows,
            iteration_totals_snapshots=iteration_totals_snapshots,
            mip_state_rows=mip_state_rows,
        )

        # --- Step a: Find worst scenario ---
        eval_states_before = {}
        eval_per_cluster_before = None
        eval_before_cache_hit = False
        eval_before_cache_key = None
        if iteration == 1 and current_first_stages == baseline_first_stages:
            current_totals = dict(baseline_totals)
            current_eval_per_cluster = _load_baseline_per_cluster_costs(
                baseline_output_dir,
                clusters,
                base_inst.S,
            )
            worst_scenario, worst_cost = _worst_from_costs(current_totals)
            eval_before_cache_hit = True
            eval_before_cache_key = "stage2-baseline"
            print("  Reusing Stage 2 baseline cluster-sum totals and per-cluster costs for eval_before")
        else:
            worst_scenario, worst_cost, current_totals, eval_per_cluster_before, eval_states_before, eval_before_cache_hit, eval_before_cache_key = find_worst_scenario_cluster_sum_cached(
                current_first_stages, clusters, base_inst,
                _eval_mipgap, _eval_timelimit,
                cluster_sum_eval_cache_dir, phase_label=f"iter{iteration}/eval_before"
            )
            current_eval_per_cluster = eval_per_cluster_before

        if worst_scenario is None:
            stop_reason = "No feasible scenarios found during evaluation."
            break

        if not eval_before_cache_hit:
            for cid, by_s in sorted(eval_states_before.items()):
                for scenario, mip_state in sorted(by_s.items()):
                    append_mip_state_rows(
                        mip_state_rows, iteration, "eval_before", "cluster_eval", cid, scenario, mip_state
                    )
        else:
            print(f"  Reused cached cluster-sum eval for eval_before (key={eval_before_cache_key})")

        print(f"\n  Iteration {iteration}/{limit}")
        print(f"  Worst scenario: {worst_scenario} ({format_cost(worst_cost)})")
        eval_source_label = "cluster-sum"
        print_intermediate_totals(iteration, "before", eval_source_label, current_totals)
        iteration_totals_snapshots.append({
            "iteration": iteration,
            "phase": "before",
            "source": eval_source_label,
            "totals": dict(current_totals),
        })
        save_iteration_totals_csv(reopt_live_dir, iteration_totals_snapshots)
        save_mip_states_csv(reopt_live_dir, mip_state_rows)
        append_reopt_event(
            reopt_events_path,
            {
                "event": "eval_before_complete",
                "iteration": iteration,
                "phase": "eval_before",
                "worst_scenario": worst_scenario,
                "worst_cost": worst_cost,
                "cache_hit": eval_before_cache_hit,
                "cache_key": eval_before_cache_key,
                "totals": dict(current_totals),
            },
        )
        save_reopt_current_state(
            baseline_run_dir,
            reopt_live_dir,
            started_at=run_start,
            current_phase="eval_before_complete",
            iteration=iteration,
            A=A,
            best_iteration=best_iteration,
            best_worst_cost=best_worst_cost,
            stop_reason=stop_reason,
            current_totals=current_totals,
            iteration_rows=iteration_rows,
            iteration_totals_snapshots=iteration_totals_snapshots,
            mip_state_rows=mip_state_rows,
        )

        if best_iteration == 0 and worst_cost is not None and best_worst_cost == float("inf"):
            best_worst_cost = worst_cost
            best_first_stages = copy.deepcopy(current_first_stages)
            best_totals = dict(current_totals)

        target_cluster_ids = sorted(clusters)
        stage1_covered_cluster_ids = [
            cid for cid in target_cluster_ids
            if stage1_coverage_allows_skip(
                cid,
                worst_scenario,
                stage1_active_sets,
                current_first_stages.get(cid),
                baseline_first_stages.get(cid),
            )
        ]

        if worst_scenario in A:
            stop_reason = (
                f"Converged: worst scenario {worst_scenario} is already in A={sorted(A)}."
            )
            print(f"  {stop_reason}")
            break
        if target_cluster_ids and len(stage1_covered_cluster_ids) == len(target_cluster_ids):
            stop_reason = (
                f"Converged: worst scenario {worst_scenario} is already covered by Stage 1 active sets "
                f"for target clusters {target_cluster_ids}."
            )
            print(f"  {stop_reason}")
            break

        # --- Step c: Expand A ---
        A.append(worst_scenario)
        A_set = set(A)
        print(f"  A = {sorted(A)}")
        focused_eval_cluster_ids = []
        focused_eval_timelimit = None
        focused_eval_costs = {}
        focused_eval_master_gaps = {}

        print(f"  Target clusters: {target_cluster_ids}")
        if stage1_covered_cluster_ids:
            print(f"  Skipping Stage-1-covered clusters: {stage1_covered_cluster_ids}")
        append_reopt_event(
            reopt_events_path,
            {
                "event": "scenario_added_to_A",
                "iteration": iteration,
                "phase": "cluster_solve",
                "A": sorted(A),
                "added_scenario": worst_scenario,
                "target_cluster_ids": target_cluster_ids,
                "target_cluster_cost_shares": {},
                "target_cluster_ranking_signal": {},
                "pressure_cluster_ids": [],
                "stage1_covered_cluster_ids": stage1_covered_cluster_ids,
                "focused_eval_cluster_ids": focused_eval_cluster_ids,
                "focused_eval_timelimit": focused_eval_timelimit,
                "focused_eval_costs": focused_eval_costs,
                "focused_eval_master_gaps": focused_eval_master_gaps,
            },
        )
        save_reopt_current_state(
            baseline_run_dir,
            reopt_live_dir,
            started_at=run_start,
            current_phase="cluster_solve",
            iteration=iteration,
            A=A,
            best_iteration=best_iteration,
            best_worst_cost=best_worst_cost,
            stop_reason=stop_reason,
            current_totals=current_totals,
            iteration_rows=iteration_rows,
            iteration_totals_snapshots=iteration_totals_snapshots,
            mip_state_rows=mip_state_rows,
        )

        # --- Step d: Re-solve each cluster with D=A ---
        pre_iter_first_stages = current_first_stages
        candidate_first_stages = copy.deepcopy(current_first_stages)
        cluster_solve_successful = []
        cluster_solve_failed = []
        cluster_solve_skipped = list(stage1_covered_cluster_ids)
        target_clusters = {
            cid: clusters[cid]
            for cid in target_cluster_ids
            if cid in clusters and cid not in stage1_covered_cluster_ids
        }
        reopt_master_timelimit_by_cluster = {}
        reopt_total_timelimit_by_cluster = {}

        if reopt_parallel_clusters and len(target_clusters) > 1:
            parallel_results = solve_clusters_parallel(
                target_clusters, base_inst, A_set, scenario_mode, A_set,
                iteration=iteration,
                max_workers=len(target_clusters),
                master_timelimit_overrides=None,
                total_timelimit_overrides=None,
                stage1_active_sets=stage1_active_sets,
            )
            for cid in sorted(parallel_results):
                result = parallel_results[cid]
                append_mip_state_rows(
                    mip_state_rows,
                    iteration,
                    "cluster_solve",
                    "cluster_master",
                    cid,
                    worst_scenario,
                    result.get("mip_state"),
                )
                new_fs = result.get("first_stage")
                if new_fs is None:
                    print(f"  Cluster {cid}: solve failed, keeping old solution")
                    cluster_solve_failed.append(cid)
                    continue
                candidate_first_stages[cid] = new_fs
                cluster_solve_successful.append(cid)
        else:
            for cid, customer_list in target_clusters.items():
                effective_D = sorted(set(A_set) | set(stage1_active_sets.get(cid) or []))
                inst = prepare_cluster_instance(base_inst, customer_list, cid, A_set, scenario_mode, stage1_D=stage1_active_sets.get(cid))
                cluster_log_path = run_layout["cspp_reopt_stage"] / "logs" / f"iter_{iteration:02d}__cluster_{int(cid)}.log"
                result = solve_cluster_with_D(
                    inst,
                    cid,
                    effective_D,
                    master_timelimit_override=None,
                    total_timelimit_override=None,
                    log_path=cluster_log_path,
                )
                append_mip_state_rows(
                    mip_state_rows,
                    iteration,
                    "cluster_solve",
                    "cluster_master",
                    cid,
                    worst_scenario,
                    result.get("mip_state"),
                )

                new_fs = result.get("first_stage")
                if new_fs is None:
                    print(f"  Cluster {cid}: solve failed, keeping old solution")
                    cluster_solve_failed.append(cid)
                    continue

                candidate_first_stages[cid] = new_fs
                cluster_solve_successful.append(cid)
                print(f"  Cluster {cid}: candidate updated")

        candidate_change_summary, candidate_change_by_cluster = compute_first_stage_changes(
            current_first_stages, candidate_first_stages
        )
        print(
            "  First-stage candidate delta: "
            f"changed_clusters={candidate_change_summary['num_changed_clusters']}/{len(candidate_change_by_cluster)} | "
            f"arcs +{candidate_change_summary['total_added_arcs']} "
            f"-{candidate_change_summary['total_removed_arcs']} "
            f"(net {candidate_change_summary['net_arc_change']:+d})"
        )
        if candidate_change_summary["num_changed_clusters"] == 0:
            print("    No first-stage arc changes in candidate.")
        else:
            for cid in candidate_change_summary["changed_clusters"]:
                d = candidate_change_by_cluster[cid]
                print(
                    f"    Cluster {cid}: {d['prev_count']} -> {d['new_count']} arcs "
                    f"(+{d['added_count']} / -{d['removed_count']})"
                )
        save_mip_states_csv(reopt_live_dir, mip_state_rows)
        append_reopt_event(
            reopt_events_path,
            {
                "event": "cluster_solve_complete",
                "iteration": iteration,
                "phase": "cluster_solve",
                "target_cluster_ids": target_cluster_ids,
                "reopt_master_timelimit_by_cluster": {str(cid): val for cid, val in reopt_master_timelimit_by_cluster.items()},
                "reopt_total_timelimit_by_cluster": {str(cid): val for cid, val in reopt_total_timelimit_by_cluster.items()},
                "successful_clusters": sorted(cluster_solve_successful),
                "failed_clusters": sorted(cluster_solve_failed),
                "skipped_clusters": sorted(cluster_solve_skipped),
                "candidate_change_summary": candidate_change_summary,
            },
        )
        save_reopt_current_state(
            baseline_run_dir,
            reopt_live_dir,
            started_at=run_start,
            current_phase="eval_after",
            iteration=iteration,
            A=A,
            best_iteration=best_iteration,
            best_worst_cost=best_worst_cost,
            stop_reason=stop_reason,
            current_totals=current_totals,
            iteration_rows=iteration_rows,
            iteration_totals_snapshots=iteration_totals_snapshots,
            mip_state_rows=mip_state_rows,
        )

        # --- Step e: Re-evaluate candidate over all scenarios ---
        eval_states_after = {}
        eval_after_cache_hit = False
        eval_after_cache_key = None
        changed_eval_cluster_ids = list(candidate_change_summary["changed_clusters"])
        changed_eval_timelimit = _eval_timelimit if changed_eval_cluster_ids else None
        if candidate_change_summary["num_changed_clusters"] == 0:
            new_totals = dict(current_totals)
            new_worst, new_worst_cost = _worst_from_costs(new_totals)
            eval_after_cache_hit = True
            eval_after_cache_key = "unchanged-first-stage"
            print("  Candidate first stage unchanged; reusing current evaluation results for eval_after")
        else:
            print(
                "  Re-evaluating changed clusters only: "
                f"{changed_eval_cluster_ids} across {len(base_inst.S)} scenarios"
            )
            candidate_eval_updates, eval_states_after = evaluate_cluster_subset_all_scenarios(
                candidate_first_stages,
                clusters,
                base_inst,
                changed_eval_cluster_ids,
                _eval_mipgap, _eval_timelimit,
            )
            new_totals, candidate_eval_per_cluster = merge_cluster_sum_evaluation(
                current_eval_per_cluster,
                candidate_eval_updates,
                base_inst.S,
            )
            new_worst, new_worst_cost = _worst_from_costs(new_totals)
            eval_after_cache_key = "changed-clusters-only"

        if not eval_after_cache_hit:
            for cid, by_s in sorted(eval_states_after.items()):
                for scenario, mip_state in sorted(by_s.items()):
                    append_mip_state_rows(
                        mip_state_rows, iteration, "eval_after", "cluster_eval", cid, scenario, mip_state
                    )
        else:
            print(f"  Reused cached cluster-sum eval for eval_after (key={eval_after_cache_key})")

        print_intermediate_totals(iteration, "after", eval_source_label, new_totals)
        iteration_totals_snapshots.append({
            "iteration": iteration,
            "phase": "after",
            "source": eval_source_label,
            "totals": dict(new_totals),
        })
        save_iteration_totals_csv(reopt_live_dir, iteration_totals_snapshots)
        save_mip_states_csv(reopt_live_dir, mip_state_rows)
        append_reopt_event(
            reopt_events_path,
            {
                "event": "eval_after_complete",
                "iteration": iteration,
                "phase": "eval_after",
                "candidate_worst_scenario": new_worst,
                "candidate_worst_cost": new_worst_cost,
                "cache_hit": eval_after_cache_hit,
                "cache_key": eval_after_cache_key,
                "totals": dict(new_totals),
                "target_cluster_ids": target_cluster_ids,
                "changed_eval_cluster_ids": changed_eval_cluster_ids,
                "changed_eval_timelimit": changed_eval_timelimit,
            },
        )

        # --- Step f: Accept/reject this iteration by robust objective ---
        accepted_iteration = False
        if new_worst_cost is None and worst_cost is None:
            accepted_iteration = True
        elif new_worst_cost is not None and worst_cost is None:
            accepted_iteration = True
        elif new_worst_cost is not None and worst_cost is not None:
            accepted_iteration = new_worst_cost <= worst_cost + robust_accept_eps

        if accepted_iteration:
            current_first_stages = candidate_first_stages
            current_totals = new_totals
            if candidate_change_summary["num_changed_clusters"] > 0:
                current_eval_per_cluster = candidate_eval_per_cluster
            accepted_clusters = sorted(candidate_change_summary["changed_clusters"])
            rejected_clusters = sorted(cluster_solve_failed)
            print(
                f"  Iteration ACCEPTED (robust): worst {format_cost(worst_cost)} -> {format_cost(new_worst_cost)} "
                f"(scenario {worst_scenario} -> {new_worst})"
            )
        else:
            accepted_clusters = []
            rejected_clusters = sorted(set(cluster_solve_successful) | set(cluster_solve_failed))
            print(
                f"  Iteration REJECTED (robust): candidate worst {format_cost(new_worst_cost)} "
                f"is worse than current {format_cost(worst_cost)}"
            )

        applied_change_summary, _ = compute_first_stage_changes(
            pre_iter_first_stages, current_first_stages
        )
        print(
            "  First-stage applied delta: "
            f"changed_clusters={applied_change_summary['num_changed_clusters']} | "
            f"arcs +{applied_change_summary['total_added_arcs']} "
            f"-{applied_change_summary['total_removed_arcs']} "
            f"(net {applied_change_summary['net_arc_change']:+d})"
        )
        print_intermediate_totals(iteration, "applied", eval_source_label, current_totals)
        iteration_totals_snapshots.append({
            "iteration": iteration,
            "phase": "applied",
            "source": eval_source_label,
            "totals": dict(current_totals),
        })
        save_iteration_totals_csv(reopt_live_dir, iteration_totals_snapshots)

        # Save iteration outputs for the applied first-stage state
        iter_dir = iteration_output_dir(baseline_run_dir, iteration, A)
        iter_dir.mkdir(parents=True, exist_ok=True)
        save_first_stage_csv_from_dict(iter_dir, current_first_stages, clusters, base_inst)
        save_first_stage_jsons(iter_dir, current_first_stages)

        if accepted_iteration and new_worst_cost is not None and new_worst_cost < best_worst_cost:
            best_worst_cost = new_worst_cost
            best_iteration = iteration
            best_first_stages = copy.deepcopy(current_first_stages)
            best_totals = dict(current_totals)

        iter_time = time.time() - iter_start
        current_worst_after, current_worst_after_cost = _worst_from_costs(current_totals)
        print(f"  Current worst after iteration: {current_worst_after} ({format_cost(current_worst_after_cost)}) | "
              f"Best so far: iter {best_iteration} ({format_cost(best_worst_cost)}) | "
              f"Time: {format_duration(iter_time)}")

        iteration_rows.append({
            "iteration": iteration,
            "D_set": sorted(A),
            "worst_scenario": worst_scenario,
            "worst_cost": worst_cost,
            "candidate_worst_scenario": new_worst,
            "candidate_worst_cost": new_worst_cost,
            "iteration_accepted": accepted_iteration,
            "target_cluster_ids": target_cluster_ids,
            "target_cluster_cost_shares": {},
            "target_cluster_ranking_signal": {},
            "pressure_cluster_ids": [],
            "stage1_covered_cluster_ids": stage1_covered_cluster_ids,
            "focused_eval_cluster_ids": focused_eval_cluster_ids,
            "focused_eval_timelimit": focused_eval_timelimit,
            "focused_eval_costs": focused_eval_costs,
            "focused_eval_master_gaps": focused_eval_master_gaps,
            "changed_eval_cluster_ids": changed_eval_cluster_ids,
            "changed_eval_timelimit": changed_eval_timelimit,
            "reopt_master_timelimit_by_cluster": {str(cid): val for cid, val in reopt_master_timelimit_by_cluster.items()},
            "reopt_total_timelimit_by_cluster": {str(cid): val for cid, val in reopt_total_timelimit_by_cluster.items()},
            "candidate_fs_changed_clusters": candidate_change_summary["num_changed_clusters"],
            "candidate_fs_added_arcs": candidate_change_summary["total_added_arcs"],
            "candidate_fs_removed_arcs": candidate_change_summary["total_removed_arcs"],
            "candidate_fs_net_arc_change": candidate_change_summary["net_arc_change"],
            "candidate_fs_changed_cluster_ids": candidate_change_summary["changed_clusters"],
            "applied_fs_changed_clusters": applied_change_summary["num_changed_clusters"],
            "applied_fs_added_arcs": applied_change_summary["total_added_arcs"],
            "applied_fs_removed_arcs": applied_change_summary["total_removed_arcs"],
            "applied_fs_net_arc_change": applied_change_summary["net_arc_change"],
            "applied_fs_changed_cluster_ids": applied_change_summary["changed_clusters"],
            "cluster_solve_successful": sorted(cluster_solve_successful),
            "cluster_solve_failed": sorted(cluster_solve_failed),
            "cluster_solve_skipped": sorted(cluster_solve_skipped),
            "accepted_clusters": sorted(accepted_clusters),
            "rejected_clusters": sorted(rejected_clusters),
            "best_iteration": best_iteration,
            "best_worst_cost": best_worst_cost,
            "eval_method": eval_method,
            "eval_before_cache_hit": eval_before_cache_hit,
            "eval_before_cache_key": eval_before_cache_key,
            "eval_after_cache_hit": eval_after_cache_hit,
            "eval_after_cache_key": eval_after_cache_key,
        })
        save_iteration_csv(reopt_live_dir, iteration_rows)
        save_mip_states_csv(reopt_live_dir, mip_state_rows)
        append_reopt_event(
            reopt_events_path,
            {
                "event": "iteration_complete",
                "iteration": iteration,
                "phase": "iteration_complete",
                "iteration_accepted": accepted_iteration,
                "best_iteration": best_iteration,
                "best_worst_cost": best_worst_cost if best_worst_cost != float("inf") else None,
                "current_totals": dict(current_totals),
            },
        )
        save_reopt_current_state(
            baseline_run_dir,
            reopt_live_dir,
            started_at=run_start,
            current_phase="iteration_complete",
            iteration=iteration,
            A=A,
            best_iteration=best_iteration,
            best_worst_cost=best_worst_cost,
            stop_reason=stop_reason,
            current_totals=current_totals,
            iteration_rows=iteration_rows,
            iteration_totals_snapshots=iteration_totals_snapshots,
            mip_state_rows=mip_state_rows,
        )

        continue_with_shifted_worst = (
            not accepted_iteration
            and candidate_change_summary["num_changed_clusters"] > 0
            and new_worst is not None
            and new_worst != worst_scenario
        )
        if continue_with_shifted_worst:
            A = sorted(set(A) | {new_worst})
            print(
                "  Candidate changed the worst scenario but was rejected; "
                f"continuing with expanded A={sorted(A)} for another iteration."
            )

        if applied_change_summary["num_changed_clusters"] == 0:
            if continue_with_shifted_worst:
                continue
            stop_reason = (
                f"Converged: applied first stage unchanged after re-solving on A={sorted(A)}."
            )
            print(f"  {stop_reason}")
            break

    # --- Save summary outputs ---
    summary_dir = run_layout["cspp_reopt_summary"]
    summary_dir.mkdir(parents=True, exist_ok=True)

    # Final totals from best iteration
    if best_totals is not None:
        final_totals = dict(best_totals)
        print("  Final totals reused from best evaluated state.")
    else:
        _, _, final_totals, _, _, _, _ = find_worst_scenario_cluster_sum_cached(
            best_first_stages, clusters, base_inst,
            _eval_mipgap, _eval_timelimit,
            cluster_sum_eval_cache_dir, phase_label="final_best"
        )
    reopt_maps_exported = 0
    reopt_maps_note = None

    print_output_paths({
        "Summary": summary_txt,
        "Iterations CSV": iter_csv,
        "Iteration totals CSV": iter_totals_csv,
        "MIP states CSV": mip_csv,
        "Live state": reopt_current_state_path,
        "Live events": reopt_events_path,
        "Live iterations CSV": final_live_iter_csv,
        "Live iteration totals CSV": final_live_iter_totals_csv,
        "Live MIP states CSV": final_live_mip_csv,
        "Totals comparison": totals_csv,
        "Route maps": reopt_maps_dir if reopt_maps_exported > 0 else "(none)",
        "Reopt directory": run_layout["cspp_reopt"],
    })

    return run_layout["cspp_reopt"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="CSPP cluster reoptimization (Stage 3): iterative worst-scenario "
                    "reoptimization with per-cluster acceptance checking."
    )
    add_common_args(parser)
    parser.add_argument(
        "--output_dir",
        type=str,
        default=None,
        help="Baseline run dir (runs/<run_id>/results).",
    )
    args, unknown = parser.parse_known_args()
    if unknown:
        print(f"Ignoring unrecognized arguments: {' '.join(unknown)}")

    run(
        output_dir=args.output_dir,
        config=config_from_args(args),
    )
