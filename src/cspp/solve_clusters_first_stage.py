import argparse
import contextlib
import json
import math
import os
import re
import platform
import sys
import time
from pathlib import Path
from typing import Dict

import pandas as pd
from gurobipy import GRB

from algorithm_types import AlgorithmType, AlgorithmOptions
from classes import AlgorithmParams, TimeoutException
from algorithm import ourAlgorithm, toenAlgorithm, rodrAlgorithm
from applications import Application
from applications.cspp.instance import create_mercedes_instance, create_volvo_instance
import applications.cspp.model as model

from logging_utils import (
    print_header, print_subheader, print_config, print_step_header,
    print_cluster_start, print_cluster_result,
    print_output_paths, format_duration
)
from lieferdaten.runtime import build_run_dir
from lieferdaten.runtime import get_run_layout
from lieferdaten.runtime import merge_run_config
from lieferdaten.runtime import merge_results_json
from json_artifacts import read_json
from json_artifacts import read_table_rows
from json_artifacts import write_event_log
from json_artifacts import write_json
from json_artifacts import write_table
from cspp.fixed_costs import default_fixed_truck_cost
from frontend_exports import export_frontend_contract


DEFAULT_CONFIG = {
    "vehicle_type": "mercedes",
    "scenarios_to_use": None,
    "gap": 0.05,
    "timelimit_master_iter": 600,
    "heur_timelimit": 60.0,
    "var_timelimit_minimum": 60.0,
    "stage1_var_timelimit_factor": 1.0,
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
    "reopt_scenario_mode": "S_D_A",
    "reopt_eval_mipgap": 0.05,
    "reopt_max_iterations": None,
    "stage1_max_iterations": None,
    "gurobi_threads": int(os.environ.get("GUROBI_THREADS", "16")),
    "parallel_total_threads": int(
        os.environ.get("CSPP_PARALLEL_TOTAL_THREADS", str(os.cpu_count() or 1))
    ),
    "parallel_max_workers": None,
}

CLUSTER_SIZE_AWARE_TIMELIMIT_KEYS = {
    "timelimit_master_iter",
    "heur_timelimit",
    "var_timelimit_minimum",
    "second_stage_eval_timelimit",
}
# Shared cluster-size-aware time limit tiers by customer count.
CLUSTER_SIZE_TIMELIMIT_TIERS = [
    (12, 0.2),
    (14, 0.25),
    (16, 0.5),
]

STAGE1_MASTER_TIMELIMIT_RULES = {
    "xsmall": 120.0,
    "small": 180.0,
    "medium": 240.0,
    "hard": 300.0,
    "xhard": 360.0,
}

STAGE1_VAR_TIMELIMIT_MINIMUM_RULES = {
    "xsmall": 30.0,
    "small": 45.0,
    "medium": 60.0,
    "hard": 90.0,
    "xhard": 120.0,
}

EVALUATION_TIMELIMIT_RULES = {
    "xsmall": 45.0,
    "small": 60.0,
    "medium": 120.0,
    "hard": 180.0,
    "xhard": 240.0,
}
CONTINUATION_MAX_EXTRA_ROUNDS = 3
CONTINUATION_LATE_IMPROVEMENT_FRACTION = 0.8

# Runtime globals (set by apply_config)
vehicle_type = None
scenarios_to_use = None
gap = None
timelimit_master_iter = None
heur_timelimit = None
var_timelimit_minimum = None
stage1_var_timelimit_factor = None
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
reopt_scenario_mode = None
reopt_eval_mipgap = None
reopt_max_iterations = None
stage1_max_iterations = None
gurobi_threads = None
parallel_total_threads = None
parallel_max_workers = None
explicit_config_overrides = {}

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MAX_TOTAL_MIP_THREADS = 64
MIP_THREADS_PER_SOLVE = 4


def _safe_int(value):
    try:
        return int(value)
    except Exception:
        return None


def _to_int_or_none(value):
    try:
        if value in {"", None}:
            return None
        return int(value)
    except Exception:
        return None


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
    parser.add_argument("--timelimit-master-iter", type=float, default=None)
    parser.add_argument("--heur-timelimit", type=float, default=None)
    parser.add_argument("--var-timelimit-minimum", type=float, default=None)
    parser.add_argument("--stage1-var-timelimit-factor", type=float, default=None)
    
    parser.add_argument("--initial-soc-fraction", type=float, default=None)
    parser.add_argument("--distance-multiplier", type=float, default=None)
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
    parser.add_argument("--reopt-eval-mipgap", type=float, default=0.05)
    parser.add_argument("--reopt-max-iterations", type=int, default=None)
    parser.add_argument("--stage1-max-iterations", type=int, default=None,
                        help="Cap on Stage-1 outer iterations (None = no cap).")
    parser.add_argument("--gurobi-threads", type=int, default=None)
    parser.add_argument("--parallel-total-threads", type=int, default=None)
    parser.add_argument("--parallel-max-workers", type=int, default=None)
    
    


def config_from_args(args):
    return {
        "vehicle_type": args.vehicle_type,
        "scenarios_to_use": args.scenarios_to_use,
        "gap": args.gap,
        "timelimit_master_iter": args.timelimit_master_iter,
        "heur_timelimit": args.heur_timelimit,
        "var_timelimit_minimum": args.var_timelimit_minimum,
        "stage1_var_timelimit_factor": args.stage1_var_timelimit_factor,
        "initial_soc_fraction": args.initial_soc_fraction,
        "distance_multiplier": args.distance_multiplier,
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
        "reopt_eval_mipgap": args.reopt_eval_mipgap,
        "reopt_max_iterations": args.reopt_max_iterations,
        "stage1_max_iterations": args.stage1_max_iterations,
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

    global vehicle_type, scenarios_to_use, gap, timelimit_master_iter, heur_timelimit, var_timelimit_minimum, stage1_var_timelimit_factor, initial_master_scenario_mode, alg
    global clustering_method, initial_soc_fraction, distance_multiplier, demand_multiplier, split_deliveries
    global d_cost, h, F, charger_lifespan_years, operating_days_per_year, charger_cost_multiplier, max_tours_per_truck
    global reuse_first_stage_dir, export_first_stage, export_first_stage_only_installed
    global second_stage_eval_timelimit, second_stage_eval_mipgap
    global reopt_unit, reopt_loop
    global reopt_scenario_mode, reopt_eval_mipgap
    global reopt_max_iterations, stage1_max_iterations, gurobi_threads, parallel_total_threads, parallel_max_workers
    global explicit_config_overrides

    vehicle_type = cfg["vehicle_type"]
    scenarios_to_use = cfg["scenarios_to_use"]
    gap = cfg["gap"]
    timelimit_master_iter = cfg["timelimit_master_iter"]
    heur_timelimit = float(cfg["heur_timelimit"])
    var_timelimit_minimum = float(cfg["var_timelimit_minimum"])
    stage1_var_timelimit_factor = float(cfg["stage1_var_timelimit_factor"])
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
    reopt_scenario_mode = cfg["reopt_scenario_mode"]
    reopt_eval_mipgap = cfg["reopt_eval_mipgap"]
    reopt_max_iterations = cfg["reopt_max_iterations"]
    stage1_max_iterations = cfg["stage1_max_iterations"]
    gurobi_threads = max(1, int(cfg["gurobi_threads"]))
    parallel_total_threads = max(1, int(cfg["parallel_total_threads"]))
    parallel_max_workers = (
        None if cfg["parallel_max_workers"] is None else max(1, int(cfg["parallel_max_workers"]))
    )
    explicit_config_overrides = overrides
    return cfg


apply_config()


def get_base_export_dir():
    return PROJECT_ROOT / "exports"


def latest_state_path_file(base_export_dir):
    return Path(base_export_dir) / "latest_first_stage_run.txt"


def get_parallel_total_threads():
    return max(1, int(parallel_total_threads or os.cpu_count() or 1))


def _env_flag(name, default=False):
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() not in {"0", "false", "no", "off"}


def plan_parallelism(task_count, max_threads_per_task=None, max_workers=None):
    """
    Compute a consistent worker/thread plan.

    Strategy:
    - Use as many parallel workers as possible up to the available thread budget.
    - Cap per-worker solver threads so total requested threads stay within budget.
    """
    if task_count <= 0:
        return 0, 0

    total_budget = get_parallel_total_threads()
    worker_cap = task_count
    if max_workers is not None:
        worker_cap = min(worker_cap, max(1, int(max_workers)))
    if parallel_max_workers is not None:
        worker_cap = min(worker_cap, parallel_max_workers)

    worker_count = min(task_count, worker_cap, total_budget)
    worker_count = max(1, worker_count)

    if max_threads_per_task is None:
        threads_per_worker = max(1, total_budget // worker_count)
    else:
        threads_per_worker = max(
            1,
            min(int(max_threads_per_task), total_budget // worker_count),
        )

    return worker_count, threads_per_worker


def plan_fixed_mip_parallelism(
    task_count,
    mip_threads_per_solve=MIP_THREADS_PER_SOLVE,
    max_total_threads=MAX_TOTAL_MIP_THREADS,
    max_workers=None,
):
    """
    Compute worker count for a fixed per-MIP thread allocation.
    """
    if task_count <= 0:
        return 0, max(1, int(mip_threads_per_solve))

    threads_per_worker = max(1, int(mip_threads_per_solve))
    total_budget = min(get_parallel_total_threads(), max(1, int(max_total_threads)))
    worker_cap = max(1, total_budget // threads_per_worker)
    worker_cap = min(worker_cap, int(task_count))
    if max_workers is not None:
        worker_cap = min(worker_cap, max(1, int(max_workers)))
    if parallel_max_workers is not None:
        worker_cap = min(worker_cap, parallel_max_workers)

    worker_count = max(1, worker_cap)
    return worker_count, threads_per_worker


def effective_config(overrides=None):
    cfg = dict(DEFAULT_CONFIG)
    cfg.update(explicit_config_overrides)
    if overrides:
        cfg.update({k: v for k, v in overrides.items() if v is not None})
    return cfg


def is_explicit_config_override(key):
    if key not in explicit_config_overrides:
        return False
    if key in CLUSTER_SIZE_AWARE_TIMELIMIT_KEYS:
        default_value = DEFAULT_CONFIG.get(key)
        override_value = explicit_config_overrides.get(key)
        try:
            return not math.isclose(float(override_value), float(default_value), rel_tol=0.0, abs_tol=1e-9)
        except (TypeError, ValueError):
            return override_value != default_value
    return True


def default_timelimit_for_customer_count(key, customer_count):
    value = DEFAULT_CONFIG[key]
    if key not in CLUSTER_SIZE_AWARE_TIMELIMIT_KEYS:
        return value
    if customer_count is None:
        return value
    n = int(customer_count)
    for threshold, factor in CLUSTER_SIZE_TIMELIMIT_TIERS:
        if n <= threshold:
            return float(value) * factor
    return float(value)


def effective_timelimit_for_customer_count(key, customer_count):
    current_value = globals()[key]
    if is_explicit_config_override(key):
        return current_value
    return default_timelimit_for_customer_count(key, customer_count)


def compute_cluster_complexity_metrics(inst):
    """Compute simple cluster signals for adaptive time-limit selection."""
    if inst is None:
        return {
            "customers": None,
            "split_customers": None,
            "avg_depot_km": None,
            "p90_depot_km": None,
            "max_depot_km": None,
            "mean_active_customers": None,
            "p90_active_customers": None,
            "max_active_customers": None,
            "mean_demand_per_active_customer": None,
            "p90_demand_per_active_customer": None,
            "mean_total_demand_kg": None,
            "p90_total_demand_kg": None,
            "max_total_demand_kg": None,
            "demand_mean": None,
            "demand_std": None,
            "demand_cv": None,
            "demand_peak_mean": None,
            "scenario_nonzero_mean": None,
            "depot_mean_distance_km": None,
        }

    base_customers = list(getattr(inst, "J_base", getattr(inst, "J", [])) or [])
    split_customers = list(getattr(inst, "J", []) or [])
    depot = getattr(inst, "i0", None)
    scenarios = list(getattr(inst, "S", []) or [])
    distances = []
    active_counts = []
    demand_per_active = []
    scenario_total_demands = []

    for customer in base_customers:
        dist_to = _safe_float(getattr(inst, "l", {}).get((depot, customer), None))
        if dist_to is not None:
            distances.append(dist_to)

    for scenario in scenarios:
        active = 0
        total_demand = 0.0
        for customer in split_customers:
            demand = _safe_float(getattr(inst, "beta", {}).get((scenario, customer), None)) or 0.0
            total_demand += demand
            if demand > 0:
                active += 1
        active_counts.append(float(active))
        scenario_total_demands.append(float(total_demand))
        demand_per_active.append(total_demand / active if active > 0 else 0.0)

    depot_series = pd.Series(distances, dtype=float)
    active_series = pd.Series(active_counts, dtype=float)
    dpa_series = pd.Series(demand_per_active, dtype=float)
    total_demand_series = pd.Series(scenario_total_demands, dtype=float)
    demand_mean = _safe_float(total_demand_series.mean())
    demand_std = _safe_float(total_demand_series.std(ddof=0)) if not total_demand_series.empty else None
    demand_cv = None
    if demand_mean is not None and abs(demand_mean) > 1e-12 and demand_std is not None:
        demand_cv = demand_std / demand_mean

    return {
        "customers": len(base_customers),
        "split_customers": len(split_customers),
        "avg_depot_km": _safe_float(depot_series.mean()),
        "depot_mean_distance_km": _safe_float(depot_series.mean()),
        "p90_depot_km": _safe_float(depot_series.quantile(0.9)) if not depot_series.empty else None,
        "max_depot_km": _safe_float(depot_series.max()) if not depot_series.empty else None,
        "mean_active_customers": _safe_float(active_series.mean()),
        "scenario_nonzero_mean": _safe_float(active_series.mean()),
        "p90_active_customers": _safe_float(active_series.quantile(0.9)) if not active_series.empty else None,
        "max_active_customers": _safe_float(active_series.max()) if not active_series.empty else None,
        "mean_demand_per_active_customer": _safe_float(dpa_series.mean()),
        "p90_demand_per_active_customer": _safe_float(dpa_series.quantile(0.9)) if not dpa_series.empty else None,
        "mean_total_demand_kg": demand_mean,
        "p90_total_demand_kg": _safe_float(total_demand_series.quantile(0.9)) if not total_demand_series.empty else None,
        "max_total_demand_kg": _safe_float(total_demand_series.max()) if not total_demand_series.empty else None,
        "demand_mean": demand_mean,
        "demand_std": demand_std,
        "demand_cv": demand_cv,
        "demand_peak_mean": _safe_float(total_demand_series.quantile(0.9)) if not total_demand_series.empty else None,
    }


def classify_cluster_runtime_bucket_base(metrics):
    """Classify a cluster into a shared structural runtime bucket."""
    customers = _safe_int(metrics.get("customers"))
    avg_depot_km = _safe_float(metrics.get("avg_depot_km"))
    mean_active_customers = _safe_float(metrics.get("mean_active_customers"))

    if customers is None:
        return "xhard"
    if customers <= 12:
        return "xsmall"
    if customers <= 14:
        return "small"
    if (
        customers <= 16
        and avg_depot_km is not None and avg_depot_km < 15.0
        and mean_active_customers is not None and mean_active_customers < 8.5
    ):
        return "medium"
    if (
        customers <= 20
        and avg_depot_km is not None and avg_depot_km < 30.0
        and mean_active_customers is not None and mean_active_customers < 10.0
    ):
        return "hard"
    return "xhard"

def classify_cluster_runtime_bucket(metrics):
    """Classify a cluster into a shared runtime bucket for Stage 1 and Stage 2."""
    return classify_cluster_runtime_bucket_base(metrics)


def classify_stage1_master_timelimit_bucket(metrics):
    """Classify a cluster into an aggressive Stage-1 master MIP time-limit bucket."""
    return classify_cluster_runtime_bucket(metrics)


def classify_evaluation_timelimit_bucket(metrics):
    """Classify a second-stage evaluation instance into an evidence-based time-limit bucket."""
    return classify_cluster_runtime_bucket(metrics)


def effective_stage1_master_timelimit(inst):
    """Return the adaptive Stage-1 master time limit and the metrics that drove it."""
    metrics = compute_cluster_complexity_metrics(inst)
    if is_explicit_config_override("timelimit_master_iter"):
        return float(timelimit_master_iter), metrics, "explicit_override"
    bucket = classify_stage1_master_timelimit_bucket(metrics)
    return STAGE1_MASTER_TIMELIMIT_RULES[bucket], metrics, bucket


def effective_stage1_var_timelimit_minimum(inst, cluster_metrics=None, bucket=None):
    """Return the Stage-1 second-stage minimum time slice for one scenario solve."""
    metrics = cluster_metrics if cluster_metrics is not None else compute_cluster_complexity_metrics(inst)
    customer_count = _safe_int(metrics.get("customers"))
    customer_floor = effective_timelimit_for_customer_count(
        "var_timelimit_minimum",
        customer_count if customer_count is not None else 0,
    )
    if is_explicit_config_override("var_timelimit_minimum"):
        return float(customer_floor)
    runtime_bucket = bucket or classify_stage1_master_timelimit_bucket(metrics)
    bucket_floor = STAGE1_VAR_TIMELIMIT_MINIMUM_RULES.get(runtime_bucket, customer_floor)
    return max(float(customer_floor), float(bucket_floor))


def effective_evaluation_timelimit(inst):
    """Return the second-stage evaluation time limit plus the metrics and bucket that drove it."""
    metrics = compute_cluster_complexity_metrics(inst)
    if is_explicit_config_override("second_stage_eval_timelimit"):
        return float(second_stage_eval_timelimit), metrics, "explicit_override"
    bucket = classify_evaluation_timelimit_bucket(metrics)
    return EVALUATION_TIMELIMIT_RULES[bucket], metrics, bucket


def build_runtime_landmarks(points):
    """Return the first time the incumbent is within several tolerances of the final incumbent."""
    finite_points = [
        (float(runtime_sec), float(best_obj))
        for runtime_sec, best_obj in points
        if runtime_sec is not None and best_obj is not None and math.isfinite(best_obj) and best_obj < 1e50
    ]
    if not finite_points:
        return {}

    finite_points.sort()
    final_runtime, final_obj = finite_points[-1]
    out = {
        "time_to_within_20pct_final_obj": final_runtime,
        "time_to_within_10pct_final_obj": final_runtime,
        "time_to_within_5pct_final_obj": final_runtime,
        "time_to_within_2pct_final_obj": final_runtime,
        "time_to_within_1pct_final_obj": final_runtime,
    }
    tolerances = [
        ("time_to_within_20pct_final_obj", 0.20),
        ("time_to_within_10pct_final_obj", 0.10),
        ("time_to_within_5pct_final_obj", 0.05),
        ("time_to_within_2pct_final_obj", 0.02),
        ("time_to_within_1pct_final_obj", 0.01),
    ]
    for key, tol in tolerances:
        limit = final_obj * (1.0 + tol)
        hit = next((runtime_sec for runtime_sec, best_obj in finite_points if best_obj <= limit), final_runtime)
        out[key] = hit
    return out


def get_cluster_live_dir(output_dir, cluster_id):
    return Path(output_dir) / "cluster_live" / f"cluster_{int(cluster_id)}"


def load_master_progress_points(output_dir, cluster_id):
    events_path = get_cluster_live_dir(output_dir, cluster_id) / "event_log.json"
    if not events_path.exists():
        return []
    payload = read_json(events_path, {})
    rows = payload.get("rows") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []
    points = []
    for event in rows:
        if not isinstance(event, dict):
            continue
        if event.get("event") != "master_incumbent":
            continue
        runtime_sec = _safe_float(event.get("runtime_sec"))
        best_obj = _safe_float(event.get("best_obj"))
        if runtime_sec is None or best_obj is None:
            continue
        points.append((runtime_sec, best_obj))
    return points


def analyze_progress_points(points):
    finite_points = [
        (float(runtime_sec), float(best_obj))
        for runtime_sec, best_obj in points
        if runtime_sec is not None and best_obj is not None and math.isfinite(best_obj)
    ]
    if not finite_points:
        return {
            "time_to_first_incumbent_sec": None,
            "last_improvement_time_sec": None,
        }
    finite_points.sort()
    best_so_far = None
    last_improvement_time_sec = None
    for runtime_sec, best_obj in finite_points:
        if best_so_far is None or best_obj < best_so_far - 1e-9:
            best_so_far = best_obj
            last_improvement_time_sec = runtime_sec
    return {
        "time_to_first_incumbent_sec": finite_points[0][0],
        "last_improvement_time_sec": last_improvement_time_sec,
    }


def should_continue_from_progress(points, round_timelimit_sec):
    if round_timelimit_sec is None or round_timelimit_sec <= 0:
        return False
    progress_info = analyze_progress_points(points)
    last_improvement_time_sec = progress_info.get("last_improvement_time_sec")
    if last_improvement_time_sec is None:
        return False
    return last_improvement_time_sec >= CONTINUATION_LATE_IMPROVEMENT_FRACTION * float(round_timelimit_sec)


def _slug_token(value):
    text = str(value).strip().lower()
    text = text.replace(".", "p")
    text = re.sub(r"[^a-z0-9_-]+", "", text)
    return text or "na"


def _run_id_value_token(value):
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return str(value).replace(".", "p")
    return _slug_token(value)


def build_run_id(config_overrides=None):
    """Build short run id: YYYY-MM-DD_HH-MM_veh_cluster."""
    from datetime import datetime

    veh = vehicle_type or "mercedes"
    cluster = clustering_method or "geographic"

    if config_overrides:
        non_none = {k: v for k, v in config_overrides.items() if v is not None}
        veh = non_none.get("vehicle_type", veh)
        cluster = non_none.get("clustering_method", cluster)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    return f"{timestamp}_{veh}_{cluster}"


def build_given_d_run_id(D_set, S_set, config_overrides=None, include_s=True):
    """Build deterministic run id for the fixed-D variant."""
    d_label = "-".join(str(s) for s in sorted(D_set))
    run_parts = []
    base_run = build_run_id(config_overrides=config_overrides)
    if base_run != "default":
        run_parts.append(base_run)
    run_parts.append(f"D-{d_label}")
    if include_s:
        s_label = "-".join(str(s) for s in sorted(S_set))
        run_parts.append(f"S-{s_label}")
    return "__".join(run_parts)


def _resolve_run_dir():
    """Resolve the current run directory from RUN_DIR."""
    env_run_dir = os.environ.get("RUN_DIR")
    if env_run_dir:
        return Path(env_run_dir)
    raise RuntimeError("RUN_DIR must be set for solve_clusters_first_stage.")


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
    # Robust fallback order: try any available method-specific assignment file in run_dir.
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
            assignments_csv = prep_dir / "cluster_assignments.json"
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
                        cluster_id = _safe_int(row.get("cluster_id"))
                        client_num = _safe_int(row.get("client_num"))
                        if cluster_id is None or client_num is None:
                            continue
                        clusters.setdefault(cluster_id, []).append(client_num)
                    if clusters:
                        return {cluster_id: sorted(values) for cluster_id, values in sorted(clusters.items())}
            if assignments_csv.exists():
                cluster_df = pd.DataFrame(read_table_rows(assignments_csv))
                if "cluster_id" in cluster_df.columns and "client_num" in cluster_df.columns:
                    return cluster_df.groupby("cluster_id")["client_num"].apply(list).to_dict()
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


def combine_first_stage_solutions(first_stage_by_entity):
    combined_a = {}
    combined_a_wh = {model.WAREHOUSE_CHARGER_TYPE: 1}
    for first_stage in first_stage_by_entity.values():
        if not first_stage:
            continue
        a_dict = first_stage[0] if isinstance(first_stage, tuple) else first_stage
        for key, val in a_dict.items():
            if float(val) >= 0.5:
                combined_a[key] = max(combined_a.get(key, 0.0), float(val))
    return combined_a, combined_a_wh


def compute_fleet_bounds(inst, customer_subset=None):
    """
    Compute K_max and M_max as upper bounds that guarantee feasibility.

    K_max is derived from the worst-case total demand across all scenarios:
        K_max = ceil(alpha * max_s sum_j beta(s,j) / (L * M_max))
    where alpha >= 1 is a safety factor that accounts for route-feasibility
    constraints beyond pure capacity (energy limits, spatial spread, etc.).

    Args:
        inst: Instance with scenarios S, demands beta, and capacity L
        customer_subset: If provided, compute bounds for this subset of customers.
                        If None, use inst.J (all customers in the instance).

    Returns:
        (K_max, M_max) tuple - guaranteed to be sufficient upper bounds
    """
    import math

    customers = customer_subset if customer_subset is not None else inst.J
    n_customers = len(customers)

    # M_max: max tours per truck
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

    # Floor: at least 1; cap: never exceed number of customers (original bound)
    K_max = max(1, min(K_demand, n_customers))

    return K_max, M_max


def apply_demand_based_limits(inst, customer_subset=None):
    """
    Apply K_max and M_max to an instance based on demand.

    Args:
        inst: Instance to modify
        customer_subset: If provided, compute bounds for this subset of customers.
                        If None, use inst.J (all customers in the instance).
    """
    inst.K_max, inst.M_max = compute_fleet_bounds(inst, customer_subset)


def create_base_instance(apply_fleet_limits=True, arc_set_path=None):
    """
    Create the base instance with all customers.

    Args:
        apply_fleet_limits: If True, compute and apply K_max/M_max based on
                           total demand across all customers (global bounds).
                           If False, leave K_max=1, M_max=1 (to be set later per-cluster).
    """
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
        # Global fleet bounds based on all customers
        apply_demand_based_limits(base_inst)
    return base_inst


def check_energy_feasibility(inst, cluster_id, verbose=True):
    """
    Check which customers can be served given energy constraints.

    A customer is energy-feasible if a truck can:
    1. Start with full battery (charged at warehouse)
    2. Travel from depot to customer (with full load = P_max consumption)
    3. Charge at customer (if charger installed) - assume fastest charger available
    4. Travel from customer back to depot (empty = P_min consumption)
    5. Arrive with at least 20% SoC (minimum SoC constraint)

    Energy model: P(load) = P_min + (load/L) * (P_max - P_min)
    - P_min: kWh/km for empty truck
    - P_max: kWh/km for fully loaded truck

    Returns:
        dict: {customer_id: {"feasible": bool, "round_trip_energy": float, "available_energy": float}}
    """
    depot = inst.i0
    C = inst.C  # Battery capacity
    P_min = inst.P_min  # kWh/km empty
    P_max = inst.P_max  # kWh/km fully loaded
    min_soc = 0.2 * C  # 20% minimum SoC at arrival

    # Available energy: start with full battery, must return with 20% SoC
    available_energy = C - min_soc

    # Get fastest charger speed (kW) for potential en-route charging
    fastest_charger_kw = max(inst.kappa.values()) if inst.kappa else 0

    results = {}
    infeasible_customers = []

    for j in inst.J:
        # Get distances for round trip (depot -> customer -> depot)
        dist_to = inst.l.get((depot, j), float('inf'))
        dist_from = inst.l.get((j, depot), float('inf'))

        # Worst-case energy: travel TO customer fully loaded (P_max), return empty (P_min)
        energy_to = dist_to * P_max
        energy_from = dist_from * P_min
        round_trip_energy = energy_to + energy_from

        # Check if feasible without en-route charging
        feasible_direct = round_trip_energy <= available_energy

        # Check if feasible with en-route charging at customer
        # Assume reasonable charging time (e.g., unloading time + 1 hour max waiting)
        # For a 150kW charger, 1 hour = 150 kWh potential charge
        max_charge_time_hours = 1.5  # Conservative estimate
        max_charge_at_customer = fastest_charger_kw * max_charge_time_hours

        # With charging: need enough to GET there, then charge, then return
        # Must arrive at customer with enough to charge and return
        energy_needed_to_reach = energy_to
        energy_needed_to_return = energy_from + min_soc  # Need 20% SoC at depot

        # Feasible if: we can reach customer AND (charge enough to return OR don't need charging)
        feasible_with_charging = (
            energy_needed_to_reach <= C - min_soc and  # Can reach customer with 20% remaining
            energy_needed_to_return <= C  # Return trip possible from full charge
        )

        feasible = feasible_direct or feasible_with_charging

        results[j] = {
            "feasible": feasible,
            "feasible_direct": feasible_direct,
            "feasible_with_charging": feasible_with_charging,
            "round_trip_energy": round_trip_energy,
            "available_energy": available_energy,
            "dist_to": dist_to,
            "dist_from": dist_from,
            "energy_to": energy_to,
            "energy_from": energy_from,
        }

        if not feasible:
            infeasible_customers.append(j)

    if infeasible_customers and verbose:
        print(f"  Energy diagnostics for cluster {cluster_id}:")
        print(f"    Battery: {C:.1f} kWh, Usable (with 20% reserve): {available_energy:.1f} kWh")
        print(f"    Consumption: P_min={P_min:.3f} kWh/km (empty), P_max={P_max:.3f} kWh/km (full)")
        print(f"    Fastest charger: {fastest_charger_kw:.0f} kW")
        print(f"    {len(infeasible_customers)} customers CANNOT be served (even with charging):")
        for j in infeasible_customers[:5]:  # Show first 5
            info = results[j]
            base_j = inst.pseudo_to_base.get(j, j) if hasattr(inst, 'pseudo_to_base') else j
            print(f"      Customer {base_j}: round-trip={info['round_trip_energy']:.1f} kWh")
            print(f"        to: {info['dist_to']:.1f}km ({info['energy_to']:.1f}kWh), from: {info['dist_from']:.1f}km ({info['energy_from']:.1f}kWh)")
        if len(infeasible_customers) > 5:
            print(f"      ... and {len(infeasible_customers) - 5} more")

    return results


def check_scenario_feasibility(inst, cluster_id, verbose=True):
    """
    Check which scenarios have demand for energy-infeasible customers.

    Returns:
        dict: {scenario: {"feasible": bool, "infeasible_customers": list}}
    """
    energy_results = check_energy_feasibility(inst, cluster_id, verbose=verbose)
    infeasible_by_energy = {j for j, info in energy_results.items() if not info["feasible"]}

    if not infeasible_by_energy:
        return {}  # All customers are energy-feasible

    scenario_results = {}
    infeasible_scenarios = []

    for s in inst.S:
        # Get customers with demand in this scenario
        customers_with_demand = [j for j in inst.J if inst.beta.get((s, j), 0) > 0]
        infeasible_in_scenario = [j for j in customers_with_demand if j in infeasible_by_energy]

        feasible = len(infeasible_in_scenario) == 0
        scenario_results[s] = {
            "feasible": feasible,
            "infeasible_customers": infeasible_in_scenario,
            "total_customers_with_demand": len(customers_with_demand),
        }

        if not feasible:
            infeasible_scenarios.append(s)

    if infeasible_scenarios and verbose:
        print(f"  Scenarios with infeasible customers in cluster {cluster_id}:")
        for s in infeasible_scenarios[:5]:  # Show first 5
            info = scenario_results[s]
            cust_list = [inst.pseudo_to_base.get(j, j) if hasattr(inst, 'pseudo_to_base') else j
                        for j in info["infeasible_customers"]]
            print(f"    Scenario {s}: {len(info['infeasible_customers'])} infeasible customers: {cust_list[:3]}{'...' if len(cust_list) > 3 else ''}")
        if len(infeasible_scenarios) > 5:
            print(f"    ... and {len(infeasible_scenarios) - 5} more scenarios")

    return scenario_results


def diagnose_infeasibility(inst, cluster_id):
    """
    Check and report any potential infeasibility issues for a cluster.

    This helps diagnose why the optimization might fail by checking:
    - Energy feasibility (can customers be reached?)
    - Scenario feasibility (which scenarios have unreachable customers?)

    Args:
        inst: Instance to check
        cluster_id: Cluster identifier for logging
    """
    # Check energy feasibility
    energy_results = check_energy_feasibility(inst, cluster_id, verbose=False)
    infeasible_customers = [j for j, info in energy_results.items() if not info["feasible"]]

    if infeasible_customers:
        print(f"  WARNING: {len(infeasible_customers)} energy-infeasible customers in cluster {cluster_id}")
        for j in infeasible_customers[:5]:
            info = energy_results[j]
            base_j = inst.pseudo_to_base.get(j, j) if hasattr(inst, 'pseudo_to_base') else j
            print(f"    Customer {base_j}: needs {info['round_trip_energy']:.0f} kWh, capacity={inst.C:.0f} kWh")
            print(f"      to: {info['dist_to']:.1f}km ({info['energy_to']:.0f}kWh), from: {info['dist_from']:.1f}km ({info['energy_from']:.0f}kWh)")
        if len(infeasible_customers) > 5:
            print(f"    ... and {len(infeasible_customers) - 5} more")

        # Check which scenarios are affected
        scenario_results = check_scenario_feasibility(inst, cluster_id, verbose=False)
        infeasible_scenarios = [s for s, info in scenario_results.items() if not info["feasible"]]
        if infeasible_scenarios:
            print(f"  Affected scenarios: {len(infeasible_scenarios)}/{len(inst.S)}")
            print(f"    Scenario IDs: {infeasible_scenarios[:10]}{'...' if len(infeasible_scenarios) > 10 else ''}")


def build_cluster_instance(base_inst, customer_list, cluster_id, check_feasibility=True):
    """
    Build a cluster-specific instance from the base instance.

    K_max and M_max are computed based on the cluster's demand (per-cluster bounds).
    The arc set is set to None (complete graph within cluster) to ensure feasibility.

    Args:
        base_inst: The base instance with all customers
        customer_list: List of customer IDs for this cluster
        cluster_id: Identifier for this cluster
        check_feasibility: If True, diagnose and report potential infeasibility issues.
    """
    import copy
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

    # For cluster-based solving, always use complete graph within clusters
    # The sparse arc set from generate_arc_set.py is designed for the full problem,
    # but for independent cluster solving, we need all intra-cluster arcs to ensure feasibility
    inst.A = None  # Complete graph within cluster

    inst.strings.ALG_INTRO_TEXT = f"CSPP Cluster {cluster_id} ({len(inst.J_base)} base, {len(inst.J)} split, {len(inst.S)} scenarios)\n"
    inst.strings.UNIQUE_IDENTIFIER = f"{inst.name}-cluster{cluster_id}"
    # Per-cluster fleet bounds based on this cluster's customers
    apply_demand_based_limits(inst)

    # Check and report potential infeasibility issues
    if check_feasibility:
        diagnose_infeasibility(inst, cluster_id)

    return inst


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


def _normalize_first_stage_payload(first_stage):
    if not first_stage:
        return None
    a_dict, a_wh = first_stage if isinstance(first_stage, tuple) else (first_stage, {})
    normalized_a = {}
    for key, val in (a_dict or {}).items():
        try:
            j, tau = key
            normalized_a[(int(j), int(tau))] = float(val)
        except Exception:
            continue
    normalized_a_wh = {}
    for tau, val in (a_wh or {}).items():
        try:
            normalized_a_wh[int(tau)] = float(val)
        except Exception:
            continue
    if not normalized_a_wh:
        normalized_a_wh = {model.WAREHOUSE_CHARGER_TYPE: 1.0}
    return normalized_a, normalized_a_wh


def load_best_first_stage_from_live(output_dir, cluster_id):
    live_path = Path(output_dir) / "cluster_live" / f"cluster_{int(cluster_id)}" / "best_first_stage_current.json"
    if not live_path.exists():
        return None
    try:
        payload = json.loads(live_path.read_text(encoding="utf-8"))
    except Exception:
        return None

    a_dict = {}
    for row in payload.get("a", []) or []:
        try:
            j = int(row.get("j"))
            tau = int(row.get("tau"))
            val = float(row.get("val", 0.0))
        except Exception:
            continue
        if math.isfinite(val):
            a_dict[(j, tau)] = val

    a_wh = {}
    for row in payload.get("a_wh", []) or []:
        try:
            tau = int(row.get("tau"))
            val = float(row.get("val", 0.0))
        except Exception:
            continue
        if math.isfinite(val):
            a_wh[tau] = val

    normalized = _normalize_first_stage_payload((a_dict, a_wh))
    if not normalized:
        return None
    return normalized


def resolve_export_first_stage(result, output_dir=None, cluster_id=None):
    normalized = _normalize_first_stage_payload(result.get("first_stage"))
    if normalized:
        return normalized
    if output_dir is None or cluster_id is None:
        return None
    if not result.get("timeout"):
        return None
    return load_best_first_stage_from_live(output_dir, cluster_id)


def serialize_first_stage_solution(first_stage, only_installed=True):
    if not first_stage:
        return {
            "a": [],
            "a_wh": [{"tau": int(model.WAREHOUSE_CHARGER_TYPE), "val": 1.0}],
            "installed_customer_chargers": 0,
        }
    a_dict, _ = first_stage if isinstance(first_stage, tuple) else (first_stage, {})
    a_records = []
    for (j, tau), val in sorted(a_dict.items()):
        if only_installed and float(val) < 0.5:
            continue
        a_records.append({"j": int(j), "tau": int(tau), "val": float(val)})
    return {
        "a": a_records,
        "a_wh": [{"tau": int(model.WAREHOUSE_CHARGER_TYPE), "val": 1.0}],
        "installed_customer_chargers": len(a_records),
    }


class FirstStageClusterProgressReporter:
    def __init__(self, output_dir, cluster_id, inst, export_only_installed=True, run_dir=None):
        self.output_dir = Path(output_dir)
        self.cluster_id = int(cluster_id)
        self.inst = inst
        self.export_only_installed = export_only_installed
        self.run_dir = Path(run_dir).expanduser().resolve() if run_dir else None
        self.cluster_dir = self.output_dir / "cluster_live" / f"cluster_{self.cluster_id}"
        self.cluster_dir.mkdir(parents=True, exist_ok=True)
        self.live_state_path = self.cluster_dir / "live_state.json"
        self.events_path = self.cluster_dir / "event_log.json"
        self.best_first_stage_path = self.cluster_dir / "best_first_stage_current.json"
        self.started_at = time.time()
        self.event_counter = 0
        self.events: list[dict] = []
        self.latest_state = {
            "cluster_id": self.cluster_id,
            "status": "initialized",
            "current_phase": "initialized",
            "elapsed_sec": 0.0,
            "customers": len(getattr(inst, "J_base", getattr(inst, "J", []))),
            "split_customers": len(getattr(inst, "J", [])),
            "scenarios_total": len(getattr(inst, "S", [])),
            "current_iteration": None,
            "D": [],
            "remaining_scenarios": [],
            "selected_scenario": None,
            "reached_gap": None,
            "master": {},
            "master_callback": {},
            "best_first_stage": serialize_first_stage_solution(None, only_installed=export_only_installed),
            "last_event": None,
        }
        self._write_live_state()

    def _write_live_state(self):
        self.latest_state["elapsed_sec"] = max(0.0, time.time() - self.started_at)
        self.live_state_path.write_text(
            json.dumps(self.latest_state, indent=2, ensure_ascii=True),
            encoding="utf-8",
        )
        if self.run_dir is not None:
            try:
                export_frontend_contract(self.run_dir)
            except Exception:
                pass

    def _append_event(self, payload):
        self.event_counter += 1
        event = dict(payload)
        event["cluster_id"] = self.cluster_id
        event["event_index"] = self.event_counter
        event["elapsed_sec"] = max(0.0, time.time() - self.started_at)
        self.events.append(event)
        write_event_log(self.events_path, self.events)
        self.latest_state["last_event"] = event

    def __call__(self, payload):
        event = dict(payload)
        phase = event.get("phase")
        if phase:
            self.latest_state["current_phase"] = phase
        if event.get("event") in {"completed", "final_solution"}:
            self.latest_state["status"] = "completed"
        elif event.get("event") == "timeout":
            self.latest_state["status"] = "timeout"
        else:
            self.latest_state["status"] = "running"

        if "iteration" in event:
            self.latest_state["current_iteration"] = event.get("iteration")
        if "D" in event:
            self.latest_state["D"] = list(event.get("D") or [])
        if "remaining_scenarios" in event:
            self.latest_state["remaining_scenarios"] = list(event.get("remaining_scenarios") or [])
        if "selected_scenario" in event:
            self.latest_state["selected_scenario"] = event.get("selected_scenario")
        if "reached_gap" in event:
            self.latest_state["reached_gap"] = event.get("reached_gap")
        if event.get("event") == "master_solved":
            self.latest_state["master"] = {
                "runtime_sec": event.get("master_runtime_sec"),
                "mip_gap": event.get("master_mip_gap"),
                "obj_val": event.get("master_obj_val"),
                "obj_bound": event.get("master_obj_bound"),
                "first_stage_objective": event.get("first_stage_objective"),
                "second_stage_bound": event.get("second_stage_bound"),
                "used_bound": event.get("used_bound"),
            }
            serialized_fs = serialize_first_stage_solution(
                event.get("first_stage"),
                only_installed=self.export_only_installed,
            )
            self.latest_state["best_first_stage"] = serialized_fs
            best_payload = {
                "cluster_id": self.cluster_id,
                "iteration": event.get("iteration"),
                "D": event.get("D") or [],
                **serialized_fs,
            }
            self.best_first_stage_path.write_text(
                json.dumps(best_payload, indent=2, ensure_ascii=True),
                encoding="utf-8",
            )
        if event.get("event") in {"master_incumbent", "master_progress"}:
            self.latest_state["master_callback"] = {
                "event": event.get("event"),
                "runtime_sec": event.get("runtime_sec"),
                "best_obj": event.get("best_obj"),
                "best_bound": event.get("best_bound"),
                "gap": event.get("gap"),
                "nodecount": event.get("nodecount"),
            }
        if event.get("event") == "scenario_evaluated":
            self.latest_state["last_scenario_evaluation"] = {
                "selected_scenario": event.get("selected_scenario"),
                "scenario_runtime_sec": event.get("scenario_runtime_sec"),
                "scenario_mip_gap": event.get("scenario_mip_gap"),
                "lower_bound": event.get("lower_bound"),
                "upper_bound": event.get("upper_bound"),
                "interrupt_reason": event.get("interrupt_reason"),
            }
        if event.get("event") == "scenarios_added":
            self.latest_state["last_added_to_D"] = event.get("added_to_D") or []
            self.latest_state["bounds"] = {
                "lower_bounds": event.get("lower_bounds") or {},
                "upper_bounds": event.get("upper_bounds") or {},
            }
            self.latest_state["message"] = event.get("message")
        if event.get("event") in {"completed", "timeout"}:
            self.latest_state["stop_reason"] = event.get("stop_reason")

        self._append_event(event)
        self._write_live_state()


def compute_rel_gap(best_obj, best_bound):
    best_obj = _safe_float(best_obj)
    best_bound = _safe_float(best_bound)
    if best_obj is None or best_bound is None:
        return None
    denom = abs(best_obj)
    if denom <= 1e-12:
        return 0.0 if abs(best_bound - best_obj) <= 1e-9 else None
    return abs(best_obj - best_bound) / denom


def make_master_progress_callback(progress_reporter, throttle_sec=15.0):
    last_best_obj = None
    last_progress_runtime = None

    def callback(model, where):
        nonlocal last_best_obj, last_progress_runtime
        if where == GRB.Callback.MIPSOL:
            runtime = _safe_float(model.cbGet(GRB.Callback.RUNTIME))
            best_obj = _safe_float(model.cbGet(GRB.Callback.MIPSOL_OBJBST))
            best_bound = _safe_float(model.cbGet(GRB.Callback.MIPSOL_OBJBND))
            nodecount = _safe_float(model.cbGet(GRB.Callback.MIPSOL_NODCNT))
            if best_obj is None:
                return
            if last_best_obj is not None and abs(best_obj - last_best_obj) <= 1e-9:
                return
            progress_reporter(
                {
                    "event": "master_incumbent",
                    "phase": "solving_master_problem",
                    "runtime_sec": runtime,
                    "best_obj": best_obj,
                    "best_bound": best_bound,
                    "gap": compute_rel_gap(best_obj, best_bound),
                    "nodecount": nodecount,
                }
            )
            last_best_obj = best_obj
            last_progress_runtime = runtime
            return

        if where != GRB.Callback.MIP:
            return

        runtime = _safe_float(model.cbGet(GRB.Callback.RUNTIME))
        if runtime is None:
            return
        if last_progress_runtime is not None and runtime - last_progress_runtime < throttle_sec:
            return

        best_obj = _safe_float(model.cbGet(GRB.Callback.MIP_OBJBST))
        best_bound = _safe_float(model.cbGet(GRB.Callback.MIP_OBJBND))
        nodecount = _safe_float(model.cbGet(GRB.Callback.MIP_NODCNT))
        progress_reporter(
            {
                "event": "master_progress",
                "phase": "solving_master_problem",
                "runtime_sec": runtime,
                "best_obj": best_obj,
                "best_bound": best_bound,
                "gap": compute_rel_gap(best_obj, best_bound),
                "nodecount": nodecount,
            }
        )
        last_progress_runtime = runtime

    return callback

def save_first_stage_csv(output_dir, cluster_results):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    fs_path = output_dir / "cluster_first_stage.json"
    rows = []
    for cluster_id, result in cluster_results.items():
        first_stage = resolve_export_first_stage(result, output_dir=output_dir, cluster_id=cluster_id)
        if not first_stage:
            continue
        a_dict = first_stage[0] if isinstance(first_stage, tuple) else first_stage
        for (j, tau), val in a_dict.items():
            rows.append({"cluster_id": cluster_id, "customer_id": j, "charger_type": tau, "value": float(val)})
    write_table(fs_path, ["cluster_id", "customer_id", "charger_type", "value"], rows)
    return fs_path


def extract_master_mip_state(stats):
    """Extract a compact first-stage master MIP state snapshot."""
    if stats is None or getattr(stats, "master_problem", None) is None:
        return None
    grb_model = getattr(stats.master_problem, "model", None)
    if grb_model is None:
        return None

    def _safe_float(value):
        try:
            out = float(value)
        except Exception:
            return None
        return out if math.isfinite(out) else None

    def _safe_int(value):
        try:
            return int(value)
        except Exception:
            return None

    status_code = _safe_int(getattr(grb_model, "Status", None))
    solcount = _safe_int(getattr(grb_model, "SolCount", 0)) or 0
    obj_val = _safe_float(getattr(grb_model, "ObjVal", None)) if solcount > 0 else None
    mip_gap = _safe_float(getattr(grb_model, "MIPGap", None)) if solcount > 0 else None

    return {
        "status_code": status_code,
        "solcount": solcount,
        "runtime_sec": _safe_float(getattr(grb_model, "Runtime", None)),
        "nodecount": _safe_float(getattr(grb_model, "NodeCount", None)),
        "obj_val": obj_val,
        "obj_bound": _safe_float(getattr(grb_model, "ObjBound", None)),
        "mip_gap": mip_gap,
    }


def build_first_stage_runtime_row(cluster_id, result):
    stats = result.get("stats")
    inst = result.get("instance")
    output_dir = result.get("output_dir")
    first_stage = resolve_export_first_stage(result, output_dir=output_dir, cluster_id=cluster_id)
    mip_state = result.get("mip_state")
    if mip_state is None:
        mip_state = extract_master_mip_state(stats)

    installed_customer_chargers = 0
    if first_stage:
        a_dict = first_stage[0] if isinstance(first_stage, tuple) else first_stage
        installed_customer_chargers = sum(1 for val in a_dict.values() if float(val) >= 0.5)

    status = "SOLVED"
    if result.get("reused"):
        status = "REUSED"
    elif result.get("infeasible"):
        status = "FAILED"
    elif result.get("timeout"):
        status = "TIME_LIMIT"

    row = {
        "cluster_id": int(cluster_id),
        "status": status,
        "runtime_sec": float(result.get("time", 0.0) or 0.0),
        "customers": len(getattr(inst, "J_base", getattr(inst, "J", []))) if inst is not None else None,
        "split_customers": len(getattr(inst, "J", [])) if inst is not None else None,
        "scenarios": len(getattr(inst, "S", [])) if inst is not None else None,
        "fleet_k_max": getattr(inst, "K_max", None) if inst is not None else None,
        "fleet_m_max": getattr(inst, "M_max", None) if inst is not None else None,
        "installed_customer_chargers": installed_customer_chargers,
        "iterations": getattr(stats, "ITERATIONS", None) if stats is not None else None,
        "reached_gap": getattr(stats, "reached_gap", None) if stats is not None else None,
        "objective": _safe_float(getattr(stats, "objective", None)) if stats is not None else None,
        "timeout": bool(result.get("timeout", False)),
        "infeasible": bool(result.get("infeasible", False)),
        "error": result.get("error"),
        "runtime_bucket_base": result.get("runtime_bucket_base"),
        "stage1_master_timelimit_bucket": result.get("stage1_master_timelimit_bucket"),
        "runtime_bucket_final": result.get("runtime_bucket_final", result.get("stage1_master_timelimit_bucket")),
        "demand_promotion_applied": result.get("demand_promotion_applied"),
        "effective_timelimit_master_iter": result.get("effective_timelimit_master_iter"),
        "effective_timelimit_per_round_sec": result.get("effective_timelimit_per_round_sec"),
        "effective_timelimit_total_sec": result.get("effective_timelimit_total_sec"),
        "extension_rounds_used": result.get("extension_rounds_used"),
        "effective_heur_timelimit": result.get("effective_heur_timelimit"),
        "effective_var_timelimit_minimum": result.get("effective_var_timelimit_minimum"),
    }
    row.update(compute_cluster_complexity_metrics(inst))
    runtime_points = load_master_progress_points(output_dir, cluster_id) if output_dir is not None else []
    row.update(build_runtime_landmarks(runtime_points))
    row.update(analyze_progress_points(runtime_points))
    if mip_state:
        row.update({
            "master_status_code": mip_state.get("status_code"),
            "master_solcount": mip_state.get("solcount"),
            "master_runtime_sec": mip_state.get("runtime_sec"),
            "master_nodecount": mip_state.get("nodecount"),
            "master_obj_val": mip_state.get("obj_val"),
            "master_obj_bound": mip_state.get("obj_bound"),
            "master_mip_gap": mip_state.get("mip_gap"),
        })
    return row


def _safe_float(value):
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def save_first_stage_runtime_analysis(output_dir, cluster_results):
    path = Path(output_dir) / "cluster_runtime_analysis.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        result.get("runtime_analysis") or build_first_stage_runtime_row(cluster_id, result)
        for cluster_id, result in sorted(cluster_results.items())
    ]
    columns = [
        "cluster_id", "status", "runtime_sec", "customers", "split_customers", "scenarios",
        "fleet_k_max", "fleet_m_max", "installed_customer_chargers", "iterations", "reached_gap",
        "objective", "timeout", "infeasible", "error", "runtime_bucket_base",
        "stage1_master_timelimit_bucket", "runtime_bucket_final", "demand_promotion_applied",
        "effective_timelimit_master_iter", "effective_heur_timelimit", "effective_var_timelimit_minimum",
        "effective_timelimit_per_round_sec", "effective_timelimit_total_sec", "extension_rounds_used",
        "demand_mean", "demand_std", "demand_cv", "demand_peak_mean", "customer_spread_km",
        "depot_mean_distance_km", "scenario_nonzero_mean", "mean_demand_per_active_customer",
        "mean_total_demand_kg", "p90_total_demand_kg", "max_total_demand_kg",
        "p90_demand_per_active_customer", "time_to_within_20pct_final_obj", "time_to_within_10pct_final_obj",
        "time_to_within_5pct_final_obj", "time_to_within_2pct_final_obj", "time_to_within_1pct_final_obj",
        "time_to_first_incumbent_sec", "last_improvement_time_sec",
        "master_status_code", "master_solcount", "master_runtime_sec", "master_nodecount",
        "master_obj_val", "master_obj_bound", "master_mip_gap",
    ]
    write_table(path, columns, rows)
    return path


def save_first_stage_progress_snapshot(run_dir, output_dir, cluster_results, total_clusters, started_at):
    output_dir = Path(output_dir)
    snapshot_path = output_dir / "cluster_progress_snapshot.json"
    completed = len(cluster_results)
    solved = sum(1 for result in cluster_results.values() if result.get("first_stage") is not None)
    failed = sum(1 for result in cluster_results.values() if result.get("first_stage") is None)
    timeouts = sum(1 for result in cluster_results.values() if result.get("timeout"))
    elapsed_sec = max(0.0, time.time() - started_at)
    rows = [
        result.get("runtime_analysis") or build_first_stage_runtime_row(cluster_id, result)
        for cluster_id, result in sorted(cluster_results.items())
    ]
    payload = {
        "stage": "solve_clusters_first_stage",
        "status": "completed" if completed >= total_clusters and total_clusters > 0 else ("running" if completed > 0 else "pending"),
        "completed_clusters": completed,
        "total_clusters": total_clusters,
        "solved_clusters": solved,
        "failed_clusters": failed,
        "timeout_clusters": timeouts,
        "elapsed_sec": elapsed_sec,
        "cluster_live_dir": str(output_dir / "cluster_live"),
        "clusters": rows,
    }
    snapshot_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    merge_results_json(
        run_dir,
        "metrics.first_stage_progress",
        {
            "completed_clusters": completed,
            "total_clusters": total_clusters,
            "solved_clusters": solved,
            "failed_clusters": failed,
            "timeout_clusters": timeouts,
            "elapsed_sec": elapsed_sec,
        },
    )
    try:
        export_frontend_contract(run_dir)
    except Exception:
        pass
    return snapshot_path


def save_first_stage_current_state(run_dir, output_dir, cluster_results, total_clusters, started_at):
    output_dir = Path(output_dir)
    live_root = output_dir / "cluster_live"
    current_state_path = output_dir / "current_state.json"

    live_states: dict[int, dict] = {}
    if live_root.exists():
        for cluster_dir in sorted(live_root.glob("cluster_*")):
            try:
                cluster_id = int(cluster_dir.name.split("_")[-1])
            except Exception:
                continue
            live_state_path = cluster_dir / "live_state.json"
            if not live_state_path.exists():
                continue
            try:
                live_states[cluster_id] = json.loads(live_state_path.read_text(encoding="utf-8"))
            except Exception:
                continue

    completed_cluster_ids = sorted(int(cluster_id) for cluster_id in cluster_results.keys())
    running_cluster_ids = sorted(
        cluster_id
        for cluster_id, state in live_states.items()
        if str(state.get("status") or "").strip().lower() in {"running", "initialized"}
    )
    known_cluster_ids = sorted(set(completed_cluster_ids) | set(live_states.keys()))
    pending_cluster_ids = sorted(cluster_id for cluster_id in range(total_clusters) if cluster_id not in known_cluster_ids)

    if completed_cluster_ids and len(completed_cluster_ids) >= total_clusters:
        status = "completed"
    elif running_cluster_ids:
        status = "running"
    else:
        status = "pending"

    payload = {
        "stage": "solve_clusters_first_stage",
        "status": status,
        "elapsed_sec": max(0.0, time.time() - started_at),
        "total_clusters": total_clusters,
        "completed_clusters": len(completed_cluster_ids),
        "running_cluster_ids": running_cluster_ids,
        "completed_cluster_ids": completed_cluster_ids,
        "pending_cluster_ids": pending_cluster_ids,
        "active_parallel_workers": len(running_cluster_ids),
        "clusters": [
            {
                "cluster_id": cluster_id,
                "status": (live_states.get(cluster_id) or {}).get("status") or ("completed" if cluster_id in cluster_results else "pending"),
                "current_phase": (live_states.get(cluster_id) or {}).get("current_phase"),
                "current_iteration": (live_states.get(cluster_id) or {}).get("current_iteration"),
                "elapsed_sec": (live_states.get(cluster_id) or {}).get("elapsed_sec"),
            }
            for cluster_id in sorted(set(range(total_clusters)) | set(known_cluster_ids))
        ],
    }
    current_state_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    return current_state_path


def save_cluster_active_sets(output_dir, cluster_results):
    """Save per-cluster active scenario sets (final D) from Stage 1."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "cluster_active_sets.json"
    rows = []
    for cluster_id, result in sorted(cluster_results.items()):
        final_D = result.get("final_D") or []
        for scenario in final_D:
            rows.append({"cluster_id": int(cluster_id), "scenario": int(scenario)})
    write_table(path, ["cluster_id", "scenario"], rows)
    return path


def export_first_stage_intermediate_state(run_dir, output_dir, first_stage_dir, cluster_results, total_clusters, started_at):
    for cluster_id, result in sorted(cluster_results.items()):
        first_stage = resolve_export_first_stage(result, output_dir=output_dir, cluster_id=cluster_id)
        if export_first_stage and first_stage:
            fs_path = Path(first_stage_dir) / f"cluster_{cluster_id}_first_stage.json"
            export_first_stage_solution(
                fs_path,
                cluster_id,
                first_stage,
                only_installed=export_first_stage_only_installed,
            )
    first_stage_csv = save_first_stage_csv(output_dir, cluster_results)
    runtime_csv = save_first_stage_runtime_analysis(output_dir, cluster_results)
    snapshot_json = save_first_stage_progress_snapshot(
        run_dir, output_dir, cluster_results, total_clusters, started_at
    )
    current_state_json = save_first_stage_current_state(
        run_dir, output_dir, cluster_results, total_clusters, started_at
    )
    return {
        "first_stage_csv": first_stage_csv,
        "runtime_csv": runtime_csv,
        "snapshot_json": snapshot_json,
        "current_state_json": current_state_json,
    }


def _solve_cluster_round(
    inst,
    cluster_id,
    cluster_num=None,
    total_clusters=None,
    total_timelimit_override=None,
    *,
    progress_output_dir=None,
    start_sc_override=None,
    max_iterations_override=None,
    master_timelimit_override=None,
):
    """Solve a single cluster optimization problem.

    The algorithm's output (iteration info, etc.) is kept visible.
    Only raw Gurobi solver output is suppressed via OutputFlag in the models.
    """
    cluster_customer_count = len(getattr(inst, "J_base", getattr(inst, "J", [])))
    effective_master_timelimit, cluster_metrics, master_timelimit_bucket = effective_stage1_master_timelimit(inst)
    if master_timelimit_override is not None:
        effective_master_timelimit = float(master_timelimit_override)
    base_bucket = classify_cluster_runtime_bucket_base(cluster_metrics)
    final_bucket = master_timelimit_bucket
    effective_heur_timelimit = effective_timelimit_for_customer_count(
        "heur_timelimit", cluster_customer_count
    )
    effective_var_timelimit_minimum = effective_stage1_var_timelimit_minimum(
        inst,
        cluster_metrics=cluster_metrics,
        bucket=master_timelimit_bucket,
    )
    type = AlgorithmType(
        options=AlgorithmOptions(
            VAR_TIMELIMIT_FACTOR=stage1_var_timelimit_factor,
            VAR_TIMELIMIT_MINIMUM=effective_var_timelimit_minimum,
        )
    )
    appl = Application(
        inst=inst,
        MasterModel=model.MasterModel,
        SecondStageModel=model.SecondStageModel
    )

    start_sc = (
        sorted(int(s) for s in start_sc_override)
        if start_sc_override is not None
        else select_initial_scenarios(inst)
    )
    live_stage_root = os.environ.get("CSPP_LIVE_STAGE_ROOT")
    if progress_output_dir is not None:
        live_output_dir = Path(progress_output_dir)
    elif live_stage_root:
        live_output_dir = Path(live_stage_root)
    elif os.environ.get("RUN_DIR"):
        live_output_dir = get_run_layout(Path(os.environ.get("RUN_DIR")))["cspp_first_stage"]
    else:
        live_output_dir = PROJECT_ROOT / "exports" / "cluster_live_fallback"
    progress_reporter = FirstStageClusterProgressReporter(
        output_dir=live_output_dir,
        cluster_id=cluster_id,
        inst=inst,
        export_only_installed=export_first_stage_only_installed,
        run_dir=os.environ.get("RUN_DIR"),
    )
    params = AlgorithmParams(
        app=appl,
        start_sc=start_sc,
        desired_gap=gap,
        MASTER_P=gap,
        HEURTIMELIMIT=effective_heur_timelimit,
        total_timelimit=(
            float(total_timelimit_override)
            if total_timelimit_override is not None
            else math.inf
        ),
        master_timelimit=effective_master_timelimit,
        max_iterations=(
            max_iterations_override
            if max_iterations_override is not None
            else stage1_max_iterations
        ),
        n_threads=gurobi_threads,
        progress_callback=progress_reporter,
        master_callback=make_master_progress_callback(progress_reporter),
    )
    avg_depot_km = cluster_metrics.get("avg_depot_km")
    mean_active_customers = cluster_metrics.get("mean_active_customers")
    avg_depot_label = f"{avg_depot_km:.1f}km" if avg_depot_km is not None else "n/a"
    mean_active_label = f"{mean_active_customers:.1f}" if mean_active_customers is not None else "n/a"

    print_cluster_start(cluster_id, len(inst.J), cluster_num, total_clusters)
    print(f"  Target gap: {gap*100:.0f}%")
    print(
        f"  Complexity: base={cluster_metrics.get('customers')} "
        f"| avg depot={avg_depot_label} "
        f"| mean active={mean_active_label}"
    )
    if alg == "our":
        print(
            f"  Master MIP limit/iter: {format_duration(effective_master_timelimit)} ({master_timelimit_bucket})"
            f" | Second-stage MIP: VAR({stage1_var_timelimit_factor:.2f} x master runtime, min {format_duration(effective_var_timelimit_minimum)})"
            f" | Heuristic: parallel up to {gurobi_threads} worker(s), {format_duration(params.HEURTIMELIMIT)} each"
        )
    else:
        print(
            f"  Master MIP limit/iter: {format_duration(effective_master_timelimit)} ({master_timelimit_bucket})"
            f" | Second-stage MIP: none"
            f" | Heuristic: {format_duration(params.HEURTIMELIMIT)}"
        )
    print(f"  Fleet bounds: K_max={inst.K_max}, M_max={inst.M_max}")
    print(f"  Initial scenarios in D: {start_sc if start_sc else '[]'}")

    cluster_start_time = time.time()

    try:
        # Run the algorithm - its output (iterations, progress) will be visible
        if alg == "our":
            s = ourAlgorithm(params=params, type=type)
        elif alg == "toen":
            s = toenAlgorithm(params=params)
        elif alg == "rodr":
            s = rodrAlgorithm(params=params)
        else:
            raise Exception("Wrong alg name given.")
        timeout_reached = False
        infeasible = False
    except TimeoutException as tex:
        s = tex.stats
        s.reached_gap = tex.reached_gap
        timeout_reached = True
        infeasible = False
    except KeyboardInterrupt:
        # Re-raise to allow Ctrl+C to stop the program
        print("\n  Interrupted by user (Ctrl+C)")
        raise
    except Exception as e:
        cluster_time = time.time() - cluster_start_time
        print(f"  ERROR: {e}")
        return {
            'stats': None,
            'instance': inst,
            'timeout': False,
            'infeasible': True,
            'time': cluster_time,
            'first_stage': None,
            'error': str(e)
        }

    cluster_time = time.time() - cluster_start_time
    print_cluster_result(
        cluster_id, cluster_time, s.reached_gap, s.ITERATIONS, timeout_reached,
        objective=getattr(s, 'objective', None)
    )

    return {
        'stats': s,
        'instance': inst,
        'timeout': timeout_reached,
        'infeasible': False,
        'time': cluster_time,
        'first_stage': s.first_stage,
        'final_D': getattr(s, 'final_D', None) or [],
        'mip_state': extract_master_mip_state(s),
        'runtime_bucket_base': base_bucket,
        'stage1_master_timelimit_bucket': master_timelimit_bucket,
        'runtime_bucket_final': final_bucket,
        'demand_promotion_applied': final_bucket != base_bucket,
        'effective_timelimit_master_iter': effective_master_timelimit,
        'effective_timelimit_per_round_sec': effective_master_timelimit,
        'effective_timelimit_total_sec': (
            float(total_timelimit_override)
            if total_timelimit_override is not None
            else None
        ),
        'extension_rounds_used': 0,
        'effective_total_timelimit': (
            float(total_timelimit_override)
            if total_timelimit_override is not None
            else None
        ),
        'effective_heur_timelimit': effective_heur_timelimit,
        'effective_var_timelimit_minimum': effective_var_timelimit_minimum,
    }


def solve_cluster(
    inst,
    cluster_id,
    cluster_num=None,
    total_clusters=None,
    total_timelimit_override=None,
    start_sc_override=None,
    max_iterations_override=None,
    master_timelimit_override=None,
):
    progress_output_dir = (
        get_run_layout(Path(os.environ.get("RUN_DIR")))["cspp_first_stage"]
        if os.environ.get("RUN_DIR")
        else PROJECT_ROOT / "exports" / "cluster_live_fallback"
    )
    base_master_timelimit = (
        float(master_timelimit_override)
        if master_timelimit_override is not None
        else None
    )
    current_master_timelimit = base_master_timelimit
    extension_rounds_used = 0

    while True:
        result = _solve_cluster_round(
            inst,
            cluster_id,
            cluster_num=cluster_num,
            total_clusters=total_clusters,
            total_timelimit_override=total_timelimit_override,
            progress_output_dir=progress_output_dir,
            start_sc_override=start_sc_override,
            max_iterations_override=max_iterations_override,
            master_timelimit_override=current_master_timelimit,
        )
        points = load_master_progress_points(progress_output_dir, cluster_id)
        progress_info = analyze_progress_points(points)
        result["last_improvement_time_sec"] = progress_info.get("last_improvement_time_sec")
        result["time_to_first_incumbent_sec"] = progress_info.get("time_to_first_incumbent_sec")
        result["extension_rounds_used"] = extension_rounds_used

        per_round_timelimit = result.get("effective_timelimit_per_round_sec")
        if base_master_timelimit is None and per_round_timelimit is not None:
            base_master_timelimit = float(per_round_timelimit)
            if current_master_timelimit is None:
                current_master_timelimit = base_master_timelimit

        missing_first_stage = result.get("first_stage") is None
        continuation_reason = None
        if missing_first_stage:
            continuation_reason = "no usable first-stage solution"
        elif result.get("timeout") and should_continue_from_progress(points, per_round_timelimit):
            continuation_reason = (
                f"late master improvement at {format_duration(result.get('last_improvement_time_sec'))}"
            )

        can_extend = (
            total_timelimit_override is None
            and extension_rounds_used < CONTINUATION_MAX_EXTRA_ROUNDS
            and continuation_reason is not None
        )
        if not can_extend:
            return result

        extension_rounds_used += 1
        current_master_timelimit = float(current_master_timelimit or base_master_timelimit or 0.0) + float(base_master_timelimit or 0.0)
        print(
            f"  Continuation round {extension_rounds_used}/{CONTINUATION_MAX_EXTRA_ROUNDS}: "
            f"{continuation_reason}; "
            f"extending master MIP limit to {format_duration(current_master_timelimit)}"
        )


def _solve_cluster_worker(args):
    cluster_id, customer_list, base_inst, worker_config = args
    apply_config(worker_config)
    inst = build_cluster_instance(base_inst, customer_list, cluster_id)
    run_dir = os.environ.get("RUN_DIR")
    if run_dir:
        log_dir = get_run_layout(Path(run_dir))["cspp_first_stage_logs"]
        log_dir.mkdir(parents=True, exist_ok=True)
        worker_log_path = log_dir / f"cluster_{cluster_id}.log"
        with open(worker_log_path, "a", encoding="utf-8") as worker_log:
            with contextlib.redirect_stdout(worker_log), contextlib.redirect_stderr(worker_log):
                result = solve_cluster(inst, cluster_id)
    else:
        result = solve_cluster(inst, cluster_id)
    if run_dir:
        result["output_dir"] = get_run_layout(Path(run_dir))["cspp_first_stage"]
    result["runtime_analysis"] = build_first_stage_runtime_row(cluster_id, result)
    result.pop("stats", None)
    result.pop("instance", None)
    return cluster_id, result


def solve_clusters_parallel(clusters, base_inst):
    import multiprocessing as mp

    if _env_flag("CSPP_STAGE1_DISABLE_MP", False):
        print("  Multiprocessing disabled via CSPP_STAGE1_DISABLE_MP=1; solving clusters sequentially")
        return None, gurobi_threads

    # On local macOS, the tour-containment Stage-1 path has shown a reproducible
    # post-solve hang in multiprocessing pool teardown after workers complete.
    # Fall back to sequential execution there so the pipeline can progress.
    if platform.system() == "Darwin" and clustering_method == "tour_containment":
        print("  Multiprocessing disabled for tour_containment on Darwin; solving clusters sequentially")
        return None, gurobi_threads

    worker_count, threads_per_worker = plan_fixed_mip_parallelism(len(clusters))
    if worker_count <= 1:
        return None, threads_per_worker

    worker_config = effective_config({"gurobi_threads": threads_per_worker})
    worker_args = [
        (cluster_id, customer_list, base_inst, worker_config)
        for cluster_id, customer_list in clusters.items()
    ]

    print(
        f"  Launching {worker_count} parallel cluster solve worker(s) "
        f"with {threads_per_worker} Gurobi thread(s) each"
    )
    ctx = mp.get_context("fork")
    results = {}
    with ctx.Pool(processes=worker_count) as pool:
        for cluster_id, result in pool.imap_unordered(_solve_cluster_worker, worker_args):
            results[cluster_id] = result
            status = "OK" if result.get("first_stage") is not None else "FAILED"
            print(f"  Cluster {cluster_id}: {status}")
    return results, threads_per_worker


def select_initial_scenarios(inst):
    if initial_master_scenario_mode == "empty":
        return []
    if initial_master_scenario_mode == "max_total_demand":
        if not inst.S:
            return []
        scenario_totals = {
            s: sum(inst.beta.get((s, j), 0.0) for j in inst.J)
            for s in inst.S
        }
        return [max(scenario_totals, key=scenario_totals.get)]
    raise ValueError(f"Unknown initial master scenario mode: {initial_master_scenario_mode}")


def run(args=None, config=None):
    cfg = apply_config(config)
    run_start_time = time.time()
    clusters = load_clusters()
    if args.only_cluster is not None:
        clusters = {k: v for k, v in clusters.items() if k == args.only_cluster}
    # Don't apply fleet limits on base instance; they'll be set per-cluster
    base_inst = create_base_instance(apply_fleet_limits=False)

    run_id = build_run_id(config_overrides=config)

    base_export_dir = get_base_export_dir()

    # Resolve run dir from env var (set by run.py) or build from run_id
    env_run_dir = os.environ.get("RUN_DIR")
    if env_run_dir:
        run_dir = Path(env_run_dir)
    else:
        resolved_run_dir = _resolve_run_dir()
        if resolved_run_dir is not None:
            run_dir = resolved_run_dir
        else:
            run_dir = build_run_dir(
                max_distance_km=None,
                vehicle_type=vehicle_type,
                clustering_method=clustering_method,
                scenarios_to_use=scenarios_to_use,
                run_name=run_id,
            )

    run_layout = get_run_layout(run_dir)
    log_dir = run_layout["cspp_first_stage_logs"]
    output_dir = run_layout["cspp_first_stage"]
    maps_dir = run_layout["cspp_first_stage_route_maps"]
    first_stage_dir = run_layout["cspp_first_stage_json"]

    for d in (base_export_dir, run_dir, log_dir, output_dir, maps_dir, first_stage_dir):
        d.mkdir(parents=True, exist_ok=True)

    latest_state_file = latest_state_path_file(base_export_dir)
    merge_run_config(
        run_dir,
        {
            "vehicle_type": vehicle_type,
            "scenarios_to_use": scenarios_to_use,
            "gap": gap,
            "timelimit_master_iter": timelimit_master_iter,
            "heur_timelimit": heur_timelimit,
            "var_timelimit_minimum": var_timelimit_minimum,
            "cluster_master_mip_timelimit": timelimit_master_iter,
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
            "charger_lifespan_years": charger_lifespan_years,
            "operating_days_per_year": operating_days_per_year,
            "charger_cost_multiplier": charger_cost_multiplier,
            "max_tours_per_truck": max_tours_per_truck,
            "reuse_first_stage_dir": str(reuse_first_stage_dir) if reuse_first_stage_dir else None,
            "export_first_stage": export_first_stage,
            "export_first_stage_only_installed": export_first_stage_only_installed,
            "second_stage_eval_timelimit": second_stage_eval_timelimit,
            "second_stage_eval_timelimit": second_stage_eval_timelimit,
            "second_stage_eval_mipgap": second_stage_eval_mipgap,
            "reopt_scenario_mode": reopt_scenario_mode,
            "reopt_eval_mipgap": reopt_eval_mipgap,
            "reopt_max_iterations": reopt_max_iterations,
            "stage1_max_iterations": stage1_max_iterations,
            "explicit_cspp_overrides": cfg,
            "last_stage": "solve_clusters_first_stage",
        },
    )

    # Print header and configuration
    print_step_header(1, "FIRST-STAGE CLUSTER OPTIMIZATION")
    print_config({
        "Run ID": run_id,
        "Vehicle": vehicle_type,
        "Clustering": clustering_method,
        "Scenarios": len(base_inst.S),
        "Algorithm": alg,
        "Target gap": f"{gap*100:.0f}%",
        "Master MIP limit/iter": format_duration(timelimit_master_iter),
        "Heuristic limit": format_duration(heur_timelimit),
        "Second-stage minimum": format_duration(var_timelimit_minimum),
        "Initial master scenarios": initial_master_scenario_mode,
        "Max tours/truck": max_tours_per_truck,
        "Charger cost multiplier": f"{charger_cost_multiplier:.2f}x",
        "Parallel thread budget": get_parallel_total_threads(),
        "Max Gurobi threads/solve": gurobi_threads,
        "Total clusters": len(clusters),
        "Total customers": sum(len(c) for c in clusters.values()),
    })

    cluster_results = {}
    total_clusters = len(clusters)

    if reuse_first_stage_dir is not None:
        if not reuse_first_stage_dir.exists():
            print(f"\nERROR: reuse_first_stage_dir not found: {reuse_first_stage_dir}")
            sys.exit(1)

        print_subheader(f"Reusing first-stage solutions from: {reuse_first_stage_dir}")
        for cluster_id, customer_list in clusters.items():
            inst = build_cluster_instance(base_inst, customer_list, cluster_id)
            fs_path = reuse_first_stage_dir / f"cluster_{cluster_id}_first_stage.json"
            if not fs_path.exists():
                print(f"  Cluster {cluster_id}: MISSING first-stage file")
                cluster_results[cluster_id] = {
                    'stats': None,
                    'instance': inst,
                    'timeout': False,
                    'infeasible': True,
                    'time': 0.0,
                    'first_stage': None,
                    'error': "Missing first-stage file",
                    'reused': False,
                    'output_dir': output_dir,
                }
                cluster_results[cluster_id]["runtime_analysis"] = build_first_stage_runtime_row(
                    cluster_id, cluster_results[cluster_id]
                )
                export_first_stage_intermediate_state(
                    run_dir, output_dir, first_stage_dir, cluster_results, total_clusters, run_start_time
                )
                continue
            with open(fs_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            a = {(int(r["j"]), int(r["tau"])): float(r["val"]) for r in data.get("a", [])}
            a_wh = {int(model.WAREHOUSE_CHARGER_TYPE): 1.0}
            cluster_results[cluster_id] = {
                'stats': None,
                'instance': inst,
                'timeout': False,
                'infeasible': False,
                'time': 0.0,
                'first_stage': (a, a_wh),
                'reused': True,
                'output_dir': output_dir,
            }
            cluster_results[cluster_id]["runtime_analysis"] = build_first_stage_runtime_row(
                cluster_id, cluster_results[cluster_id]
            )
            print(f"  Cluster {cluster_id}: Loaded ({len(a)} chargers)")
            export_first_stage_intermediate_state(
                run_dir, output_dir, first_stage_dir, cluster_results, total_clusters, run_start_time
            )
    else:
        print_subheader("Solving Clusters")
        save_first_stage_current_state(run_dir, output_dir, cluster_results, total_clusters, run_start_time)
        parallel_results, _ = solve_clusters_parallel(clusters, base_inst)
        if parallel_results is not None:
            for cluster_id in sorted(parallel_results):
                result = parallel_results[cluster_id]
                result.setdefault("runtime_analysis", build_first_stage_runtime_row(cluster_id, result))
                cluster_results[cluster_id] = result
                export_first_stage_intermediate_state(
                    run_dir, output_dir, first_stage_dir, cluster_results, total_clusters, run_start_time
                )
        else:
            for idx, (cluster_id, customer_list) in enumerate(clusters.items(), 1):
                inst = build_cluster_instance(base_inst, customer_list, cluster_id)
                result = solve_cluster(inst, cluster_id, cluster_num=idx, total_clusters=total_clusters)
                result["output_dir"] = output_dir
                result["runtime_analysis"] = build_first_stage_runtime_row(cluster_id, result)
                cluster_results[cluster_id] = result

                export_first_stage_intermediate_state(
                    run_dir, output_dir, first_stage_dir, cluster_results, total_clusters, run_start_time
                )

    first_stage_exports = export_first_stage_intermediate_state(
        run_dir, output_dir, first_stage_dir, cluster_results, total_clusters, run_start_time
    )
    active_sets_path = save_cluster_active_sets(output_dir, cluster_results)
    latest_state_file.parent.mkdir(parents=True, exist_ok=True)
    latest_state_file.write_text(str(run_dir))

    # Print summary
    run_time = time.time() - run_start_time
    n_solved = sum(1 for r in cluster_results.values() if r.get('first_stage') is not None)
    n_timeout = sum(1 for r in cluster_results.values() if r.get('timeout', False))

    print_subheader("Step 1 Summary")
    print(f"  Completed in {format_duration(run_time)}")
    print(f"  Clusters solved: {n_solved}/{total_clusters}")
    if n_timeout > 0:
        print(f"  Timeouts: {n_timeout}")

    print_output_paths({
        "Output directory": output_dir,
        "First-stage CSV": first_stage_exports["first_stage_csv"],
        "Runtime analysis": first_stage_exports["runtime_csv"],
        "Progress snapshot": first_stage_exports["snapshot_json"],
        "State file": latest_state_file,
    })
    return output_dir



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CSPP cluster first-stage optimization.")
    add_common_args(parser)
    parser.add_argument("--only-cluster", type=int, default=None)
    parser.add_argument("--run-name", type=str, default=None)
    args, unknown = parser.parse_known_args()
    if unknown:
        print(f"Ignoring unrecognized arguments: {' '.join(unknown)}")
    
    # Manually inject args into the config-from-args logic if it does not use them
    # But for now ensure run() has access to them
    run(args=args, config=config_from_args(args))
