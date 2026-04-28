import argparse
import ast
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import threading

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import pandas as pd
import numpy as np
from gurobipy import GRB

import applications.cspp.model as model
from json_artifacts import read_event_log
from json_artifacts import read_json
from json_artifacts import read_table_rows
from json_artifacts import write_event_log
from json_artifacts import write_json
from json_artifacts import write_table
import solve_clusters_first_stage as step1
from route_map_export import plot_unused_customers, plot_warehouse

from logging_utils import (
    print_subheader, print_config, print_step_header,
    print_scenario_start, print_scenario_result,
    print_output_paths, format_duration, format_cost
)
from lieferdaten.runtime import get_run_layout
from lieferdaten.runtime import merge_results_json
from lieferdaten.runtime import merge_run_config
from lieferdaten.runtime import resolve_run_root
from frontend_exports import export_frontend_contract

MAX_TOTAL_MIP_THREADS = 64
MIP_THREADS_PER_SOLVE = 4


def base_node(inst, node):
    if hasattr(inst, "pseudo_to_base") and inst.pseudo_to_base:
        return inst.pseudo_to_base.get(node, node)
    return node


def display_node(inst, node):
    base = base_node(inst, node)
    if base == node or not hasattr(inst, "base_to_pseudo") or not inst.base_to_pseudo:
        return str(node)
    pseudo_list = inst.base_to_pseudo.get(base, [])
    try:
        idx = pseudo_list.index(node) + 1
    except ValueError:
        idx = 0
    return f"{base}#{idx}" if idx > 0 else str(base)

def load_first_stage_csv(output_dir, base_inst, clusters):
    output_dir = Path(output_dir)
    fs_path = output_dir / "cluster_first_stage.json"
    if not fs_path.exists():
        raise FileNotFoundError(f"Missing first-stage JSON: {fs_path}")

    cluster_results = {}
    for cluster_id, customer_list in clusters.items():
        inst = step1.build_cluster_instance(base_inst, customer_list, cluster_id)
        cluster_results[cluster_id] = {
            "stats": None,
            "instance": inst,
            "timeout": False,
            "infeasible": False,
            "time": 0.0,
            # Default to empty first-stage (no customer chargers)
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


def save_scenario_costs(output_dir, scenario_total_costs, cluster_scenario_costs):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    totals_path = output_dir / "scenario_total_costs.json"
    write_table(
        totals_path,
        ["scenario", "total_cost"],
        [{"scenario": s, "total_cost": cost} for s, cost in sorted(scenario_total_costs.items())],
    )

    cluster_path = output_dir / "cluster_scenario_costs.json"
    cluster_rows = []
    for cid, sc_costs in cluster_scenario_costs.items():
        for s, cost in sc_costs.items():
            cluster_rows.append({"cluster_id": cid, "scenario": s, "cost": cost})
    write_table(cluster_path, ["cluster_id", "scenario", "cost"], cluster_rows)


def extract_solver_mip_state(grb_model):
    def _safe_float(value):
        try:
            out = float(value)
        except Exception:
            return None
        return out if np.isfinite(out) else None

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


def build_scenario_runtime_row(cluster_id, scenario, result):
    mip_state = result.get("mip_state") or {}
    return {
        "cluster_id": int(cluster_id),
        "scenario": int(scenario),
        "status": result.get("status"),
        "runtime_sec": mip_state.get("runtime_sec"),
        "nodecount": mip_state.get("nodecount"),
        "obj_val": mip_state.get("obj_val"),
        "obj_bound": mip_state.get("obj_bound"),
        "mip_gap": mip_state.get("mip_gap"),
        "solcount": mip_state.get("solcount"),
        "demand": result.get("demand"),
        "cost": result.get("cost"),
        "installed_chargers": len(result.get("chargers", {})),
        "runtime_bucket_base": result.get("runtime_bucket_base"),
        "runtime_bucket_final": result.get("runtime_bucket_final"),
        "demand_promotion_applied": result.get("demand_promotion_applied"),
        "effective_timelimit_per_round_sec": result.get("effective_timelimit_per_round_sec"),
        "effective_timelimit_total_sec": result.get("effective_timelimit_total_sec"),
        "extension_rounds_used": result.get("extension_rounds_used"),
        "time_to_first_incumbent_sec": result.get("time_to_first_incumbent_sec"),
        "last_improvement_time_sec": result.get("last_improvement_time_sec"),
        "error": result.get("error"),
    }


def save_scenario_runtime_analysis(output_dir, runtime_rows):
    output_dir = Path(output_dir)
    runtime_path = output_dir / "scenario_cluster_runtime_analysis.json"
    summary_path = output_dir / "scenario_cluster_runtime_summary.json"
    output_dir.mkdir(parents=True, exist_ok=True)

    ordered_rows = sorted(runtime_rows, key=lambda row: (row["cluster_id"], row["scenario"]))
    write_table(
        runtime_path,
        [
            "cluster_id", "scenario", "status", "runtime_sec", "nodecount", "obj_val",
            "obj_bound", "mip_gap", "solcount", "demand", "cost", "installed_chargers",
            "runtime_bucket_base", "runtime_bucket_final", "demand_promotion_applied",
            "effective_timelimit_per_round_sec", "effective_timelimit_total_sec", "extension_rounds_used",
            "time_to_first_incumbent_sec", "last_improvement_time_sec", "error",
        ],
        ordered_rows,
    )
    per_cluster = {}
    for row in ordered_rows:
        cid = row["cluster_id"]
        summary = per_cluster.setdefault(
            cid,
            {
                "cluster_id": cid,
                "evaluated_scenarios": 0,
                "feasible_scenarios": 0,
                "runtime_values": [],
                "cost_values": [],
                "max_mip_gap": None,
            },
        )
        summary["evaluated_scenarios"] += 1
        if row.get("status") in {"OPTIMAL_OR_FEASIBLE", "TIME_LIMIT_INCUMBENT"}:
            summary["feasible_scenarios"] += 1
        if row.get("runtime_sec") is not None:
            summary["runtime_values"].append(float(row["runtime_sec"]))
        if row.get("cost") is not None:
            summary["cost_values"].append(float(row["cost"]))
        if row.get("mip_gap") is not None:
            current_gap = float(row["mip_gap"])
            summary["max_mip_gap"] = current_gap if summary["max_mip_gap"] is None else max(summary["max_mip_gap"], current_gap)

    summary_rows = []
    for cid in sorted(per_cluster):
        summary = per_cluster[cid]
        runtimes = summary["runtime_values"]
        costs = summary["cost_values"]
        runtime_total = sum(runtimes) if runtimes else None
        runtime_mean = (runtime_total / len(runtimes)) if runtimes else None
        runtime_max = max(runtimes) if runtimes else None
        cost_mean = (sum(costs) / len(costs)) if costs else None
        cost_max = max(costs) if costs else None
        summary_rows.append(
            {
                "cluster_id": cid,
                "evaluated_scenarios": summary["evaluated_scenarios"],
                "feasible_scenarios": summary["feasible_scenarios"],
                "runtime_total_sec": runtime_total,
                "runtime_mean_sec": runtime_mean,
                "runtime_max_sec": runtime_max,
                "cost_mean": cost_mean,
                "cost_max": cost_max,
                "max_mip_gap": summary["max_mip_gap"],
            }
        )
    write_table(
        summary_path,
        [
            "cluster_id", "evaluated_scenarios", "feasible_scenarios", "runtime_total_sec",
            "runtime_mean_sec", "runtime_max_sec", "cost_mean", "cost_max", "max_mip_gap",
        ],
        summary_rows,
    )
    return runtime_path, summary_path


def save_scenario_progress_snapshot(
    run_dir,
    output_dir,
    runtime_rows,
    scenario_total_costs,
    scenario_statuses,
    total_cluster_solves,
    total_scenarios,
    total_clusters,
    started_at,
):
    output_dir = Path(output_dir)
    snapshot_path = output_dir / "scenario_progress_snapshot.json"
    completed_cluster_solves = len(runtime_rows)
    completed_scenarios = len(scenario_statuses)
    payload = {
        "stage": "scenario_evaluation",
        "status": "completed" if completed_scenarios >= total_scenarios and total_scenarios > 0 else ("running" if completed_cluster_solves > 0 else "pending"),
        "elapsed_sec": max(0.0, time.time() - started_at),
        "total_scenarios": total_scenarios,
        "completed_scenarios": completed_scenarios,
        "total_clusters": total_clusters,
        "total_cluster_solves": total_cluster_solves,
        "completed_cluster_solves": completed_cluster_solves,
        "scenario_statuses": {str(k): v for k, v in sorted(scenario_statuses.items())},
        "scenario_total_costs": {str(k): v for k, v in sorted(scenario_total_costs.items())},
        "cluster_runtime_rows": sorted(runtime_rows, key=lambda row: (row["cluster_id"], row["scenario"])),
    }
    snapshot_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    merge_results_json(
        run_dir,
        "metrics.scenario_evaluation_progress",
        {
            "elapsed_sec": payload["elapsed_sec"],
            "total_scenarios": total_scenarios,
            "completed_scenarios": completed_scenarios,
            "total_clusters": total_clusters,
            "total_cluster_solves": total_cluster_solves,
            "completed_cluster_solves": completed_cluster_solves,
        },
    )
    try:
        export_frontend_contract(run_dir)
    except Exception:
        pass
    return snapshot_path


class ScenarioEvaluationLiveReporter:
    def __init__(self, output_dir):
        self.output_dir = Path(output_dir)
        self.live_dir = self.output_dir / "solver_live"
        self.live_dir.mkdir(parents=True, exist_ok=True)
        self.current_path = self.live_dir / "current_solver_states.json"
        self.events_path = self.live_dir / "solver_events.json"
        self.lock = threading.Lock()
        self.states = {}
        self.events = []

    def _key(self, cluster_id, scenario):
        return f"cluster_{int(cluster_id)}__scenario_{int(scenario)}"

    def emit(self, payload):
        cluster_id = payload.get("cluster_id")
        scenario = payload.get("scenario")
        if cluster_id is None or scenario is None:
            return
        key = self._key(cluster_id, scenario)
        with self.lock:
            self.states[key] = _json_safe(payload)
            self.events.append(_json_safe(payload))
            write_event_log(self.events_path, self.events)
            write_json(self.current_path, {"states": self.states})
        try:
            export_frontend_contract(self.output_dir.parents[1])
        except Exception:
            pass


def _json_safe(value):
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def compute_rel_gap(best_obj, best_bound):
    if best_obj is None or best_bound is None:
        return None
    denom = abs(best_obj)
    if denom <= 1e-12:
        return 0.0 if abs(best_bound - best_obj) <= 1e-9 else None
    return abs(best_obj - best_bound) / denom


def _record_second_stage_progress(payload, live_reporter=None, trajectory_recorder=None):
    safe_payload = _json_safe(payload)
    if trajectory_recorder is not None:
        trajectory_recorder.append(safe_payload)
    if live_reporter is not None:
        live_reporter.emit(safe_payload)


def make_second_stage_progress_callback(
    cluster_id,
    scenario,
    live_reporter=None,
    trajectory_recorder=None,
    throttle_sec=15.0,
):
    last_best_obj = None
    last_progress_runtime = None

    def callback(model, where):
        nonlocal last_best_obj, last_progress_runtime
        if where == GRB.Callback.MIPSOL:
            runtime = float(model.cbGet(GRB.Callback.RUNTIME))
            best_obj = float(model.cbGet(GRB.Callback.MIPSOL_OBJBST))
            best_bound = float(model.cbGet(GRB.Callback.MIPSOL_OBJBND))
            nodecount = float(model.cbGet(GRB.Callback.MIPSOL_NODCNT))
            if last_best_obj is not None and abs(best_obj - last_best_obj) <= 1e-9:
                return
            _record_second_stage_progress(
                {
                    "event": "incumbent",
                    "cluster_id": int(cluster_id),
                    "scenario": int(scenario),
                    "runtime_sec": runtime,
                    "best_obj": best_obj,
                    "best_bound": best_bound,
                    "gap": compute_rel_gap(best_obj, best_bound),
                    "nodecount": nodecount,
                },
                live_reporter=live_reporter,
                trajectory_recorder=trajectory_recorder,
            )
            last_best_obj = best_obj
            last_progress_runtime = runtime
            return

        if where != GRB.Callback.MIP:
            return
        runtime = float(model.cbGet(GRB.Callback.RUNTIME))
        if last_progress_runtime is not None and runtime - last_progress_runtime < throttle_sec:
            return
        best_obj = float(model.cbGet(GRB.Callback.MIP_OBJBST))
        best_bound = float(model.cbGet(GRB.Callback.MIP_OBJBND))
        nodecount = float(model.cbGet(GRB.Callback.MIP_NODCNT))
        _record_second_stage_progress(
            {
                "event": "progress",
                "cluster_id": int(cluster_id),
                "scenario": int(scenario),
                "runtime_sec": runtime,
                "best_obj": best_obj,
                "best_bound": best_bound,
                "gap": compute_rel_gap(best_obj, best_bound),
                "nodecount": nodecount,
            },
            live_reporter=live_reporter,
            trajectory_recorder=trajectory_recorder,
        )
        last_progress_runtime = runtime

    return callback


def save_warmstart_solution(warmstart_dir, scenario, combined_solution):
    """
    Save combined cluster solutions as warmstart for global second-stage.

    The warmstart format matches SecondStageModel._apply_warmstart:
    (y, u, t, r, c_arr, p, omega, c_dep, c_ret, p_wh, omega_wh, p_overnight)
    """
    warmstart_dir = Path(warmstart_dir)
    warmstart_dir.mkdir(parents=True, exist_ok=True)

    # Convert tuple keys to strings for JSON serialization
    def serialize_dict(d):
        return {str(k): v for k, v in d.items()}

    data = {
        "scenario": scenario,
        "y": serialize_dict(combined_solution[0]),
        "u": serialize_dict(combined_solution[1]),
        "t": serialize_dict(combined_solution[2]),
        "r": serialize_dict(combined_solution[3]),
        "c_arr": serialize_dict(combined_solution[4]),
        "p": serialize_dict(combined_solution[5]),
        "omega": serialize_dict(combined_solution[6]),
        "c_dep": serialize_dict(combined_solution[7]),
        "c_ret": serialize_dict(combined_solution[8]),
        "p_wh": serialize_dict(combined_solution[9]),
        "omega_wh": serialize_dict(combined_solution[10]),
        "p_overnight": serialize_dict(combined_solution[11]),
    }

    path = warmstart_dir / f"scenario_{scenario}_warmstart.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)


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

    # Convert string keys back to tuples
    def deserialize_dict(d, key_type="tuple"):
        result = {}
        for k, v in d.items():
            if key_type == "tuple":
                # Parse string like "(1, 2)" or "(1, 2, 3, 4)" back to tuple
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


def solve_cluster_for_scenario(
    cluster_id,
    result,
    scenario,
    mipgap,
    timelimit,
    threads,
    live_reporter=None,
    capture_trajectory=False,
    progress_throttle_sec=15.0,
    log_path=None,
):
    """Solve a single cluster's second-stage for one scenario. Thread-safe (Gurobi releases GIL)."""
    inst = result['instance']
    first_stage = result['first_stage']
    trajectory = [] if capture_trajectory else None
    cluster_metrics = step1.compute_cluster_complexity_metrics(inst)
    runtime_bucket_base = step1.classify_cluster_runtime_bucket_base(cluster_metrics)
    runtime_bucket_final = step1.classify_evaluation_timelimit_bucket(cluster_metrics)

    if result.get('infeasible', False):
        result_dict = {
            "cluster_id": cluster_id,
            "status": "INFEASIBLE",
            "runtime_bucket_base": runtime_bucket_base,
            "runtime_bucket_final": runtime_bucket_final,
            "demand_promotion_applied": runtime_bucket_final != runtime_bucket_base,
        }
        if trajectory is not None:
            result_dict["trajectory"] = trajectory
        return result_dict
    if not first_stage:
        result_dict = {
            "cluster_id": cluster_id,
            "status": "NO_SOLUTION",
            "runtime_bucket_base": runtime_bucket_base,
            "runtime_bucket_final": runtime_bucket_final,
            "demand_promotion_applied": runtime_bucket_final != runtime_bucket_base,
        }
        if trajectory is not None:
            result_dict["trajectory"] = trajectory
        return result_dict

    a_sol = first_stage[0] if isinstance(first_stage, tuple) else first_stage
    chargers = {}
    for (j, tau), val in a_sol.items():
        if val >= 0.5:
            chargers[(j, tau)] = inst.kappa[tau]

    cluster_demand = sum(inst.beta.get((scenario, j), 0) for j in inst.J)
    if cluster_demand == 0:
        result_dict = {
            "cluster_id": cluster_id,
            "status": "SKIP",
            "chargers": chargers,
            "demand": cluster_demand,
            "runtime_bucket_base": runtime_bucket_base,
            "runtime_bucket_final": runtime_bucket_final,
            "demand_promotion_applied": runtime_bucket_final != runtime_bucket_base,
            "effective_timelimit_per_round_sec": float(timelimit),
            "effective_timelimit_total_sec": float(timelimit),
            "extension_rounds_used": 0,
        }
        if trajectory is not None:
            result_dict["trajectory"] = trajectory
        return result_dict

    ss_model = model.SecondStageModel(
        inst,
        scenario,
        first_stage,
        name=f"Route_{cluster_id}_{scenario}",
    )
    ss_model.Params.OutputFlag = 0 if log_path is None else 1
    ss_model.Params.Threads = threads
    ss_model.Params.MIPGap = mipgap

    def _run_round(round_timelimit, round_log_path, runtime_offset_sec):
        ss_model.Params.OutputFlag = 0 if log_path is None else 1
        ss_model.Params.TimeLimit = round_timelimit
        if round_log_path is not None:
            round_log_path = Path(round_log_path)
            round_log_path.parent.mkdir(parents=True, exist_ok=True)
            ss_model.Params.LogFile = str(round_log_path)
        round_trajectory = [] if trajectory is not None else None
        if live_reporter is not None or round_trajectory is not None:
            _record_second_stage_progress(
                {
                    "event": "started",
                    "cluster_id": int(cluster_id),
                    "scenario": int(scenario),
                    "runtime_sec": float(runtime_offset_sec),
                },
                live_reporter=live_reporter,
                trajectory_recorder=round_trajectory,
            )
            ss_model.optimize(
                make_second_stage_progress_callback(
                    cluster_id,
                    scenario,
                    live_reporter=live_reporter,
                    trajectory_recorder=round_trajectory,
                    throttle_sec=progress_throttle_sec,
                )
            )
        else:
            ss_model.optimize()
        mip_state = extract_solver_mip_state(ss_model)
        mip_state["runtime_sec"] = float(getattr(ss_model, "_accRuntime", 0.0) or 0.0)
        if live_reporter is not None or round_trajectory is not None:
            _record_second_stage_progress(
                {
                    "event": "finished",
                    "cluster_id": int(cluster_id),
                    "scenario": int(scenario),
                    "runtime_sec": float(mip_state["runtime_sec"] or 0.0),
                    "best_obj": mip_state.get("obj_val"),
                    "best_bound": mip_state.get("obj_bound"),
                    "gap": mip_state.get("mip_gap"),
                    "nodecount": mip_state.get("nodecount"),
                    "status_code": mip_state.get("status_code"),
                },
                live_reporter=live_reporter,
                trajectory_recorder=round_trajectory,
            )
        if trajectory is not None and isinstance(round_trajectory, list):
            for event in round_trajectory:
                event_copy = dict(event)
                runtime_sec = event_copy.get("runtime_sec")
                if runtime_sec is not None:
                    event_copy["runtime_sec"] = float(runtime_sec) + float(runtime_offset_sec)
                trajectory.append(event_copy)
        return ss_model, mip_state

    total_runtime_sec = 0.0
    extension_rounds_used = 0
    final_model = None
    mip_state = None
    time_to_first_incumbent_sec = None
    last_improvement_time_sec = None

    try:
        while True:
            round_log_path = None
            if log_path is not None:
                base_log_path = Path(log_path)
                round_log_path = base_log_path
            round_target_sec = float(timelimit) * (extension_rounds_used + 1)
            round_start_sec = float(getattr(ss_model, "_accRuntime", 0.0) or 0.0)
            round_timelimit = max(0.0, round_target_sec - round_start_sec)
            final_model, mip_state = _run_round(
                round_timelimit,
                round_log_path,
                round_start_sec,
            )
            total_runtime_sec = float(mip_state.get("runtime_sec") or 0.0)
            progress_metrics = step1.analyze_progress_points(
                [
                    (event.get("runtime_sec"), event.get("best_obj"))
                    for event in (trajectory or [])
                    if isinstance(event, dict)
                ]
            )
            time_to_first_incumbent_sec = progress_metrics.get("time_to_first_incumbent_sec")
            last_improvement_time_sec = progress_metrics.get("last_improvement_time_sec")

            status = final_model.Status
            solcount = final_model.SolCount
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

        status = final_model.Status
        solcount = final_model.SolCount
        mip_gap_val = float(final_model.MIPGap) if solcount > 0 else None

        if solcount == 0:
            if status == GRB.INFEASIBLE:
                result_dict = {"cluster_id": cluster_id, "status": "INFEASIBLE", "chargers": chargers, "demand": cluster_demand, "mip_state": mip_state}
            elif status == GRB.TIME_LIMIT:
                result_dict = {"cluster_id": cluster_id, "status": "TIME_LIMIT_NO_INCUMBENT", "chargers": chargers, "demand": cluster_demand, "mip_state": mip_state}
            else:
                result_dict = {"cluster_id": cluster_id, "status": "NO_SOLUTION", "chargers": chargers, "demand": cluster_demand, "mip_state": mip_state}
            result_dict.update(
                {
                    "runtime_bucket_base": runtime_bucket_base,
                    "runtime_bucket_final": runtime_bucket_final,
                    "demand_promotion_applied": runtime_bucket_final != runtime_bucket_base,
                    "effective_timelimit_per_round_sec": float(timelimit),
                    "effective_timelimit_total_sec": float(total_runtime_sec),
                    "extension_rounds_used": extension_rounds_used,
                    "time_to_first_incumbent_sec": time_to_first_incumbent_sec,
                    "last_improvement_time_sec": last_improvement_time_sec,
                }
            )
            if trajectory is not None:
                result_dict["trajectory"] = trajectory
            return result_dict

        result_dict = {
            "cluster_id": cluster_id,
            "status": "TIME_LIMIT_INCUMBENT" if status == GRB.TIME_LIMIT else "OPTIMAL_OR_FEASIBLE",
            "chargers": chargers,
            "demand": cluster_demand,
            "cost": float(final_model.ObjVal),
            "mip_gap": mip_gap_val,
            "obj_bound": None,
            "mip_state": mip_state,
            "runtime_bucket_base": runtime_bucket_base,
            "runtime_bucket_final": runtime_bucket_final,
            "demand_promotion_applied": runtime_bucket_final != runtime_bucket_base,
            "effective_timelimit_per_round_sec": float(timelimit),
            "effective_timelimit_total_sec": float(total_runtime_sec),
            "extension_rounds_used": extension_rounds_used,
            "time_to_first_incumbent_sec": time_to_first_incumbent_sec,
            "last_improvement_time_sec": last_improvement_time_sec,
        }
        try:
            result_dict["obj_bound"] = float(final_model.ObjBound)
        except:
            pass

        # Extract solution
        ss_sol = final_model.get_second_stage_solution()
        result_dict["solution"] = ss_sol
        result_dict["inst"] = inst
        if trajectory is not None:
            result_dict["trajectory"] = trajectory

        return result_dict

    except KeyboardInterrupt:
        raise
    except Exception as e:
        result_dict = {
            "cluster_id": cluster_id,
            "status": "ERROR",
            "error": str(e),
            "chargers": chargers if 'chargers' in locals() else {},
            "demand": cluster_demand if 'cluster_demand' in locals() else 0,
            "mip_state": None,
            "runtime_bucket_base": runtime_bucket_base,
            "runtime_bucket_final": runtime_bucket_final,
            "demand_promotion_applied": runtime_bucket_final != runtime_bucket_base,
        }
        if trajectory is not None:
            result_dict["trajectory"] = trajectory
        return result_dict


def _solve_cluster_for_scenario(cluster_id, result, scenario, mipgap, timelimit, threads, live_reporter=None):
    return solve_cluster_for_scenario(
        cluster_id,
        result,
        scenario,
        mipgap,
        timelimit,
        threads,
        live_reporter=live_reporter,
    )


def get_max_concurrent_mips(total_thread_budget, mip_threads_per_solve=MIP_THREADS_PER_SOLVE):
    return max(1, int(total_thread_budget) // max(1, int(mip_threads_per_solve)))


def update_scenario_status(current, candidate):
    severity = {
        "INFEASIBLE": 5,
        "TIME_LIMIT_NO_INCUMBENT": 4,
        "NO_SOLUTION": 3,
        "TIME_LIMIT_INCUMBENT": 2,
        "OPTIMAL_OR_FEASIBLE": 1,
    }
    if current is None:
        return candidate
    return candidate if severity.get(candidate, 0) > severity.get(current, 0) else current


def is_feasible_scenario_status(status):
    return status in {"OPTIMAL_OR_FEASIBLE", "TIME_LIMIT_INCUMBENT"}


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


def run(output_dir=None, mipgap=0.05, config=None):
    """
    Run scenario evaluation (Step 2): evaluate first-stage solutions per cluster.

    Args:
        output_dir: Path to runs/<run_id>/06_scenario_evaluation/results
        mipgap: MIP gap for second-stage solves
    """
    run_start_time = time.time()
    step1.apply_config(config)
    total_thread_budget = min(step1.get_parallel_total_threads(), MAX_TOTAL_MIP_THREADS)

    output_dir = resolve_output_dir(output_dir)
    run_dir = resolve_run_root(output_dir)
    run_layout = get_run_layout(run_dir)
    run_id = run_dir.name
    first_stage_dir = run_layout["cspp_first_stage"]
    maps_dir = run_layout["cspp_scenario_route_maps"]
    warmstart_dir = run_layout["cspp_scenario_warmstarts"]
    maps_dir.mkdir(parents=True, exist_ok=True)
    warmstart_dir.mkdir(parents=True, exist_ok=True)

    clusters = step1.load_clusters()
    base_inst = step1.create_base_instance(apply_fleet_limits=False)
    cluster_results = load_first_stage_csv(first_stage_dir, base_inst, clusters)
    default_eval_timelimit = None
    eval_bucket_labels = set()
    for result in cluster_results.values():
        cluster_eval_timelimit, _, eval_bucket = step1.effective_evaluation_timelimit(result["instance"])
        if default_eval_timelimit is None:
            default_eval_timelimit = cluster_eval_timelimit
        eval_bucket_labels.add(eval_bucket)
    merge_run_config(
        run_dir,
        {
            "scenario_evaluation_mipgap": mipgap,
            "scenario_evaluation_timelimit": default_eval_timelimit,
            "second_stage_eval_timelimit": default_eval_timelimit,
            "scenario_evaluation_timelimit_source": (
                "second_stage_eval_timelimit"
                if step1.is_explicit_config_override("second_stage_eval_timelimit")
                else "complexity_aware_default"
            ),
            "last_stage": "scenario_evaluation",
        },
    )

    # Load coordinates from run dir's cspp/data
    coords_file = run_layout["cspp_data"] / "coordinates.json"
    if not coords_file.exists():
        project_root = Path(__file__).resolve().parents[2]
        coords_file = project_root / "src" / "cspp" / "core" / "applications" / "cspp" / "instances" / "coordinates.json"
    node_coords = {}
    if coords_file.exists():
        coords_df = pd.DataFrame(read_table_rows(coords_file))
        for _, row in coords_df.iterrows():
            node_idx = int(row['node_index'])
            node_coords[node_idx] = (row['latitude'], row['longitude'])

    # Print header and configuration
    print_step_header(2, "SCENARIO EVALUATION (PER-CLUSTER)")
    print_config({
        "Run ID": run_id,
        "Scenarios": len(base_inst.S),
        "Clusters": len(clusters),
        "MIP gap": f"{mipgap*100:.0f}%",
        "Eval time limit/cluster": (
            format_duration(default_eval_timelimit)
            if step1.is_explicit_config_override("second_stage_eval_timelimit")
            else ", ".join(
                format_duration(step1.EVALUATION_TIMELIMIT_RULES[bucket])
                for bucket in sorted(eval_bucket_labels)
            )
        ),
        "Eval source": (
            "second_stage_eval_timelimit override"
            if step1.is_explicit_config_override("second_stage_eval_timelimit")
            else "complexity-aware buckets"
        ),
        "Parallel thread budget": total_thread_budget,
        "Max Gurobi threads/solve": MIP_THREADS_PER_SOLVE,
    })

    scenarios = base_inst.S
    scenario_total_costs = {}
    scenario_statuses = {}
    cluster_scenario_costs = {cid: {} for cid in clusters.keys()}
    total_scenarios = len(scenarios)
    total_clusters = len(clusters)
    total_cluster_solves = total_scenarios * total_clusters
    threads_per_mip = MIP_THREADS_PER_SOLVE
    max_concurrent_mips = get_max_concurrent_mips(total_thread_budget, threads_per_mip)
    runtime_rows = []
    live_reporter = ScenarioEvaluationLiveReporter(output_dir)
    print(
        f"  Launching up to {max_concurrent_mips} concurrent cluster/scenario MIP(s) "
        f"and {threads_per_mip} Gurobi thread(s) per MIP"
    )

    cluster_solve_results_by_scenario = {scenario: {} for scenario in scenarios}
    with ThreadPoolExecutor(max_workers=max_concurrent_mips) as executor:
        futures = {}
        for scenario_idx, scenario in enumerate(scenarios, 1):
            print_scenario_start(scenario, scenario_idx, total_scenarios)
            for cluster_id, result in cluster_results.items():
                effective_timelimit, _, _ = step1.effective_evaluation_timelimit(result["instance"])
                future = executor.submit(
                    _solve_cluster_for_scenario,
                    cluster_id,
                    result,
                    scenario,
                    mipgap,
                    effective_timelimit,
                    threads_per_mip,
                    live_reporter,
                )
                futures[future] = scenario

        for future in as_completed(futures):
            scenario = futures[future]
            res = future.result()
            runtime_rows.append(build_scenario_runtime_row(res["cluster_id"], scenario, res))
            cluster_solve_results_by_scenario[scenario][res["cluster_id"]] = res
            save_scenario_runtime_analysis(output_dir, runtime_rows)
            save_scenario_progress_snapshot(
                run_dir,
                output_dir,
                runtime_rows,
                scenario_total_costs,
                scenario_statuses,
                total_cluster_solves,
                total_scenarios,
                total_clusters,
                run_start_time,
            )

    for scenario in scenarios:
        cluster_solve_results = cluster_solve_results_by_scenario[scenario]
        scenario_status = "OPTIMAL_OR_FEASIBLE"

        all_routes_data = []
        all_chargers = {}
        all_waits = {}
        total_scenario_demand = 0
        total_charged = 0
        total_wait = 0
        total_wh_recharge = 0
        total_overnight = 0
        total_cluster_second_stage_cost = 0.0
        clusters_solved = 0
        cluster_gaps = []  # Track MIP gaps for each cluster
        cluster_bounds = []  # Track objective bounds

        # For combining cluster solutions into global warmstart
        global_truck_offset = 0  # Offset for renumbering trucks across clusters
        combined_y = {}
        combined_u = {}
        combined_t = {}
        combined_r = {}
        combined_c_arr = {}
        combined_p = {}
        combined_omega = {}
        combined_c_dep = {}
        combined_c_ret = {}
        combined_p_wh = {}
        combined_omega_wh = {}
        combined_p_overnight = {}

        # --- Sequential aggregation of parallel results ---
        for cluster_id in sorted(cluster_solve_results.keys()):
            res = cluster_solve_results[cluster_id]
            status_str = res["status"]

            if status_str in ("INFEASIBLE", "NO_SOLUTION", "TIME_LIMIT_NO_INCUMBENT"):
                scenario_status = update_scenario_status(scenario_status, status_str)
                total_scenario_demand += res.get("demand", 0)
                all_chargers.update(res.get("chargers", {}))
                continue
            if status_str == "SKIP":
                total_scenario_demand += res.get("demand", 0)
                all_chargers.update(res.get("chargers", {}))
                continue
            if status_str == "ERROR":
                print(f"    Cluster {cluster_id}: solve error - {res.get('error', '?')}")
                total_scenario_demand += res.get("demand", 0)
                continue

            all_chargers.update(res.get("chargers", {}))
            total_scenario_demand += res.get("demand", 0)

            if status_str == "TIME_LIMIT_INCUMBENT":
                scenario_status = update_scenario_status(scenario_status, "TIME_LIMIT_INCUMBENT")

            clusters_solved += 1
            cluster_cost = res["cost"]
            cluster_scenario_costs[cluster_id][scenario] = cluster_cost
            total_cluster_second_stage_cost += cluster_cost

            if res["mip_gap"] is not None:
                cluster_gaps.append(res["mip_gap"])
            if res["obj_bound"] is not None:
                cluster_bounds.append(res["obj_bound"])

            # --- solution extraction & warmstart assembly ---
            try:
                inst = res["inst"]
                ss_sol = res["solution"]
                y_sol, u_sol, t_sol, r_sol, c_arr_sol, p_sol, omega_sol, c_dep_sol, c_ret_sol, p_wh_sol, omega_wh_sol, p_overnight_sol = ss_sol
                depot = inst.i0
                K_range = range(1, inst.K_max + 1)
                M_range = range(1, inst.M_max + 1)

                # --- warmstart assembly (truck renumbering across clusters) ---
                local_to_global_truck = {}
                next_global_truck = global_truck_offset + 1

                for k in K_range:
                    if y_sol.get(k, 0) > 0.5:
                        local_to_global_truck[k] = next_global_truck
                        global_k = next_global_truck
                        next_global_truck += 1

                        combined_y[global_k] = y_sol[k]
                        combined_p_overnight[global_k] = p_overnight_sol.get(k, 0)

                        for m in M_range:
                            if u_sol.get((k, m), 0) > 0.5:
                                combined_u[(global_k, m)] = u_sol[(k, m)]
                                combined_c_dep[(global_k, m)] = c_dep_sol.get((k, m), 0)
                                combined_c_ret[(global_k, m)] = c_ret_sol.get((k, m), 0)
                                combined_p_wh[(global_k, m)] = p_wh_sol.get((k, m), 0)
                                combined_omega_wh[(global_k, m)] = omega_wh_sol.get((k, m), 0)

                        M_max_tour = M_range[-1]
                        if (global_k, M_max_tour) not in combined_c_ret:
                            combined_c_ret[(global_k, M_max_tour)] = c_ret_sol.get((k, M_max_tour), 0)

                total_overnight += sum(p_overnight_sol.values())

                for (v1, v2, k, m), val in t_sol.items():
                    if k in local_to_global_truck and val > 0:
                        combined_t[(v1, v2, local_to_global_truck[k], m)] = val
                for (v1, v2, k, m), val in r_sol.items():
                    if k in local_to_global_truck and val > 0.5:
                        combined_r[(v1, v2, local_to_global_truck[k], m)] = val
                for (j, k, m), val in c_arr_sol.items():
                    if k in local_to_global_truck:
                        combined_c_arr[(j, local_to_global_truck[k], m)] = val
                for (j, k, m), val in p_sol.items():
                    if k in local_to_global_truck:
                        combined_p[(j, local_to_global_truck[k], m)] = val
                for (j, k, m), val in omega_sol.items():
                    if k in local_to_global_truck:
                        combined_omega[(j, local_to_global_truck[k], m)] = val

                global_truck_offset = next_global_truck - 1

                # --- route extraction for maps / reports ---
                for k in K_range:
                    if y_sol.get(k, 0) < 0.5:
                        continue

                    last_used_tour = max(
                        (m for m in M_range if u_sol.get((k, m), 0) >= 0.5),
                        default=None
                    )

                    for m in M_range:
                        if u_sol.get((k, m), 0) < 0.5:
                            continue

                        tour_arcs = [(v1, v2) for (v1, v2, tk, tm), val in r_sol.items()
                                     if tk == k and tm == m and val >= 0.5 and v1 != v2]
                        if not tour_arcs:
                            continue

                        route = [depot]
                        current = depot
                        visited = {depot}
                        for v1, v2 in tour_arcs:
                            if v1 == depot:
                                current = v2
                                route.append(current)
                                visited.add(current)
                                break

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

                        charged_on_tour = sum(p_sol.get((j, k, m), 0) for j in route if j != depot)
                        wait_on_tour = sum(omega_sol.get((j, k, m), 0) for j in route if j != depot)

                        all_routes_data.append({
                            'cluster': cluster_id,
                            'truck': k,
                            'tour': m,
                            'route': route,
                            'c_dep': c_dep_sol.get((k, m), 0),
                            'c_ret': c_ret_sol.get((k, m), 0),
                            'p_wh': p_wh_sol.get((k, m), 0),
                            'p_overnight': p_overnight_sol.get(k, 0) if m == last_used_tour else 0,
                            'charged': charged_on_tour,
                            'wait': wait_on_tour,
                            'sol_objects': (c_arr_sol, p_sol, omega_sol, inst)
                        })

                        total_charged += charged_on_tour
                        total_wait += wait_on_tour
                        total_wh_recharge += p_wh_sol.get((k, m), 0)

                        for j in route:
                            if j != depot:
                                base_j = base_node(inst, j)
                                all_waits[base_j] = all_waits.get(base_j, 0) + omega_sol.get((j, k, m), 0)

            except KeyboardInterrupt:
                raise
            except Exception as e:
                print(f"    Cluster {cluster_id}: solution extraction error - {e}")

        # Compute mean gap across clusters
        mean_gap = sum(cluster_gaps) / len(cluster_gaps) if cluster_gaps else None

        # Print scenario summary with gap info
        mean_gap_str = f"{mean_gap*100:.2f}%" if mean_gap is not None else "N/A"
        max_gap_str = f"{max(cluster_gaps)*100:.2f}%" if cluster_gaps else "N/A"
        scenario_cost = total_cluster_second_stage_cost if is_feasible_scenario_status(scenario_status) else None
        print(f"  Status: {scenario_status} | Cost: {format_cost(scenario_cost)} | Gap (mean): {mean_gap_str} (max: {max_gap_str}) | Routes: {len(all_routes_data)} | Clusters: {clusters_solved}")

        # Save combined warmstart solution for global second-stage
        if combined_y:
            combined_solution = (
                combined_y, combined_u, combined_t, combined_r,
                combined_c_arr, combined_p, combined_omega,
                combined_c_dep, combined_c_ret, combined_p_wh, combined_omega_wh,
                combined_p_overnight
            )
            save_warmstart_solution(warmstart_dir, scenario, combined_solution)
            print(f"  Warmstart saved: {len(combined_y)} trucks, {len(combined_u)} tours")

        # Export route data as JSON for later re-plotting
        if all_routes_data:
            route_export = []
            for r_data in all_routes_data:
                inst = r_data['sol_objects'][3]
                route_export.append({
                    'cluster': int(r_data['cluster']),
                    'truck': int(r_data['truck']),
                    'tour': int(r_data['tour']),
                    'route': [int(base_node(inst, n)) for n in r_data['route']],
                    'charged': float(r_data['charged']),
                    'wait': float(r_data['wait']),
                })
            route_json_path = maps_dir / f"routes_scenario_{scenario}.json"
            write_json(route_json_path, {
                'scenario': int(scenario),
                'demand_kg': float(total_scenario_demand),
                'routes': route_export,
                'chargers': {f"{c_j},{tau}": float(pwr) for (c_j, tau), pwr in all_chargers.items()},
                'waits': {str(k): float(v) for k, v in all_waits.items()},
            })

        if node_coords:
            # Compute shared bounding box from all node coordinates
            all_lons = [lon for lat, lon in node_coords.values()]
            all_lats = [lat for lat, lon in node_coords.values()]
            lon_pad = (max(all_lons) - min(all_lons)) * 0.08
            lat_pad = (max(all_lats) - min(all_lats)) * 0.08
            bbox = (min(all_lons) - lon_pad, max(all_lons) + lon_pad,
                    min(all_lats) - lat_pad, max(all_lats) + lat_pad)

            fig, ax = plt.subplots(1, 1, figsize=(8, 7))

            all_customers = set()
            for c_list in clusters.values():
                all_customers.update(c_list)

            served_customers = set()
            for r_data in all_routes_data:
                inst = r_data['sol_objects'][3]
                for node in r_data['route']:
                    if node != base_inst.i0:
                        served_customers.add(base_node(inst, node))

            # Plot unserved customers as light gray
            for j in all_customers:
                if j not in served_customers and j in node_coords:
                    lat, lon = node_coords[j]
                    ax.scatter(lon, lat, s=15, c='#cccccc', edgecolors='white', linewidths=0.2, zorder=1)

            # Plot served customers: green if charger, orange if not
            for j in served_customers:
                if j in node_coords:
                    lat, lon = node_coords[j]
                    has_charger = any(j == c_j for (c_j, _) in all_chargers.keys())
                    charger_power = 0
                    for (c_j, tau), pwr in all_chargers.items():
                        if c_j == j:
                            charger_power = pwr
                    color = '#2ca02c' if has_charger else '#ff7f0e'
                    ax.scatter(lon, lat, s=40, c=color, edgecolors='black', linewidths=0.3, zorder=4)

                    if charger_power > 0:
                        ax.annotate(f"{int(charger_power)}kW", (lon, lat), xytext=(5, 3),
                                    textcoords='offset points', fontsize=5, color='#333333', zorder=5)

            # Plot routes colored by cluster
            cmap = plt.colormaps.get_cmap('tab20')
            for r_data in all_routes_data:
                inst = r_data['sol_objects'][3]
                route = [base_node(inst, n) for n in r_data['route']]
                color = cmap(r_data['cluster'] % 20)
                for k in range(len(route) - 1):
                    u, v = route[k], route[k+1]
                    if u in node_coords and v in node_coords:
                        lat1, lon1 = node_coords[u]
                        lat2, lon2 = node_coords[v]
                        ax.plot([lon1, lon2], [lat1, lat2], c=color, alpha=0.6, linewidth=1.2, zorder=3)

            # Warehouse as blue square
            if base_inst.i0 in node_coords:
                wh_lat, wh_lon = node_coords[base_inst.i0]
                ax.scatter(wh_lon, wh_lat, s=180, c='#4363d8', marker='s', zorder=5,
                           edgecolors='black', linewidths=0.6)

            ax.set_title(f"Scenario {scenario} — {total_scenario_demand:,.0f} kg demand, {len(all_routes_data)} routes", fontsize=11)
            ax.set_xlim(bbox[0], bbox[1])
            ax.set_ylim(bbox[2], bbox[3])
            ax.set_xticks([])
            ax.set_yticks([])
            ax.set_xlabel("")
            ax.set_ylabel("")
            ax.axis("off")
            ax.set_aspect(1.4)
            fig.tight_layout()

            map_path = maps_dir / f"combined_scenario_{scenario}.png"
            plt.savefig(map_path, dpi=300, bbox_inches='tight')
            map_pdf_path = maps_dir / f"combined_scenario_{scenario}.pdf"
            plt.savefig(map_pdf_path, dpi=300, bbox_inches='tight')
            plt.close(fig)

        report_path = maps_dir / f"combined_scenario_{scenario}.json"
        customer_charge_cost = total_charged * base_inst.d_cost
        wh_charge_cost = total_wh_recharge * base_inst.d_cost
        wait_cost = total_wait * base_inst.h
        truck_cost = len(set((r['truck'], r['cluster']) for r in all_routes_data)) * base_inst.F
        overnight_cost = total_overnight * base_inst.d_cost
        total_second_stage_cost = customer_charge_cost + wh_charge_cost + wait_cost + truck_cost + overnight_cost
        write_json(
            report_path,
            {
                "scenario": int(scenario),
                "total_demand_kg": float(total_scenario_demand),
                "total_routes": int(len(all_routes_data)),
                "total_trucks_used": int(len(set((r['truck'], r['cluster']) for r in all_routes_data))),
                "total_charged_kwh": float(total_charged),
                "total_warehouse_recharge_kwh": float(total_wh_recharge),
                "total_wait_hours": float(total_wait),
                "costs": {
                    "customer_charging": float(customer_charge_cost),
                    "warehouse_charging": float(wh_charge_cost),
                    "overnight_charging": float(overnight_cost),
                    "waiting_time": float(wait_cost),
                    "truck_fixed_costs": float(truck_cost),
                    "total_second_stage": float(total_second_stage_cost),
                },
            },
        )

        scenario_total_costs[scenario] = scenario_cost
        scenario_statuses[scenario] = scenario_status
        save_scenario_costs(output_dir, scenario_total_costs, cluster_scenario_costs)
        save_scenario_progress_snapshot(
            run_dir,
            output_dir,
            runtime_rows,
            scenario_total_costs,
            scenario_statuses,
            total_cluster_solves,
            total_scenarios,
            total_clusters,
            run_start_time,
        )

    save_scenario_costs(output_dir, scenario_total_costs, cluster_scenario_costs)
    runtime_csv, runtime_summary_csv = save_scenario_runtime_analysis(output_dir, runtime_rows)
    scenario_snapshot_json = save_scenario_progress_snapshot(
        run_dir,
        output_dir,
        runtime_rows,
        scenario_total_costs,
        scenario_statuses,
        total_cluster_solves,
        total_scenarios,
        total_clusters,
        run_start_time,
    )

    # Summary outputs (compressed from previous comprehensive summary)
    summary_file = output_dir / "cluster_summary.json"
    write_json(
        summary_file,
        {
            "vehicle_type": step1.vehicle_type,
            "scenarios": int(len(scenarios)),
            "fleet_sizing": "per-cluster",
            "eval_timelimit_seconds": float(default_eval_timelimit) if default_eval_timelimit is not None else None,
            "scenario_total_costs": {str(k): v for k, v in scenario_total_costs.items()},
            "scenario_statuses": {str(k): v for k, v in scenario_statuses.items()},
        },
    )

    # Print step summary
    run_time = time.time() - run_start_time
    feasible_costs = {k: v for k, v in scenario_total_costs.items() if v is not None}
    worst_scenario = max(feasible_costs, key=feasible_costs.get) if feasible_costs else None

    print_subheader("Step 2 Summary")
    print(f"  Completed in {format_duration(run_time)}")
    print(f"  Scenarios evaluated: {len(scenarios)}")
    print(f"  Feasible scenarios: {len(feasible_costs)}/{len(scenarios)}")
    if feasible_costs:
        print(f"  Cost range: {format_cost(min(feasible_costs.values()))} - {format_cost(max(feasible_costs.values()))}")
        print(f"  Worst scenario: {worst_scenario} ({format_cost(feasible_costs[worst_scenario])})")

    print_output_paths({
        "Summary": summary_file,
        "Scenario costs": output_dir / "scenario_total_costs.json",
        "Cluster runtime analysis": runtime_csv,
        "Cluster runtime summary": runtime_summary_csv,
        "Progress snapshot": scenario_snapshot_json,
        "Route maps": maps_dir,
    })
    return output_dir


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CSPP cluster scenario evaluation.")
    step1.add_common_args(parser)
    parser.add_argument(
        "--output_dir",
        type=str,
        default=None,
        help="Path to runs/<run_id>/results.",
    )
    parser.add_argument("--mipgap", type=float, default=0.05, help="MIP gap for per-cluster second-stage solves")
    args, unknown = parser.parse_known_args()
    if unknown:
        print(f"Ignoring unrecognized arguments: {' '.join(unknown)}")
    run(
        output_dir=args.output_dir,
        mipgap=args.mipgap,
        config=step1.config_from_args(args),
    )
