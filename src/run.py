#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import Iterable

from clustering.config import DEFAULT_CLUSTERING_METHOD, PYTHONPATH_ENTRIES as CLUSTER_PYTHONPATH_ENTRIES, SCRIPTS as CLUSTER_SCRIPTS
from cspp.config import PIPELINE_ORDER as CSPP_PIPELINE_ORDER, PYTHONPATH_ENTRIES as CSPP_PYTHONPATH_ENTRIES, SCRIPTS as CSPP_SCRIPTS
from export_comparison import refresh_run_catalog
from frontend_exports import export_frontend_contract
from instance_payload import import_instance_payload, load_instance_payload, payload_has_cluster_assignments
from lieferdaten.runtime import build_run_dir, ensure_run_subdirs, get_run_layout, merge_run_config, parse_optional_non_negative_float
from run_summary import write_run_summary

ROOT_DIR = Path(__file__).resolve().parent.parent


def _env(entries: Iterable[Path]) -> dict[str, str]:
    env = os.environ.copy()
    current = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = os.pathsep.join([str(p) for p in entries] + ([current] if current else []))
    cache = ROOT_DIR / ".cache"
    env.setdefault("MPLCONFIGDIR", str(cache / "matplotlib"))
    Path(env["MPLCONFIGDIR"]).mkdir(parents=True, exist_ok=True)
    return env


def _run(script: Path, env: dict[str, str], args: list[str] | None = None) -> None:
    cmd = [sys.executable, str(script), *(args or [])]
    print(f"\n=== Running {script.relative_to(ROOT_DIR)} ===")
    subprocess.run(cmd, cwd=ROOT_DIR, env=env, check=True)


def _append(args: list[str], flag: str, value: object | None) -> None:
    if value is not None:
        args.extend([flag, str(value)])


def _run_dir(args: argparse.Namespace) -> Path:
    if args.output_run_dir:
        return Path(args.output_run_dir).expanduser().resolve()
    return build_run_dir(vehicle_type=args.vehicle_type, clustering_method=args.clustering_method, scenarios_to_use=args.scenarios_to_use, run_name=args.run_name)


def _cspp_args(args: argparse.Namespace, stage: str, run_dir: Path) -> list[str]:
    out: list[str] = []
    for flag, value in [
        ("--vehicle-type", args.vehicle_type), ("--scenarios-to-use", args.scenarios_to_use),
        ("--clustering-method", args.clustering_method), ("--gap", args.gap),
        ("--gurobi-threads", args.gurobi_threads), ("--d-cost", args.d_cost), ("--h", args.h),
        ("--max-tours-per-truck", args.max_tours_per_truck), ("--charger-cost-multiplier", args.charger_cost_multiplier),
        ("--second-stage-eval-timelimit", args.second_stage_eval_timelimit),
        ("--second-stage-eval-mipgap", args.second_stage_eval_mipgap), ("--reopt-eval-mipgap", args.reopt_eval_mipgap),
        ("--reopt-max-iterations", args.reopt_max_iterations),
    ]:
        _append(out, flag, value)
    if stage != "cluster_reoptimization":
        for flag, value in [("--timelimit-master-iter", args.timelimit_master_iter), ("--heur-timelimit", args.heur_timelimit), ("--var-timelimit-minimum", args.var_timelimit_minimum)]:
            _append(out, flag, value)
    if stage == "solve_clusters_first_stage":
        _append(out, "--stage1-max-iterations", args.stage1_max_iterations)
    if stage in {"scenario_evaluation", "cluster_reoptimization"}:
        _append(out, "--output_dir", get_run_layout(run_dir)["cspp_scenario_evaluation"])
    return out


def _run_clustering(args: argparse.Namespace, run_dir: Path, *, manual: bool = False) -> None:
    env = _env(CLUSTER_PYTHONPATH_ENTRIES)
    env["RUN_DIR"] = str(run_dir)
    method = args.clustering_method or DEFAULT_CLUSTERING_METHOD
    env["CLUSTERING_METHOD"] = method
    if method == "angular_slices_store_count": env["ANGULAR_SLICES_DEMAND_AWARE"] = "0"
    if method == "angular_slices": env["ANGULAR_SLICES_DEMAND_AWARE"] = "1"
    if args.gurobi_threads is not None: env["GUROBI_THREADS"] = str(args.gurobi_threads)
    stage = "build_clusters_manual" if manual else {
        "geographic": "build_clusters_geographic",
        "angular_slices": "build_clusters_angular_slices",
        "angular_slices_store_count": "build_clusters_angular_slices",
        "tour_containment": "build_clusters_tour_containment",
    }[method]
    merge_run_config(run_dir, {"selected_domain": "clustering", "selected_target": stage, "clustering_method": method})
    _run(CLUSTER_SCRIPTS[stage], env)


def _run_cspp(args: argparse.Namespace, run_dir: Path) -> None:
    env = _env(CSPP_PYTHONPATH_ENTRIES)
    env["RUN_DIR"] = str(run_dir)
    if args.gurobi_threads is not None: env["GUROBI_THREADS"] = str(args.gurobi_threads)
    merge_run_config(run_dir, {"selected_domain": "cspp", "selected_target": args.target, "clustering_method": args.clustering_method, "vehicle_type": args.vehicle_type, "scenarios_to_use": args.scenarios_to_use})
    stages = CSPP_PIPELINE_ORDER if args.target == "full" else [args.target]
    for stage in stages:
        _run(CSPP_SCRIPTS[stage], env, _cspp_args(args, stage, run_dir))
    write_run_summary(run_dir)
    export_frontend_contract(run_dir)
    refresh_run_catalog()


def cmd_import(args: argparse.Namespace) -> int:
    run_dir = _run_dir(args)
    import_instance_payload(args.instance_payload, run_dir)
    print(run_dir)
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    run_dir = _run_dir(args)
    ensure_run_subdirs(run_dir)
    manual = False
    if args.instance_payload:
        payload = load_instance_payload(args.instance_payload)
        import_instance_payload(args.instance_payload, run_dir)
        manual = payload_has_cluster_assignments(payload)
    if args.domain in {"all", "clustering"}:
        _run_clustering(args, run_dir, manual=manual)
    if args.domain in {"all", "cspp"}:
        _run_cspp(args, run_dir)
    return 0


def cmd_list(_: argparse.Namespace) -> int:
    print("Domains: all, clustering, cspp")
    print("CSPP stages:")
    for stage in CSPP_PIPELINE_ORDER:
        print(f"  - {stage}")
    print("Clustering methods: geographic, angular_slices, angular_slices_store_count, tour_containment")
    return 0


def add_shared(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--instance-payload", help="Public JSON instance payload to import before running.")
    parser.add_argument("--run-name")
    parser.add_argument("--output-run-dir")
    parser.add_argument("--vehicle-type", choices=["mercedes", "volvo"], default="mercedes")
    parser.add_argument("--scenarios-to-use", type=int, default=None)
    parser.add_argument("--clustering-method", choices=["geographic", "angular_slices", "angular_slices_store_count", "tour_containment"], default="geographic")
    parser.add_argument("--gurobi-threads", type=int, default=16)
    parser.add_argument("--d-cost", type=float, default=None)
    parser.add_argument("--h", type=float, default=None)
    parser.add_argument("--max-tours-per-truck", type=int, default=None)
    parser.add_argument("--charger-cost-multiplier", type=float, default=None)
    parser.add_argument("--gap", type=float, default=None)
    parser.add_argument("--timelimit-master-iter", type=float, default=None)
    parser.add_argument("--heur-timelimit", type=float, default=None)
    parser.add_argument("--var-timelimit-minimum", type=float, default=None)
    parser.add_argument("--second-stage-eval-timelimit", type=float, default=None)
    parser.add_argument("--second-stage-eval-mipgap", type=float, default=0.05)
    parser.add_argument("--reopt-eval-mipgap", type=float, default=0.05)
    parser.add_argument("--reopt-max-iterations", type=int, default=None)
    parser.add_argument("--stage1-max-iterations", type=int, default=None)


def main() -> int:
    parser = argparse.ArgumentParser(description="Public CSPP pipeline runner")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list").set_defaults(func=cmd_list)
    imp = sub.add_parser("import-instance", help="Import a public instance payload into exports/runs/<name>.")
    imp.add_argument("instance_payload")
    add_shared(imp)
    imp.set_defaults(func=cmd_import)
    run = sub.add_parser("run", help="Run clustering and/or CSPP stages.")
    run.add_argument("domain", choices=["all", "clustering", "cspp"])
    run.add_argument("target", nargs="?", default="full")
    add_shared(run)
    run.set_defaults(func=cmd_run)
    args = parser.parse_args()
    try:
        return args.func(args)
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
