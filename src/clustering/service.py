from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from clustering.config import CLUSTERING_DIR
from clustering.config import PYTHONPATH_ENTRIES
from lieferdaten.runtime import get_run_layout
from lieferdaten.runtime import merge_run_config
from lieferdaten.runtime import resolve_run_root
from webserver_runtime import WEBSERVER_UPLOAD_ROOT

METHOD_SPECS: dict[str, dict[str, Any]] = {
    "geographic": {
        "script": CLUSTERING_DIR / "build_clusters_geographic.py",
        "label": "Geographic",
        "description": "K-means clustering with balancing and arc-set generation.",
        "manual": False,
    },
    "angular_slices": {
        "script": CLUSTERING_DIR / "build_clusters_angular_slices.py",
        "label": "Angular Slices (Demand-Aware)",
        "description": "Warehouse-centered angular sectors with demand-aware balancing.",
        "manual": False,
        "env": {"ANGULAR_SLICES_DEMAND_AWARE": "1"},
    },
    "angular_slices_store_count": {
        "script": CLUSTERING_DIR / "build_clusters_angular_slices.py",
        "label": "Angular Slices (Store-Count)",
        "description": "Warehouse-centered angular sectors with equal store-count balancing (no demand weighting).",
        "manual": False,
        "env": {"ANGULAR_SLICES_DEMAND_AWARE": "0"},
    },
    "tour_containment": {
        "script": CLUSTERING_DIR / "build_clusters_tour_containment.py",
        "label": "Tour Containment",
        "description": "Preserve whole historical tours as much as possible.",
        "manual": False,
    },
}


def canonicalize_method(method: str) -> str:
    return method


def list_clustering_methods() -> list[dict[str, Any]]:
    return [
        {"method": key, **value, "script": str(value["script"].name)}
        for key, value in METHOD_SPECS.items()
    ]


def _env_with_pythonpath() -> dict[str, str]:
    env = os.environ.copy()
    current = env.get("PYTHONPATH", "")
    entries = [str(path) for path in PYTHONPATH_ENTRIES]
    env["PYTHONPATH"] = os.pathsep.join(entries + ([current] if current else []))
    return env


def stage_manual_assignments(run_root: str | Path, upload_path: str | Path | None = None) -> Path:
    run_dir = resolve_run_root(run_root)
    layout = get_run_layout(run_dir)
    source = Path(upload_path).resolve() if upload_path else (WEBSERVER_UPLOAD_ROOT / "instance_json" / run_dir.name / "cluster_assignments.json").resolve()
    if not source.exists():
        raise FileNotFoundError(f"Manual cluster assignments not found: {source}")
    target = layout["run"] / "prep" / "clustering" / "manual_assignments.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(source.read_bytes())
    return target


def run_clustering_method(run_root: str | Path, method: str, *, gurobi_threads: int = 16) -> subprocess.CompletedProcess[str]:
    run_dir = resolve_run_root(run_root)
    canonical_method = canonicalize_method(method)
    if canonical_method not in METHOD_SPECS:
        raise ValueError(f"Unsupported clustering method: {method}")
    spec = METHOD_SPECS[canonical_method]
    if spec.get("manual"):
        stage_manual_assignments(run_dir)

    env = _env_with_pythonpath()
    env["RUN_DIR"] = str(run_dir)
    env["CLUSTERING_METHOD"] = canonical_method
    env["GUROBI_THREADS"] = str(gurobi_threads)
    for k, v in spec.get("env", {}).items():
        env[k] = v
    merge_run_config(
        run_dir,
        {
            "selected_domain": "clustering",
            "selected_target": "full",
            "clustering_method": canonical_method,
            "gurobi_threads": gurobi_threads,
        },
    )
    return subprocess.run(
        [sys.executable, str(spec["script"])],
        cwd=Path(__file__).resolve().parents[2],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
