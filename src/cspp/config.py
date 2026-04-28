from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
CSPP_DIR = SRC_DIR / "cspp"
CORE_DIR = CSPP_DIR / "core"

PIPELINE_ORDER = [
    "solve_clusters_first_stage",
    "scenario_evaluation",
    "cluster_reoptimization",
]

DEFAULT_SCRIPT = "run_cspp_single_instance"

SCRIPTS = {
    "run_cspp_single_instance": CSPP_DIR / "run_cspp_single_instance.py",
    "solve_clusters_first_stage": CSPP_DIR / "solve_clusters_first_stage.py",
    "scenario_evaluation": CSPP_DIR / "scenario_evaluation.py",
    "cluster_reoptimization": CSPP_DIR / "cluster_reoptimization.py",
}

PYTHONPATH_ENTRIES = [SRC_DIR, CSPP_DIR, CORE_DIR]
