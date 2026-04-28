from pathlib import Path
import os

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
CLUSTERING_DIR = SRC_DIR / "clustering"

PIPELINE_ORDER = [
    "build_clusters_geographic",
    "build_clusters_angular_slices",
    "build_clusters_tour_containment",
]

SCRIPTS = {
    "build_clusters_geographic": CLUSTERING_DIR / "build_clusters_geographic.py",
    "build_clusters_angular_slices": CLUSTERING_DIR / "build_clusters_angular_slices.py",
    "build_clusters_tour_containment": CLUSTERING_DIR / "build_clusters_tour_containment.py",
    "build_clusters_manual": CLUSTERING_DIR / "build_clusters_manual.py",
}

PYTHONPATH_ENTRIES = [SRC_DIR, SRC_DIR / "cspp"]

DEFAULT_CLUSTERING_METHOD = "geographic"
DEFAULT_CLUSTER_TARGET_SIZE = 16
DEFAULT_CLUSTER_MIN_SIZE = 0
DEFAULT_CLUSTER_MAX_SIZE = 16
DEFAULT_CLUSTER_MIP_GAP = 0.05


def cluster_generation_params(default_target=DEFAULT_CLUSTER_TARGET_SIZE, default_min=DEFAULT_CLUSTER_MIN_SIZE, default_max=DEFAULT_CLUSTER_MAX_SIZE):
    target = int(os.environ.get("CLUSTER_TARGET_SIZE", str(default_target)))
    min_size = int(os.environ.get("CLUSTER_MIN_SIZE", str(default_min)))
    max_size = int(os.environ.get("CLUSTER_MAX_SIZE", str(default_max)))
    return target, min_size, max_size


def choose_cluster_count(n_customers, target_customers_per_cluster, max_cluster_size, min_clusters=2):
    by_target = round(n_customers / target_customers_per_cluster)
    by_max_size = (n_customers + max_cluster_size - 1) // max_cluster_size
    return max(min_clusters, by_target, by_max_size)


def cluster_mip_params(default_timelimit, default_gap=DEFAULT_CLUSTER_MIP_GAP):
    timelimit = int(default_timelimit)
    gap = float(os.environ.get("CLUSTER_MIP_GAP", str(default_gap)))
    return timelimit, gap
