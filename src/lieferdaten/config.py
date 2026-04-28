from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
SCRIPTS_DIR = SRC_DIR / "lieferdaten"

PIPELINE_ORDER = [
    "process_tour_data",
    "generate_instance_data",
    "combine_tours",
]

SCRIPTS = {
    "process_tour_data": SCRIPTS_DIR / "process_tour_data.py",
    "generate_instance_data": SCRIPTS_DIR / "generate_instance_data.py",
    "combine_tours": SCRIPTS_DIR / "combine_tours.py",
}

PYTHONPATH_ENTRIES = [
    SRC_DIR,
    SRC_DIR / "cspp",
]

# Lieferdaten defaults
DEFAULT_MAX_DISTANCE_FROM_WAREHOUSE_KM = 100.0
