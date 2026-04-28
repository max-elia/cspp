from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MODEL_DIR = PROJECT_ROOT / "exports" / "unloading_time_model"
DEFAULT_MODEL_PATH = DEFAULT_MODEL_DIR / "unloading_time_model.json"

DEFAULT_MAX_REASONABLE_DURATION_MINUTES = 300
DEFAULT_WEIGHT_CATEGORY_BINS = [0.0, 1000.0, 2500.0, 5000.0, 10000.0, float("inf")]
DEFAULT_WEIGHT_CATEGORY_LABELS = [
    "0-1,000 kg",
    "1,000-2,500 kg",
    "2,500-5,000 kg",
    "5,000-10,000 kg",
    "> 10,000 kg",
]
DEFAULT_WEIGHT_CATEGORY_UPPER_PERCENTILE = 0.99

# Global once-for-all regression on cleaned stops with conservative filtering:
# valid values, positive weight/duration, duration <= 300 min, and only the
# top 1% duration tail removed within broad weight bins.
DEFAULT_UNLOADING_INTERCEPT_MINUTES = 11.511648398618156
DEFAULT_UNLOADING_SLOPE_MINUTES_PER_KG = 0.00418444281105551


@dataclass(frozen=True)
class UnloadingTimeModel:
    intercept_minutes: float = DEFAULT_UNLOADING_INTERCEPT_MINUTES
    slope_minutes_per_kg: float = DEFAULT_UNLOADING_SLOPE_MINUTES_PER_KG
    sample_size: int = 0
    r_squared: float = 0.0
    correlation: float = 0.0
    max_duration_minutes: int = DEFAULT_MAX_REASONABLE_DURATION_MINUTES
    upper_weight_bin_percentile: float = DEFAULT_WEIGHT_CATEGORY_UPPER_PERCENTILE
    source: str = "built_in_defaults"
    input_data_path: str = ""
    report_path: str = ""
    plot_path: str = ""

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "UnloadingTimeModel":
        return cls(
            intercept_minutes=float(payload.get("intercept_minutes", DEFAULT_UNLOADING_INTERCEPT_MINUTES)),
            slope_minutes_per_kg=float(payload.get("slope_minutes_per_kg", DEFAULT_UNLOADING_SLOPE_MINUTES_PER_KG)),
            sample_size=int(payload.get("sample_size", 0) or 0),
            r_squared=float(payload.get("r_squared", 0.0) or 0.0),
            correlation=float(payload.get("correlation", 0.0) or 0.0),
            max_duration_minutes=int(
                payload.get("max_duration_minutes", DEFAULT_MAX_REASONABLE_DURATION_MINUTES)
                or DEFAULT_MAX_REASONABLE_DURATION_MINUTES
            ),
            upper_weight_bin_percentile=float(
                payload.get("upper_weight_bin_percentile", DEFAULT_WEIGHT_CATEGORY_UPPER_PERCENTILE)
                or DEFAULT_WEIGHT_CATEGORY_UPPER_PERCENTILE
            ),
            source=str(payload.get("source", "built_in_defaults")),
            input_data_path=str(payload.get("input_data_path", "")),
            report_path=str(payload.get("report_path", "")),
            plot_path=str(payload.get("plot_path", "")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "intercept_minutes": self.intercept_minutes,
            "slope_minutes_per_kg": self.slope_minutes_per_kg,
            "sample_size": self.sample_size,
            "r_squared": self.r_squared,
            "correlation": self.correlation,
            "max_duration_minutes": self.max_duration_minutes,
            "upper_weight_bin_percentile": self.upper_weight_bin_percentile,
            "source": self.source,
            "input_data_path": self.input_data_path,
            "report_path": self.report_path,
            "plot_path": self.plot_path,
        }


def load_unloading_time_model(model_path: str | Path | None = None) -> UnloadingTimeModel:
    path = Path(model_path) if model_path is not None else DEFAULT_MODEL_PATH
    if path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))
        return UnloadingTimeModel.from_dict(payload)
    return UnloadingTimeModel()


def unloading_time_minutes(
    demand_kg: float,
    model: UnloadingTimeModel | None = None,
) -> float:
    demand_kg = float(demand_kg)
    if demand_kg <= 0:
        return 0.0
    current_model = model or load_unloading_time_model()
    return current_model.intercept_minutes + current_model.slope_minutes_per_kg * demand_kg


def unloading_time_hours(
    demand_kg: float,
    model: UnloadingTimeModel | None = None,
) -> float:
    return unloading_time_minutes(demand_kg, model=model) / 60.0
