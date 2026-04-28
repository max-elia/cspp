"""Vehicle-specific calibration for the CSPP fixed truck-use cost F."""

from __future__ import annotations

TRUCK_USEFUL_LIFE_YEARS = 9
DEFAULT_OPERATING_DAYS_PER_YEAR = 290

# German truck-driver labour input.
DRIVER_GROSS_ANNUAL_SALARY_EUR = 36_000.0
GERMANY_NON_WAGE_COST_SHARE = 0.233
WORKING_DAYS_PER_MONTH = 21.0

# Representative public market observations for the vehicle classes used in the
# thesis. These values are used only for the daily fixed-cost proxy in F.
VEHICLE_PURCHASE_PRICE_EUR = {
    "mercedes": 202_700.0,
    "volvo": 191_950.0,
}


def driver_day_cost() -> float:
    """Employer-side daily driver cost in EUR/day."""
    monthly_gross = DRIVER_GROSS_ANNUAL_SALARY_EUR / 12.0
    monthly_employer_cost = monthly_gross * (1.0 + GERMANY_NON_WAGE_COST_SHARE)
    return monthly_employer_cost / WORKING_DAYS_PER_MONTH


def annualized_vehicle_day_cost(
    vehicle_type: str,
    *,
    operating_days_per_year: int = DEFAULT_OPERATING_DAYS_PER_YEAR,
    useful_life_years: int = TRUCK_USEFUL_LIFE_YEARS,
) -> float:
    """Straight-line daily vehicle capital cost in EUR/day."""
    try:
        purchase_price = VEHICLE_PURCHASE_PRICE_EUR[vehicle_type]
    except KeyError as exc:
        raise ValueError(f"Unknown vehicle type: {vehicle_type}") from exc

    return purchase_price / (useful_life_years * operating_days_per_year)


def default_fixed_truck_cost(
    vehicle_type: str,
    *,
    operating_days_per_year: int = DEFAULT_OPERATING_DAYS_PER_YEAR,
    useful_life_years: int = TRUCK_USEFUL_LIFE_YEARS,
) -> float:
    """
    Combined daily fixed truck-use cost in EUR/day.

    The parameter keeps truck capital cost and one driver-day inside the single
    model parameter F.
    """
    return round(
        annualized_vehicle_day_cost(
            vehicle_type,
            operating_days_per_year=operating_days_per_year,
            useful_life_years=useful_life_years,
        )
        + driver_day_cost(),
        2,
    )
