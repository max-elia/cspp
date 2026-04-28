"""
Charger type configuration for CSPP.

This is the primary source of charger definitions.
"""

CHARGER_TYPES = [
    {
        "type_id": 1,
        "name": "22kW_AC",
        "power_kw": 22.0,
        "total_cost_eur": 7500.0,
        "volvo_available": True,
        "mercedes_available": True,
    },
    {
        "type_id": 2,
        "name": "43kW_AC",
        "power_kw": 43.0,
        "total_cost_eur": 8500.0,
        "volvo_available": True,
        "mercedes_available": False,
    },
    {
        "type_id": 3,
        "name": "40kW_DC",
        "power_kw": 40.0,
        "total_cost_eur": 22500.0,
        "volvo_available": True,
        "mercedes_available": True,
    },
    {
        "type_id": 4,
        "name": "50kW_DC",
        "power_kw": 50.0,
        "total_cost_eur": 33000.0,
        "volvo_available": True,
        "mercedes_available": True,
    },
    {
        "type_id": 5,
        "name": "90kW_DC",
        "power_kw": 90.0,
        "total_cost_eur": 60000.0,
        "volvo_available": True,
        "mercedes_available": True,
    },
    {
        "type_id": 6,
        "name": "120kW_DC",
        "power_kw": 120.0,
        "total_cost_eur": 75000.0,
        "volvo_available": True,
        "mercedes_available": True,
    },
    {
        "type_id": 7,
        "name": "150kW_DC",
        "power_kw": 150.0,
        "total_cost_eur": 100000.0,
        "volvo_available": True,
        "mercedes_available": True,
    },
    {
        "type_id": 8,
        "name": "250kW_DC",
        "power_kw": 250.0,
        "total_cost_eur": 130000.0,
        "volvo_available": True,
        "mercedes_available": False,
    },
]
