"""
Charging Station Placement Problem (CSPP) package.

This package provides optimization models and instance data for the
two-stage robust optimization of charging station placement.
"""

from .instance import (
    Instance,
    create_instance,
    create_mercedes_instance,
    create_volvo_instance,
    MERCEDES_EACTROS,
    VOLVO_FM_ELECTRIC,
)

from .model import MasterModel, SecondStageModel, P

__all__ = [
    # Core classes
    "Instance",
    "MasterModel",
    "SecondStageModel",
    "P",
    # Instance creation
    "create_instance",
    "create_mercedes_instance",
    "create_volvo_instance",
    # Vehicle type constants
    "MERCEDES_EACTROS",
    "VOLVO_FM_ELECTRIC",
]
