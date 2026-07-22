"""Alert dispatch for sync failures + degraded-sync conditions (#784)."""
from drt.alerts.conditions import TrippedCondition, evaluate_conditions
from drt.alerts.dispatcher import (
    build_context,
    build_degraded_context,
    dispatch_alerts,
    dispatch_targets,
)

__all__ = [
    "dispatch_alerts",
    "dispatch_targets",
    "build_context",
    "build_degraded_context",
    "evaluate_conditions",
    "TrippedCondition",
]
