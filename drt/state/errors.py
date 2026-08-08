"""Errors shared by remote state-store implementations."""


class StateContentionError(RuntimeError):
    """A state update could not win its conditional-write retry budget.

    State is the recovery checkpoint for a sync, so silently dropping it would
    recreate the lost-update failure that remote state exists to prevent.
    History and DLQ persistence have their own best-effort contracts instead.
    """

