"""Post-sync degradation evaluation (#784).

Pure functions over a finished :class:`SyncResult` plus the DLQ depth — no I/O,
no config loading — so it's trivially unit-testable. The CLI seam (``_run_one``)
supplies the inputs; see :func:`evaluate_conditions`.

Per masukai's decisions on #784:
- ``row_errors_pct = failed / rows_extracted`` (**failed only** — ``skipped`` is a
  normal outcome via #757 match_policy / ``--limit`` / ``lookups.check_only``, so
  folding it into an *error* rate would false-fire on healthy syncs), and it is
  **0% when rows_extracted == 0** (empty-source is the separate rows_extracted
  condition — never a divide-by-zero or a false 100%).
- ``duration_seconds`` is skipped when ``SyncResult.duration_seconds`` is unset.
"""

from __future__ import annotations

from dataclasses import dataclass

from drt.config.models import DegradedConditions
from drt.destinations.base import SyncResult


@dataclass(frozen=True)
class TrippedCondition:
    """One condition that fired, with the numbers behind it (for message + JSON)."""

    metric: str
    operator: str
    threshold: float
    actual: float


def row_errors_pct(result: SyncResult) -> float:
    """failed / rows_extracted as a percentage; 0.0 when nothing was extracted."""
    if result.rows_extracted <= 0:
        return 0.0
    return result.failed / result.rows_extracted * 100.0


def evaluate_conditions(
    result: SyncResult,
    dlq_depth: int,
    conditions: DegradedConditions,
) -> list[TrippedCondition]:
    """Return every degradation condition that tripped for this sync run.

    Order is fixed (row_errors_pct, duration_seconds, rows_extracted, dlq_depth)
    so the coalesced message and JSON are deterministic.
    """
    tripped: list[TrippedCondition] = []

    def check(metric: str, actual: float | None) -> None:
        threshold = getattr(conditions, metric)
        if threshold is None or actual is None:
            return
        if threshold.compares(actual):
            tripped.append(
                TrippedCondition(
                    metric=metric,
                    operator=threshold.operator,
                    threshold=threshold.value,
                    actual=round(float(actual), 4),
                )
            )

    check("row_errors_pct", row_errors_pct(result))
    check("duration_seconds", result.duration_seconds)  # None -> skipped
    check("rows_extracted", float(result.rows_extracted))
    check("dlq_depth", float(dlq_depth))
    return tripped
