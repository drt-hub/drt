"""Change guards: limits on how much one run may change (#1218).

Pure: given the counts a plan computed and the configured limits, say which
guards trip. A guard that cannot be evaluated (a percentage whose denominator
the strategy cannot report) trips instead of passing, so a missing number never
reads as "within limits".
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class GuardTrip:
    guard: str
    limit: float
    observed: float | None  # None: could not be evaluated
    message: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "guard": self.guard,
            "limit": self.limit,
            "observed": self.observed,
            "message": self.message,
        }


class GuardTripped(Exception):
    """A run-time change guard refused destructive finalization."""

    def __init__(self, trips: list[GuardTrip], *, replan_required: bool = False) -> None:
        self.trips = trips
        self.replan_required = replan_required
        suffix = " Re-run `drt plan` and review the larger change." if replan_required else ""
        super().__init__(
            "Change guards tripped; nothing was deleted or swapped: "
            + "; ".join(trip.message for trip in trips)
            + suffix
        )


class GuardOutcomeUncertain(GuardTripped):
    """A guard tripped, but its destructive statement may not have rolled back."""

    def __init__(self, guard_error: GuardTripped, rollback_error: BaseException) -> None:
        self.trips = guard_error.trips
        self.replan_required = guard_error.replan_required
        Exception.__init__(
            self,
            "Change guards tripped, but rollback failed; the destination outcome is "
            "uncertain and the DELETE may have taken effect: "
            f"{type(rollback_error).__name__}: {rollback_error}",
        )


def _pct(part: int, whole: int) -> float:
    """For display only: limits are compared exactly (see :func:`_exceeds`)."""
    return round(100.0 * part / whole, 2)


def _exceeds(part: int, whole: int, limit_pct: float) -> bool:
    """``part / whole`` strictly above ``limit_pct``, with no rounding.

    Cross-multiplied so a limit of 0 trips on one row in 20,001 and a limit of
    0.33 trips on 1 in 300, which a two-decimal rounding would let through.
    """
    return Decimal(part) * 100 > Decimal(str(limit_pct)) * Decimal(whole)


def evaluate_guards(
    guards: Any,
    *,
    creates: int,
    updates: int,
    deletes: int,
    source_rows: int,
    delete_baseline: int | None,
) -> list[GuardTrip]:
    """Return the guards that trip for these counts (empty when none, or none set)."""
    if guards is None:
        return []
    trips: list[GuardTrip] = []

    limit = guards.max_creates
    if limit is not None and creates > limit:
        trips.append(
            GuardTrip(
                "max_creates",
                limit,
                creates,
                f"max_creates: the plan creates {creates} rows (limit {limit})",
            )
        )

    limit = guards.max_deletes
    if limit is not None and deletes > limit:
        trips.append(
            GuardTrip(
                "max_deletes",
                limit,
                deletes,
                f"max_deletes: the plan deletes {deletes} rows (limit {limit})",
            )
        )

    limit = guards.max_delete_pct
    if limit is not None and deletes > 0:
        if not delete_baseline:
            trips.append(
                GuardTrip(
                    "max_delete_pct",
                    limit,
                    None,
                    "max_delete_pct: cannot be evaluated because this delete strategy does "
                    f"not report how many rows it looked at ({deletes} would be deleted); "
                    "use max_deletes instead",
                )
            )
        else:
            observed = _pct(deletes, delete_baseline)
            if _exceeds(deletes, delete_baseline, limit):
                trips.append(
                    GuardTrip(
                        "max_delete_pct",
                        limit,
                        observed,
                        f"max_delete_pct: the plan deletes {observed}% of {delete_baseline} "
                        f"rows (limit {limit}%)",
                    )
                )

    limit = guards.max_updates_pct
    if limit is not None and updates > 0:
        if source_rows <= 0:
            trips.append(
                GuardTrip(
                    "max_updates_pct",
                    limit,
                    None,
                    "max_updates_pct: cannot be evaluated without a source row count",
                )
            )
        else:
            observed = _pct(updates, source_rows)
            if _exceeds(updates, source_rows, limit):
                trips.append(
                    GuardTrip(
                        "max_updates_pct",
                        limit,
                        observed,
                        f"max_updates_pct: the plan updates {observed}% of {source_rows} "
                        f"source rows (limit {limit}%)",
                    )
                )
    return trips


def enforce_delete_guards(
    guards: Any,
    *,
    deletes: int,
    baseline: int | None,
    force: bool,
    reviewed_deletes: int | None = None,
    reviewed_baseline: int | None = None,
    reviewed_guards: set[str] | None = None,
) -> list[GuardTrip]:
    """Evaluate only run-time delete guards and refuse unless forced.

    ``max_creates`` and ``max_updates_pct`` remain plan/apply-only: the SQL
    finalize seam can count the rows a destructive DELETE/swap would remove,
    but it cannot recover exact create/update classifications after batches
    have already been written.
    """
    if guards is None:
        return []

    # Feed a delete-only view through the one canonical evaluator. A shallow
    # proxy prevents max_creates/max_updates_pct from becoming run-time guards
    # if their evaluation rules change later.
    class _DeleteGuards:
        max_creates = None
        max_updates_pct = None
        max_deletes = guards.max_deletes
        max_delete_pct = guards.max_delete_pct

    trips = evaluate_guards(
        _DeleteGuards(),
        creates=0,
        updates=0,
        deletes=deletes,
        source_rows=0,
        delete_baseline=baseline,
    )
    if trips and not force:
        raise GuardTripped(trips)
    if trips and reviewed_guards is not None:
        # ``drt apply --force-guards`` is approval of the reviewed plan, not a
        # blank cheque for a larger change that appears between review and the
        # destination transaction. Plain ``drt run --force-guards`` leaves
        # reviewed_guards as None and retains its deliberate force-all behavior.
        unreviewed: list[GuardTrip] = []
        for trip in trips:
            if trip.guard not in reviewed_guards or reviewed_deletes is None:
                unreviewed.append(trip)
                continue
            if trip.guard == "max_deletes":
                larger = deletes > reviewed_deletes
            elif reviewed_baseline and baseline:
                larger = deletes * reviewed_baseline > reviewed_deletes * baseline
            else:
                # An unavailable percentage was reviewed only for the concrete
                # delete count in that plan. Do not let the force cover more.
                larger = deletes > reviewed_deletes
            if larger:
                unreviewed.append(trip)
        if unreviewed:
            raise GuardTripped(unreviewed, replan_required=True)
    return trips
