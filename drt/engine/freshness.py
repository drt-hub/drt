"""Sync-level freshness from run history (#1224, ``alerts.on_stale``).

Answers one question per sync: *when did this sync last succeed, and is that
older than ``alerts.on_stale.max_age``?* It reads the run history (local,
object-store or warehouse-backed), so it works for every destination. Nothing
here runs on a timer; whoever calls it (cron, CI, ``drt serve``) decides when
(ADR 0004: drt stays daemon-free).

Design rules, each closing a way a watchdog could go quietly green:

* Only a ``success`` entry resets the clock, never a ``dry_run`` one. ``partial``
  and ``failed`` runs do not, because the destination did not receive the full data.
* The newest success is chosen by **completion** time, not by history order (history
  is ordered by start time, and runs can overlap).
* A sync that has run but has no success in the inspected window is **stale**, not
  ``never_run``: ``never_run`` means there is no run on record at all.
* A last success in the future (clock skew) is an error, never "fresh".
* When history holds no success at all (e.g. history is disabled) the sync's stored
  state is used; that records the run's *start* time, so ``max_age`` should exceed
  the run's duration in that setup.

Known limit: a run interrupted gracefully (SIGTERM) after delivering part of the
data is recorded with status ``success`` and cannot be told apart here.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from drt.config.duration import parse_max_age

FreshnessState = Literal["fresh", "stale", "never_run", "clock_skew"]
Severity = Literal["ok", "warn", "error"]

# How many recent runs to inspect for the last success. Retention prunes history
# anyway; a sync whose recent runs hold no success is reported stale, not green.
_HISTORY_WINDOW = 200
# A last success this far in the future is clock skew, not "just now".
_MAX_FUTURE_SKEW = timedelta(minutes=5)


@dataclass(frozen=True)
class FreshnessVerdict:
    sync_name: str
    state: FreshnessState
    severity: Severity
    max_age_seconds: int
    last_success_at: str | None
    age_seconds: int | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "sync": self.sync_name,
            "state": self.state,
            "severity": self.severity,
            "max_age_seconds": self.max_age_seconds,
            "last_success_at": self.last_success_at,
            "age_seconds": self.age_seconds,
        }

    def message(self) -> str:
        limit = _human(self.max_age_seconds)
        if self.state == "never_run":
            return f"has no run on record (max_age {limit})"
        if self.state == "clock_skew":
            return f"last success is dated {self.last_success_at}, in the future (clock skew?)"
        if self.last_success_at is None:
            return f"no successful run among its recent runs (max_age {limit})"
        assert self.age_seconds is not None
        return f"last succeeded {_human(self.age_seconds)} ago (max_age {limit})"


def _human(seconds: int) -> str:
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if seconds >= size:
            return f"{seconds // size}{unit}"
    return f"{seconds}s"


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _last_success(sync_name: str, history: Any, stored: Any) -> tuple[datetime | None, bool]:
    """``(newest success time, whether the sync has any run on record)``."""
    seen_run = False
    best: datetime | None = None
    if history is not None:
        for entry in history.read(sync_name, limit=_HISTORY_WINDOW):
            seen_run = True
            if entry.status != "success" or getattr(entry, "dry_run", False):
                continue
            when = _parse_time(entry.completed_at) or _parse_time(entry.started_at)
            if when is not None and (best is None or when > best):
                best = when
    if best is None and stored is not None:
        seen_run = True
        if stored.status == "success":
            best = _parse_time(stored.last_run_at)
    return best, seen_run


def evaluate_freshness(
    syncs: Iterable[Any],
    history: Any,
    states: dict[str, Any] | None,
    now: datetime | None = None,
) -> list[FreshnessVerdict]:
    """One verdict per sync that configures ``alerts.on_stale`` (others are skipped)."""
    moment = now or datetime.now(timezone.utc)
    verdicts: list[FreshnessVerdict] = []
    for sync in syncs:
        config = getattr(getattr(sync, "alerts", None), "on_stale", None)
        if config is None:
            continue
        limit = int(parse_max_age(config.max_age).total_seconds())
        last, seen_run = _last_success(
            sync.name, history, states.get(sync.name) if states is not None else None
        )
        if last is None:
            if seen_run:  # it ran, but nothing succeeded recently: that is stale
                verdicts.append(FreshnessVerdict(sync.name, "stale", "error", limit, None, None))
            else:
                verdicts.append(
                    FreshnessVerdict(sync.name, "never_run", config.never_run, limit, None, None)
                )
            continue
        if last - moment > _MAX_FUTURE_SKEW:
            verdicts.append(
                FreshnessVerdict(sync.name, "clock_skew", "error", limit, last.isoformat(), None)
            )
            continue
        age = max(int((moment - last).total_seconds()), 0)
        stale = age > limit
        verdicts.append(
            FreshnessVerdict(
                sync.name,
                "stale" if stale else "fresh",
                "error" if stale else "ok",
                limit,
                last.isoformat(),
                age,
            )
        )
    return verdicts


def has_failure(verdicts: Iterable[FreshnessVerdict]) -> bool:
    """True when any verdict is an error (stale, clock skew, or ``never_run: error``)."""
    return any(v.severity == "error" for v in verdicts)
