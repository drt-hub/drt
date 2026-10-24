"""Sync-level freshness from run history (#1224, ``alerts.on_stale``).

Answers one question per sync: *when did this sync last succeed, and is that
older than ``alerts.on_stale.max_age``?* It reads the run history (local,
object-store or warehouse-backed), so it works for every destination. Nothing
here runs on a timer; whoever calls it (cron, CI, ``drt serve``) decides when
(ADR 0004: drt stays daemon-free).

A run counts as fresh only when its status is ``success``. ``partial`` and
``failed`` runs do not reset the clock, because the destination did not receive
the full data. When history has no successful entry (e.g. history is disabled)
the sync's stored state is used as a fallback.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

from drt.config.duration import parse_max_age

FreshnessState = Literal["fresh", "stale", "never_run"]
Severity = Literal["ok", "warn", "error"]

# How far back to look for the last success. Retention prunes history anyway; a
# sync with no success in this window is judged on its stored state instead.
_HISTORY_WINDOW = 200


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
            return f"has no successful run on record (max_age {limit})"
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


def _last_success(sync_name: str, history: Any, state: Any) -> datetime | None:
    if history is not None:
        for entry in history.read(sync_name, limit=_HISTORY_WINDOW):
            if entry.status == "success":
                when = _parse_time(entry.completed_at) or _parse_time(entry.started_at)
                if when is not None:
                    return when
    stored = state.get(sync_name) if state is not None else None
    if stored is not None and stored.status == "success":
        return _parse_time(stored.last_run_at)
    return None


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
        last = _last_success(sync.name, history, states)
        if last is None:
            verdicts.append(
                FreshnessVerdict(sync.name, "never_run", config.never_run, limit, None, None)
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
    """True when any verdict is an error (a stale sync, or never-run with ``never_run: error``)."""
    return any(v.severity == "error" for v in verdicts)
