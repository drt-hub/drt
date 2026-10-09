"""Change guards: config validation, evaluator, plan reporting (#1218)."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from drt.config.sync_options import GuardsConfig, SyncOptions
from drt.engine.diff import DiffResult
from drt.engine.guards import evaluate_guards
from drt.engine.plan import build_plan, render_markdown, render_text

_COUNTS: dict[str, Any] = {
    "creates": 0,
    "updates": 0,
    "deletes": 0,
    "source_rows": 100,
    "delete_baseline": 100,
}


def _eval(guards: GuardsConfig | None, **counts: Any) -> list[Any]:
    return evaluate_guards(guards, **{**_COUNTS, **counts})


def test_no_guards_never_trips() -> None:
    assert _eval(None, creates=10**6, deletes=10**6) == []
    assert _eval(GuardsConfig(), creates=10**6, deletes=10**6) == []


def test_absolute_limits_trip_only_above_the_limit() -> None:
    guards = GuardsConfig(max_creates=10, max_deletes=5)

    assert _eval(guards, creates=10, deletes=5) == []
    names = [t.guard for t in _eval(guards, creates=11, deletes=6)]
    assert names == ["max_creates", "max_deletes"]


def test_delete_percentage_uses_the_baseline() -> None:
    guards = GuardsConfig(max_delete_pct=10)

    assert _eval(guards, deletes=10, delete_baseline=100) == []
    trip = _eval(guards, deletes=11, delete_baseline=100)[0]
    assert (trip.guard, trip.observed, trip.limit) == ("max_delete_pct", 11.0, 10)
    assert "11.0% of 100" in trip.message


def test_delete_percentage_that_cannot_be_evaluated_trips_instead_of_passing() -> None:
    guards = GuardsConfig(max_delete_pct=10)

    for baseline in (None, 0):
        trip = _eval(guards, deletes=3, delete_baseline=baseline)[0]
        assert trip.observed is None
        assert "cannot be evaluated" in trip.message
    assert _eval(guards, deletes=0, delete_baseline=None) == []  # nothing to guard


def test_update_percentage_is_of_source_rows() -> None:
    guards = GuardsConfig(max_updates_pct=50)

    assert _eval(guards, updates=50, source_rows=100) == []
    assert _eval(guards, updates=51, source_rows=100)[0].observed == 51.0
    assert "cannot be evaluated" in _eval(guards, updates=1, source_rows=0)[0].message


@pytest.mark.parametrize(
    "bad",
    [
        {"max_creates": -1},
        {"max_delete_pct": 101},
        {"max_updates_pct": -0.1},
        {"max_delete": 5},  # a typo must fail loudly, not silently disable the guard
    ],
)
def test_guards_config_rejects_bad_values_and_unknown_keys(bad: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        GuardsConfig(**bad)


def test_guards_parse_from_sync_options() -> None:
    options = SyncOptions(guards={"max_deletes": 5, "max_delete_pct": 10})  # type: ignore[arg-type]

    assert options.guards is not None and options.guards.max_deletes == 5
    assert SyncOptions().guards is None


_BASE: dict[str, Any] = {
    "sync_name": "s",
    "sync_mode": "mirror",
    "match_policy": "upsert",
    "destination": "postgres",
    "config_fingerprint": "sha256:cfg",
    "environment_fingerprint": "sha256:env",
    "plan_key": b"k",
    "drt_version": "1.1.0",
    "key_columns": ["id"],
    "created_at": "2026-10-09T00:00:00+00:00",
}


def _mirror_diff(deleted: int, baseline: int | None = 100) -> DiffResult:
    return DiffResult(
        deleted=[{"id": i} for i in range(deleted)],
        delete_reason="mirror",
        total_source_rows=100 - deleted,
        delete_baseline=baseline,
    )


def test_plan_reports_tripped_guards_in_json_text_and_markdown() -> None:
    guards = GuardsConfig(max_deletes=5, max_delete_pct=50)
    plan = build_plan(_mirror_diff(20), **_BASE, guards=guards)

    data = plan.to_dict()
    assert data["guards"]["configured"] == {"max_deletes": 5, "max_delete_pct": 50.0}
    assert [t["guard"] for t in data["guards"]["tripped"]] == ["max_deletes"]
    assert "GUARD TRIPPED" in render_text(plan)
    assert "**Guard tripped:**" in render_markdown(plan)


def test_plan_without_guards_or_trips_reports_none() -> None:
    clean = build_plan(_mirror_diff(1), **_BASE, guards=GuardsConfig(max_deletes=5)).to_dict()
    unset = build_plan(_mirror_diff(1), **_BASE).to_dict()

    assert clean["guards"]["tripped"] == []
    assert unset["guards"] == {"configured": None, "tripped": []}
