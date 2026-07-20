"""Tests for degraded-sync alert conditions (#784) — pure eval + config schema."""

from __future__ import annotations

import pytest

from drt.alerts.conditions import evaluate_conditions, row_errors_pct
from drt.config.models import (
    AlertsConfig,
    ConditionThreshold,
    DegradedConditions,
    OnDegradedConfig,
)
from drt.destinations.base import SyncResult


def _result(**kw: object) -> SyncResult:
    base: dict[str, object] = {
        "rows_extracted": 100,
        "success": 100,
        "failed": 0,
        "skipped": 0,
        "duration_seconds": 10.0,
    }
    base.update(kw)
    return SyncResult(**base)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# ConditionThreshold — operators + validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kwargs, actual, expected",
    [
        ({"gt": 1}, 2, True),
        ({"gt": 1}, 1, False),
        ({"lt": 5}, 3, True),
        ({"lt": 5}, 5, False),
        ({"gte": 3}, 3, True),
        ({"gte": 3}, 2, False),
        ({"lte": 3}, 3, True),
        ({"lte": 3}, 4, False),
        ({"eq": 0}, 0, True),
        ({"eq": 0}, 1, False),
    ],
)
def test_threshold_operators(kwargs: dict, actual: float, expected: bool) -> None:
    assert ConditionThreshold(**kwargs).compares(actual) is expected


def test_threshold_exposes_operator_and_value() -> None:
    t = ConditionThreshold(gte=300)
    assert t.operator == "gte"
    assert t.value == 300.0


def test_threshold_requires_exactly_one_operator() -> None:
    with pytest.raises(ValueError, match="exactly one of gt/lt/gte/lte/eq"):
        ConditionThreshold()
    with pytest.raises(ValueError, match="exactly one of gt/lt/gte/lte/eq"):
        ConditionThreshold(gt=1, lt=2)


# ---------------------------------------------------------------------------
# row_errors_pct — failed-only + zero guard (masukai's decisions)
# ---------------------------------------------------------------------------


def test_row_errors_pct_is_failed_over_extracted() -> None:
    assert row_errors_pct(_result(rows_extracted=1000, failed=40, success=960)) == 4.0


def test_row_errors_pct_excludes_skipped() -> None:
    """#757: skipped is a normal outcome (match_policy / --limit / check_only) —
    folding it into an *error* rate would false-fire on healthy syncs."""
    healthy = _result(rows_extracted=1000, success=20, failed=0, skipped=980)
    assert row_errors_pct(healthy) == 0.0


def test_row_errors_pct_zero_when_nothing_extracted() -> None:
    """rows_extracted == 0 -> 0%, never a divide-by-zero or a false 100%."""
    assert row_errors_pct(_result(rows_extracted=0, success=0, failed=0)) == 0.0


# ---------------------------------------------------------------------------
# evaluate_conditions
# ---------------------------------------------------------------------------


def test_each_metric_trips_independently() -> None:
    result = _result(rows_extracted=1000, success=950, failed=50, duration_seconds=400.0)
    conds = DegradedConditions(
        row_errors_pct=ConditionThreshold(gt=1),
        duration_seconds=ConditionThreshold(gt=300),
        dlq_depth=ConditionThreshold(gt=0),
    )
    tripped = evaluate_conditions(result, dlq_depth=12, conditions=conds)
    got = {t.metric: (t.operator, t.threshold, t.actual) for t in tripped}
    assert got == {
        "row_errors_pct": ("gt", 1.0, 5.0),
        "duration_seconds": ("gt", 300.0, 400.0),
        "dlq_depth": ("gt", 0.0, 12.0),
    }


def test_rows_extracted_zero_condition_fires_not_error_pct() -> None:
    """Empty source trips the rows_extracted guard, not the error-rate metric."""
    conds = DegradedConditions(
        rows_extracted=ConditionThreshold(eq=0),
        row_errors_pct=ConditionThreshold(gt=1),
    )
    tripped = evaluate_conditions(_result(rows_extracted=0, success=0), 0, conds)
    assert [t.metric for t in tripped] == ["rows_extracted"]


def test_duration_none_is_skipped() -> None:
    conds = DegradedConditions(duration_seconds=ConditionThreshold(gt=1))
    tripped = evaluate_conditions(_result(duration_seconds=None), 0, conds)
    assert tripped == []


def test_no_conditions_trips_nothing() -> None:
    healthy = _result(rows_extracted=100, success=100, failed=0, duration_seconds=5.0)
    conds = DegradedConditions(
        row_errors_pct=ConditionThreshold(gt=1),
        duration_seconds=ConditionThreshold(gt=300),
        rows_extracted=ConditionThreshold(eq=0),
        dlq_depth=ConditionThreshold(gt=100),
    )
    assert evaluate_conditions(healthy, dlq_depth=0, conditions=conds) == []


def test_trip_order_is_deterministic() -> None:
    """Fixed metric order -> deterministic coalesced message / JSON."""
    result = _result(rows_extracted=100, failed=100, success=0, duration_seconds=999.0)
    conds = DegradedConditions(
        dlq_depth=ConditionThreshold(gt=0),
        rows_extracted=ConditionThreshold(gt=0),
        duration_seconds=ConditionThreshold(gt=1),
        row_errors_pct=ConditionThreshold(gt=1),
    )
    order = [t.metric for t in evaluate_conditions(result, 5, conds)]
    assert order == ["row_errors_pct", "duration_seconds", "rows_extracted", "dlq_depth"]


# ---------------------------------------------------------------------------
# config parse — masukai's mapping syntax
# ---------------------------------------------------------------------------


def test_mapping_syntax_parses() -> None:
    import yaml

    cfg = AlertsConfig.model_validate(
        yaml.safe_load(
            """
            on_failure: []
            on_degraded:
              channels:
                - {type: slack, webhook_url: https://hooks.example/x}
              conditions:
                row_errors_pct: { gt: 1 }
                duration_seconds: { gt: 300 }
                dlq_depth: { gt: 0 }
            """
        )
    )
    assert isinstance(cfg.on_degraded, OnDegradedConfig)
    assert cfg.on_degraded.conditions.row_errors_pct.operator == "gt"  # type: ignore[union-attr]
    assert len(cfg.on_degraded.channels) == 1


def test_on_degraded_optional_backcompat() -> None:
    """No on_degraded -> None; existing on_failure-only configs unchanged."""
    cfg = AlertsConfig.model_validate({"on_failure": []})
    assert cfg.on_degraded is None
