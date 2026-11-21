"""Pure range-arithmetic tests for ADR 0021 windowed backfills."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from drt.engine.backfill import (
    _window_count,
    apply_lag_then_clamp,
    canonicalize_bound,
    plan_windows,
    resume_low,
)


def test_timestamp_bounds_canonicalize_to_naive_utc() -> None:
    assert canonicalize_bound("2025-01-01T09:30:00+09:00") == "2025-01-01 00:30:00"
    assert canonicalize_bound("2025-01-01T00:30:00") == "2025-01-01 00:30:00"


def test_integer_bounds_remain_exact() -> None:
    value = 9223372036854775807
    canonical = canonicalize_bound(value)

    assert canonical == value
    assert isinstance(canonical, int)


@pytest.mark.parametrize("value", [-(2**63) - 1, 2**63])
def test_integer_bounds_reject_values_outside_signed_64_bit(value: int) -> None:
    with pytest.raises(ValueError, match="signed 64-bit integer range"):
        canonicalize_bound(value)


@pytest.mark.parametrize(
    "value",
    ["2025-01-01T00:00:00.1234567Z", "2025-01-01T00:00:00,123456789+00:00"],
)
def test_timestamp_bounds_reject_sub_microsecond_precision(value: str) -> None:
    with pytest.raises(ValueError, match="sub-microsecond bounds are not supported"):
        canonicalize_bound(value)


def test_final_timestamp_window_is_clipped_to_ceiling() -> None:
    assert list(
        plan_windows(
            "2025-01-01T00:00:00Z",
            "2025-01-03T06:00:00Z",
            "1d",
        )
    ) == [
        ("2025-01-01 00:00:00", "2025-01-02 00:00:00"),
        ("2025-01-02 00:00:00", "2025-01-03 00:00:00"),
        ("2025-01-03 00:00:00", "2025-01-03 06:00:00"),
    ]


def test_dst_inputs_still_use_24_hour_elapsed_windows() -> None:
    windows = list(
        plan_windows(
            "2025-03-08T00:00:00-05:00",
            "2025-03-10T00:00:00-04:00",
            "1d",
        )
    )

    assert windows == [
        ("2025-03-08 05:00:00", "2025-03-09 05:00:00"),
        ("2025-03-09 05:00:00", "2025-03-10 04:00:00"),
    ]


@pytest.mark.parametrize(
    ("start", "ceiling", "window"),
    [
        (10, 10, 3),
        (11, 10, 3),
        ("2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z", "1h"),
        ("2025-01-02T00:00:00Z", "2025-01-01T00:00:00Z", "1h"),
    ],
)
def test_empty_plan_when_start_is_at_or_after_ceiling(
    start: str | int, ceiling: str | int, window: str | int
) -> None:
    assert list(plan_windows(start, ceiling, window)) == []


def test_integer_windows_are_exact_and_final_window_is_partial() -> None:
    assert list(plan_windows(0, 25, 10)) == [(0, 10), (10, 20), (20, 25)]


def test_huge_window_count_fails_before_iteration() -> None:
    with pytest.raises(ValueError, match="above the safety limit of 1,000,000"):
        list(plan_windows(0, 1_000_001, 1))


@pytest.mark.parametrize(
    ("checkpoint", "expected_low", "expected_windows"),
    [
        (15, 15, [(15, 25), (25, 30)]),
        (30, 30, []),
        (35, 35, []),
    ],
)
def test_resume_from_integer_checkpoint_inside_at_or_after_range(
    checkpoint: int, expected_low: int, expected_windows: list[tuple[int, int]]
) -> None:
    assert resume_low(0, checkpoint) == expected_low
    assert list(plan_windows(0, 30, 10, checkpoint=checkpoint)) == expected_windows


def test_durable_checkpoint_wins_even_before_configured_start() -> None:
    assert resume_low(10, 5) == 5
    assert list(plan_windows(10, 30, 10, checkpoint=5)) == [(5, 15), (15, 25), (25, 30)]


def test_timestamp_resume_checkpoint_is_canonicalized() -> None:
    assert resume_low("2025-01-01T00:00:00Z", "2025-01-02T09:00:00+09:00") == "2025-01-02 00:00:00"


@pytest.mark.parametrize(
    ("start", "checkpoint", "lag", "expected"),
    [
        (10, 30, 5, 25),
        (10, 15, 20, 10),
        (
            "2025-01-01T00:00:00Z",
            "2025-01-03T00:00:00Z",
            "12 hours",
            "2025-01-02 12:00:00",
        ),
        (
            "2025-01-01T00:00:00Z",
            "2025-01-01T06:00:00Z",
            "12 hours",
            "2025-01-01 00:00:00",
        ),
    ],
)
def test_apply_lag_then_clamp_to_configured_start(
    start: str | int,
    checkpoint: str | int,
    lag: str | int,
    expected: str | int,
) -> None:
    assert apply_lag_then_clamp(start, checkpoint, lag) == expected


@pytest.mark.parametrize(
    "value",
    ["0001-01-01T00:00:00+14:00", "9999-12-31T23:59:59-14:00"],
)
def test_timestamp_utc_normalization_overflow_is_a_config_value_error(value: str) -> None:
    with pytest.raises(ValueError, match="must be an ISO-8601 timestamp"):
        canonicalize_bound(value, field_name="sync.backfill.start")


def test_backfill_pure_helper_error_branches_are_explicit() -> None:
    invalid: Any

    invalid = True
    with pytest.raises(ValueError, match="not boolean"):
        canonicalize_bound(invalid)

    invalid = 1.5
    with pytest.raises(ValueError, match="ISO-8601"):
        canonicalize_bound(invalid)

    with pytest.raises(TypeError, match="same arithmetic type"):
        _window_count(1, timedelta(hours=1))

    with pytest.raises(ValueError, match="must not be boolean"):
        resume_low(True, 1)
    with pytest.raises(ValueError, match="checkpoint type"):
        resume_low(0, "1")
    with pytest.raises(ValueError, match="checkpoint type"):
        resume_low("2025-01-01", 1)

    with pytest.raises(ValueError, match="must not be boolean"):
        apply_lag_then_clamp(0, 10, True)
    with pytest.raises(ValueError, match="checkpoint type"):
        apply_lag_then_clamp(0, "10", 1)
    with pytest.raises(ValueError, match="require an integer"):
        apply_lag_then_clamp(0, 10, "1h")
    with pytest.raises(ValueError, match="positive integer"):
        apply_lag_then_clamp(0, 10, 0)
    with pytest.raises(ValueError, match="checkpoint type"):
        apply_lag_then_clamp("2025-01-01", 10, "1h")
    with pytest.raises(ValueError, match="require a duration"):
        apply_lag_then_clamp("2025-01-01", "2025-01-02", 1)


@pytest.mark.parametrize("window", [True, 0, object()])
def test_plan_windows_rejects_invalid_runtime_window_types(window: object) -> None:
    with pytest.raises(ValueError, match="sync.backfill.window"):
        list(plan_windows(0, 10, window))


def test_numeric_plan_rejects_timestamp_start_at_runtime() -> None:
    with pytest.raises(ValueError, match="sync.backfill.start must be an integer"):
        list(plan_windows("2025-01-01", 10, 1))
