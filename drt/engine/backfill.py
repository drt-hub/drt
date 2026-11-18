"""Pure planning and row validation for windowed backfills (ADR 0021).

This module deliberately performs no I/O and imports no engine orchestration
code. It is usable by config validation now and by the sequential engine loop
in a later ADR slice.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any, TypeAlias, cast, overload

from drt.config.duration import parse_duration
from drt.destinations.row_errors import RowError, record_preview

BackfillBound: TypeAlias = str | int
BackfillWindow: TypeAlias = tuple[BackfillBound, BackfillBound]

MAX_BACKFILL_WINDOWS = 1_000_000
MIN_INT64 = -(2**63)
MAX_INT64 = 2**63 - 1


class WindowCursorRangeError(ValueError):
    """A source returned a non-null cursor outside its requested window."""


def stringify_cursor_value(value: Any) -> str:
    """Return the existing #475 stable cursor representation.

    Aware datetimes are normalized to UTC and made naive before rendering;
    other values retain their normal string representation.
    """
    if isinstance(value, datetime) and value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return str(value)


def _parse_timestamp(value: BackfillBound, *, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an ISO-8601 timestamp.")
    text = value.strip()
    fractional = re.search(r"[.,](\d+)", text)
    if fractional is not None and len(fractional.group(1)) > 6:
        raise ValueError(
            f"{field_name} uses sub-microsecond precision; "
            "sub-microsecond bounds are not supported."
        )
    iso = text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
    try:
        parsed = datetime.fromisoformat(iso)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (OverflowError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an ISO-8601 timestamp; got {value!r}.") from exc


def _parse_integer(value: BackfillBound, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer.")
    if not MIN_INT64 <= value <= MAX_INT64:
        raise ValueError(f"{field_name} must be within the signed 64-bit integer range.")
    return value


@overload
def canonicalize_bound(value: int, *, field_name: str = "sync.backfill bound") -> int: ...


@overload
def canonicalize_bound(value: str, *, field_name: str = "sync.backfill bound") -> str: ...


def canonicalize_bound(
    value: BackfillBound, *, field_name: str = "sync.backfill bound"
) -> BackfillBound:
    """Canonicalize an integer exactly or an ISO timestamp to naive UTC text."""
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer or ISO-8601 timestamp, not boolean.")
    if isinstance(value, int):
        return _parse_integer(value, field_name=field_name)
    return stringify_cursor_value(_parse_timestamp(value, field_name=field_name))


@overload
def _window_count(delta: int, step: int) -> int: ...


@overload
def _window_count(delta: timedelta, step: timedelta) -> int: ...


def _window_count(delta: int | timedelta, step: int | timedelta) -> int:
    if isinstance(delta, int) and isinstance(step, int):
        integer_quotient, integer_remainder = divmod(delta, step)
        return integer_quotient + int(bool(integer_remainder))
    if isinstance(delta, timedelta) and isinstance(step, timedelta):
        duration_quotient, duration_remainder = divmod(delta, step)
        return duration_quotient + int(bool(duration_remainder))
    raise TypeError("backfill delta and window must use the same arithmetic type")


def _check_window_count(count: int) -> None:
    if count > MAX_BACKFILL_WINDOWS:
        raise ValueError(
            "sync.backfill would plan "
            f"{count:,} windows, above the safety limit of {MAX_BACKFILL_WINDOWS:,}; "
            "increase sync.backfill.window or narrow the range."
        )


def resume_low(start: BackfillBound, checkpoint: BackfillBound | None) -> BackfillBound:
    """Use the durable checkpoint when present, otherwise the configured start."""
    if checkpoint is None:
        return canonicalize_bound(start, field_name="sync.backfill.start")

    if isinstance(start, bool) or isinstance(checkpoint, bool):
        raise ValueError("sync.backfill resume bounds must not be boolean.")
    if isinstance(start, int):
        if not isinstance(checkpoint, int):
            raise ValueError("sync.backfill checkpoint type must match sync.backfill.start.")
        return checkpoint
    if not isinstance(checkpoint, str):
        raise ValueError("sync.backfill checkpoint type must match sync.backfill.start.")

    _parse_timestamp(start, field_name="sync.backfill.start")
    checkpoint_dt = _parse_timestamp(checkpoint, field_name="sync.backfill checkpoint")
    return stringify_cursor_value(checkpoint_dt)


def apply_lag_then_clamp(
    start: BackfillBound,
    checkpoint: BackfillBound,
    lag: str | int,
) -> BackfillBound:
    """Subtract checkpoint lag, then clamp only that overlap to config start.

    ADR 0021 applies this operation only to durable checkpoints. Config starts
    and CLI overrides are never lagged.
    """
    if isinstance(start, bool) or isinstance(checkpoint, bool) or isinstance(lag, bool):
        raise ValueError("sync.backfill lag bounds must not be boolean.")
    if isinstance(start, int):
        if not isinstance(checkpoint, int):
            raise ValueError("sync.backfill checkpoint type must match sync.backfill.start.")
        if not isinstance(lag, int):
            raise ValueError("numeric sync.backfill checkpoints require an integer watermark.lag.")
        if lag <= 0:
            raise ValueError("watermark.lag must be a positive integer.")
        return max(start, checkpoint - lag)

    if not isinstance(checkpoint, str):
        raise ValueError("sync.backfill checkpoint type must match sync.backfill.start.")
    if not isinstance(lag, str):
        raise ValueError("timestamp sync.backfill checkpoints require a duration watermark.lag.")
    start_dt = _parse_timestamp(start, field_name="sync.backfill.start")
    checkpoint_dt = _parse_timestamp(checkpoint, field_name="sync.backfill checkpoint")
    lag_delta = parse_duration(lag, field_name="watermark.lag")
    return stringify_cursor_value(max(start_dt, checkpoint_dt - lag_delta))


def plan_windows(
    start: BackfillBound,
    end_or_ceiling: BackfillBound,
    window: str | int,
    *,
    checkpoint: BackfillBound | None = None,
) -> Iterator[BackfillWindow]:
    """Yield ascending, non-overlapping ``[low, high)`` windows.

    The final high is clipped to ``end_or_ceiling``. Timestamp arithmetic is
    performed after conversion to UTC, so durations are elapsed time rather
    than local-calendar time. Supplying a durable checkpoint resumes at
    the checkpoint exactly; configured start is used only without one.
    """
    low = resume_low(start, checkpoint)

    if isinstance(window, bool):
        raise ValueError("sync.backfill.window must be a duration or positive integer.")
    if isinstance(window, int):
        if window <= 0:
            raise ValueError("sync.backfill.window must be a positive integer.")
        low_int = _parse_integer(low, field_name="sync.backfill.start")
        ceiling_int = _parse_integer(end_or_ceiling, field_name="sync.backfill ceiling")
        if low_int >= ceiling_int:
            return
        _check_window_count(_window_count(ceiling_int - low_int, window))
        while low_int < ceiling_int:
            high_int = min(low_int + window, ceiling_int)
            yield (low_int, high_int)
            low_int = high_int
        return

    if not isinstance(window, str):
        raise ValueError("sync.backfill.window must be a duration or positive integer.")
    step = parse_duration(window, field_name="sync.backfill.window")
    low_dt = _parse_timestamp(low, field_name="sync.backfill.start")
    ceiling_dt = _parse_timestamp(end_or_ceiling, field_name="sync.backfill ceiling")
    if low_dt >= ceiling_dt:
        return
    _check_window_count(_window_count(ceiling_dt - low_dt, step))
    while low_dt < ceiling_dt:
        remaining = ceiling_dt - low_dt
        high_dt = ceiling_dt if remaining <= step else low_dt + step
        yield (stringify_cursor_value(low_dt), stringify_cursor_value(high_dt))
        low_dt = high_dt


def _canonicalize_row_cursor(value: Any, low: BackfillBound) -> BackfillBound:
    """Canonicalize a source row cursor using the window's arithmetic family."""
    if isinstance(low, int) and not isinstance(low, bool):
        if isinstance(value, bool) or not isinstance(value, int):
            raise WindowCursorRangeError(
                "sync.backfill source returned a non-integral cursor value "
                f"{value!r} for an integer window."
            )
        return cast(int, canonicalize_bound(value, field_name="source row cursor"))
    if isinstance(value, datetime):
        value = stringify_cursor_value(value)
    if not isinstance(value, str):
        raise WindowCursorRangeError(
            "sync.backfill source returned a non-timestamp cursor value "
            f"{value!r} for a timestamp window."
        )
    try:
        return canonicalize_bound(value, field_name="source row cursor")
    except ValueError as exc:
        raise WindowCursorRangeError(str(exc)) from exc


def validate_window_row(
    record: dict[str, Any],
    cursor_field: str,
    low: BackfillBound,
    high: BackfillBound,
    *,
    batch_index: int = 0,
) -> RowError | None:
    """Validate one extracted row before it reaches a destination load.

    A missing or null cursor is a pinpointed row error that the slice-3 loop
    can apply normal ``on_error``/DLQ handling to. Any non-null cursor outside
    ``[low, high)`` indicates a broken source predicate/capability and raises:
    continuing could certify a range the source did not actually honor.
    """
    canonical_low = canonicalize_bound(low, field_name="sync.backfill window low")
    canonical_high = canonicalize_bound(high, field_name="sync.backfill window high")
    if isinstance(canonical_low, int) != isinstance(canonical_high, int):
        raise ValueError("sync.backfill window bounds must use the same timestamp or integer type.")
    if isinstance(canonical_low, int):
        assert isinstance(canonical_high, int)
        if canonical_low >= canonical_high:
            raise ValueError("sync.backfill window low must be less than high.")
    else:
        assert isinstance(canonical_high, str)
        if canonical_low >= canonical_high:
            raise ValueError("sync.backfill window low must be less than high.")

    value: Any = record
    for segment in cursor_field.split("."):
        if not isinstance(value, Mapping) or segment not in value:
            value = None
            break
        value = value[segment]
    if value is None:
        return RowError(
            batch_index=batch_index,
            record_preview=record_preview(record),
            http_status=None,
            error_message=(
                f"sync.backfill row has null or missing cursor_field {cursor_field!r}; "
                "the row cannot be assigned to a window."
            ),
        )

    canonical_value = _canonicalize_row_cursor(value, canonical_low)
    if isinstance(canonical_low, int):
        assert isinstance(canonical_high, int)
        assert isinstance(canonical_value, int)
        in_range = canonical_low <= canonical_value < canonical_high
    else:
        assert isinstance(canonical_high, str)
        assert isinstance(canonical_value, str)
        in_range = canonical_low <= canonical_value < canonical_high
    if not in_range:
        raise WindowCursorRangeError(
            "sync.backfill source returned cursor "
            f"{canonical_value!r} outside requested half-open window "
            f"[{canonical_low!r}, {canonical_high!r}); refusing to load the row."
        )
    return None
