"""Duration-string parsing for sync-level config knobs.

The shared grammar accepts compact durations such as ``"6h"`` and long
durations such as ``"6 hours"``. Individual callers may layer their own
maximum-duration policy on top.
"""

from __future__ import annotations

import re
from datetime import timedelta

_UNIT_TO_KWARG: dict[str, str] = {
    "day": "days",
    "days": "days",
    "hour": "hours",
    "hours": "hours",
    "minute": "minutes",
    "minutes": "minutes",
    "second": "seconds",
    "seconds": "seconds",
    "week": "weeks",
    "weeks": "weeks",
}
_COMPACT_UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days", "w": "weeks"}


def parse_duration(text: str, *, field_name: str = "duration") -> timedelta:
    """Parse a duration string like ``"6h"``, ``"1 hour"``, or ``"7 days"``.

    Raises ``ValueError`` with a config-facing message (prefixed with
    ``field_name``) on any malformed input.
    """
    compact = re.fullmatch(r"\s*(\d+)\s*([smhdw])\s*", text)
    if compact:
        value_str = compact.group(1)
        value = int(value_str)
        if value <= 0:
            raise ValueError(
                f"Invalid {field_name} value: {value_str!r}. Must be a positive integer."
            )
        try:
            return timedelta(**{_COMPACT_UNITS[compact.group(2)]: value})
        except OverflowError:
            raise ValueError(f"Invalid {field_name}: {text[:40]!r} is too large.") from None

    parts = text.strip().split()
    if len(parts) != 2:
        raise ValueError(
            f"Invalid {field_name} format: {text!r}. Use format like '7 days' or '1 hour'."
        )
    value_str, unit = parts
    try:
        value = int(value_str)
    except ValueError:
        raise ValueError(
            f"Invalid {field_name} value: {value_str!r}. Must be an integer."
        ) from None
    if value <= 0:
        raise ValueError(f"Invalid {field_name} value: {value_str!r}. Must be a positive integer.")
    kwarg = _UNIT_TO_KWARG.get(unit.lower())
    if kwarg is None:
        raise ValueError(
            f"Invalid {field_name} unit: {unit!r}. "
            "Use day(s), hour(s), minute(s), second(s), or week(s)."
        )
    try:
        return timedelta(**{kwarg: value})
    except OverflowError:
        raise ValueError(f"Invalid {field_name}: {text[:40]!r} is too large.") from None


_MAX_MAX_AGE = timedelta(days=3650)


def parse_max_age(text: str, *, field_name: str = "max_age") -> timedelta:
    """Parse ``"6h"`` / ``"30m"`` / ``"2d"`` / ``"1w"`` or the long ``"6 hours"`` form."""
    try:
        result = parse_duration(text, field_name=field_name)
    except OverflowError:
        raise ValueError(f"Invalid {field_name}: {text[:40]!r} is too large.") from None
    if result > _MAX_MAX_AGE:
        raise ValueError(f"Invalid {field_name}: {text[:40]!r} is longer than 10 years.")
    return result
