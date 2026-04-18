"""Test runner — builds validation queries for drt test."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from drt.config.models import SyncTest


@dataclass
class TestResult:
    test_name: str
    passed: bool
    message: str


def _safe_table(table: str) -> str:
    """Basic table name validation."""
    for ch in table:
        if not (ch.isalnum() or ch in "._"):
            raise ValueError(f"Invalid character in table name: {ch!r}")
    return table


def _safe_column(column: str) -> str:
    """Basic column name validation."""
    for ch in column:
        if not (ch.isalnum() or ch in "._"):
            raise ValueError(f"Invalid character in column name: {ch!r}")
    return column


def _parse_max_age(max_age_str: str) -> timedelta:
    """Parse max_age string like '7 days', '1 hour', etc."""
    parts = max_age_str.strip().split()
    if len(parts) != 2:
        msg = (
            f"Invalid max_age format: {max_age_str!r}. "
            "Use format like '7 days' or '1 hour'"
        )
        raise ValueError(msg)
    
    value_str, unit = parts
    try:
        value = int(value_str)
    except ValueError:
        raise ValueError(f"Invalid max_age value: {value_str!r}. Must be an integer.")
    
    unit_lower = unit.lower()
    if unit_lower in ("day", "days"):
        return timedelta(days=value)
    elif unit_lower in ("hour", "hours"):
        return timedelta(hours=value)
    elif unit_lower in ("minute", "minutes"):
        return timedelta(minutes=value)
    elif unit_lower in ("second", "seconds"):
        return timedelta(seconds=value)
    elif unit_lower in ("week", "weeks"):
        return timedelta(weeks=value)
    else:
        msg = (
            f"Unknown time unit: {unit!r}. "
            "Supported: days, hours, minutes, seconds, weeks"
        )
        raise ValueError(msg)


def build_test_query(test: SyncTest, table: str) -> tuple[str, Callable[[int], bool]]:
    """Return (SQL query, check_function) for a test.

    The query returns a single integer.
    The check function returns True if the test passes.
    """
    safe_table = _safe_table(table)

    if test.row_count is not None:
        rc = test.row_count
        query = f"SELECT COUNT(*) FROM {safe_table}"

        def check_row_count(val: int) -> bool:
            if rc.min is not None and val < rc.min:
                return False
            if rc.max is not None and val > rc.max:
                return False
            return True

        return query, check_row_count

    if test.not_null is not None:
        nn = test.not_null
        conditions = " OR ".join(f"{col} IS NULL" for col in nn.columns)
        query = f"SELECT COUNT(*) FROM {safe_table} WHERE {conditions}"

        def check_not_null(val: int) -> bool:
            return val == 0

        return query, check_not_null

    if test.freshness is not None:
        fresh = test.freshness
        safe_col = _safe_column(fresh.column)
        max_age_delta = _parse_max_age(fresh.max_age)
        threshold = datetime.now(timezone.utc) - max_age_delta
        
        # Count rows where column is older than max_age (stale data)
        query = f"SELECT COUNT(*) FROM {safe_table} WHERE {safe_col} < '{threshold.isoformat()}'"

        def check_freshness(val: int) -> bool:
            # Test passes if there are no stale rows
            return val == 0

        return query, check_freshness

    if test.unique is not None:
        uniq = test.unique
        cols = ", ".join(_safe_column(col) for col in uniq.columns)
        query = (
            f"SELECT COUNT(*) - COUNT(DISTINCT {cols}) FROM {safe_table}"
        )

        def check_unique(val: int) -> bool:
            # Test passes if duplicate count is 0
            return val == 0

        return query, check_unique

    if test.accepted_values is not None:
        av = test.accepted_values
        safe_col = _safe_column(av.column)
        placeholders = ", ".join(f"'{val}'" for val in av.values)
        query = f"SELECT COUNT(*) FROM {safe_table} WHERE {safe_col} NOT IN ({placeholders})"

        def check_accepted_values(val: int) -> bool:
            # Test passes if there are no invalid values
            return val == 0

        return query, check_accepted_values

    raise ValueError("No test type defined in SyncTest.")

