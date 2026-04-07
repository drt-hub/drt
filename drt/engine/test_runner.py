"""Test runner — builds validation queries for drt test."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

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


def build_test_query(
    test: SyncTest, table: str
) -> tuple[str, Callable[[int], bool]]:
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

    raise ValueError("No test type defined in SyncTest.")
