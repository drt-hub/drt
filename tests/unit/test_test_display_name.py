"""Tests for _test_display_name helper."""

from __future__ import annotations

from drt.cli.main import _test_display_name
from drt.config.models import (
    AcceptedValuesTest,
    FreshnessTest,
    NotNullTest,
    RowCountTest,
    SyncTest,
    UniqueTest,
)


def test_display_name_row_count_min_max() -> None:
    t = SyncTest(row_count=RowCountTest(min=1, max=100))
    assert _test_display_name(t) == "row_count(min=1, max=100)"


def test_display_name_not_null() -> None:
    t = SyncTest(not_null=NotNullTest(columns=["id", "name"]))
    assert _test_display_name(t) == "not_null(id, name)"


def test_display_name_freshness() -> None:
    t = SyncTest(freshness=FreshnessTest(column="updated_at", max_age="7 days"))
    assert _test_display_name(t) == "freshness(updated_at, max_age=7 days)"


def test_display_name_unique() -> None:
    t = SyncTest(unique=UniqueTest(columns=["email"]))
    assert _test_display_name(t) == "unique(email)"


def test_display_name_unique_multi() -> None:
    t = SyncTest(unique=UniqueTest(columns=["first", "last"]))
    assert _test_display_name(t) == "unique(first, last)"


def test_display_name_accepted_values() -> None:
    t = SyncTest(
        accepted_values=AcceptedValuesTest(
            column="status", values=["active", "inactive"]
        )
    )
    assert _test_display_name(t) == "accepted_values(status: active, inactive)"