"""Tests for drt test runner."""

from __future__ import annotations

import pytest

from drt.config.models import NotNullTest, RowCountTest, SyncTest
from drt.engine.test_runner import build_test_query


def test_build_row_count_min() -> None:
    t = SyncTest(row_count=RowCountTest(min=1))
    query, check = build_test_query(t, "public.users")
    assert "COUNT(*)" in query
    assert check(5) is True
    assert check(0) is False


def test_build_row_count_max() -> None:
    t = SyncTest(row_count=RowCountTest(max=100))
    _, check = build_test_query(t, "public.users")
    assert check(50) is True
    assert check(101) is False


def test_build_row_count_min_max() -> None:
    t = SyncTest(row_count=RowCountTest(min=10, max=100))
    _, check = build_test_query(t, "public.users")
    assert check(50) is True
    assert check(5) is False
    assert check(101) is False


def test_build_not_null() -> None:
    t = SyncTest(not_null=NotNullTest(columns=["id", "name"]))
    query, check = build_test_query(t, "public.users")
    assert "id" in query
    assert "name" in query
    assert "NULL" in query.upper()
    assert check(0) is True
    assert check(3) is False


def test_build_unknown_test_raises() -> None:
    with pytest.raises(ValueError, match="Exactly one sync test must be configured"):
        SyncTest()


def test_safe_table_rejects_injection() -> None:
    t = SyncTest(row_count=RowCountTest(min=1))
    with pytest.raises(ValueError, match="Invalid character"):
        build_test_query(t, "users; DROP TABLE")
