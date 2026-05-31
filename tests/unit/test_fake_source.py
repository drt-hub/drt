"""Tests for :class:`drt.sources.fake.FakeSource` (#364)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from drt.sources.base import Source
from drt.sources.fake import FakeSource


@pytest.fixture
def profile() -> Any:
    """Sources never inspect the profile object — a sentinel is enough."""
    return MagicMock(name="profile")


def test_satisfies_source_protocol() -> None:
    """``FakeSource`` is structurally a ``Source``."""
    assert isinstance(FakeSource(), Source)


def test_extract_yields_configured_rows_in_order(profile: Any) -> None:
    rows = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    source = FakeSource(rows=rows)
    yielded = list(source.extract("SELECT * FROM users", profile))
    assert yielded == rows


def test_extract_yields_nothing_when_rows_empty(profile: Any) -> None:
    """Default empty list still produces a valid iterator."""
    source = FakeSource()
    assert list(source.extract("SELECT * FROM empty", profile)) == []


def test_extract_records_each_query(profile: Any) -> None:
    """``queries_executed`` lets tests assert what the engine actually issued."""
    source = FakeSource(rows=[{"id": 1}])
    list(source.extract("SELECT * FROM t WHERE id > 0", profile))
    list(source.extract("SELECT * FROM t WHERE id > 1", profile))
    assert source.queries_executed == [
        "SELECT * FROM t WHERE id > 0",
        "SELECT * FROM t WHERE id > 1",
    ]


def test_extract_records_query_even_when_no_rows(profile: Any) -> None:
    """A no-row source still records the queries the engine issued."""
    source = FakeSource()
    list(source.extract("SELECT 1", profile))
    assert source.queries_executed == ["SELECT 1"]


def test_extract_is_a_generator(profile: Any) -> None:
    """``extract`` returns an iterator, not a pre-materialised list."""
    source = FakeSource(rows=[{"id": 1}, {"id": 2}])
    result = source.extract("SELECT *", profile)
    # consume one row, leave one pending — exercises lazy iteration
    assert next(result) == {"id": 1}
    assert next(result) == {"id": 2}
    with pytest.raises(StopIteration):
        next(result)


def test_test_connection_returns_true_by_default(profile: Any) -> None:
    assert FakeSource().test_connection(profile) is True


def test_test_connection_returns_false_when_flag_set(profile: Any) -> None:
    assert FakeSource(connection_ok=False).test_connection(profile) is False
