"""Tests for Redshift source connector."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from drt.config.credentials import RedshiftProfile  # noqa: E402

# ---------------------------------------------------------------------------
# FakeRedshiftSource (test double)
# ---------------------------------------------------------------------------


class FakeRedshiftSource:
    """Fake Redshift source for testing without a real cluster."""

    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self._rows = rows or []
        self.queries_executed: list[str] = []
        self.schema_set: str | None = None

    def extract(self, query: str, config: RedshiftProfile) -> Iterator[dict[str, Any]]:
        """Yield rows and track the query."""
        self.queries_executed.append(query)
        if config.schema:
            self.schema_set = config.schema
        yield from self._rows

    def test_connection(self, config: RedshiftProfile) -> bool:
        """Always succeeds for the fake."""
        return True


# ---------------------------------------------------------------------------
# Profile tests
# ---------------------------------------------------------------------------


def test_redshift_profile_defaults() -> None:
    """RedshiftProfile has sensible defaults."""
    profile = RedshiftProfile(type="redshift")
    assert profile.port == 5439  # Redshift default, not Postgres 5432
    assert profile.schema == "public"
    assert profile.host == ""
    assert profile.dbname == ""


def test_redshift_profile_custom_schema() -> None:
    """RedshiftProfile accepts custom schema."""
    profile = RedshiftProfile(
        type="redshift",
        host="cluster.xxx.redshift.amazonaws.com",
        port=5439,
        dbname="warehouse",
        user="analyst",
        password_env="RS_PASS",
        schema="analytics",
    )
    assert profile.schema == "analytics"
    assert profile.host == "cluster.xxx.redshift.amazonaws.com"


# ---------------------------------------------------------------------------
# FakeRedshiftSource tests
# ---------------------------------------------------------------------------


def test_fake_redshift_source_extract() -> None:
    """FakeRedshiftSource yields configured rows."""
    rows = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ]
    source = FakeRedshiftSource(rows)
    config = RedshiftProfile(
        type="redshift",
        host="test",
        dbname="test",
        user="test",
        schema="public",
    )

    result = list(source.extract("SELECT * FROM users", config))

    assert result == rows
    assert source.queries_executed == ["SELECT * FROM users"]
    assert source.schema_set == "public"


def test_fake_redshift_source_tracks_schema() -> None:
    """FakeRedshiftSource tracks which schema was used."""
    source = FakeRedshiftSource([])
    config = RedshiftProfile(
        type="redshift",
        host="test",
        dbname="test",
        user="test",
        schema="analytics",
    )

    list(source.extract("SELECT 1", config))

    assert source.schema_set == "analytics"


def test_fake_redshift_source_test_connection() -> None:
    """FakeRedshiftSource.test_connection always returns True."""
    source = FakeRedshiftSource()
    config = RedshiftProfile(type="redshift")
    assert source.test_connection(config) is True


def test_fake_redshift_source_empty() -> None:
    """FakeRedshiftSource handles empty result set."""
    source = FakeRedshiftSource([])
    config = RedshiftProfile(type="redshift")

    result = list(source.extract("SELECT * FROM empty", config))

    assert result == []


# ---------------------------------------------------------------------------
# Integration with engine (using FakeRedshiftSource)
# ---------------------------------------------------------------------------


def test_redshift_source_with_engine_pattern(tmp_path: pytest.TempPathFactory) -> None:
    """Verify FakeRedshiftSource follows the Source protocol."""
    from drt.sources.base import Source

    source = FakeRedshiftSource([{"id": 1}])

    # Should match the Source protocol (duck typing)
    assert hasattr(source, "extract")
    assert hasattr(source, "test_connection")
    # Protocol check via isinstance with runtime_checkable
    assert isinstance(source, Source)
