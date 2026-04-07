"""Tests for destination query helpers."""

from __future__ import annotations

from drt.config.models import (
    PostgresDestinationConfig,
    RestApiDestinationConfig,
)
from drt.destinations.query import get_table_name, is_queryable


def test_postgres_is_queryable() -> None:
    config = PostgresDestinationConfig(
        type="postgres",
        host="localhost",
        dbname="test",
        table="public.users",
        upsert_key=["id"],
    )
    assert is_queryable(config) is True


def test_rest_api_is_not_queryable() -> None:
    config = RestApiDestinationConfig(
        type="rest_api",
        url="http://example.com",
        method="POST",
    )
    assert is_queryable(config) is False


def test_get_table_name_postgres() -> None:
    config = PostgresDestinationConfig(
        type="postgres",
        host="localhost",
        dbname="test",
        table="public.users",
        upsert_key=["id"],
    )
    assert get_table_name(config) == "public.users"
