"""Contract tests — verify all Source implementations conform to Protocol."""

from __future__ import annotations

import inspect

import pytest

from drt.sources.base import Source
from drt.sources.bigquery import BigQuerySource
from drt.sources.clickhouse import ClickHouseSource
from drt.sources.duckdb import DuckDBSource
from drt.sources.postgres import PostgresSource
from drt.sources.redshift import RedshiftSource
from drt.sources.snowflake import SnowflakeSource
from drt.sources.sqlite import SQLiteSource

ALL_SOURCES = [
    BigQuerySource,
    ClickHouseSource,
    DuckDBSource,
    PostgresSource,
    RedshiftSource,
    SnowflakeSource,
    SQLiteSource,
]


@pytest.mark.parametrize(
    "cls", ALL_SOURCES, ids=lambda c: c.__name__
)
def test_implements_source_protocol(cls: type) -> None:
    assert isinstance(cls(), Source)


@pytest.mark.parametrize(
    "cls", ALL_SOURCES, ids=lambda c: c.__name__
)
def test_extract_method_signature(cls: type) -> None:
    sig = inspect.signature(cls.extract)
    params = list(sig.parameters.keys())
    assert params == ["self", "query", "config"]


@pytest.mark.parametrize(
    "cls", ALL_SOURCES, ids=lambda c: c.__name__
)
def test_test_connection_method_signature(cls: type) -> None:
    sig = inspect.signature(cls.test_connection)
    params = list(sig.parameters.keys())
    assert params == ["self", "config"]
