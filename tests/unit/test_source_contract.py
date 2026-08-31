"""Contract tests — verify all Source implementations conform to Protocol."""

from __future__ import annotations

import inspect

import pytest

from drt.sources.base import Source
from drt.sources.bigquery import BigQuerySource
from drt.sources.clickhouse import ClickHouseSource
from drt.sources.databricks import DatabricksSource
from drt.sources.deltalake import DeltaLakeSource
from drt.sources.duckdb import DuckDBSource
from drt.sources.iceberg import IcebergSource
from drt.sources.mysql import MySQLSource
from drt.sources.postgres import PostgresSource
from drt.sources.redshift import RedshiftSource
from drt.sources.rest_api import RestApiSource
from drt.sources.snowflake import SnowflakeSource
from drt.sources.sqlite import SQLiteSource
from drt.sources.sqlserver import SQLServerSource

ALL_SOURCES = [
    BigQuerySource,
    ClickHouseSource,
    DatabricksSource,
    DuckDBSource,
    MySQLSource,
    PostgresSource,
    RedshiftSource,
    RestApiSource,
    SnowflakeSource,
    SQLiteSource,
    SQLServerSource,
    DeltaLakeSource,
    IcebergSource,
]


@pytest.mark.parametrize("cls", ALL_SOURCES, ids=lambda c: c.__name__)
def test_implements_source_protocol(cls: type) -> None:
    assert isinstance(cls(), Source)


@pytest.mark.parametrize("cls", ALL_SOURCES, ids=lambda c: c.__name__)
def test_extract_method_signature(cls: type) -> None:
    """``query_tags`` (#768) is keyword-only with a ``None`` default on every
    source — additive, so a caller passing only ``(query, config)`` keeps
    working unchanged."""
    sig = inspect.signature(cls.extract)
    params = list(sig.parameters.keys())
    assert params == ["self", "query", "config", "query_tags"]
    query_tags_param = sig.parameters["query_tags"]
    assert query_tags_param.kind is inspect.Parameter.KEYWORD_ONLY
    assert query_tags_param.default is None


@pytest.mark.parametrize("cls", ALL_SOURCES, ids=lambda c: c.__name__)
def test_test_connection_method_signature(cls: type) -> None:
    sig = inspect.signature(cls.test_connection)
    params = list(sig.parameters.keys())
    assert params == ["self", "config"]
