"""Empty-batch contract for SQL destinations (Step 2b of #364 follow-up).

Completes the empty-batch invariant suite for the four SQL destinations:
``postgres`` / ``mysql`` / ``clickhouse`` / ``snowflake``. Together
with Step 1 (HTTP, PR #593) and Step 2a (file, PR #594) this locks the
empty-batch behaviour for 12 of drt's destinations.

Why no driver mocks
-------------------

The SQL destinations all use lazy driver imports inside their methods
(``import psycopg2`` / ``pymysql`` / ``clickhouse_connect`` /
``snowflake.connector`` lives **inside** ``_connect`` or directly
inside ``load`` after the short-circuit), and their classes hold no
top-level driver references. CI's minimal install (``[dev,mcp,duckdb]``)
includes **none** of ``[postgres,mysql,clickhouse,snowflake]`` extras
— so if a destination ever reaches the driver import on empty input,
the test crashes with ``ModuleNotFoundError`` and surfaces the bug
immediately. No mock infrastructure required.

This means the two contracts here — Protocol satisfaction +
``SyncResult`` shape — carry a third implicit assertion: **the driver
was never imported**. If the test passes on a no-extras install, the
short-circuit holds.

Adding a new SQL destination
----------------------------

Append a ``pytest.param(...)`` entry to ``SQL_DESTINATIONS``. Configs
need only pass Pydantic validation (the ``host`` / credential values
are never dereferenced on empty input), so dummy values are fine.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from drt.config.models import (
    ClickHouseDestinationConfig,
    MySQLDestinationConfig,
    PostgresDestinationConfig,
    RateLimitConfig,
    SnowflakeDestinationConfig,
    SyncOptions,
)
from drt.destinations.base import Destination, SyncResult
from drt.destinations.clickhouse import ClickHouseDestination
from drt.destinations.mysql import MySQLDestination
from drt.destinations.postgres import PostgresDestination
from drt.destinations.snowflake import SnowflakeDestination

SQL_DESTINATIONS: list[Any] = [
    pytest.param(
        PostgresDestination,
        lambda: PostgresDestinationConfig(
            type="postgres",
            host="localhost",
            dbname="testdb",
            table="public.test",
            upsert_key=["id"],
        ),
        id="postgres",
    ),
    pytest.param(
        MySQLDestination,
        lambda: MySQLDestinationConfig(
            type="mysql",
            host="localhost",
            dbname="testdb",
            table="test",
            upsert_key=["id"],
        ),
        id="mysql",
    ),
    pytest.param(
        ClickHouseDestination,
        lambda: ClickHouseDestinationConfig(
            type="clickhouse",
            host="localhost",
            database="testdb",
            table="test",
        ),
        id="clickhouse",
    ),
    pytest.param(
        SnowflakeDestination,
        lambda: SnowflakeDestinationConfig(
            type="snowflake",
            account_env="SF_ACCOUNT",
            user_env="SF_USER",
            password_env="SF_PASSWORD",
            database="testdb",
            # ``schema`` is the YAML key (alias); Python attribute is
            # ``schema_`` to avoid shadowing BaseModel.schema().
            **{"schema": "public"},
            table="test",
            warehouse="wh",
        ),
        id="snowflake",
    ),
]


@pytest.fixture
def empty_sync_options() -> SyncOptions:
    """Minimal SyncOptions — defaults sufficient for the empty-batch path."""
    return SyncOptions(
        mode="full",
        batch_size=100,
        on_error="skip",
        rate_limit=RateLimitConfig(requests_per_second=0),
    )


@pytest.mark.parametrize("destination_class, config_factory", SQL_DESTINATIONS)
def test_satisfies_destination_protocol(
    destination_class: type,
    config_factory: Callable[[], Any],
) -> None:
    """Every SQL destination satisfies the ``Destination`` Protocol."""
    dest = destination_class()
    assert isinstance(dest, Destination)


@pytest.mark.parametrize("destination_class, config_factory", SQL_DESTINATIONS)
def test_empty_batch_returns_empty_sync_result_without_driver_import(
    destination_class: type,
    config_factory: Callable[[], Any],
    empty_sync_options: SyncOptions,
) -> None:
    """``load([])`` returns empty ``SyncResult`` and never imports the driver.

    CI's minimal install excludes the SQL extras (``[postgres]``,
    ``[mysql]``, ``[clickhouse]``, ``[snowflake]``). If a destination
    reaches its driver import on empty input, this test crashes with
    ``ModuleNotFoundError`` — the failure mode is the diagnostic.
    """
    dest = destination_class()
    config = config_factory()

    result = dest.load([], config, empty_sync_options)

    assert isinstance(result, SyncResult)
    assert result.success == 0
    assert result.failed == 0
    assert result.skipped == 0
