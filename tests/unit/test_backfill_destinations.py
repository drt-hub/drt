"""Fail-closed destination allowlist for ADR 0021 windowed backfill."""

from __future__ import annotations

from typing import Any

import pytest

from drt.config.models import (
    BigQueryDestinationConfig,
    DatabricksDestinationConfig,
    FileDestinationConfig,
    MySQLDestinationConfig,
    PostgresDestinationConfig,
    SalesforceBulkDestinationConfig,
    SnowflakeDestinationConfig,
)
from drt.connectors.registry import (
    get_destination,
    registered_destination_types,
    windowed_backfill_destination_types,
)
from drt.destinations.base import (
    ensure_windowed_backfill_destination,
    windowed_backfill_unsupported_reason,
)

SUPPORTED_TYPES = {"bigquery", "databricks", "mysql", "postgres", "snowflake"}


@pytest.mark.parametrize("destination_type", registered_destination_types())
def test_every_registered_destination_defaults_to_unsupported_unless_declared(
    destination_type: str,
) -> None:
    """A newly registered built-in or plugin cannot silently join the allowlist."""
    assert (destination_type in windowed_backfill_destination_types()) == (
        destination_type in SUPPORTED_TYPES
    )


@pytest.mark.parametrize(
    "config",
    [
        PostgresDestinationConfig(
            type="postgres", connection_string_env="PG_URL", table="events", upsert_key=["id"]
        ),
        MySQLDestinationConfig(
            type="mysql", connection_string_env="MYSQL_URL", table="events", upsert_key=["id"]
        ),
        SnowflakeDestinationConfig(
            type="snowflake",
            account_env="SF_ACCOUNT",
            user_env="SF_USER",
            password_env="SF_PASSWORD",
            database="DB",
            schema="PUBLIC",
            table="EVENTS",
            warehouse="WH",
            mode="merge",
            upsert_key=["id"],
        ),
        DatabricksDestinationConfig(
            type="databricks",
            host_env="DBX_HOST",
            http_path_env="DBX_PATH",
            token_env="DBX_TOKEN",
            catalog="main",
            schema="default",
            table="events",
            mode="merge",
            upsert_key=["id"],
        ),
        BigQueryDestinationConfig(
            type="bigquery",
            project="project",
            dataset="dataset",
            table="events",
            mode="merge",
            upsert_key=["id"],
        ),
    ],
    ids=lambda config: config.type,
)
def test_proven_keyed_destinations_are_supported(config: Any) -> None:
    ensure_windowed_backfill_destination(get_destination(config), config)


@pytest.mark.parametrize(
    "config",
    [
        BigQueryDestinationConfig(
            type="bigquery", project="p", dataset="d", table="t", mode="insert"
        ),
        SnowflakeDestinationConfig(
            type="snowflake",
            account_env="A",
            user_env="U",
            password_env="P",
            database="D",
            schema="S",
            table="T",
            warehouse="W",
            mode="insert",
        ),
        DatabricksDestinationConfig(
            type="databricks",
            host_env="H",
            http_path_env="P",
            token_env="T",
            catalog="C",
            schema="S",
            table="T",
            mode="insert",
        ),
    ],
    ids=lambda config: config.type,
)
def test_append_only_warehouse_modes_are_excluded(config: Any) -> None:
    with pytest.raises(ValueError, match="append-only"):
        ensure_windowed_backfill_destination(get_destination(config), config)


@pytest.mark.parametrize(
    "config",
    [
        PostgresDestinationConfig(
            type="postgres", connection_string_env="PG_URL", table="events", upsert_key=[]
        ),
        MySQLDestinationConfig(
            type="mysql", connection_string_env="MYSQL_URL", table="events", upsert_key=[]
        ),
        SnowflakeDestinationConfig(
            type="snowflake",
            account_env="A",
            user_env="U",
            password_env="P",
            database="D",
            schema="S",
            table="T",
            warehouse="W",
            mode="merge",
        ),
        DatabricksDestinationConfig(
            type="databricks",
            host_env="H",
            http_path_env="P",
            token_env="T",
            catalog="C",
            schema="S",
            table="T",
            mode="merge",
        ),
        BigQueryDestinationConfig(
            type="bigquery", project="p", dataset="d", table="t", mode="merge"
        ),
    ],
    ids=lambda config: config.type,
)
def test_supported_destination_families_require_a_nonempty_key(config: Any) -> None:
    with pytest.raises(ValueError, match="upsert_key"):
        ensure_windowed_backfill_destination(get_destination(config), config)


@pytest.mark.parametrize(
    "config, expected_reason",
    [
        (
            SalesforceBulkDestinationConfig(
                type="salesforce_bulk",
                instance_url="https://example.my.salesforce.com",
                object_name="Contact",
                client_id_env="CLIENT_ID",
                client_secret_env="CLIENT_SECRET",
                username_env="USERNAME",
                password_env="PASSWORD",
            ),
            "does not declare",
        ),
        (FileDestinationConfig(type="file", path="out.csv"), "does not declare"),
    ],
    ids=["salesforce-untrustworthy-batch-index", "overwrite-on-first-write-file"],
)
def test_review_regression_destinations_are_excluded(config: Any, expected_reason: str) -> None:
    with pytest.raises(ValueError, match=expected_reason):
        ensure_windowed_backfill_destination(get_destination(config), config)


def test_plugin_without_capability_defaults_to_unsupported() -> None:
    reason = windowed_backfill_unsupported_reason(
        object(), FileDestinationConfig(type="file", path="x")
    )
    assert reason is not None and "plugins default to unsupported" in reason
