"""Contract tests — verify all Destination implementations conform to Protocol."""

from __future__ import annotations

import inspect

import pytest

from drt.destinations.airtable import AirtableDestination
from drt.destinations.base import ConnectionTestable, Destination, SyncResult
from drt.destinations.clickhouse import ClickHouseDestination
from drt.destinations.discord import DiscordDestination
from drt.destinations.file import FileDestination
from drt.destinations.github_actions import GitHubActionsDestination
from drt.destinations.google_sheets import GoogleSheetsDestination
from drt.destinations.hubspot import HubSpotDestination
from drt.destinations.klaviyo import KlaviyoDestination
from drt.destinations.mysql import MySQLDestination
from drt.destinations.notion import NotionDestination
from drt.destinations.parquet import ParquetDestination
from drt.destinations.postgres import PostgresDestination
from drt.destinations.rest_api import RestApiDestination
from drt.destinations.slack import SlackDestination
from drt.destinations.snowflake import SnowflakeDestination
from drt.destinations.teams import TeamsDestination
from drt.destinations.zendesk import ZendeskDestination

ALL_DESTINATIONS = [
    AirtableDestination,
    ClickHouseDestination,
    DiscordDestination,
    FileDestination,
    GitHubActionsDestination,
    GoogleSheetsDestination,
    HubSpotDestination,
    KlaviyoDestination,
    MySQLDestination,
    NotionDestination,
    ParquetDestination,
    PostgresDestination,
    RestApiDestination,
    SlackDestination,
    TeamsDestination,
    ZendeskDestination,
]

CONNECTION_TESTABLE_DESTINATIONS = [
    AirtableDestination,  # first non-SQL ConnectionTestable
    ClickHouseDestination,
    KlaviyoDestination,  # non-SQL ConnectionTestable
    MySQLDestination,
    PostgresDestination,
    SnowflakeDestination,
]

NON_CONNECTION_TESTABLE_DESTINATIONS = [
    DiscordDestination,
    FileDestination,
    GitHubActionsDestination,
    GoogleSheetsDestination,
    HubSpotDestination,
    NotionDestination,
    ParquetDestination,
    RestApiDestination,
    SlackDestination,
    TeamsDestination,
]


@pytest.mark.parametrize("cls", ALL_DESTINATIONS, ids=lambda c: c.__name__)
def test_implements_destination_protocol(cls: type) -> None:
    assert isinstance(cls(), Destination)


@pytest.mark.parametrize("cls", ALL_DESTINATIONS, ids=lambda c: c.__name__)
def test_load_method_signature(cls: type) -> None:
    sig = inspect.signature(cls.load)
    params = list(sig.parameters.keys())
    assert params == ["self", "records", "config", "sync_options"]


@pytest.mark.parametrize("cls", ALL_DESTINATIONS, ids=lambda c: c.__name__)
def test_load_return_annotation(cls: type) -> None:
    sig = inspect.signature(cls.load)
    ann = sig.return_annotation
    assert ann is SyncResult or ann == "SyncResult"


@pytest.mark.parametrize("cls", CONNECTION_TESTABLE_DESTINATIONS, ids=lambda c: c.__name__)
def test_connection_testable_destinations_implement_protocol(cls: type) -> None:
    assert isinstance(cls(), ConnectionTestable)


@pytest.mark.parametrize(
    "cls",
    NON_CONNECTION_TESTABLE_DESTINATIONS,
    ids=lambda c: c.__name__,
)
def test_non_sql_destinations_do_not_implement_connection_testable(cls: type) -> None:
    assert not isinstance(cls(), ConnectionTestable)
