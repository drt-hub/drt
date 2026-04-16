"""Contract tests — verify all Destination implementations conform to Protocol."""

from __future__ import annotations

import inspect

import pytest

from drt.destinations.base import Destination, SyncResult
from drt.destinations.clickhouse import ClickHouseDestination
from drt.destinations.discord import DiscordDestination
from drt.destinations.file import FileDestination
from drt.destinations.github_actions import GitHubActionsDestination
from drt.destinations.google_sheets import GoogleSheetsDestination
from drt.destinations.hubspot import HubSpotDestination
from drt.destinations.mysql import MySQLDestination
from drt.destinations.notion import NotionDestination
from drt.destinations.parquet import ParquetDestination
from drt.destinations.postgres import PostgresDestination
from drt.destinations.rest_api import RestApiDestination
from drt.destinations.slack import SlackDestination
from drt.destinations.teams import TeamsDestination

ALL_DESTINATIONS = [
    ClickHouseDestination,
    DiscordDestination,
    FileDestination,
    GitHubActionsDestination,
    GoogleSheetsDestination,
    HubSpotDestination,
    MySQLDestination,
    NotionDestination,
    ParquetDestination,
    PostgresDestination,
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
