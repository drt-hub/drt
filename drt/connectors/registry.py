"""Connector registry for automatic discovery and instantiation of sources and destinations.

This module provides a centralized registry for all available source and destination
connectors, eliminating the need for hardcoded if-else chains in the CLI layer.

The registry enables:
- Decoupled CLI from connector implementations
- Self-contained connector registration (no main.py edits needed)
- Helpful error messages listing available connectors
- Future third-party plugin support
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from drt.destinations.base import Destination
from drt.sources.base import Source

if TYPE_CHECKING:
    from drt.config.credentials import ProfileConfig
    from drt.config.models import DestinationConfig

# Registry mappings: type_name -> (ConfigClass, ImplementationClass)
# ConfigClass stored for future plugin validation — not used in lookup yet
_destination_registry: dict[str, tuple[type[Any], type[Any]]] = {}
_source_registry: dict[str, tuple[type[Any], type[Any]]] = {}


def register_destination(
    type_name: str,
    config_class: type[Any],
    destination_class: type[Any],
) -> None:
    """Register a destination connector.

    Args:
        type_name: The type identifier (e.g., 'slack', 'rest_api')
        config_class: The Pydantic config class (e.g., SlackDestinationConfig)
        destination_class: The destination implementation class (e.g., SlackDestination)

    Raises:
        ValueError: If type_name is already registered
    """
    if type_name in _destination_registry:
        raise ValueError(
            f"Destination type '{type_name}' already registered. "
            f"Each connector type must be unique."
        )
    _destination_registry[type_name] = (config_class, destination_class)


def register_source(
    type_name: str,
    profile_class: type[Any],
    source_class: type[Any],
) -> None:
    """Register a source connector.

    Args:
        type_name: The type identifier (e.g., 'postgres', 'bigquery')
        profile_class: The credentials profile class (e.g., PostgresProfile)
        source_class: The source implementation class (e.g., PostgresSource)

    Raises:
        ValueError: If type_name is already registered
    """
    if type_name in _source_registry:
        raise ValueError(
            f"Source type '{type_name}' already registered. Each connector type must be unique."
        )
    _source_registry[type_name] = (profile_class, source_class)


def get_destination(config: DestinationConfig) -> Destination:
    """Get a destination instance for the given config.

    Args:
        config: The destination configuration object

    Returns:
        An instantiated destination connector

    Raises:
        ValueError: If the destination type is not registered
    """
    if config.type in _destination_registry:
        _, destination_class = _destination_registry[config.type]
        return destination_class()  # type: ignore[no-any-return]

    # If not found, provide helpful error message
    available = sorted(_destination_registry.keys())
    raise ValueError(
        f"Unknown destination type: '{config.type}'. Available destinations: {', '.join(available)}"
    )


def get_source(profile: ProfileConfig) -> Source:
    """Get a source instance for the given profile.

    Args:
        profile: The source profile/credentials object

    Returns:
        An instantiated source connector

    Raises:
        ValueError: If the source type is not registered
    """
    if profile.type in _source_registry:
        _, source_class = _source_registry[profile.type]
        return source_class()  # type: ignore[no-any-return]

    # If not found, provide helpful error message
    available = sorted(_source_registry.keys())
    raise ValueError(
        f"Unknown source type: '{profile.type}'. Available sources: {', '.join(available)}"
    )


def _register_all_connectors() -> None:
    """Register all built-in connectors.

    This is called automatically when the module is imported.
    """
    # Import config classes
    from drt.config.credentials import (
        BigQueryProfile,
        ClickHouseProfile,
        DatabricksProfile,
        DuckDBProfile,
        MySQLProfile,
        PostgresProfile,
        RedshiftProfile,
        RestApiProfile,
        SnowflakeProfile,
        SQLiteProfile,
        SQLServerProfile,
    )
    from drt.config.models import (
        AirtableDestinationConfig,
        AmplitudeDestinationConfig,
        AzureBlobDestinationConfig,
        BigQueryDestinationConfig,
        ClickHouseDestinationConfig,
        DatabricksDestinationConfig,
        DiscordDestinationConfig,
        ElasticsearchDestinationConfig,
        EmailSmtpDestinationConfig,
        FileDestinationConfig,
        GCSDestinationConfig,
        GitHubActionsDestinationConfig,
        GoogleAdsDestinationConfig,
        GoogleSheetsDestinationConfig,
        HubSpotDestinationConfig,
        IntercomDestinationConfig,
        JiraDestinationConfig,
        KlaviyoDestinationConfig,
        LinearDestinationConfig,
        MixpanelDestinationConfig,
        MySQLDestinationConfig,
        NotionDestinationConfig,
        ParquetDestinationConfig,
        PostgresDestinationConfig,
        RestApiDestinationConfig,
        S3DestinationConfig,
        SalesforceBulkDestinationConfig,
        SendGridDestinationConfig,
        SlackDestinationConfig,
        SnowflakeDestinationConfig,
        StagedUploadDestinationConfig,
        TeamsDestinationConfig,
        TwilioDestinationConfig,
        ZendeskDestinationConfig,
    )

    # Import destination classes
    from drt.destinations.airtable import AirtableDestination
    from drt.destinations.amplitude import AmplitudeDestination
    from drt.destinations.azure_blob import AzureBlobDestination
    from drt.destinations.bigquery import BigQueryDestination
    from drt.destinations.clickhouse import ClickHouseDestination
    from drt.destinations.databricks import DatabricksDestination
    from drt.destinations.discord import DiscordDestination
    from drt.destinations.elasticsearch import ElasticsearchDestination
    from drt.destinations.email_smtp import EmailSmtpDestination
    from drt.destinations.file import FileDestination
    from drt.destinations.gcs import GCSDestination
    from drt.destinations.github_actions import GitHubActionsDestination
    from drt.destinations.google_ads import GoogleAdsDestination
    from drt.destinations.google_sheets import GoogleSheetsDestination
    from drt.destinations.hubspot import HubSpotDestination
    from drt.destinations.intercom import IntercomDestination
    from drt.destinations.jira import JiraDestination
    from drt.destinations.klaviyo import KlaviyoDestination
    from drt.destinations.linear import LinearDestination
    from drt.destinations.mixpanel import MixpanelDestination
    from drt.destinations.mysql import MySQLDestination
    from drt.destinations.notion import NotionDestination
    from drt.destinations.parquet import ParquetDestination
    from drt.destinations.postgres import PostgresDestination
    from drt.destinations.rest_api import RestApiDestination
    from drt.destinations.s3 import S3Destination
    from drt.destinations.salesforce_bulk import SalesforceBulkDestination
    from drt.destinations.sendgrid import SendGridDestination
    from drt.destinations.slack import SlackDestination
    from drt.destinations.snowflake import SnowflakeDestination
    from drt.destinations.staged_upload import StagedUploadDestination
    from drt.destinations.teams import TeamsDestination
    from drt.destinations.twilio import TwilioDestination
    from drt.destinations.zendesk import ZendeskDestination

    # Import source classes
    from drt.sources.bigquery import BigQuerySource
    from drt.sources.clickhouse import ClickHouseSource
    from drt.sources.databricks import DatabricksSource
    from drt.sources.duckdb import DuckDBSource
    from drt.sources.mysql import MySQLSource
    from drt.sources.postgres import PostgresSource
    from drt.sources.redshift import RedshiftSource
    from drt.sources.rest_api import RestApiSource
    from drt.sources.snowflake import SnowflakeSource
    from drt.sources.sqlite import SQLiteSource
    from drt.sources.sqlserver import SQLServerSource

    # Register all destinations
    register_destination("rest_api", RestApiDestinationConfig, RestApiDestination)
    register_destination("slack", SlackDestinationConfig, SlackDestination)
    register_destination("twilio", TwilioDestinationConfig, TwilioDestination)
    register_destination("discord", DiscordDestinationConfig, DiscordDestination)
    register_destination("github_actions", GitHubActionsDestinationConfig, GitHubActionsDestination)
    register_destination("hubspot", HubSpotDestinationConfig, HubSpotDestination)
    register_destination("amplitude", AmplitudeDestinationConfig, AmplitudeDestination)
    register_destination("mixpanel", MixpanelDestinationConfig, MixpanelDestination)
    register_destination("zendesk", ZendeskDestinationConfig, ZendeskDestination)
    register_destination("jira", JiraDestinationConfig, JiraDestination)
    register_destination("sendgrid", SendGridDestinationConfig, SendGridDestination)
    register_destination("google_sheets", GoogleSheetsDestinationConfig, GoogleSheetsDestination)
    register_destination("postgres", PostgresDestinationConfig, PostgresDestination)
    register_destination("mysql", MySQLDestinationConfig, MySQLDestination)
    register_destination("teams", TeamsDestinationConfig, TeamsDestination)
    register_destination("clickhouse", ClickHouseDestinationConfig, ClickHouseDestination)
    register_destination("parquet", ParquetDestinationConfig, ParquetDestination)
    register_destination("file", FileDestinationConfig, FileDestination)
    register_destination("s3", S3DestinationConfig, S3Destination)
    register_destination("gcs", GCSDestinationConfig, GCSDestination)
    register_destination("azure_blob", AzureBlobDestinationConfig, AzureBlobDestination)
    register_destination("email_smtp", EmailSmtpDestinationConfig, EmailSmtpDestination)
    register_destination("elasticsearch", ElasticsearchDestinationConfig, ElasticsearchDestination)
    register_destination("linear", LinearDestinationConfig, LinearDestination)
    register_destination("google_ads", GoogleAdsDestinationConfig, GoogleAdsDestination)
    register_destination("notion", NotionDestinationConfig, NotionDestination)
    register_destination(
        "salesforce_bulk", SalesforceBulkDestinationConfig, SalesforceBulkDestination
    )
    register_destination("staged_upload", StagedUploadDestinationConfig, StagedUploadDestination)
    register_destination("intercom", IntercomDestinationConfig, IntercomDestination)
    register_destination("snowflake", SnowflakeDestinationConfig, SnowflakeDestination)
    register_destination("databricks", DatabricksDestinationConfig, DatabricksDestination)
    register_destination("bigquery", BigQueryDestinationConfig, BigQueryDestination)
    register_destination("airtable", AirtableDestinationConfig, AirtableDestination)
    register_destination("klaviyo", KlaviyoDestinationConfig, KlaviyoDestination)

    # Register all sources
    register_source("bigquery", BigQueryProfile, BigQuerySource)
    register_source("duckdb", DuckDBProfile, DuckDBSource)
    register_source("sqlite", SQLiteProfile, SQLiteSource)
    register_source("postgres", PostgresProfile, PostgresSource)
    register_source("redshift", RedshiftProfile, RedshiftSource)
    register_source("clickhouse", ClickHouseProfile, ClickHouseSource)
    register_source("mysql", MySQLProfile, MySQLSource)
    register_source("snowflake", SnowflakeProfile, SnowflakeSource)
    register_source("databricks", DatabricksProfile, DatabricksSource)
    register_source("sqlserver", SQLServerProfile, SQLServerSource)
    register_source("rest_api", RestApiProfile, RestApiSource)


# Auto-register all connectors on import
_register_all_connectors()
