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

from dataclasses import dataclass
from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from drt.config.models import DestinationConfig
    from drt.config.profiles import ProfileConfigLike
    from drt.destinations.base import Destination, StagedDestination
    from drt.sources.base import Source


@dataclass(frozen=True)
class _LazyConnector:
    """Import one built-in implementation only when it is instantiated.

    Config classes stay eagerly registered because config parsing and plugin
    discrimination need them. Third-party registrations continue to store the
    concrete class supplied by the plugin; only drt's built-ins use this
    private loader.
    """

    module: str
    class_name: str

    def resolve(self) -> type[Any]:
        connector_class = getattr(import_module(self.module), self.class_name)
        if not isinstance(connector_class, type):
            raise TypeError(f"{self.module}.{self.class_name} is not a class")
        return connector_class


_ConnectorClass = type[Any] | _LazyConnector

# Registry mappings: type_name -> (ConfigClass, ImplementationClass)
# ConfigClass stored for future plugin validation — not used in lookup yet
_destination_registry: dict[str, tuple[type[Any], _ConnectorClass]] = {}
_source_registry: dict[str, tuple[type[Any], _ConnectorClass]] = {}


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


def _register_builtin_destination(
    type_name: str,
    config_class: type[Any],
    module: str,
    class_name: str,
) -> None:
    """Register a built-in without importing its implementation module."""
    if type_name in _destination_registry:
        raise ValueError(
            f"Destination type '{type_name}' already registered. "
            f"Each connector type must be unique."
        )
    _destination_registry[type_name] = (config_class, _LazyConnector(module, class_name))


def _register_builtin_source(
    type_name: str,
    profile_class: type[Any],
    module: str,
    class_name: str,
) -> None:
    """Register a built-in without importing its implementation module."""
    if type_name in _source_registry:
        raise ValueError(
            f"Source type '{type_name}' already registered. Each connector type must be unique."
        )
    _source_registry[type_name] = (profile_class, _LazyConnector(module, class_name))


def _ensure_plugins_loaded() -> None:
    """Make plugin registration happen before the registry answers a question.

    Parse success depends on what is registered (#997), and registration used to
    happen only in the Typer root callback — so the same sync YAML parsed under
    ``drt run`` and failed under the MCP server, dagster-drt, and the
    Airflow/Prefect ``run_drt_sync()`` entry point, with an error indistinguishable
    from a typo. Every lookup below funnels through here instead, so the answer no
    longer depends on which entry point the caller came in by.

    :func:`drt.plugins.load_plugins` is cached per process and isolates a broken
    plugin's exception onto its own entry, so this is cheap and cannot fail the
    caller. Imported lazily to keep ``drt.plugins`` off this module's import path.
    """
    from drt.plugins import load_plugins

    load_plugins()


def is_registered_destination(type_name: str) -> bool:
    """Is ``type_name`` a destination the registry can resolve right now?

    Used by :data:`drt.config.sync_options.DestinationConfig`'s discriminator to
    tell "a plugin registered this" from "you typed it wrong" (#997). A pure
    membership test on purpose: it decides which union member parses the
    payload, nothing more. Validating a plugin's fields against the registered
    ``config_class`` is the deliberately deferred second pass ADR 0009 sketches.

    Reflects whatever has been registered by the time it is called — built-ins
    at import of this module, plugins once :func:`drt.plugins.load_plugins` has
    run (the CLI root callback does this before any command parses config).
    """
    _ensure_plugins_loaded()
    return type_name in _destination_registry


def destination_config_class(type_name: str) -> type[Any] | None:
    """The config class registered for ``type_name``, or ``None`` (#997).

    :class:`~drt.config.base.GenericDestinationConfig` validates a plugin
    payload against this when it is a pydantic model, so a plugin gets its own
    config class — with its own defaults, field types, and ``describe()`` —
    rather than the catch-all. The registry has always stored it; before #997
    nothing read it.
    """
    _ensure_plugins_loaded()
    entry = _destination_registry.get(type_name)
    return entry[0] if entry else None


def registered_destination_types() -> tuple[str, ...]:
    """Every destination type the registry can resolve, sorted (#997).

    Lets :class:`~drt.config.base.GenericDestinationConfig` enumerate the plugin
    types in its JSON Schema, so ``drt validate``'s schema pass accepts exactly
    the types its pydantic pass accepts.
    """
    _ensure_plugins_loaded()
    return tuple(sorted(_destination_registry))


def registered_source_types() -> tuple[str, ...]:
    """Every source type the registry can resolve, sorted (#997).

    Only used to make ``load_profile()``'s failure message name the plugin types
    that *are* installed, so "unsupported" does not read as "your plugin was
    never seen".
    """
    _ensure_plugins_loaded()
    return tuple(sorted(_source_registry))


def source_profile_class(type_name: str) -> type[Any] | None:
    """The profile class registered for ``type_name``, or ``None``.

    ``load_profile()`` builds a plugin's profile from this once its hand-written
    ``if source_type == ...`` chain has run out of built-ins (#997). The
    destination side has no equivalent because pydantic constructs the config
    itself; profiles are plain dataclasses with no validator to hook.
    """
    _ensure_plugins_loaded()
    entry = _source_registry.get(type_name)
    return entry[0] if entry else None


def get_destination(config: DestinationConfig) -> Destination | StagedDestination:
    """Get a destination instance for the given config.

    Args:
        config: The destination configuration object

    Returns:
        An instantiated destination connector

    Raises:
        ValueError: If the destination type is not registered
    """
    if config.type in _destination_registry:
        config_class, destination_class = _destination_registry[config.type]
        if isinstance(destination_class, _LazyConnector):
            destination_class = destination_class.resolve()
            _destination_registry[config.type] = (config_class, destination_class)
        return destination_class()  # type: ignore[no-any-return]

    # If not found, provide helpful error message
    available = sorted(_destination_registry.keys())
    raise ValueError(
        f"Unknown destination type: '{config.type}'. Available destinations: {', '.join(available)}"
    )


def get_source(profile: ProfileConfigLike) -> Source:
    """Get a source instance for the given profile.

    Args:
        profile: The source profile/credentials object

    Returns:
        An instantiated source connector

    Raises:
        ValueError: If the source type is not registered
    """
    if profile.type in _source_registry:
        profile_class, source_class = _source_registry[profile.type]
        if isinstance(source_class, _LazyConnector):
            source_class = source_class.resolve()
            _source_registry[profile.type] = (profile_class, source_class)
        return source_class()  # type: ignore[no-any-return]

    # If not found, provide helpful error message
    available = sorted(_source_registry.keys())
    raise ValueError(
        f"Unknown source type: '{profile.type}'. Available sources: {', '.join(available)}"
    )


def _register_all_connectors() -> None:
    """Register all built-in connectors.

    Config/profile classes are imported immediately because validation needs
    them. Implementation modules are named here but imported by
    :func:`get_destination` or :func:`get_source` only when selected.
    """
    # Import config classes
    from drt.config.credentials import (
        BigQueryProfile,
        ClickHouseProfile,
        DatabricksProfile,
        DeltaLakeProfile,
        DuckDBProfile,
        IcebergProfile,
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
        MetaConversionsDestinationConfig,
        MixpanelDestinationConfig,
        MySQLDestinationConfig,
        NotionDestinationConfig,
        ParquetDestinationConfig,
        PipedriveDestinationConfig,  # <-- ADDED
        PostgresDestinationConfig,
        RestApiDestinationConfig,
        S3DestinationConfig,
        SalesforceBulkDestinationConfig,
        SendGridDestinationConfig,
        SlackDestinationConfig,
        SnowflakeDestinationConfig,
        SQSDestinationConfig,
        StagedUploadDestinationConfig,
        TeamsDestinationConfig,
        TwilioDestinationConfig,
        ZendeskDestinationConfig,
    )

    # Register all destinations
    destinations = (
        ("rest_api", RestApiDestinationConfig, "rest_api", "RestApiDestination"),
        ("slack", SlackDestinationConfig, "slack", "SlackDestination"),
        ("sqs", SQSDestinationConfig, "sqs", "SQSDestination"),
        ("twilio", TwilioDestinationConfig, "twilio", "TwilioDestination"),
        ("discord", DiscordDestinationConfig, "discord", "DiscordDestination"),
        (
            "github_actions",
            GitHubActionsDestinationConfig,
            "github_actions",
            "GitHubActionsDestination",
        ),
        ("hubspot", HubSpotDestinationConfig, "hubspot", "HubSpotDestination"),
        ("pipedrive", PipedriveDestinationConfig, "pipedrive", "PipedriveDestination"),
        ("amplitude", AmplitudeDestinationConfig, "amplitude", "AmplitudeDestination"),
        ("mixpanel", MixpanelDestinationConfig, "mixpanel", "MixpanelDestination"),
        ("zendesk", ZendeskDestinationConfig, "zendesk", "ZendeskDestination"),
        ("jira", JiraDestinationConfig, "jira", "JiraDestination"),
        ("sendgrid", SendGridDestinationConfig, "sendgrid", "SendGridDestination"),
        (
            "google_sheets",
            GoogleSheetsDestinationConfig,
            "google_sheets",
            "GoogleSheetsDestination",
        ),
        ("postgres", PostgresDestinationConfig, "postgres", "PostgresDestination"),
        ("mysql", MySQLDestinationConfig, "mysql", "MySQLDestination"),
        ("teams", TeamsDestinationConfig, "teams", "TeamsDestination"),
        ("clickhouse", ClickHouseDestinationConfig, "clickhouse", "ClickHouseDestination"),
        ("parquet", ParquetDestinationConfig, "parquet", "ParquetDestination"),
        ("file", FileDestinationConfig, "file", "FileDestination"),
        ("s3", S3DestinationConfig, "s3", "S3Destination"),
        ("gcs", GCSDestinationConfig, "gcs", "GCSDestination"),
        ("azure_blob", AzureBlobDestinationConfig, "azure_blob", "AzureBlobDestination"),
        ("email_smtp", EmailSmtpDestinationConfig, "email_smtp", "EmailSmtpDestination"),
        (
            "elasticsearch",
            ElasticsearchDestinationConfig,
            "elasticsearch",
            "ElasticsearchDestination",
        ),
        ("linear", LinearDestinationConfig, "linear", "LinearDestination"),
        ("google_ads", GoogleAdsDestinationConfig, "google_ads", "GoogleAdsDestination"),
        (
            "meta_conversions",
            MetaConversionsDestinationConfig,
            "meta_conversions",
            "MetaConversionsDestination",
        ),
        ("notion", NotionDestinationConfig, "notion", "NotionDestination"),
        (
            "salesforce_bulk",
            SalesforceBulkDestinationConfig,
            "salesforce_bulk",
            "SalesforceBulkDestination",
        ),
        (
            "staged_upload",
            StagedUploadDestinationConfig,
            "staged_upload",
            "StagedUploadDestination",
        ),
        ("intercom", IntercomDestinationConfig, "intercom", "IntercomDestination"),
        ("snowflake", SnowflakeDestinationConfig, "snowflake", "SnowflakeDestination"),
        ("databricks", DatabricksDestinationConfig, "databricks", "DatabricksDestination"),
        ("bigquery", BigQueryDestinationConfig, "bigquery", "BigQueryDestination"),
        ("airtable", AirtableDestinationConfig, "airtable", "AirtableDestination"),
        ("klaviyo", KlaviyoDestinationConfig, "klaviyo", "KlaviyoDestination"),
    )
    for type_name, config_class, module_name, class_name in destinations:
        _register_builtin_destination(
            type_name,
            config_class,
            f"drt.destinations.{module_name}",
            class_name,
        )

    # Register all sources
    sources = (
        ("bigquery", BigQueryProfile, "bigquery", "BigQuerySource"),
        ("duckdb", DuckDBProfile, "duckdb", "DuckDBSource"),
        ("sqlite", SQLiteProfile, "sqlite", "SQLiteSource"),
        ("postgres", PostgresProfile, "postgres", "PostgresSource"),
        ("redshift", RedshiftProfile, "redshift", "RedshiftSource"),
        ("clickhouse", ClickHouseProfile, "clickhouse", "ClickHouseSource"),
        ("mysql", MySQLProfile, "mysql", "MySQLSource"),
        ("snowflake", SnowflakeProfile, "snowflake", "SnowflakeSource"),
        ("databricks", DatabricksProfile, "databricks", "DatabricksSource"),
        ("sqlserver", SQLServerProfile, "sqlserver", "SQLServerSource"),
        ("rest_api", RestApiProfile, "rest_api", "RestApiSource"),
        ("deltalake", DeltaLakeProfile, "deltalake", "DeltaLakeSource"),
        ("iceberg", IcebergProfile, "iceberg", "IcebergSource"),
    )
    for type_name, profile_class, module_name, class_name in sources:
        _register_builtin_source(
            type_name,
            profile_class,
            f"drt.sources.{module_name}",
            class_name,
        )


# Auto-register all connectors on import
_register_all_connectors()
