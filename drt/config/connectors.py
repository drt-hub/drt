"""Centralized definitions of available source and destination connectors.

This module serves as the single source of truth for supported connector types,
display names, and metadata. Used by CLI commands, tests, and MCP server.
"""

from __future__ import annotations

# Available source connectors: (type, display_name)
SOURCES = [
    ("bigquery", "BigQuery"),
    ("clickhouse", "ClickHouse"),
    ("databricks", "Databricks"),
    ("deltalake", "Delta Lake"),
    ("duckdb", "DuckDB"),
    ("iceberg", "Iceberg"),
    ("mysql", "MySQL"),
    ("postgres", "PostgreSQL"),
    ("redshift", "Redshift"),
    ("rest_api", "REST API"),
    ("snowflake", "Snowflake"),
    ("sqlite", "SQLite"),
    ("sqlserver", "SQL Server"),
]

# Available destination connectors: (type, display_name)
# NOTE: must stay in sync with the connector registry (drt/connectors/registry.py).
# `tests/unit/test_cli_list_connectors.py::test_DESTINATIONS_matches_registry`
# fails the build if a registered destination is missing here (or vice versa).
DESTINATIONS = [
    ("airtable", "Airtable"),
    ("amplitude", "Amplitude"),
    ("azure_blob", "Azure Blob Storage"),
    ("bigquery", "BigQuery"),
    ("clickhouse", "ClickHouse"),
    ("databricks", "Databricks"),
    ("discord", "Discord"),
    ("elasticsearch", "Elasticsearch"),
    ("email_smtp", "Email"),
    ("file", "File"),
    ("gcs", "Google Cloud Storage"),
    ("github_actions", "GitHub Actions"),
    ("google_ads", "Google Ads"),
    ("google_sheets", "Google Sheets"),
    ("hubspot", "HubSpot"),
    ("intercom", "Intercom"),
    ("jira", "Jira"),
    ("klaviyo", "Klaviyo"),
    ("linear", "Linear"),
    ("mixpanel", "Mixpanel"),
    ("mysql", "MySQL"),
    ("notion", "Notion"),
    ("parquet", "Parquet"),
    ("postgres", "PostgreSQL"),
    ("rest_api", "REST API"),
    ("s3", "Amazon S3"),
    ("salesforce_bulk", "Salesforce Bulk"),
    ("sendgrid", "SendGrid"),
    ("slack", "Slack"),
    ("snowflake", "Snowflake"),
    ("staged_upload", "Staged Upload"),
    ("teams", "Microsoft Teams"),
    ("twilio", "Twilio"),
    ("zendesk", "Zendesk"),
]


# pip extra required per connector type. Types not listed ship in drt-core
# (DuckDB, SQLite, REST API, and every webhook / SaaS destination). The extra
# name equals the type except where noted.
_EXTRAS: dict[str, str] = {
    "bigquery": "bigquery",
    "postgres": "postgres",
    "redshift": "redshift",
    "clickhouse": "clickhouse",
    "snowflake": "snowflake",
    "mysql": "mysql",
    "databricks": "databricks",
    "sqlserver": "sqlserver",
    "deltalake": "deltalake",
    "iceberg": "iceberg",
    "s3": "s3",
    "gcs": "gcs",
    "azure_blob": "azure",  # extra name differs from the type
    "parquet": "parquet",
    "google_sheets": "sheets",  # extra name differs from the type
}


def install_target(connector_type: str) -> str:
    """pip install target for a connector type; ``"(core)"`` when no extra is needed."""
    extra = _EXTRAS.get(connector_type)
    return f"drt-core[{extra}]" if extra else "(core)"


def connector_inventory() -> dict[str, list[dict[str, str]]]:
    """Sources + destinations as ``{name, type, install}`` dicts, derived from
    the ``SOURCES`` / ``DESTINATIONS`` SSoT above.

    Consumed by the MCP ``drt_list_connectors`` tool so the inventory can't
    drift out of lockstep with the registry (which ``test_cli_list_connectors``
    already keeps aligned with these lists).
    """
    return {
        "sources": [
            {"name": name, "type": t, "install": install_target(t)} for t, name in SOURCES
        ],
        "destinations": [
            {"name": name, "type": t, "install": install_target(t)} for t, name in DESTINATIONS
        ],
    }
