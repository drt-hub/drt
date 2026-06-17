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
    ("duckdb", "DuckDB"),
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
