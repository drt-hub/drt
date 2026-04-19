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
    ("snowflake", "Snowflake"),
    ("sqlite", "SQLite"),
    ("sqlserver", "SQL Server"),
]

# Available destination connectors: (type, display_name)
DESTINATIONS = [
    ("clickhouse", "ClickHouse"),
    ("discord", "Discord"),
    ("email_smtp", "Email"),
    ("file", "File"),
    ("github_actions", "GitHub Actions"),
    ("google_ads", "Google Ads"),
    ("google_sheets", "Google Sheets"),
    ("hubspot", "HubSpot"),
    ("intercom", "Intercom"),
    ("jira", "Jira"),
    ("linear", "Linear"),
    ("mysql", "MySQL"),
    ("notion", "Notion"),
    ("parquet", "Parquet"),
    ("postgres", "PostgreSQL"),
    ("rest_api", "REST API"),
    ("sendgrid", "SendGrid"),
    ("slack", "Slack"),
    ("staged_upload", "Staged Upload"),
    ("teams", "Microsoft Teams"),
    ("twilio", "Twilio"),
]
