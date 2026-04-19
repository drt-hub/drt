"""Tests for drt sources and drt destinations commands."""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from drt.cli.main import app

runner = CliRunner()


# ---------------------------------------------------------------------------
# drt sources
# ---------------------------------------------------------------------------

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


def test_sources_command_succeeds() -> None:
    """drt sources should exit with code 0."""
    result = runner.invoke(app, ["sources"])
    assert result.exit_code == 0
    assert "Available sources:" in result.output


@pytest.mark.parametrize("source_type,description", SOURCES)
def test_sources_command_contains_connector(source_type: str, description: str) -> None:
    """drt sources should list each available source connector."""
    result = runner.invoke(app, ["sources"])
    assert result.exit_code == 0
    assert source_type in result.output
    assert description in result.output


def test_sources_command_contains_mysql() -> None:
    """drt sources should list MySQL."""
    result = runner.invoke(app, ["sources"])
    assert "mysql" in result.output
    assert "MySQL" in result.output


def test_sources_command_contains_redshift() -> None:
    """drt sources should list Redshift."""
    result = runner.invoke(app, ["sources"])
    assert "redshift" in result.output
    assert "Redshift" in result.output


def test_sources_command_contains_clickhouse() -> None:
    """drt sources should list ClickHouse."""
    result = runner.invoke(app, ["sources"])
    assert "clickhouse" in result.output
    assert "ClickHouse" in result.output


def test_sources_command_contains_sqlite() -> None:
    """drt sources should list SQLite."""
    result = runner.invoke(app, ["sources"])
    assert "sqlite" in result.output
    assert "SQLite" in result.output


def test_sources_command_contains_databricks() -> None:
    """drt sources should list Databricks."""
    result = runner.invoke(app, ["sources"])
    assert "databricks" in result.output
    assert "Databricks" in result.output


def test_sources_command_contains_sqlserver() -> None:
    """drt sources should list SQL Server."""
    result = runner.invoke(app, ["sources"])
    assert "sqlserver" in result.output
    assert "SQL Server" in result.output


def test_sources_command_header() -> None:
    """drt sources should have a header."""
    result = runner.invoke(app, ["sources"])
    assert "Available sources:" in result.output


# ---------------------------------------------------------------------------
# drt destinations
# ---------------------------------------------------------------------------

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


def test_destinations_command_succeeds() -> None:
    """drt destinations should exit with code 0."""
    result = runner.invoke(app, ["destinations"])
    assert result.exit_code == 0
    assert "Available destinations:" in result.output


@pytest.mark.parametrize("dest_type,description", DESTINATIONS)
def test_destinations_command_contains_connector(dest_type: str, description: str) -> None:
    """drt destinations should list each available destination connector."""
    result = runner.invoke(app, ["destinations"])
    assert result.exit_code == 0
    assert dest_type in result.output
    assert description in result.output
