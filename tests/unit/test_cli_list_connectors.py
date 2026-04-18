"""Tests for drt sources and drt destinations commands."""

from __future__ import annotations

from typer.testing import CliRunner

from drt.cli.main import app

runner = CliRunner()


# ---------------------------------------------------------------------------
# drt sources
# ---------------------------------------------------------------------------


def test_sources_command_succeeds() -> None:
    """drt sources should exit with code 0."""
    result = runner.invoke(app, ["sources"])
    assert result.exit_code == 0


def test_sources_command_contains_bigquery() -> None:
    """drt sources should list BigQuery."""
    result = runner.invoke(app, ["sources"])
    assert "bigquery" in result.output
    assert "BigQuery" in result.output


def test_sources_command_contains_postgres() -> None:
    """drt sources should list PostgreSQL."""
    result = runner.invoke(app, ["sources"])
    assert "postgres" in result.output
    assert "PostgreSQL" in result.output


def test_sources_command_contains_duckdb() -> None:
    """drt sources should list DuckDB."""
    result = runner.invoke(app, ["sources"])
    assert "duckdb" in result.output
    assert "DuckDB" in result.output


def test_sources_command_contains_snowflake() -> None:
    """drt sources should list Snowflake."""
    result = runner.invoke(app, ["sources"])
    assert "snowflake" in result.output
    assert "Snowflake" in result.output


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


def test_destinations_command_succeeds() -> None:
    """drt destinations should exit with code 0."""
    result = runner.invoke(app, ["destinations"])
    assert result.exit_code == 0


def test_destinations_command_contains_rest_api() -> None:
    """drt destinations should list REST API."""
    result = runner.invoke(app, ["destinations"])
    assert "rest_api" in result.output
    assert "REST API" in result.output


def test_destinations_command_contains_slack() -> None:
    """drt destinations should list Slack."""
    result = runner.invoke(app, ["destinations"])
    assert "slack" in result.output
    assert "Slack" in result.output


def test_destinations_command_contains_hubspot() -> None:
    """drt destinations should list HubSpot."""
    result = runner.invoke(app, ["destinations"])
    assert "hubspot" in result.output
    assert "HubSpot" in result.output


def test_destinations_command_contains_discord() -> None:
    """drt destinations should list Discord."""
    result = runner.invoke(app, ["destinations"])
    assert "discord" in result.output
    assert "Discord" in result.output


def test_destinations_command_contains_github_actions() -> None:
    """drt destinations should list GitHub Actions."""
    result = runner.invoke(app, ["destinations"])
    assert "github_actions" in result.output
    assert "GitHub Actions" in result.output


def test_destinations_command_contains_jira() -> None:
    """drt destinations should list Jira."""
    result = runner.invoke(app, ["destinations"])
    assert "jira" in result.output
    assert "Jira" in result.output


def test_destinations_command_contains_sendgrid() -> None:
    """drt destinations should list SendGrid."""
    result = runner.invoke(app, ["destinations"])
    assert "sendgrid" in result.output
    assert "SendGrid" in result.output


def test_destinations_command_contains_google_sheets() -> None:
    """drt destinations should list Google Sheets."""
    result = runner.invoke(app, ["destinations"])
    assert "google_sheets" in result.output
    assert "Google Sheets" in result.output


def test_destinations_command_contains_postgres() -> None:
    """drt destinations should list PostgreSQL."""
    result = runner.invoke(app, ["destinations"])
    assert "postgres" in result.output
    assert "PostgreSQL" in result.output


def test_destinations_command_contains_mysql() -> None:
    """drt destinations should list MySQL."""
    result = runner.invoke(app, ["destinations"])
    assert "mysql" in result.output
    assert "MySQL" in result.output


def test_destinations_command_contains_teams() -> None:
    """drt destinations should list Microsoft Teams."""
    result = runner.invoke(app, ["destinations"])
    assert "teams" in result.output
    assert "Microsoft Teams" in result.output


def test_destinations_command_contains_clickhouse() -> None:
    """drt destinations should list ClickHouse."""
    result = runner.invoke(app, ["destinations"])
    assert "clickhouse" in result.output
    assert "ClickHouse" in result.output


def test_destinations_command_contains_parquet() -> None:
    """drt destinations should list Parquet."""
    result = runner.invoke(app, ["destinations"])
    assert "parquet" in result.output
    assert "Parquet" in result.output


def test_destinations_command_contains_file() -> None:
    """drt destinations should list File."""
    result = runner.invoke(app, ["destinations"])
    assert "file" in result.output
    assert "File" in result.output


def test_destinations_command_contains_email() -> None:
    """drt destinations should list Email (SMTP)."""
    result = runner.invoke(app, ["destinations"])
    assert "email_smtp" in result.output
    assert "Email" in result.output


def test_destinations_command_contains_linear() -> None:
    """drt destinations should list Linear."""
    result = runner.invoke(app, ["destinations"])
    assert "linear" in result.output
    assert "Linear" in result.output


def test_destinations_command_contains_google_ads() -> None:
    """drt destinations should list Google Ads."""
    result = runner.invoke(app, ["destinations"])
    assert "google_ads" in result.output
    assert "Google Ads" in result.output


def test_destinations_command_contains_notion() -> None:
    """drt destinations should list Notion."""
    result = runner.invoke(app, ["destinations"])
    assert "notion" in result.output
    assert "Notion" in result.output


def test_destinations_command_contains_staged_upload() -> None:
    """drt destinations should list Staged Upload."""
    result = runner.invoke(app, ["destinations"])
    assert "staged_upload" in result.output
    assert "Staged Upload" in result.output


def test_destinations_command_contains_twilio() -> None:
    """drt destinations should list Twilio."""
    result = runner.invoke(app, ["destinations"])
    assert "twilio" in result.output
    assert "Twilio" in result.output


def test_destinations_command_contains_intercom() -> None:
    """drt destinations should list Intercom."""
    result = runner.invoke(app, ["destinations"])
    assert "intercom" in result.output
    assert "Intercom" in result.output


def test_destinations_command_header() -> None:
    """drt destinations should have a header."""
    result = runner.invoke(app, ["destinations"])
    assert "Available destinations:" in result.output
