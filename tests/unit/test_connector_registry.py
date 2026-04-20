"""Tests for the connector registry system."""

from __future__ import annotations

import pytest

from drt.config.credentials import DuckDBProfile, PostgresProfile
from drt.config.models import RestApiDestinationConfig, SlackDestinationConfig
from drt.connectors import get_destination, get_source


class TestConnectorRegistry:
    """Test the connector registry system."""

    def test_get_destination_slack(self):
        """Get a Slack destination from registry."""
        config = SlackDestinationConfig(
            type="slack",
            webhook_url_env="SLACK_WEBHOOK_URL",
        )
        destination = get_destination(config)
        assert destination is not None
        assert type(destination).__name__ == "SlackDestination"

    def test_get_destination_rest_api(self):
        """Get a REST API destination from registry."""
        config = RestApiDestinationConfig(
            type="rest_api",
            url="https://api.example.com/webhook",
        )
        destination = get_destination(config)
        assert destination is not None
        assert type(destination).__name__ == "RestApiDestination"

    def test_get_source_postgres(self):
        """Get a Postgres source from registry."""
        profile = PostgresProfile(
            type="postgres",
            host="localhost",
            port=5432,
            dbname="test",
            user="test",
        )
        source = get_source(profile)
        assert source is not None
        assert type(source).__name__ == "PostgresSource"

    def test_get_source_duckdb(self):
        """Get a DuckDB source from registry."""
        profile = DuckDBProfile(
            type="duckdb",
            database=":memory:",
        )
        source = get_source(profile)
        assert source is not None
        assert type(source).__name__ == "DuckDBSource"

    def test_unknown_destination_error_message(self):
        """Error message for unknown destination lists available options."""
        # Create a mock config object with unknown type
        class UnknownDestinationConfig:
            type = "unknown_destination"

        config = UnknownDestinationConfig()  # type: ignore
        with pytest.raises(ValueError) as exc_info:
            get_destination(config)  # type: ignore

        # Check error message lists available destinations
        error_msg = str(exc_info.value)
        assert "Unknown destination type" in error_msg
        assert "slack" in error_msg
        assert "rest_api" in error_msg

    def test_unknown_source_error_message(self):
        """Error message for unknown source lists available options."""
        # Create a mock profile object with unknown type
        class UnknownProfile:
            type = "unknown_source"

        profile = UnknownProfile()  # type: ignore
        with pytest.raises(ValueError) as exc_info:
            get_source(profile)  # type: ignore

        # Check error message lists available sources
        error_msg = str(exc_info.value)
        assert "Unknown source type" in error_msg
        assert "postgres" in error_msg
        assert "duckdb" in error_msg
