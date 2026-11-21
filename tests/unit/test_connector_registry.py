"""Tests for the connector registry system."""

from __future__ import annotations

import subprocess
import sys

import pytest

from drt.config.credentials import DuckDBProfile, PostgresProfile
from drt.config.models import (
    AmplitudeDestinationConfig,
    RestApiDestinationConfig,
    SlackDestinationConfig,
    SyncConfig,
    ZendeskDestinationConfig,
)
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

    def test_get_destination_zendesk(self):
        """Get a Zendesk destination from registry."""
        config = ZendeskDestinationConfig(
            type="zendesk",
            subdomain="example",
            email="bot@example.com",
            api_token="token",
        )
        destination = get_destination(config)
        assert destination is not None
        assert type(destination).__name__ == "ZendeskDestination"

    def test_get_destination_amplitude(self):
        """Get an Amplitude destination from registry."""
        config = AmplitudeDestinationConfig(
            type="amplitude",
            api_key="test-key",
            endpoint="identify",
        )
        destination = get_destination(config)
        assert destination is not None
        assert type(destination).__name__ == "AmplitudeDestination"

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

    def test_builtin_implementations_are_imported_only_when_selected(self):
        """Config registration must not eagerly import every connector implementation."""
        code = """
import sys
from drt.config.models import SlackDestinationConfig
from drt.config.profiles import SQLiteProfile
from drt.connectors import registry

assert "drt.destinations.airtable" not in sys.modules
assert "drt.destinations.slack" not in sys.modules
assert "drt.sources.bigquery" not in sys.modules
assert "drt.sources.sqlite" not in sys.modules
registry.get_destination(
    SlackDestinationConfig(type="slack", webhook_url_env="SLACK_WEBHOOK_URL")
)
registry.get_source(SQLiteProfile(type="sqlite", database=":memory:"))
assert "drt.destinations.slack" in sys.modules
assert "drt.destinations.airtable" not in sys.modules
assert "drt.sources.sqlite" in sys.modules
assert "drt.sources.bigquery" not in sys.modules
"""
        completed = subprocess.run(
            [sys.executable, "-c", code],
            check=False,
            capture_output=True,
            text=True,
        )

        assert completed.returncode == 0, completed.stderr

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

    def test_duplicate_destination_registration(self):
        """Registering a destination with duplicate type raises ValueError."""
        from drt.connectors.registry import register_destination

        # Try to register a destination with an existing type
        class FakeConfig:
            pass

        class FakeDestination:
            pass

        with pytest.raises(ValueError) as exc_info:
            register_destination("slack", FakeConfig, FakeDestination)

        error_msg = str(exc_info.value)
        assert "already registered" in error_msg
        assert "slack" in error_msg

    def test_duplicate_source_registration(self):
        """Registering a source with duplicate type raises ValueError."""
        from drt.connectors.registry import register_source

        # Try to register a source with an existing type
        class FakeProfile:
            pass

        class FakeSource:
            pass

        with pytest.raises(ValueError) as exc_info:
            register_source("postgres", FakeProfile, FakeSource)

        error_msg = str(exc_info.value)
        assert "already registered" in error_msg
        assert "postgres" in error_msg

    def test_cli_get_destination_dispatcher(self):
        """Test CLI dispatcher function for getting destinations."""
        from drt.cli.main import _get_destination

        # Create a minimal sync config with a slack destination
        sync = SyncConfig(
            name="test_sync",
            model="test_model",
            destination={
                "type": "slack",
                "webhook_url_env": "SLACK_WEBHOOK_URL",
            },
        )

        destination = _get_destination(sync)
        assert destination is not None
        assert type(destination).__name__ == "SlackDestination"

    def test_cli_get_source_dispatcher(self):
        """Test CLI dispatcher function for getting sources."""
        from drt.cli.main import _get_source

        # Create a postgres profile
        profile = PostgresProfile(
            type="postgres",
            host="localhost",
            port=5432,
            dbname="test",
            user="test",
        )

        source = _get_source(profile)
        assert source is not None
        assert type(source).__name__ == "PostgresSource"
