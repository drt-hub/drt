"""Unit tests for ClickHouse destination.

Uses a mock clickhouse-connect client — no real database required.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.models import ClickHouseDestinationConfig, SyncOptions
from drt.destinations.clickhouse import ClickHouseDestination

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _options(**kwargs: Any) -> SyncOptions:
    return SyncOptions(**kwargs)


def _config(**overrides: Any) -> ClickHouseDestinationConfig:
    defaults: dict[str, Any] = {
        "type": "clickhouse",
        "host": "localhost",
        "database": "default",
        "user": "default",
        "password": "",
        "table": "analytics_scores",
    }
    defaults.update(overrides)
    return ClickHouseDestinationConfig(**defaults)


def _fake_client() -> MagicMock:
    client = MagicMock()
    return client


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


class TestClickHouseDestinationConfig:
    def test_valid_config(self) -> None:
        config = _config()
        assert config.table == "analytics_scores"
        assert config.upsert_key is None
        assert config.port == 8123

    def test_upsert_key_optional(self) -> None:
        config = _config(upsert_key=["id", "ts"])
        assert config.upsert_key == ["id", "ts"]

    def test_host_env_instead_of_host(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CH_HOST", "ch.example.com")
        config = _config(host=None, host_env="CH_HOST")
        assert config.host_env == "CH_HOST"

    def test_missing_host_and_host_env_raises(self) -> None:
        with pytest.raises(ValueError, match="host"):
            _config(host=None, host_env=None)

    def test_missing_database_and_database_env_raises(self) -> None:
        with pytest.raises(ValueError, match="database"):
            _config(database=None, database_env=None)

    def test_connection_string_env_skips_validation(self) -> None:
        config = _config(
            host=None,
            host_env=None,
            database=None,
            database_env=None,
            connection_string_env="CH_DSN",
        )
        assert config.connection_string_env == "CH_DSN"

    def test_default_port(self) -> None:
        config = _config()
        assert config.port == 8123

    def test_custom_port(self) -> None:
        config = _config(port=9000)
        assert config.port == 9000

    def test_secure_default_false(self) -> None:
        config = _config()
        assert config.secure is False


# ---------------------------------------------------------------------------
# Load behavior
# ---------------------------------------------------------------------------


class TestClickHouseDestinationLoad:
    @patch("drt.destinations.clickhouse.ClickHouseDestination._connect")
    def test_success_insert(self, mock_connect: MagicMock) -> None:
        client = _fake_client()
        mock_connect.return_value = client

        records = [
            {"id": 1, "score": 0.95, "updated_at": "2026-03-31"},
            {"id": 2, "score": 0.80, "updated_at": "2026-03-31"},
        ]
        result = ClickHouseDestination().load(records, _config(), _options())

        assert result.success == 2
        assert result.failed == 0
        assert client.insert.call_count == 2
        client.close.assert_called_once()

    @patch("drt.destinations.clickhouse.ClickHouseDestination._connect")
    def test_empty_records(self, mock_connect: MagicMock) -> None:
        result = ClickHouseDestination().load([], _config(), _options())
        assert result.success == 0
        assert result.failed == 0
        mock_connect.assert_not_called()

    @patch("drt.destinations.clickhouse.ClickHouseDestination._connect")
    def test_row_error_on_error_skip(self, mock_connect: MagicMock) -> None:
        client = _fake_client()
        # First row fails, second succeeds
        client.insert.side_effect = [Exception("type mismatch"), None]
        mock_connect.return_value = client

        records = [
            {"id": 1, "score": 0.5},
            {"id": 2, "score": 0.9},
        ]
        result = ClickHouseDestination().load(records, _config(), _options(on_error="skip"))

        assert result.failed == 1
        assert result.success == 1
        assert len(result.row_errors) == 1
        assert "type mismatch" in result.row_errors[0].error_message

    @patch("drt.destinations.clickhouse.ClickHouseDestination._connect")
    def test_row_error_on_error_fail(self, mock_connect: MagicMock) -> None:
        client = _fake_client()
        client.insert.side_effect = Exception("connection lost")
        mock_connect.return_value = client

        records = [
            {"id": 1, "score": 0.5},
            {"id": 2, "score": 0.9},
        ]
        result = ClickHouseDestination().load(records, _config(), _options(on_error="fail"))

        assert result.failed == 1
        assert result.success == 0
        # Should stop after first failure
        assert client.insert.call_count == 1

    @patch("drt.destinations.clickhouse.ClickHouseDestination._connect")
    def test_connection_closed_on_success(self, mock_connect: MagicMock) -> None:
        client = _fake_client()
        mock_connect.return_value = client

        ClickHouseDestination().load([{"id": 1, "score": 0.5}], _config(), _options())
        client.close.assert_called_once()

    @patch("drt.destinations.clickhouse.ClickHouseDestination._connect")
    def test_connection_closed_on_error(self, mock_connect: MagicMock) -> None:
        client = _fake_client()
        client.insert.side_effect = Exception("fail")
        mock_connect.return_value = client

        ClickHouseDestination().load(
            [{"id": 1, "score": 0.5}], _config(), _options(on_error="fail")
        )
        client.close.assert_called_once()

    @patch("drt.destinations.clickhouse.ClickHouseDestination._connect")
    def test_insert_passes_correct_columns(self, mock_connect: MagicMock) -> None:
        client = _fake_client()
        mock_connect.return_value = client

        records = [{"id": 1, "name": "test", "value": 42}]
        ClickHouseDestination().load(records, _config(), _options())

        call_args = client.insert.call_args
        assert call_args[0][0] == "analytics_scores"  # table name
        assert call_args[1]["column_names"] == ["id", "name", "value"]

    @patch("drt.destinations.clickhouse.ClickHouseDestination._connect")
    def test_row_error_preview_truncated(self, mock_connect: MagicMock) -> None:
        client = _fake_client()
        client.insert.side_effect = Exception("fail")
        mock_connect.return_value = client

        big_record = {"id": 1, "data": "x" * 500}
        result = ClickHouseDestination().load([big_record], _config(), _options(on_error="skip"))

        assert len(result.row_errors[0].record_preview) <= 200


# ---------------------------------------------------------------------------
# Replace mode
# ---------------------------------------------------------------------------


class TestClickHouseReplaceMode:
    @patch("drt.destinations.clickhouse.ClickHouseDestination._connect")
    def test_replace_truncates_then_inserts(self, mock_connect: MagicMock) -> None:
        client = _fake_client()
        mock_connect.return_value = client

        records = [
            {"id": 1, "score": 0.95},
            {"id": 2, "score": 0.80},
        ]
        dest = ClickHouseDestination()
        result = dest.load(records, _config(), _options(mode="replace"))

        assert result.success == 2
        assert result.failed == 0
        client.command.assert_called_once_with("TRUNCATE TABLE analytics_scores")
        assert client.insert.call_count == 2

    @patch("drt.destinations.clickhouse.ClickHouseDestination._connect")
    def test_replace_truncates_only_once_across_batches(self, mock_connect: MagicMock) -> None:
        client = _fake_client()
        mock_connect.return_value = client

        dest = ClickHouseDestination()
        dest.load([{"id": 1, "score": 0.5}], _config(), _options(mode="replace"))
        dest.load([{"id": 2, "score": 0.9}], _config(), _options(mode="replace"))

        # TRUNCATE should be called exactly once
        client.command.assert_called_once()

    @patch("drt.destinations.clickhouse.ClickHouseDestination._connect")
    def test_replace_no_truncate_on_normal_mode(self, mock_connect: MagicMock) -> None:
        client = _fake_client()
        mock_connect.return_value = client

        dest = ClickHouseDestination()
        dest.load([{"id": 1, "score": 0.5}], _config(), _options(mode="full"))

        client.command.assert_not_called()
