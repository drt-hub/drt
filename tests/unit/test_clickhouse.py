"""Unit tests for ClickHouse source.

Uses a mock clickhouse-connect client — no real database required.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.credentials import ClickHouseProfile
from drt.sources.clickhouse import ClickHouseSource

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _config(**overrides: Any) -> ClickHouseProfile:
    defaults: dict[str, Any] = {
        "type": "clickhouse",
        "host": "localhost",
        "port": 8123,
        "database": "default",
        "user": "default",
        "password": "testpassword",
    }
    defaults.update(overrides)
    return ClickHouseProfile(**defaults)


def _fake_client() -> MagicMock:
    client = MagicMock()
    return client


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestClickHouseSource:
    def test_extract_returns_rows(self) -> None:
        source = ClickHouseSource()
        config = _config()

        # Mock client and its query result
        mock_client = _fake_client()
        mock_result = MagicMock()
        mock_result.column_names = ["id", "name"]
        mock_result.result_rows = [(1, "Alice"), (2, "Bob")]
        mock_client.query.return_value = mock_result

        with patch.object(ClickHouseSource, "_connect", return_value=mock_client):
            results = list(source.extract("SELECT * FROM users", config))

        assert len(results) == 2
        assert results[0] == {"id": 1, "name": "Alice"}
        assert results[1] == {"id": 2, "name": "Bob"}
        mock_client.close.assert_called_once()

    def test_test_connection_success(self) -> None:
        source = ClickHouseSource()
        config = _config()

        mock_client = _fake_client()
        with patch.object(ClickHouseSource, "_connect", return_value=mock_client):
            assert source.test_connection(config) is True

        mock_client.query.assert_called_with("SELECT 1")
        mock_client.close.assert_called_once()

    def test_test_connection_failure(self) -> None:
        source = ClickHouseSource()
        config = _config()

        with patch.object(ClickHouseSource, "_connect", side_effect=Exception("Connection failed")):
            assert source.test_connection(config) is False

    def test_connect_import_error(self) -> None:
        source = ClickHouseSource()
        config = _config()

        with patch("builtins.__import__", side_effect=ImportError):
            with pytest.raises(ImportError, match="ClickHouse support requires"):
                source._connect(config)

    def test_connect_parameters(self) -> None:
        source = ClickHouseSource()
        config = _config(user="analyst", database="prod")

        mock_module = MagicMock()
        with patch.dict("sys.modules", {"clickhouse_connect": mock_module}):
            source._connect(config)

            mock_module.get_client.assert_called_once_with(
                host="localhost",
                port=8123,
                database="prod",
                username="analyst",
                password="testpassword",
            )
