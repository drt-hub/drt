"""Unit tests for Snowflake source.

Uses a mock snowflake-connector-python — no real database required.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.credentials import SnowflakeProfile
from drt.sources.snowflake import SnowflakeSource


def _config(**overrides: Any) -> SnowflakeProfile:
    defaults: dict[str, Any] = {
        "type": "snowflake",
        "account": "xy12345.us-east-1",
        "user": "analyst",
        "password": "testpassword",
        "database": "ANALYTICS",
        "schema": "PUBLIC",
        "warehouse": "COMPUTE_WH",
    }
    defaults.update(overrides)
    return SnowflakeProfile(**defaults)


def _fake_cursor(columns, rows):
    cur = MagicMock()
    cur.description = [(col,) for col in columns]
    cur.fetchall.return_value = rows
    return cur


def _fake_conn(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


class TestSnowflakeSource:
    def test_extract_returns_rows(self) -> None:
        source = SnowflakeSource()
        config = _config()
        cur = _fake_cursor(["id", "name"], [(1, "Alice"), (2, "Bob")])
        conn = _fake_conn(cur)
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            results = list(source.extract("SELECT * FROM users", config))
        assert len(results) == 2
        assert results[0] == {"id": 1, "name": "Alice"}
        assert results[1] == {"id": 2, "name": "Bob"}
        cur.close.assert_called_once()
        conn.close.assert_called_once()

    def test_extract_empty_result(self) -> None:
        source = SnowflakeSource()
        config = _config()
        cur = _fake_cursor(["id"], [])
        conn = _fake_conn(cur)
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            results = list(source.extract("SELECT * FROM empty_table", config))
        assert results == []
        conn.close.assert_called_once()

    def test_test_connection_success(self) -> None:
        source = SnowflakeSource()
        config = _config()
        cur = _fake_cursor(["1"], [(1,)])
        conn = _fake_conn(cur)
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            assert source.test_connection(config) is True
        cur.execute.assert_called_with("SELECT 1")
        cur.close.assert_called_once()
        conn.close.assert_called_once()

    def test_test_connection_failure(self) -> None:
        source = SnowflakeSource()
        config = _config()
        with patch.object(SnowflakeSource, "_connect", side_effect=Exception("fail")):
            assert source.test_connection(config) is False

    def test_connect_import_error(self) -> None:
        source = SnowflakeSource()
        config = _config()
        with patch("builtins.__import__", side_effect=ImportError):
            with pytest.raises(ImportError, match="Snowflake support requires"):
                source._connect(config)

    def test_connect_parameters(self) -> None:
        source = SnowflakeSource()
        config = _config(role="ADMIN_ROLE")
        mock_module = MagicMock()
        mock_connector = MagicMock()
        mock_module.connector = mock_connector
        with patch.dict("sys.modules", {"snowflake": mock_module, "snowflake.connector": mock_connector}):
            source._connect(config)
            mock_connector.connect.assert_called_once_with(
                account="xy12345.us-east-1",
                user="analyst",
                password="testpassword",
                database="ANALYTICS",
                schema="PUBLIC",
                warehouse="COMPUTE_WH",
                role="ADMIN_ROLE",
            )

    def test_connect_without_role(self) -> None:
        source = SnowflakeSource()
        config = _config()
        mock_module = MagicMock()
        mock_connector = MagicMock()
        mock_module.connector = mock_connector
        with patch.dict("sys.modules", {"snowflake": mock_module, "snowflake.connector": mock_connector}):
            source._connect(config)
            call_kwargs = mock_connector.connect.call_args[1]
            assert "role" not in call_kwargs

    def test_connect_password_from_env(self) -> None:
        source = SnowflakeSource()
        config = _config(password=None, password_env="SNOWFLAKE_PASSWORD")
        mock_module = MagicMock()
        mock_connector = MagicMock()
        mock_module.connector = mock_connector
        with patch.dict("sys.modules", {"snowflake": mock_module, "snowflake.connector": mock_connector}), \
             patch.dict("os.environ", {"SNOWFLAKE_PASSWORD": "env_secret"}):
            source._connect(config)
            call_kwargs = mock_connector.connect.call_args[1]
            assert call_kwargs["password"] == "env_secret"
