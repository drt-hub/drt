"""Unit tests for MySQL source connector."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from drt.config.credentials import MySQLProfile
from drt.sources.mysql import MySQLSource


def _profile(**overrides) -> MySQLProfile:
    defaults = {
        "type": "mysql",
        "host": "localhost",
        "port": 3306,
        "dbname": "testdb",
        "user": "testuser",
        "password": "testpass",
    }
    defaults.update(overrides)
    return MySQLProfile(**defaults)


class TestMySQLSourceExtract:
    def test_extract_returns_dicts(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
        mock_conn.cursor.return_value = mock_cursor

        with patch("drt.sources.mysql.MySQLSource._connect", return_value=mock_conn):
            source = MySQLSource()
            rows = list(source.extract("SELECT * FROM users", _profile()))

        assert rows == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        mock_conn.close.assert_called_once()

    def test_extract_empty_result(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",)]
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cursor

        with patch("drt.sources.mysql.MySQLSource._connect", return_value=mock_conn):
            source = MySQLSource()
            rows = list(source.extract("SELECT * FROM empty_table", _profile()))

        assert rows == []
        mock_conn.close.assert_called_once()

    def test_extract_closes_connection_on_error(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("query failed")
        mock_conn.cursor.return_value = mock_cursor

        with patch("drt.sources.mysql.MySQLSource._connect", return_value=mock_conn):
            source = MySQLSource()
            with pytest.raises(Exception, match="query failed"):
                list(source.extract("SELECT bad", _profile()))

        mock_conn.close.assert_called_once()


class TestMySQLSourceTestConnection:
    def test_success(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("drt.sources.mysql.MySQLSource._connect", return_value=mock_conn):
            source = MySQLSource()
            assert source.test_connection(_profile()) is True

        mock_conn.close.assert_called_once()

    def test_failure(self):
        with patch(
            "drt.sources.mysql.MySQLSource._connect",
            side_effect=Exception("connection refused"),
        ):
            source = MySQLSource()
            assert source.test_connection(_profile()) is False


class TestMySQLSourceConnect:
    def test_uses_resolve_env(self, monkeypatch):
        monkeypatch.setenv("MYSQL_PASSWORD", "env_secret")

        mock_pymysql = MagicMock()
        mock_pymysql.connect.return_value = MagicMock()
        monkeypatch.setitem(__import__("sys").modules, "pymysql", mock_pymysql)

        source = MySQLSource()
        source._connect(_profile(password=None, password_env="MYSQL_PASSWORD"))

        mock_pymysql.connect.assert_called_once()
        call_kwargs = mock_pymysql.connect.call_args[1]
        assert call_kwargs["password"] == "env_secret"
        assert call_kwargs["charset"] == "utf8mb4"

    def test_missing_pymysql_raises(self, monkeypatch):
        monkeypatch.setitem(__import__("sys").modules, "pymysql", None)

        source = MySQLSource()
        with pytest.raises(ImportError, match="MySQL support requires"):
            source._connect(_profile())
