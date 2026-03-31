"""Unit tests for MySQL destination.

Uses a fake pymysql connection — no real database required.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.models import MySQLDestinationConfig, SyncOptions
from drt.destinations.mysql import MySQLDestination

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _options(**kwargs: Any) -> SyncOptions:
    return SyncOptions(**kwargs)


def _config(**overrides: Any) -> MySQLDestinationConfig:
    defaults: dict[str, Any] = {
        "type": "mysql",
        "host": "localhost",
        "dbname": "testdb",
        "user": "testuser",
        "password": "testpass",
        "table": "learning_profiles",
        "upsert_key": ["user_id", "company_id"],
    }
    defaults.update(overrides)
    return MySQLDestinationConfig(**defaults)


def _fake_connection() -> MagicMock:
    conn = MagicMock()
    conn.cursor.return_value = MagicMock()
    return conn


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


class TestMySQLDestinationConfig:
    def test_valid_config(self) -> None:
        config = _config()
        assert config.table == "learning_profiles"
        assert config.upsert_key == ["user_id", "company_id"]
        assert config.port == 3306

    def test_host_env_instead_of_host(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MYSQL_HOST", "db.example.com")
        config = _config(host=None, host_env="MYSQL_HOST")
        assert config.host_env == "MYSQL_HOST"

    def test_missing_host_and_host_env_raises(self) -> None:
        with pytest.raises(ValueError, match="host"):
            _config(host=None, host_env=None)

    def test_missing_dbname_and_dbname_env_raises(self) -> None:
        with pytest.raises(ValueError, match="dbname"):
            _config(dbname=None, dbname_env=None)


# ---------------------------------------------------------------------------
# SQL generation
# ---------------------------------------------------------------------------


class TestUpsertSql:
    def test_basic_upsert(self) -> None:
        sql = MySQLDestination._build_upsert_sql(
            table="learning_profiles",
            columns=["user_id", "company_id", "score"],
            update_cols=["score"],
        )
        assert "INSERT INTO `learning_profiles`" in sql
        assert "ON DUPLICATE KEY UPDATE" in sql
        assert "`score` = VALUES(`score`)" in sql

    def test_composite_upsert_key(self) -> None:
        sql = MySQLDestination._build_upsert_sql(
            table="results",
            columns=["user_id", "metric_id", "value"],
            update_cols=["value"],
        )
        assert "`user_id`, `metric_id`, `value`" in sql
        assert "`value` = VALUES(`value`)" in sql

    def test_all_columns_are_key_uses_insert_ignore(self) -> None:
        sql = MySQLDestination._build_upsert_sql(
            table="lookup",
            columns=["id"],
            update_cols=[],
        )
        assert "INSERT IGNORE INTO" in sql
        assert "ON DUPLICATE KEY" not in sql


# ---------------------------------------------------------------------------
# Load behavior
# ---------------------------------------------------------------------------


class TestMySQLDestinationLoad:
    @patch("drt.destinations.mysql.MySQLDestination._connect")
    def test_success_upsert(self, mock_connect: MagicMock) -> None:
        conn = _fake_connection()
        mock_connect.return_value = conn

        records = [
            {"user_id": 1, "company_id": 5, "score": 0.95},
            {"user_id": 2, "company_id": 5, "score": 0.80},
        ]
        result = MySQLDestination().load(records, _config(), _options())

        assert result.success == 2
        assert result.failed == 0
        assert conn.cursor().execute.call_count == 2
        conn.commit.assert_called_once()

    @patch("drt.destinations.mysql.MySQLDestination._connect")
    def test_empty_records(self, mock_connect: MagicMock) -> None:
        result = MySQLDestination().load([], _config(), _options())
        assert result.success == 0
        assert result.failed == 0
        mock_connect.assert_not_called()

    @patch("drt.destinations.mysql.MySQLDestination._connect")
    def test_row_error_on_error_skip(self, mock_connect: MagicMock) -> None:
        conn = _fake_connection()
        cur = conn.cursor()
        cur.execute.side_effect = [Exception("duplicate key"), None]
        new_cur = MagicMock()
        conn.cursor.side_effect = [cur, new_cur]
        mock_connect.return_value = conn

        records = [
            {"user_id": 1, "company_id": 5, "score": 0.5},
            {"user_id": 2, "company_id": 5, "score": 0.9},
        ]
        result = MySQLDestination().load(records, _config(), _options(on_error="skip"))

        assert result.failed == 1
        assert result.success == 1
        assert len(result.row_errors) == 1
        assert "duplicate key" in result.row_errors[0].error_message

    @patch("drt.destinations.mysql.MySQLDestination._connect")
    def test_row_error_on_error_fail(self, mock_connect: MagicMock) -> None:
        conn = _fake_connection()
        conn.cursor().execute.side_effect = Exception("constraint violation")
        mock_connect.return_value = conn

        records = [
            {"user_id": 1, "company_id": 5, "score": 0.5},
            {"user_id": 2, "company_id": 5, "score": 0.9},
        ]
        result = MySQLDestination().load(records, _config(), _options(on_error="fail"))

        assert result.failed == 1
        assert result.success == 0
        conn.rollback.assert_called_once()

    @patch("drt.destinations.mysql.MySQLDestination._connect")
    def test_connection_closed_on_success(self, mock_connect: MagicMock) -> None:
        conn = _fake_connection()
        mock_connect.return_value = conn

        MySQLDestination().load(
            [{"user_id": 1, "company_id": 5, "score": 0.5}], _config(), _options()
        )
        conn.close.assert_called_once()

    @patch("drt.destinations.mysql.MySQLDestination._connect")
    def test_connection_closed_on_error(self, mock_connect: MagicMock) -> None:
        conn = _fake_connection()
        conn.cursor().execute.side_effect = Exception("fail")
        mock_connect.return_value = conn

        MySQLDestination().load(
            [{"user_id": 1, "company_id": 5, "score": 0.5}],
            _config(),
            _options(on_error="fail"),
        )
        conn.close.assert_called_once()
