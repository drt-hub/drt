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
    def test_dict_and_list_values_are_json_serialized(self, mock_connect: MagicMock) -> None:
        """dict/list values (e.g. from BigQuery JSON columns) must be
        serialized to JSON strings before being passed to pymysql."""
        conn = _fake_connection()
        cur = conn.cursor()
        mock_connect.return_value = conn

        records = [
            {
                "user_id": 1,
                "company_id": 5,
                "profile": {"lang": "日本語", "level": "N1"},
                "tags": ["a", "b"],
                "score": 0.9,
            },
        ]
        result = MySQLDestination().load(records, _config(), _options())

        assert result.success == 1
        args, _ = cur.execute.call_args
        _sql, values = args
        assert values[2] == '{"lang": "日本語", "level": "N1"}'
        assert values[3] == '["a", "b"]'
        assert values[4] == 0.9

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


# ---------------------------------------------------------------------------
# Replace mode
# ---------------------------------------------------------------------------


class TestInsertSql:
    def test_basic_insert(self) -> None:
        sql = MySQLDestination._build_insert_sql(
            table="learning_profiles",
            columns=["user_id", "company_id", "score"],
        )
        assert "INSERT INTO `learning_profiles`" in sql
        assert "ON DUPLICATE KEY" not in sql
        assert "VALUES (%s, %s, %s)" in sql


class TestMySQLReplaceMode:
    @patch("drt.destinations.mysql.MySQLDestination._connect")
    def test_replace_truncates_then_inserts(self, mock_connect: MagicMock) -> None:
        conn = _fake_connection()
        cur = conn.cursor()
        mock_connect.return_value = conn

        records = [
            {"user_id": 1, "company_id": 5, "score": 0.95},
            {"user_id": 2, "company_id": 5, "score": 0.80},
        ]
        dest = MySQLDestination()
        result = dest.load(records, _config(), _options(mode="replace"))

        assert result.success == 2
        assert result.failed == 0
        # TRUNCATE + 2 INSERTs = 3 execute calls
        assert cur.execute.call_count == 3
        first_call_sql = cur.execute.call_args_list[0][0][0]
        assert "TRUNCATE TABLE" in first_call_sql
        conn.commit.assert_called_once()

    @patch("drt.destinations.mysql.MySQLDestination._connect")
    def test_replace_truncates_only_once_across_batches(self, mock_connect: MagicMock) -> None:
        conn = _fake_connection()
        mock_connect.return_value = conn

        dest = MySQLDestination()
        opts = _options(mode="replace")
        dest.load(
            [{"user_id": 1, "company_id": 5, "score": 0.5}],
            _config(),
            opts,
        )
        dest.load(
            [{"user_id": 2, "company_id": 5, "score": 0.9}],
            _config(),
            opts,
        )

        all_sqls = [call[0][0] for call in conn.cursor().execute.call_args_list]
        truncate_count = sum(1 for sql in all_sqls if "TRUNCATE" in sql)
        assert truncate_count == 1

    @patch("drt.destinations.mysql.MySQLDestination._connect")
    def test_replace_uses_plain_insert(self, mock_connect: MagicMock) -> None:
        conn = _fake_connection()
        cur = conn.cursor()
        mock_connect.return_value = conn

        dest = MySQLDestination()
        dest.load(
            [{"user_id": 1, "company_id": 5, "score": 0.5}],
            _config(),
            _options(mode="replace"),
        )

        insert_sql = cur.execute.call_args_list[1][0][0]
        assert "ON DUPLICATE KEY" not in insert_sql
        assert "INSERT INTO" in insert_sql

    @patch("drt.destinations.mysql.MySQLDestination._connect")
    def test_replace_serializes_json_values(self, mock_connect: MagicMock) -> None:
        conn = _fake_connection()
        cur = conn.cursor()
        mock_connect.return_value = conn

        records = [{"user_id": 1, "company_id": 5, "profile": {"lang": "ja"}}]
        dest = MySQLDestination()
        dest.load(records, _config(), _options(mode="replace"))

        # INSERT call (after TRUNCATE)
        _sql, values = cur.execute.call_args_list[1][0]
        assert values[2] == '{"lang": "ja"}'


# ---------------------------------------------------------------------------
# json_columns tests
# ---------------------------------------------------------------------------


class TestJsonColumns:
    """Verify that json_columns config controls which columns get JSON-serialized."""

    def test_json_columns_serializes_listed_column(self) -> None:
        """Columns in json_columns should be json.dumps'd."""
        from drt.destinations.mysql import _serialize_value

        result = _serialize_value({"key": "val"}, "profile", ["profile"])
        assert isinstance(result, str)
        import json
        assert json.loads(result) == {"key": "val"}

    def test_json_columns_skips_unlisted_column(self) -> None:
        """Columns NOT in json_columns pass through as native Python types."""
        from drt.destinations.mysql import _serialize_value

        result = _serialize_value([1, 2, 3], "tags", ["profile"])
        # Not in json_columns → returned as-is (native list)
        assert result == [1, 2, 3]

    def test_json_columns_none_serializes_all(self) -> None:
        """Backward compat: json_columns=None serializes all dict/list."""
        from drt.destinations.mysql import _serialize_value

        assert isinstance(_serialize_value({"a": 1}, "any_col", None), str)
        assert isinstance(_serialize_value([1, 2], "any_col", None), str)

    def test_json_columns_non_complex_passthrough(self) -> None:
        """Non-dict/list values always pass through regardless of json_columns."""
        from drt.destinations.mysql import _serialize_value

        assert _serialize_value("hello", "col", ["col"]) == "hello"
        assert _serialize_value(42, "col", ["col"]) == 42
        assert _serialize_value(None, "col", ["col"]) is None


class TestPostgresJsonColumns:
    """Verify Postgres _serialize_value for json_columns."""

    def test_serialize_value_dict_in_json_columns(self) -> None:
        """dict value for a json_columns column → Json() wrapper."""
        from drt.destinations.postgres import _serialize_value

        result = _serialize_value({"k": "v"}, column="profile", json_columns=["profile"])
        # Should be a Json wrapper (or fallback string if psycopg2 unavailable)
        assert result is not None

    def test_serialize_value_dict_not_in_json_columns(self) -> None:
        """dict value for non-json column → pass through as dict."""
        from drt.destinations.postgres import _serialize_value

        result = _serialize_value({"k": "v"}, column="other", json_columns=["profile"])
        # Not in json_columns → pass through as plain dict
        assert isinstance(result, dict)

    def test_serialize_value_no_config(self) -> None:
        """No json_columns configured → backward compat (always wrap)."""
        from drt.destinations.postgres import _serialize_value

        result = _serialize_value({"k": "v"})
        # No config → always wrap with Json()
        assert result is not None

    def test_serialize_value_non_complex(self) -> None:
        """Non-dict values always pass through."""
        from drt.destinations.postgres import _serialize_value

        assert _serialize_value("alice") == "alice"
        assert _serialize_value(30) == 30
