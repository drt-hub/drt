"""Unit tests for PostgreSQL destination.

Uses a fake psycopg2 connection — no real database required.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from psycopg2.extras import Json

from drt.config.models import PostgresDestinationConfig, SyncOptions
from drt.destinations.postgres import PostgresDestination

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _options(**kwargs: Any) -> SyncOptions:
    return SyncOptions(**kwargs)


def _config(**overrides: Any) -> PostgresDestinationConfig:
    defaults: dict[str, Any] = {
        "type": "postgres",
        "host": "localhost",
        "dbname": "testdb",
        "user": "testuser",
        "password": "testpass",
        "table": "public.scores",
        "upsert_key": ["id"],
    }
    defaults.update(overrides)
    return PostgresDestinationConfig(**defaults)


def _fake_connection() -> MagicMock:
    conn = MagicMock()
    conn.cursor.return_value = MagicMock()
    return conn


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


class TestPostgresDestinationConfig:
    def test_valid_config(self) -> None:
        config = _config()
        assert config.table == "public.scores"
        assert config.upsert_key == ["id"]

    def test_host_env_instead_of_host(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PG_HOST", "db.example.com")
        config = _config(host=None, host_env="PG_HOST")
        assert config.host_env == "PG_HOST"

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
        sql = PostgresDestination._build_upsert_sql(
            table="public.scores",
            columns=["id", "score", "updated_at"],
            upsert_key=["id"],
            update_cols=["score", "updated_at"],
        )
        assert 'INSERT INTO public.scores ("id", "score", "updated_at")' in sql
        assert "ON CONFLICT" in sql
        assert 'DO UPDATE SET "score" = EXCLUDED."score"' in sql

    def test_composite_upsert_key(self) -> None:
        sql = PostgresDestination._build_upsert_sql(
            table="results",
            columns=["user_id", "metric_id", "value"],
            upsert_key=["user_id", "metric_id"],
            update_cols=["value"],
        )
        assert '"user_id", "metric_id"' in sql
        assert 'DO UPDATE SET "value" = EXCLUDED."value"' in sql

    def test_all_columns_are_key_does_nothing(self) -> None:
        sql = PostgresDestination._build_upsert_sql(
            table="lookup",
            columns=["id"],
            upsert_key=["id"],
            update_cols=[],
        )
        assert "DO NOTHING" in sql


# ---------------------------------------------------------------------------
# Load behavior
# ---------------------------------------------------------------------------


class TestPostgresDestinationLoad:
    @patch("drt.destinations.postgres.PostgresDestination._connect")
    def test_success_upsert(self, mock_connect: MagicMock) -> None:
        conn = _fake_connection()
        mock_connect.return_value = conn

        records = [
            {"id": 1, "score": 0.95, "updated_at": "2026-03-31"},
            {"id": 2, "score": 0.80, "updated_at": "2026-03-31"},
        ]
        result = PostgresDestination().load(records, _config(), _options())

        assert result.success == 2
        assert result.failed == 0
        assert conn.cursor().execute.call_count == 2
        conn.commit.assert_called_once()

    @patch("drt.destinations.postgres.PostgresDestination._connect")
    def test_empty_records(self, mock_connect: MagicMock) -> None:
        result = PostgresDestination().load([], _config(), _options())
        assert result.success == 0
        assert result.failed == 0
        mock_connect.assert_not_called()

    @patch("drt.destinations.postgres.PostgresDestination._connect")
    def test_row_error_on_error_skip(self, mock_connect: MagicMock) -> None:
        conn = _fake_connection()
        cur = conn.cursor()
        # First row fails, second succeeds
        cur.execute.side_effect = [Exception("duplicate key"), None]
        # After rollback, return a fresh cursor for the second row
        new_cur = MagicMock()
        conn.cursor.side_effect = [cur, new_cur]
        mock_connect.return_value = conn

        records = [
            {"id": 1, "score": 0.5},
            {"id": 2, "score": 0.9},
        ]
        result = PostgresDestination().load(records, _config(), _options(on_error="skip"))

        assert result.failed == 1
        assert result.success == 1
        assert len(result.row_errors) == 1
        assert "duplicate key" in result.row_errors[0].error_message

    @patch("drt.destinations.postgres.PostgresDestination._connect")
    def test_row_error_on_error_fail(self, mock_connect: MagicMock) -> None:
        conn = _fake_connection()
        conn.cursor().execute.side_effect = Exception("constraint violation")
        mock_connect.return_value = conn

        records = [
            {"id": 1, "score": 0.5},
            {"id": 2, "score": 0.9},
        ]
        result = PostgresDestination().load(records, _config(), _options(on_error="fail"))

        assert result.failed == 1
        assert result.success == 0
        # Should stop after first failure
        conn.rollback.assert_called_once()

    @patch("drt.destinations.postgres.PostgresDestination._connect")
    def test_connection_closed_on_success(self, mock_connect: MagicMock) -> None:
        conn = _fake_connection()
        mock_connect.return_value = conn

        PostgresDestination().load([{"id": 1, "score": 0.5}], _config(), _options())
        conn.close.assert_called_once()

    @patch("drt.destinations.postgres.PostgresDestination._connect")
    def test_connection_closed_on_error(self, mock_connect: MagicMock) -> None:
        conn = _fake_connection()
        conn.cursor().execute.side_effect = Exception("fail")
        mock_connect.return_value = conn

        PostgresDestination().load([{"id": 1, "score": 0.5}], _config(), _options(on_error="fail"))
        conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# Value serialization
# ---------------------------------------------------------------------------


class TestSerializeValue:
    """Tests for _serialize_value — dict→Json wrapping for JSONB columns."""

    def test_dict_wrapped_as_json(self) -> None:
        from drt.destinations.postgres import _serialize_value

        val = {"lang": "ja", "theme": "dark"}
        result = _serialize_value(val)
        # Should be a psycopg2.extras.Json instance
        assert isinstance(result, Json)
        # The wrapped value should match the original dict
        assert result.adapted == val

    def test_non_dict_passes_through(self) -> None:
        from drt.destinations.postgres import _serialize_value

        assert _serialize_value("hello") == "hello"
        assert _serialize_value(42) == 42
        assert _serialize_value(3.14) == 3.14
        assert _serialize_value(None) is None
        assert _serialize_value([1, 2, 3]) == [1, 2, 3]

    def test_empty_dict_wrapped(self) -> None:
        from drt.destinations.postgres import _serialize_value

        result = _serialize_value({})
        assert isinstance(result, Json)
        assert result.adapted == {}

    @patch("drt.destinations.postgres.PostgresDestination._connect")
    def test_dict_value_in_upsert_is_serialized(self, mock_connect: MagicMock) -> None:
        """Integration: a record with a dict column passes Json to execute()."""
        conn = _fake_connection()
        cur = conn.cursor()
        mock_connect.return_value = conn

        records = [
            {"id": 1, "profile": {"lang": "ja", "theme": "dark"}},
        ]
        PostgresDestination().load(records, _config(), _options())

        # execute() should have been called with a Json-wrapped profile
        call_args = cur.execute.call_args[0][1]  # positional args: (sql, values)
        profile_val = call_args[1]  # second column value
        assert isinstance(profile_val, Json)
        assert profile_val.adapted == {"lang": "ja", "theme": "dark"}

    @patch("drt.destinations.postgres.PostgresDestination._connect")
    def test_dict_value_in_replace_is_serialized(self, mock_connect: MagicMock) -> None:
        """Integration: replace mode also wraps dict values with Json."""
        conn = _fake_connection()
        cur = conn.cursor()
        mock_connect.return_value = conn

        records = [
            {"id": 1, "settings": {"notify": True}},
        ]
        dest = PostgresDestination()
        dest.load(records, _config(), _options(mode="replace"))

        # Find the INSERT call (after TRUNCATE)
        insert_call = cur.execute.call_args_list[1]
        values = insert_call[0][1]
        settings_val = values[1]
        assert isinstance(settings_val, Json)
        assert settings_val.adapted == {"notify": True}


# ---------------------------------------------------------------------------
# Replace mode
# ---------------------------------------------------------------------------


class TestInsertSql:
    def test_basic_insert(self) -> None:
        sql = PostgresDestination._build_insert_sql(
            table="public.scores",
            columns=["id", "score", "updated_at"],
        )
        assert 'INSERT INTO public.scores ("id", "score", "updated_at")' in sql
        assert "ON CONFLICT" not in sql
        assert "VALUES (%s, %s, %s)" in sql


class TestPostgresReplaceMode:
    @patch("drt.destinations.postgres.PostgresDestination._connect")
    def test_replace_truncates_then_inserts(self, mock_connect: MagicMock) -> None:
        conn = _fake_connection()
        cur = conn.cursor()
        mock_connect.return_value = conn

        records = [
            {"id": 1, "score": 0.95},
            {"id": 2, "score": 0.80},
        ]
        dest = PostgresDestination()
        result = dest.load(records, _config(), _options(mode="replace"))

        assert result.success == 2
        assert result.failed == 0
        # TRUNCATE + 2 INSERTs = 3 execute calls
        assert cur.execute.call_count == 3
        first_call_sql = cur.execute.call_args_list[0][0][0]
        assert "TRUNCATE TABLE" in first_call_sql
        conn.commit.assert_called_once()

    @patch("drt.destinations.postgres.PostgresDestination._connect")
    def test_replace_truncates_only_once_across_batches(self, mock_connect: MagicMock) -> None:
        conn = _fake_connection()
        mock_connect.return_value = conn

        dest = PostgresDestination()
        # First batch
        dest.load([{"id": 1, "score": 0.5}], _config(), _options(mode="replace"))
        # Second batch — should NOT truncate again
        dest.load([{"id": 2, "score": 0.9}], _config(), _options(mode="replace"))

        all_sqls = [call[0][0] for call in conn.cursor().execute.call_args_list]
        truncate_count = sum(1 for sql in all_sqls if "TRUNCATE" in sql)
        assert truncate_count == 1

    @patch("drt.destinations.postgres.PostgresDestination._connect")
    def test_replace_uses_plain_insert(self, mock_connect: MagicMock) -> None:
        conn = _fake_connection()
        cur = conn.cursor()
        mock_connect.return_value = conn

        dest = PostgresDestination()
        dest.load([{"id": 1, "score": 0.5}], _config(), _options(mode="replace"))

        # The INSERT call (second execute, after TRUNCATE)
        insert_sql = cur.execute.call_args_list[1][0][0]
        assert "ON CONFLICT" not in insert_sql
        assert "INSERT INTO" in insert_sql
