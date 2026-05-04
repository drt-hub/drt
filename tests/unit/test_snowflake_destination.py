"""Unit tests for Snowflake destination.

Uses sys.modules injection to mock snowflake.connector — no real Snowflake
account or snowflake-connector-python install required (matches the pattern
in test_snowflake.py for the source-side connector).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.models import SnowflakeDestinationConfig, SyncOptions
from drt.destinations.snowflake import SnowflakeDestination

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _options(**kwargs: Any) -> SyncOptions:
    return SyncOptions(**kwargs)


def _config(**overrides: Any) -> SnowflakeDestinationConfig:
    defaults: dict[str, Any] = {
        "type": "snowflake",
        "account_env": "SF_ACCOUNT",
        "user_env": "SF_USER",
        "password_env": "SF_PASSWORD",
        "database": "ANALYTICS",
        "schema": "PUBLIC",  # alias form — populated into schema_ on the model
        "table": "USER_SCORES",
        "warehouse": "COMPUTE_WH",
    }
    defaults.update(overrides)
    return SnowflakeDestinationConfig.model_validate(defaults)


def _set_creds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SF_ACCOUNT", "acct.us-east-1")
    monkeypatch.setenv("SF_USER", "test_user")
    monkeypatch.setenv("SF_PASSWORD", "test_pass")


def _fake_conn() -> MagicMock:
    """Fake snowflake.connector connection with a context-managed cursor."""
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False
    conn._cur = cur  # for assertions
    return conn


def _mocked_snowflake_modules(conn: MagicMock | None = None) -> dict[str, MagicMock]:
    """Build sys.modules entries that satisfy `import snowflake.connector`."""
    mock_module = MagicMock()
    mock_connector = MagicMock()
    if conn is not None:
        mock_connector.connect.return_value = conn
    mock_module.connector = mock_connector
    return {"snowflake": mock_module, "snowflake.connector": mock_connector}


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


class TestSnowflakeDestinationConfig:
    def test_valid_config(self) -> None:
        config = _config()
        assert config.database == "ANALYTICS"
        assert config.schema_ == "PUBLIC"
        assert config.table == "USER_SCORES"
        assert config.mode == "insert"

    def test_yaml_uses_schema_alias(self) -> None:
        """YAML key `schema:` populates the `schema_` field (mypy-strict workaround)."""
        config = SnowflakeDestinationConfig.model_validate(
            {
                "type": "snowflake",
                "account_env": "SF_ACCOUNT",
                "user_env": "SF_USER",
                "password_env": "SF_PASSWORD",
                "database": "DB",
                "schema": "SCH",
                "table": "T",
                "warehouse": "WH",
            }
        )
        assert config.schema_ == "SCH"

    def test_describe_uses_schema(self) -> None:
        assert _config().describe() == "snowflake (ANALYTICS.PUBLIC.USER_SCORES)"


# ---------------------------------------------------------------------------
# Load behavior
# ---------------------------------------------------------------------------


class TestSnowflakeDestinationLoad:
    def test_empty_records_short_circuits_before_import(self) -> None:
        """No records → returns early before even attempting the snowflake import."""
        # No sys.modules patch; if load() reached the import it would raise.
        result = SnowflakeDestination().load([], _config(), _options())
        assert result.success == 0
        assert result.failed == 0

    def test_missing_credentials_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("SF_ACCOUNT", raising=False)
        monkeypatch.delenv("SF_USER", raising=False)
        monkeypatch.delenv("SF_PASSWORD", raising=False)
        with patch.dict("sys.modules", _mocked_snowflake_modules()):
            with pytest.raises(ValueError, match="Missing Snowflake credentials"):
                SnowflakeDestination().load([{"id": 1}], _config(), _options())

    def test_import_error_when_extras_missing(self) -> None:
        with patch("builtins.__import__", side_effect=ImportError):
            with pytest.raises(ImportError, match="drt-core\\[snowflake\\]"):
                SnowflakeDestination().load([{"id": 1}], _config(), _options())

    def test_insert_mode_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_snowflake_modules(conn)

        records = [
            {"id": 1, "score": 0.95},
            {"id": 2, "score": 0.80},
        ]
        with patch.dict("sys.modules", modules):
            result = SnowflakeDestination().load(records, _config(), _options())

        assert result.success == 2
        assert result.failed == 0
        cur = conn._cur
        assert cur.execute.call_count == 2
        first_sql = cur.execute.call_args_list[0][0][0]
        assert "INSERT INTO ANALYTICS.PUBLIC.USER_SCORES" in first_sql
        assert "id, score" in first_sql
        conn.close.assert_called_once()

    def test_merge_mode_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_snowflake_modules(conn)

        records = [
            {"id": 1, "score": 0.95},
            {"id": 2, "score": 0.80},
        ]
        config = _config(mode="merge", upsert_key=["id"])
        with patch.dict("sys.modules", modules):
            result = SnowflakeDestination().load(records, config, _options())

        assert result.success == 2
        sqls = [
            (call.args[0] if call.args else "")
            for call in conn._cur.execute.call_args_list
        ]
        assert any("CREATE TEMP TABLE" in s for s in sqls)
        assert any("MERGE INTO ANALYTICS.PUBLIC.USER_SCORES" in s for s in sqls)
        assert any("WHEN MATCHED THEN UPDATE" in s for s in sqls)

    def test_merge_mode_requires_upsert_key(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_creds(monkeypatch)
        modules = _mocked_snowflake_modules(_fake_conn())
        config = _config(mode="merge", upsert_key=None)
        with patch.dict("sys.modules", modules):
            with pytest.raises(ValueError, match="upsert_key is required"):
                SnowflakeDestination().load([{"id": 1}], config, _options())

    def test_insert_row_error_on_error_skip(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        conn._cur.execute.side_effect = [Exception("type mismatch"), None]
        modules = _mocked_snowflake_modules(conn)

        records = [
            {"id": 1, "score": 0.5},
            {"id": 2, "score": 0.9},
        ]
        with patch.dict("sys.modules", modules):
            result = SnowflakeDestination().load(
                records, _config(), _options(on_error="skip")
            )
        assert result.failed == 1
        assert result.success == 1
        assert len(result.row_errors) == 1
        assert "type mismatch" in result.row_errors[0].error_message
