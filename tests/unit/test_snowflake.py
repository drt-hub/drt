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
    # Since #765 rows come from iterating the cursor, not fetchall(). A fresh
    # iterator per call so a retried attempt re-reads rather than finding the
    # cursor exhausted.
    cur.__iter__.side_effect = lambda: iter(rows)
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
        modules = {
            "snowflake": mock_module,
            "snowflake.connector": mock_connector,
        }
        with patch.dict("sys.modules", modules):
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
        modules = {
            "snowflake": mock_module,
            "snowflake.connector": mock_connector,
        }
        with patch.dict("sys.modules", modules):
            source._connect(config)
            call_kwargs = mock_connector.connect.call_args[1]
            assert "role" not in call_kwargs

    def test_connect_password_from_env(self) -> None:
        source = SnowflakeSource()
        config = _config(password=None, password_env="SNOWFLAKE_PASSWORD")
        mock_module = MagicMock()
        mock_connector = MagicMock()
        mock_module.connector = mock_connector
        modules = {
            "snowflake": mock_module,
            "snowflake.connector": mock_connector,
        }
        with (
            patch.dict("sys.modules", modules),
            patch.dict("os.environ", {"SNOWFLAKE_PASSWORD": "env_secret"}),
        ):
            source._connect(config)
            call_kwargs = mock_connector.connect.call_args[1]
            assert call_kwargs["password"] == "env_secret"


class TestSnowflakeSourceKeyPairConnect:
    """Source _connect passes DER private_key for key-pair auth (#737)."""

    @staticmethod
    def _pem() -> str:
        pytest.importorskip("cryptography")
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        return key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode()

    def _profile(self, **auth: Any) -> SnowflakeProfile:
        return SnowflakeProfile(
            type="snowflake",
            account="acct",
            user="svc_user",
            database="DB",
            schema="PUBLIC",
            warehouse="WH",
            **auth,
        )

    def test_private_key_env_wins_and_passes_der(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("SF_PK", self._pem())
        fake = MagicMock()
        with patch.dict(
            "sys.modules", {"snowflake": fake, "snowflake.connector": fake.connector}
        ):
            SnowflakeSource()._connect(
                self._profile(private_key_env="SF_PK", password="ignored")
            )
        kwargs = fake.connector.connect.call_args.kwargs
        assert isinstance(kwargs["private_key"], bytes)  # DER bytes
        assert "password" not in kwargs

    def test_password_fallback_when_no_key(self) -> None:
        fake = MagicMock()
        with patch.dict(
            "sys.modules", {"snowflake": fake, "snowflake.connector": fake.connector}
        ):
            SnowflakeSource()._connect(self._profile(password="pw"))
        kwargs = fake.connector.connect.call_args.kwargs
        assert kwargs["password"] == "pw"
        assert "private_key" not in kwargs


# ---------------------------------------------------------------------------
# Transient-failure retry (#766)
# ---------------------------------------------------------------------------


class TestSnowflakeTransientClassification:
    """390114 (token expired) is a DatabaseError — and so are permanent errors.

    That collision is the reason ``with_retry`` takes a predicate rather than
    a tuple of exception types.
    """

    def test_token_expired_390114_is_transient(self) -> None:
        pytest.importorskip("snowflake.connector")
        from snowflake.connector import errors as sf_errors

        exc = sf_errors.DatabaseError(msg="Authentication token has expired", errno=390114)
        assert SnowflakeSource()._is_transient(exc) is True

    def test_operational_error_is_transient(self) -> None:
        pytest.importorskip("snowflake.connector")
        from snowflake.connector import errors as sf_errors

        assert SnowflakeSource()._is_transient(sf_errors.OperationalError(msg="conn lost")) is True

    def test_revocation_check_error_is_transient(self) -> None:
        """An unreachable CRL/OCSP endpoint — subclass of OperationalError."""
        pytest.importorskip("snowflake.connector")
        from snowflake.connector import errors as sf_errors

        exc = sf_errors.RevocationCheckError(msg="OCSP responder unreachable")
        assert SnowflakeSource()._is_transient(exc) is True

    def test_programming_error_is_not_transient(self) -> None:
        """ProgrammingError subclasses DatabaseError — must not be swept in."""
        pytest.importorskip("snowflake.connector")
        from snowflake.connector import errors as sf_errors

        exc = sf_errors.ProgrammingError(msg="SQL compilation error", errno=1003)
        assert SnowflakeSource()._is_transient(exc) is False

    def test_database_error_with_other_errno_is_not_transient(self) -> None:
        pytest.importorskip("snowflake.connector")
        from snowflake.connector import errors as sf_errors

        exc = sf_errors.DatabaseError(msg="something else", errno=1234)
        assert SnowflakeSource()._is_transient(exc) is False

    def test_unrelated_exception_is_not_transient(self) -> None:
        assert SnowflakeSource()._is_transient(ValueError("nope")) is False


class TestSnowflakeSourceRetry:
    def test_expired_token_is_retried_then_succeeds(self) -> None:
        """#654 saw long extracts outstay their session token."""
        pytest.importorskip("snowflake.connector")
        from snowflake.connector import errors as sf_errors

        attempts: list[int] = []

        def connect(_config: Any) -> MagicMock:
            attempts.append(1)
            if len(attempts) < 3:
                raise sf_errors.DatabaseError(
                    msg="Authentication token has expired", errno=390114
                )
            return _fake_conn(_fake_cursor(["id"], [(1,)]))

        with patch.object(SnowflakeSource, "_connect", side_effect=connect):
            with patch("drt.destinations.retry.time.sleep"):
                rows = list(SnowflakeSource().extract("SELECT id FROM t", _config()))

        assert rows == [{"id": 1}]
        assert len(attempts) == 3

    def test_sql_compilation_error_is_not_retried(self) -> None:
        pytest.importorskip("snowflake.connector")
        from snowflake.connector import errors as sf_errors

        attempts: list[int] = []

        def connect(_config: Any) -> MagicMock:
            attempts.append(1)
            raise sf_errors.ProgrammingError(msg="SQL compilation error", errno=1003)

        with patch.object(SnowflakeSource, "_connect", side_effect=connect):
            with patch("drt.destinations.retry.time.sleep") as sleep:
                with pytest.raises(sf_errors.ProgrammingError):
                    list(SnowflakeSource().extract("SELECT nope", _config()))

        assert len(attempts) == 1
        sleep.assert_not_called()

    def test_failure_after_first_row_is_not_retried(self) -> None:
        """Scope boundary (#766): a yielded row cannot be un-sent."""
        pytest.importorskip("snowflake.connector")
        from snowflake.connector import errors as sf_errors

        attempts: list[int] = []

        def connect(_config: Any) -> MagicMock:
            attempts.append(1)

            def exploding_rows():
                yield (1,)
                raise sf_errors.OperationalError(msg="connection reset")

            cur = MagicMock()
            cur.description = [("id",)]
            cur.__iter__.side_effect = exploding_rows
            return _fake_conn(cur)

        with patch.object(SnowflakeSource, "_connect", side_effect=connect):
            with patch("drt.destinations.retry.time.sleep"):
                gen = SnowflakeSource().extract("SELECT id FROM t", _config())
                assert next(gen) == {"id": 1}
                with pytest.raises(sf_errors.OperationalError):
                    next(gen)

        assert len(attempts) == 1


def _streaming_conn(rows, description=None):
    """A connection whose cursor streams rather than buffering.

    Snowflake's cursor is iterable and honours ``arraysize``, so the shape
    mirrors the Postgres leg rather than needing an explicit fetchmany loop.
    """
    conn = MagicMock()
    cur = conn.cursor.return_value
    cur.description = description if description is not None else [("id",), ("name",)]
    cur.__iter__.side_effect = lambda: iter(rows)
    return conn


class TestSnowflakeStreamingExtraction:
    """#765: SnowflakeSource streams instead of calling fetchall().

    ``fetchall()`` materialises the entire result set before the first row
    reaches the engine. The cursor is iterable and respects ``arraysize``, so
    iterating it fetches in batches of that size instead.
    """

    def test_does_not_call_fetchall(self):
        conn = _streaming_conn([(1, "Alice")])
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            list(SnowflakeSource().extract("SELECT 1", _config()))

        conn.cursor.return_value.fetchall.assert_not_called()

    def test_arraysize_comes_from_fetch_size(self):
        conn = _streaming_conn([(1, "Alice")])
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            list(SnowflakeSource().extract("SELECT 1", _config(fetch_size=2500)))

        assert conn.cursor.return_value.arraysize == 2500

    def test_rows_are_mapped_to_dicts(self):
        conn = _streaming_conn([(1, "Alice"), (2, "Bob")])
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            rows = list(SnowflakeSource().extract("SELECT 1", _config()))

        assert rows == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    def test_cursor_is_closed_before_the_connection(self):
        conn = _streaming_conn([(1, "Alice")])
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            list(SnowflakeSource().extract("SELECT 1", _config()))

        names = [c[0] for c in conn.mock_calls]
        assert "cursor().close" in names, "the cursor was never closed"
        assert names.index("cursor().close") < names.index("close")

    def test_connection_closes_when_the_generator_is_abandoned(self):
        """`--limit` / `--fail-fast` stop consuming mid-stream (#775/#774)."""
        conn = _streaming_conn([(i, "x") for i in range(100)])
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            gen = SnowflakeSource().extract("SELECT 1", _config())
            next(gen)
            gen.close()

        names = [c[0] for c in conn.mock_calls]
        assert "cursor().close" in names, "the cursor leaked on abandonment"
        assert names.index("cursor().close") < names.index("close")

    def test_empty_result_yields_nothing(self):
        conn = _streaming_conn([], description=[("id",)])
        with patch.object(SnowflakeSource, "_connect", return_value=conn):
            assert list(SnowflakeSource().extract("SELECT 1", _config())) == []
