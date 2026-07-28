"""Unit tests for Redshift source connector.

These tests mock psycopg2 and validate real RedshiftSource behavior without
connecting to a live Redshift cluster.
"""

from __future__ import annotations

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from drt.config.credentials import RedshiftProfile
from drt.sources.base import Source
from drt.sources.redshift import RedshiftSource


def _config(**overrides: object) -> RedshiftProfile:
    defaults: dict[str, object] = {
        "type": "redshift",
        "host": "cluster.example.redshift.amazonaws.com",
        "port": 5439,
        "dbname": "analytics",
        "user": "analyst",
        "schema": "public",
    }
    defaults.update(overrides)
    return RedshiftProfile(**defaults)


def _fake_connection() -> MagicMock:
    conn = MagicMock()
    conn.cursor.return_value = MagicMock()
    return conn


def test_redshift_profile_defaults() -> None:
    """RedshiftProfile has sensible defaults (port=5439, not Postgres 5432)."""
    profile = RedshiftProfile(type="redshift")
    assert profile.port == 5439
    assert profile.schema == "public"
    assert profile.host == ""
    assert profile.dbname == ""


def test_redshift_source_implements_source_protocol() -> None:
    source = RedshiftSource()
    assert isinstance(source, Source)


def test_connect_uses_explicit_password_over_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REDSHIFT_PASSWORD", "env-secret")
    conn = _fake_connection()
    connect = MagicMock(return_value=conn)
    fake_psycopg2 = SimpleNamespace(connect=connect)
    monkeypatch.setitem(sys.modules, "psycopg2", fake_psycopg2)

    profile = _config(password="explicit-secret", password_env="REDSHIFT_PASSWORD")
    RedshiftSource()._connect(profile)

    connect.assert_called_once_with(
        host="cluster.example.redshift.amazonaws.com",
        port=5439,
        dbname="analytics",
        user="analyst",
        password="explicit-secret",
    )


def test_connect_reads_password_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REDSHIFT_PASSWORD", "env-secret")
    conn = _fake_connection()
    connect = MagicMock(return_value=conn)
    fake_psycopg2 = SimpleNamespace(connect=connect)
    monkeypatch.setitem(sys.modules, "psycopg2", fake_psycopg2)

    profile = _config(password=None, password_env="REDSHIFT_PASSWORD")
    RedshiftSource()._connect(profile)

    connect.assert_called_once_with(
        host="cluster.example.redshift.amazonaws.com",
        port=5439,
        dbname="analytics",
        user="analyst",
        password="env-secret",
    )


def test_extract_maps_rows_to_dicts_and_sets_schema() -> None:
    source = RedshiftSource()
    conn = _fake_connection()
    cur = conn.cursor.return_value
    cur.description = [("id",), ("email",)]
    cur.fetchall.return_value = [(1, "a@example.com"), (2, "b@example.com")]

    with patch.object(source, "_connect", return_value=conn):
        result = list(source.extract("SELECT id, email FROM users", _config(schema="analytics")))

    assert result == [
        {"id": 1, "email": "a@example.com"},
        {"id": 2, "email": "b@example.com"},
    ]
    assert cur.execute.call_count == 2
    assert cur.execute.call_args_list[1].args[0] == "SELECT id, email FROM users"
    conn.close.assert_called_once()


def test_extract_incremental_query_passthrough() -> None:
    source = RedshiftSource()
    conn = _fake_connection()
    cur = conn.cursor.return_value
    cur.description = [("id",), ("updated_at",)]
    cur.fetchall.return_value = [(42, "2026-03-31T00:00:00")]

    query = (
        "SELECT id, updated_at FROM events "
        "WHERE updated_at > '2026-03-01T00:00:00' ORDER BY updated_at"
    )
    with patch.object(source, "_connect", return_value=conn):
        result = list(source.extract(query, _config(schema="public")))

    assert result == [{"id": 42, "updated_at": "2026-03-31T00:00:00"}]
    assert cur.execute.call_args_list[1].args[0] == query


def test_extract_raises_on_query_error_and_closes_connection() -> None:
    source = RedshiftSource()
    conn = _fake_connection()
    cur = conn.cursor.return_value
    cur.execute.side_effect = [None, RuntimeError("query failed")]

    with patch.object(source, "_connect", return_value=conn):
        with pytest.raises(RuntimeError, match="query failed"):
            list(source.extract("SELECT * FROM broken", _config(schema="analytics")))

    conn.close.assert_called_once()


def test_test_connection_success() -> None:
    source = RedshiftSource()
    conn = _fake_connection()

    with patch.object(source, "_connect", return_value=conn):
        ok = source.test_connection(_config())

    assert ok is True
    conn.cursor.return_value.execute.assert_called_once_with("SELECT 1")
    conn.close.assert_called_once()


def test_test_connection_returns_false_on_connection_error() -> None:
    source = RedshiftSource()

    with patch.object(source, "_connect", side_effect=RuntimeError("cannot connect")):
        ok = source.test_connection(_config())

    assert ok is False


def test_test_connection_returns_false_on_query_error() -> None:
    source = RedshiftSource()
    conn = _fake_connection()
    conn.cursor.return_value.execute.side_effect = RuntimeError("bad query")

    with patch.object(source, "_connect", return_value=conn):
        ok = source.test_connection(_config())

    assert ok is False


# ---------------------------------------------------------------------------
# Transient-failure retry (#766)
# ---------------------------------------------------------------------------


def _rows_connection(rows: list[tuple]) -> MagicMock:
    conn = MagicMock()
    cur = MagicMock()
    cur.description = [("id",), ("email",)]
    cur.fetchall.return_value = rows
    conn.cursor.return_value = cur
    return conn


@pytest.mark.parametrize("exc_name", ["OperationalError", "InterfaceError"])
def test_is_transient_true_for_connection_errors(exc_name: str) -> None:
    import psycopg2

    exc = getattr(psycopg2, exc_name)("cluster is resuming")
    assert RedshiftSource()._is_transient(exc) is True


@pytest.mark.parametrize(
    "exc_name", ["ProgrammingError", "DataError", "IntegrityError", "DatabaseError"]
)
def test_is_transient_false_for_permanent_errors(exc_name: str) -> None:
    """Siblings under DatabaseError must not be swept in by a base-class check."""
    import psycopg2

    exc = getattr(psycopg2, exc_name)("relation does not exist")
    assert RedshiftSource()._is_transient(exc) is False


def test_extract_retries_transient_connection_failure() -> None:
    """A paused serverless workgroup resuming shouldn't fail the sync."""
    import psycopg2

    attempts: list[int] = []

    def connect(_config: object) -> MagicMock:
        attempts.append(1)
        if len(attempts) < 3:
            raise psycopg2.OperationalError("could not connect to server")
        return _rows_connection([(1, "a@example.com")])

    with patch.object(RedshiftSource, "_connect", side_effect=connect):
        with patch("drt.destinations.retry.time.sleep"):
            rows = list(RedshiftSource().extract("SELECT id, email FROM users", _config()))

    assert rows == [{"id": 1, "email": "a@example.com"}]
    assert len(attempts) == 3


def test_extract_does_not_retry_permanent_error() -> None:
    import psycopg2

    attempts: list[int] = []

    def connect(_config: object) -> MagicMock:
        attempts.append(1)
        raise psycopg2.ProgrammingError('relation "nope" does not exist')

    with patch.object(RedshiftSource, "_connect", side_effect=connect):
        with patch("drt.destinations.retry.time.sleep") as sleep:
            with pytest.raises(psycopg2.ProgrammingError):
                list(RedshiftSource().extract("SELECT * FROM nope", _config()))

    assert len(attempts) == 1
    sleep.assert_not_called()


def test_extract_does_not_retry_after_first_row() -> None:
    """Scope boundary (#766): a yielded row cannot be un-sent."""
    import psycopg2

    attempts: list[int] = []

    def connect(_config: object) -> MagicMock:
        attempts.append(1)
        conn = _rows_connection([])

        def exploding_rows():
            yield (1, "a@example.com")
            raise psycopg2.OperationalError("connection reset by peer")

        conn.cursor.return_value.fetchall.return_value = exploding_rows()
        return conn

    with patch.object(RedshiftSource, "_connect", side_effect=connect):
        with patch("drt.destinations.retry.time.sleep"):
            gen = RedshiftSource().extract("SELECT id, email FROM users", _config())
            assert next(gen) == {"id": 1, "email": "a@example.com"}
            with pytest.raises(psycopg2.OperationalError):
                next(gen)

    assert len(attempts) == 1
