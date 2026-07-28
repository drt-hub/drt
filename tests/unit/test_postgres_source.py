"""Unit tests for the PostgreSQL source connector.

Covers the transient-failure retry added in #766: a connection reset or an
expired session on the way *in* is retried; a permanent error (bad SQL) is
not, and neither is anything that happens after the first row is yielded.
No real database — ``_connect`` is patched throughout.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import psycopg2
import pytest

from drt.config.credentials import PostgresProfile
from drt.sources.postgres import PostgresSource


def _profile(**overrides: object) -> PostgresProfile:
    defaults: dict = {
        "type": "postgres",
        "host": "localhost",
        "port": 5432,
        "dbname": "analytics",
        "user": "analyst",
        "password": "secret",
    }
    return PostgresProfile(**{**defaults, **overrides})


def _mock_conn(rows: list[tuple] | None = None) -> MagicMock:
    conn = MagicMock()
    cur = MagicMock()
    cur.description = [("id",), ("name",)]
    cur.fetchall.return_value = [(1, "Alice"), (2, "Bob")] if rows is None else rows
    conn.cursor.return_value = cur
    return conn


class TestExtract:
    def test_extract_returns_dicts(self) -> None:
        with patch.object(PostgresSource, "_connect", return_value=_mock_conn()):
            rows = list(PostgresSource().extract("SELECT * FROM users", _profile()))

        assert rows == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    def test_extract_closes_the_connection(self) -> None:
        conn = _mock_conn()
        with patch.object(PostgresSource, "_connect", return_value=conn):
            list(PostgresSource().extract("SELECT 1", _profile()))

        conn.close.assert_called_once()


class TestTransientClassification:
    """_is_transient must key on the exact class, not the shared base.

    psycopg2's OperationalError, ProgrammingError and DataError are all
    siblings under DatabaseError, so a naive isinstance(exc, DatabaseError)
    would retry bad SQL forever.
    """

    @pytest.mark.parametrize(
        "exc",
        [
            psycopg2.OperationalError("server closed the connection unexpectedly"),
            psycopg2.InterfaceError("connection already closed"),
        ],
    )
    def test_transient_errors(self, exc: Exception) -> None:
        assert PostgresSource()._is_transient(exc) is True

    @pytest.mark.parametrize(
        "exc",
        [
            psycopg2.ProgrammingError('relation "nope" does not exist'),
            psycopg2.DataError("invalid input syntax for type integer"),
            psycopg2.IntegrityError("duplicate key value"),
            psycopg2.DatabaseError("generic"),
            ValueError("not a driver error at all"),
        ],
    )
    def test_permanent_errors(self, exc: Exception) -> None:
        assert PostgresSource()._is_transient(exc) is False


class TestRetry:
    def test_transient_failure_is_retried_then_succeeds(self) -> None:
        """Two connection resets, then a good connection: rows still come through."""
        attempts: list[int] = []

        def connect(_config: object) -> MagicMock:
            attempts.append(1)
            if len(attempts) < 3:
                raise psycopg2.OperationalError("server closed the connection unexpectedly")
            return _mock_conn()

        with patch.object(PostgresSource, "_connect", side_effect=connect):
            with patch("drt.destinations.retry.time.sleep"):
                rows = list(PostgresSource().extract("SELECT * FROM users", _profile()))

        assert rows == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        assert len(attempts) == 3

    def test_transient_failure_during_execute_is_retried(self) -> None:
        """The retry covers query execution, not just connecting."""
        attempts: list[int] = []

        def connect(_config: object) -> MagicMock:
            attempts.append(1)
            conn = _mock_conn()
            if len(attempts) == 1:
                conn.cursor.return_value.execute.side_effect = psycopg2.OperationalError(
                    "terminating connection due to administrator command"
                )
            return conn

        with patch.object(PostgresSource, "_connect", side_effect=connect):
            with patch("drt.destinations.retry.time.sleep"):
                rows = list(PostgresSource().extract("SELECT * FROM users", _profile()))

        assert len(rows) == 2
        assert len(attempts) == 2

    def test_permanent_failure_propagates_without_retrying(self) -> None:
        """Bad SQL fails on the first attempt — retrying can't fix it."""
        attempts: list[int] = []

        def connect(_config: object) -> MagicMock:
            attempts.append(1)
            raise psycopg2.ProgrammingError('relation "nope" does not exist')

        with patch.object(PostgresSource, "_connect", side_effect=connect):
            with patch("drt.destinations.retry.time.sleep") as sleep:
                with pytest.raises(psycopg2.ProgrammingError):
                    list(PostgresSource().extract("SELECT * FROM nope", _profile()))

        assert len(attempts) == 1
        sleep.assert_not_called()

    def test_retries_are_bounded_and_the_last_error_propagates(self) -> None:
        """A warehouse that stays down surfaces the driver error, not a retry wrapper."""
        attempts: list[int] = []

        def connect(_config: object) -> MagicMock:
            attempts.append(1)
            raise psycopg2.OperationalError("could not connect to server")

        with patch.object(PostgresSource, "_connect", side_effect=connect):
            with patch("drt.destinations.retry.time.sleep"):
                with pytest.raises(psycopg2.OperationalError, match="could not connect"):
                    list(PostgresSource().extract("SELECT 1", _profile()))

        assert len(attempts) == 3  # RetryConfig() default max_attempts

    def test_failure_after_the_first_row_is_not_retried(self) -> None:
        """Scope boundary (#766): mid-iteration failures are out of scope.

        Rows already yielded have been loaded downstream and cannot be
        un-sent, so re-running the query would duplicate them.
        """
        attempts: list[int] = []

        def connect(_config: object) -> MagicMock:
            attempts.append(1)
            conn = _mock_conn()

            def exploding_rows():
                yield (1, "Alice")
                raise psycopg2.OperationalError("connection reset by peer")

            conn.cursor.return_value.fetchall.return_value = exploding_rows()
            return conn

        with patch.object(PostgresSource, "_connect", side_effect=connect):
            with patch("drt.destinations.retry.time.sleep"):
                source = PostgresSource()
                gen = source.extract("SELECT * FROM users", _profile())
                assert next(gen) == {"id": 1, "name": "Alice"}
                with pytest.raises(psycopg2.OperationalError):
                    next(gen)

        assert len(attempts) == 1  # not retried — the row already escaped

    def test_connection_is_closed_after_a_retried_attempt(self) -> None:
        """A half-open connection from a failed attempt must not leak."""
        conns: list[MagicMock] = []

        def connect(_config: object) -> MagicMock:
            conn = _mock_conn()
            if not conns:
                conn.cursor.return_value.execute.side_effect = psycopg2.OperationalError("boom")
            conns.append(conn)
            return conn

        with patch.object(PostgresSource, "_connect", side_effect=connect):
            with patch("drt.destinations.retry.time.sleep"):
                list(PostgresSource().extract("SELECT 1", _profile()))

        assert len(conns) == 2
        conns[0].close.assert_called_once()  # the failed attempt cleaned up
        conns[1].close.assert_called_once()
