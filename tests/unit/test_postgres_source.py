"""Unit tests for the PostgreSQL source connector.

Covers the transient-failure retry added in #766: a connection reset or an
expired session on the way *in* is retried; a permanent error (bad SQL) is
not, and neither is anything that happens after the first row is yielded.
No real database — ``_connect`` is patched throughout.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

# The whole module needs psycopg2: nearly every test constructs a real driver
# exception at collection time (the parametrize lists below are evaluated on
# import), so there is nothing left to run without it. CI's test matrix
# installs without the DB extras.
pytest.importorskip("psycopg2")

import psycopg2

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
    data = [(1, "Alice"), (2, "Bob")] if rows is None else rows
    cur.fetchall.return_value = data
    # Iterable as well as fetchall-able: streaming extraction (#765) consumes
    # the cursor with `for row in cur`, which is what a server-side cursor
    # supports. __iter__ is a fresh iterator per call so a test may iterate
    # twice (e.g. a retried attempt) without the second pass coming up empty.
    cur.__iter__.side_effect = lambda: iter(data)
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

        Since #765 the rows arrive by iterating the server-side cursor rather
        than from ``fetchall()``, so the failure is injected into ``__iter__``
        — but the guarantee under test is unchanged, and this is exactly the
        case streaming makes more likely (the connection is now held open for
        the whole load, so there is far more of a window for it to drop).
        """
        attempts: list[int] = []

        def connect(_config: object) -> MagicMock:
            attempts.append(1)
            conn = _mock_conn()

            def exploding_rows():
                yield (1, "Alice")
                raise psycopg2.OperationalError("connection reset by peer")

            conn.cursor.return_value.__iter__.side_effect = exploding_rows
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


@pytest.mark.parametrize("exc_name", ["InvalidPassword", "InvalidAuthorizationSpecification"])
def test_is_transient_false_for_auth_failures(exc_name: str) -> None:
    """Auth failures live *under* OperationalError in psycopg2 — an
    isinstance check against that base lets them through.

    Regression guard: retrying a wrong credential is not merely wasted work.
    Three rapid attempts can trip an account lockout policy, turning a config
    typo into an outage. Note ``pgcode`` is None on a hand-built exception
    (the server populates it), so the class check is what carries this test —
    which is exactly why the classifier matches on both.
    """
    pytest.importorskip("psycopg2")
    import psycopg2

    exc = getattr(psycopg2.errors, exc_name)("password authentication failed")
    assert PostgresSource()._is_transient(exc) is False


class TestStreamingExtraction:
    """#765: extract streams through a server-side cursor instead of fetchall().

    Measured on a real Postgres 16 with 300k rows x ~200B: fetchall() peaked at
    +182 MB RSS, a named cursor at itersize=10000 peaked at +16 MB — 11x. The
    point of these tests is the *lifecycle*, though, not the memory: a
    server-side cursor only lives while its connection is open, so #766's
    close-inside-the-retry-unit no longer works and every exit path has to be
    pinned.
    """

    def test_uses_a_named_server_side_cursor(self) -> None:
        """A plain cursor() buffers the whole result set client-side; only a
        *named* cursor makes Postgres hold it and stream it back."""
        conn = _mock_conn()
        with patch.object(PostgresSource, "_connect", return_value=conn):
            list(PostgresSource().extract("SELECT 1", _profile()))

        assert conn.cursor.call_args is not None
        assert conn.cursor.call_args.kwargs.get("name"), (
            "cursor() was called without name= — that is a client-side buffer, "
            "not a server-side cursor, and streams nothing"
        )

    def test_itersize_comes_from_fetch_size(self) -> None:
        conn = _mock_conn()
        with patch.object(PostgresSource, "_connect", return_value=conn):
            list(PostgresSource().extract("SELECT 1", _profile(fetch_size=2500)))

        assert conn.cursor.return_value.itersize == 2500

    def test_does_not_call_fetchall(self) -> None:
        """The regression this issue exists to prevent."""
        conn = _mock_conn()
        with patch.object(PostgresSource, "_connect", return_value=conn):
            list(PostgresSource().extract("SELECT 1", _profile()))

        conn.cursor.return_value.fetchall.assert_not_called()

    def test_connection_closes_after_full_iteration(self) -> None:
        conn = _mock_conn()
        with patch.object(PostgresSource, "_connect", return_value=conn):
            list(PostgresSource().extract("SELECT 1", _profile()))

        conn.close.assert_called_once()

    def test_connection_closes_when_the_generator_is_abandoned(self) -> None:
        """`--limit` / `--fail-fast` stop consuming mid-stream (#775/#774).

        fetchall() made abandonment harmless because the connection was already
        closed. Streaming does not: verified against a real Postgres that
        dropping the cursor reference and running gc leaves the server-side
        cursor open. Only a try/finally around the yield loop closes it, via
        GeneratorExit.
        """
        conn = _mock_conn(rows=[(i, "x") for i in range(100)])
        with patch.object(PostgresSource, "_connect", return_value=conn):
            gen = PostgresSource().extract("SELECT 1", _profile())
            next(gen)
            next(gen)
            gen.close()  # what GC does to an abandoned generator

        conn.close.assert_called_once()

    def test_columns_are_read_after_the_first_row_not_at_execute(self) -> None:
        """A named cursor's ``description`` is None until the first batch lands.

        This is the one real behavioural difference between a plain and a
        server-side cursor, and it is invisible to a naive mock: DECLARE does
        not touch the server, so psycopg2 has no column metadata yet. Reading
        ``cur.description`` right after ``execute()`` — which is correct for a
        plain cursor, and is what the pre-#765 code did — raises
        ``TypeError: 'NoneType' object is not iterable`` against a live
        server. Caught exactly that way, on real Postgres 16, after the mocked
        suite was green.
        """
        conn = _mock_conn()
        cur = conn.cursor.return_value
        cur.description = None  # as psycopg2 leaves it until the first fetch

        def rows():
            # description only becomes available once iteration has started
            cur.description = [("id",), ("name",)]
            yield (1, "Alice")
            yield (2, "Bob")

        cur.__iter__.side_effect = rows

        with patch.object(PostgresSource, "_connect", return_value=conn):
            result = list(PostgresSource().extract("SELECT 1", _profile()))

        assert result == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    def test_empty_result_set_does_not_touch_description(self) -> None:
        """With no rows, ``description`` may stay None forever — reading it
        unconditionally would turn an empty extract into a TypeError."""
        conn = _mock_conn(rows=[])
        conn.cursor.return_value.description = None

        with patch.object(PostgresSource, "_connect", return_value=conn):
            assert list(PostgresSource().extract("SELECT 1", _profile())) == []

    def test_failed_attempt_closes_its_own_connection(self) -> None:
        """A retried attempt must not leak the connection it opened.

        #766 got this from `finally: conn.close()` inside the retried unit.
        Streaming moves the close out, so the failure path needs its own.
        """
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
        conns[0].close.assert_called_once(), "failed attempt leaked its connection"
        conns[1].close.assert_called_once()
