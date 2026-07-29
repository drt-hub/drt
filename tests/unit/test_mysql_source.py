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
        # Since #765 rows come from iterating the SSCursor, not fetchall().
        mock_cursor.__iter__.side_effect = lambda: iter([(1, "Alice"), (2, "Bob")])
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


class TestMySQLSourceTransientClassification:
    """Transient-ness is decided by errno, not by class alone (#766).

    pymysql overloads OperationalError across the client errno space
    (2002/2003/2006/2013/2055 — link broken) and the server's, where it also
    carries permanent conditions like 1045 access denied.
    """

    @pytest.mark.parametrize("errno", [2002, 2003, 2006, 2013, 2055])
    def test_client_connection_errnos_are_transient(self, errno):
        pytest.importorskip("pymysql")
        import pymysql

        exc = pymysql.err.OperationalError(errno, "link broken")
        assert MySQLSource()._is_transient(exc) is True

    def test_interface_error_is_transient(self):
        pytest.importorskip("pymysql")
        import pymysql

        assert MySQLSource()._is_transient(pymysql.err.InterfaceError("bad connection")) is True

    @pytest.mark.parametrize(
        ("errno", "message"),
        [
            (1045, "Access denied for user 'analyst'@'host'"),
            (1049, "Unknown database 'nope'"),
            (1142, "SELECT command denied to user"),
        ],
    )
    def test_permanent_operational_errnos_are_not_retried(self, errno, message):
        """Retrying a bad password wastes time and can trip account lockout."""
        pytest.importorskip("pymysql")
        import pymysql

        assert MySQLSource()._is_transient(pymysql.err.OperationalError(errno, message)) is False

    @pytest.mark.parametrize(
        "exc_name",
        ["ProgrammingError", "DataError", "IntegrityError", "DatabaseError"],
    )
    def test_permanent_exception_classes(self, exc_name):
        pytest.importorskip("pymysql")
        import pymysql

        exc = getattr(pymysql.err, exc_name)("nope")
        assert MySQLSource()._is_transient(exc) is False

    def test_operational_error_without_errno_is_permanent(self):
        """Unclassifiable -> fail fast rather than retry blindly."""
        pytest.importorskip("pymysql")
        import pymysql

        assert MySQLSource()._is_transient(pymysql.err.OperationalError()) is False

    def test_non_driver_exception_is_permanent(self):
        assert MySQLSource()._is_transient(ValueError("unrelated")) is False


class TestMySQLSourceRetry:
    def test_transient_failure_is_retried_then_succeeds(self):
        pytest.importorskip("pymysql")
        import pymysql

        attempts = []

        def connect(_config):
            attempts.append(1)
            if len(attempts) < 3:
                raise pymysql.err.OperationalError(2006, "MySQL server has gone away")
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.description = [("id",), ("name",)]
            mock_cursor.__iter__.side_effect = lambda: iter([(1, "Alice")])
            mock_conn.cursor.return_value = mock_cursor
            return mock_conn

        with patch("drt.sources.mysql.MySQLSource._connect", side_effect=connect):
            with patch("drt.destinations.retry.time.sleep"):
                rows = list(MySQLSource().extract("SELECT * FROM users", _profile()))

        assert rows == [{"id": 1, "name": "Alice"}]
        assert len(attempts) == 3

    def test_access_denied_propagates_without_retrying(self):
        pytest.importorskip("pymysql")
        import pymysql

        attempts = []

        def connect(_config):
            attempts.append(1)
            raise pymysql.err.OperationalError(1045, "Access denied for user")

        with patch("drt.sources.mysql.MySQLSource._connect", side_effect=connect):
            with patch("drt.destinations.retry.time.sleep") as sleep:
                with pytest.raises(pymysql.err.OperationalError):
                    list(MySQLSource().extract("SELECT 1", _profile()))

        assert len(attempts) == 1
        sleep.assert_not_called()

    def test_failure_after_first_row_is_not_retried(self):
        """Scope boundary (#766): a yielded row cannot be un-sent."""
        pytest.importorskip("pymysql")
        import pymysql

        attempts = []

        def connect(_config):
            attempts.append(1)
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.description = [("id",), ("name",)]

            def exploding_rows():
                yield (1, "Alice")
                raise pymysql.err.OperationalError(2013, "Lost connection during query")

            mock_cursor.__iter__.side_effect = exploding_rows
            mock_conn.cursor.return_value = mock_cursor
            return mock_conn

        with patch("drt.sources.mysql.MySQLSource._connect", side_effect=connect):
            with patch("drt.destinations.retry.time.sleep"):
                gen = MySQLSource().extract("SELECT * FROM users", _profile())
                assert next(gen) == {"id": 1, "name": "Alice"}
                with pytest.raises(pymysql.err.OperationalError):
                    next(gen)

        assert len(attempts) == 1


class TestStreamingExtraction:
    """#765 slice 2: MySQL streams through SSCursor.

    Measured against a live MySQL 8, 300k rows x ~200B: buffered `fetchall()`
    peaked at +109 MB RSS, SSCursor at +0.8 MB.

    MySQL differs from the Postgres leg in two ways that are only visible
    against a real server, and both shape this code:

    - ``description`` is populated right after ``execute()``, unlike a psycopg2
      named cursor, so columns can be read up front.
    - ``cursor.close()`` must run **before** ``conn.close()``. Closing the
      connection under an unread SSCursor makes pymysql's teardown read from a
      socket it has already dropped, and it floods stderr with
      ``AttributeError: 'NoneType' object has no attribute 'settimeout'``
      through ``Exception ignored in``. Harmless to correctness, ruinous to a
      CLI's output.
    """

    def _conn(self, rows=None, description=None):
        conn = MagicMock()
        # The cursor must be conn's *own* child mock, not a separate one:
        # only then do its calls land in ``conn.mock_calls``, which is how the
        # close-order tests below observe ordering reliably.
        cur = conn.cursor.return_value
        cur.description = description or [("id",), ("name",)]
        data = [(1, "Alice"), (2, "Bob")] if rows is None else rows
        cur.__iter__.side_effect = lambda: iter(data)
        return conn

    def test_uses_an_unbuffered_sscursor(self):
        """A default pymysql cursor buffers the entire result set client-side."""
        import pymysql

        conn = self._conn()
        with patch("drt.sources.mysql.MySQLSource._connect", return_value=conn):
            list(MySQLSource().extract("SELECT 1", _profile()))

        assert conn.cursor.call_args.args, "cursor() was called with no cursor class"
        assert conn.cursor.call_args.args[0] is pymysql.cursors.SSCursor

    def test_does_not_call_fetchall(self):
        conn = self._conn()
        with patch("drt.sources.mysql.MySQLSource._connect", return_value=conn):
            list(MySQLSource().extract("SELECT 1", _profile()))

        conn.cursor.return_value.fetchall.assert_not_called()

    def test_cursor_is_closed_before_the_connection(self):
        """Order matters — see the class docstring. Reversed, pymysql spews
        AttributeError through ``Exception ignored in`` on every abandoned
        stream."""
        conn = self._conn()

        with patch("drt.sources.mysql.MySQLSource._connect", return_value=conn):
            list(MySQLSource().extract("SELECT 1", _profile()))

        # conn.mock_calls records the connection's own calls *and* those routed
        # through the cursor it handed out, in one ordered list. Recording via
        # side_effect on two independent mocks does not work here: the cursor
        # is a separate mock, so its calls never enter conn's ledger and the
        # observed order is an artifact rather than the real one.
        names = [c[0] for c in conn.mock_calls]
        assert "cursor().close" in names, "the cursor was never closed"
        assert names.index("cursor().close") < names.index("close"), (
            f"closed in the wrong order: {names}"
        )

    def test_connection_closes_when_the_generator_is_abandoned(self):
        """`--limit` / `--fail-fast` stop consuming mid-stream (#775/#774)."""
        conn = self._conn(rows=[(i, "x") for i in range(100)])

        with patch("drt.sources.mysql.MySQLSource._connect", return_value=conn):
            gen = MySQLSource().extract("SELECT 1", _profile())
            next(gen)
            gen.close()  # what GC does to an abandoned generator

        names = [c[0] for c in conn.mock_calls]
        assert "cursor().close" in names, "the cursor leaked on abandonment"
        assert names.index("cursor().close") < names.index("close")

    def test_failed_attempt_closes_its_own_connection(self):
        """A retried attempt must not leak the connection it opened."""
        import pymysql

        conns = []

        def connect(_config):
            conn = self._conn()
            if not conns:
                conn.cursor.return_value.execute.side_effect = pymysql.err.OperationalError(
                    2013, "Lost connection to MySQL server during query"
                )
            conns.append(conn)
            return conn

        with patch("drt.sources.mysql.MySQLSource._connect", side_effect=connect):
            with patch("drt.destinations.retry.time.sleep"):
                list(MySQLSource().extract("SELECT 1", _profile()))

        assert len(conns) == 2
        conns[0].close.assert_called_once()
