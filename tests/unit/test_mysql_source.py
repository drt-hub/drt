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
            mock_cursor.fetchall.return_value = [(1, "Alice")]
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

            mock_cursor.fetchall.return_value = exploding_rows()
            mock_conn.cursor.return_value = mock_cursor
            return mock_conn

        with patch("drt.sources.mysql.MySQLSource._connect", side_effect=connect):
            with patch("drt.destinations.retry.time.sleep"):
                gen = MySQLSource().extract("SELECT * FROM users", _profile())
                assert next(gen) == {"id": 1, "name": "Alice"}
                with pytest.raises(pymysql.err.OperationalError):
                    next(gen)

        assert len(attempts) == 1
