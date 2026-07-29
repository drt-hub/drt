"""Tests for SQL Server source."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from drt.config.credentials import SQLServerProfile
from drt.sources.base import Source
from drt.sources.sqlserver import SQLServerSource


def _profile(**overrides: object) -> SQLServerProfile:
    defaults: dict = {
        "type": "sqlserver",
        "host": "db.example.com",
        "port": 1433,
        "database": "analytics",
        "user": "drt_reader",
        "password_env": "SQLSERVER_PASSWORD",
        "schema": "dbo",
    }
    return SQLServerProfile(**{**defaults, **overrides})


def test_implements_source_protocol() -> None:
    assert isinstance(SQLServerSource(), Source)


def test_profile_describe() -> None:
    p = _profile()
    d = p.describe()
    assert d.startswith("sqlserver")
    assert "db.example.com" in d
    assert "analytics.dbo" in d


def test_profile_custom_schema() -> None:
    p = _profile(schema="sales")
    assert "analytics.sales" in p.describe()


def test_connection_import_error_handled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ImportError propagates when pymssql is not installed."""
    import sys

    try:
        import pymssql  # noqa: F401

        pytest.skip("pymssql is installed locally")
    except ImportError:
        monkeypatch.setitem(sys.modules, "pymssql", None)
        src = SQLServerSource()
        monkeypatch.setenv("SQLSERVER_PASSWORD", "fake")
        with pytest.raises(ImportError, match="drt-core\\[sqlserver\\]"):
            src._connect(_profile())


def test_resolver_ref_sqlserver(tmp_path: object) -> None:
    """ref() generates [schema].[table] for SQL Server."""
    from pathlib import Path

    from drt.engine.resolver import resolve_model_ref

    sql = resolve_model_ref("ref('users')", Path(tmp_path), _profile())
    assert sql == "SELECT * FROM [dbo].[users]"


# ---------------------------------------------------------------------------
# Transient-failure retry (#766)
# ---------------------------------------------------------------------------


def _install_fake_pymssql(monkeypatch: pytest.MonkeyPatch) -> object:
    """Provide ``pymssql`` so classification is testable.

    pymssql is an optional extra and is not installed in the default dev
    environment. The stub mirrors its real PEP 249 hierarchy (verified
    against pymssql/exceptions.py): InterfaceError under Error, and
    OperationalError / ProgrammingError / DataError / IntegrityError /
    InternalError / NotSupportedError as siblings under DatabaseError.
    """
    import sys
    import types

    class Error(Exception):
        pass

    class InterfaceError(Error):
        pass

    class DatabaseError(Error):
        pass

    class OperationalError(DatabaseError):
        pass

    class ProgrammingError(DatabaseError):
        pass

    class DataError(DatabaseError):
        pass

    class IntegrityError(DatabaseError):
        pass

    mod = types.ModuleType("pymssql")
    for cls in (
        Error,
        InterfaceError,
        DatabaseError,
        OperationalError,
        ProgrammingError,
        DataError,
        IntegrityError,
    ):
        setattr(mod, cls.__name__, cls)
    monkeypatch.setitem(sys.modules, "pymssql", mod)
    return mod


@pytest.mark.parametrize("exc_name", ["OperationalError", "InterfaceError"])
def test_is_transient_true_for_connection_errors(
    monkeypatch: pytest.MonkeyPatch, exc_name: str
) -> None:
    mod = _install_fake_pymssql(monkeypatch)
    assert SQLServerSource()._is_transient(getattr(mod, exc_name)("failover")) is True


@pytest.mark.parametrize(
    "exc_name", ["ProgrammingError", "DataError", "IntegrityError", "DatabaseError"]
)
def test_is_transient_false_for_permanent_errors(
    monkeypatch: pytest.MonkeyPatch, exc_name: str
) -> None:
    """Permanent classes are siblings of OperationalError under DatabaseError."""
    mod = _install_fake_pymssql(monkeypatch)
    assert SQLServerSource()._is_transient(getattr(mod, exc_name)("nope")) is False


@pytest.mark.parametrize(
    "message",
    [
        "Login failed for user 'drt_reader'.",
        "Adaptive Server connection failed (db.example.com:1433)\n"
        "Login failed for user 'drt_reader'.",
        "login failed for user 'sa'.",  # server casing varies
    ],
)
def test_is_transient_false_for_login_failures(
    monkeypatch: pytest.MonkeyPatch, message: str
) -> None:
    """Auth failures must not be retried, and pymssql hides them.

    ``pymssql.connect()`` translates *every* ``MSSQLDatabaseException`` into
    ``OperationalError`` — including SQL Server error 18456, "Login failed for
    user" — so the plain isinstance check lets a bad credential through and
    retries it three times. Three rapid failed logins can trip an AD account
    lockout policy, turning a config typo into an outage. Same hazard already
    handled on Postgres/Redshift (SQLSTATE 28) and MySQL (errno 1045).

    Matched on the message because the number does not survive: ``connect()``
    re-raises with ``e.args[0]`` only, dropping the exception object that
    carried ``.number``.
    """
    mod = _install_fake_pymssql(monkeypatch)
    assert SQLServerSource()._is_transient(mod.OperationalError(message)) is False


def test_is_transient_true_for_non_login_operational_errors(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """The login-failure exclusion must not swallow real connection trouble."""
    mod = _install_fake_pymssql(monkeypatch)
    exc = mod.OperationalError("Adaptive Server connection failed: server restarting")
    assert SQLServerSource()._is_transient(exc) is True


def test_extract_retries_transient_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """An Azure SQL failover shouldn't fail the sync."""
    from unittest.mock import MagicMock, patch

    mod = _install_fake_pymssql(monkeypatch)
    attempts: list[int] = []

    def connect(_config: object) -> MagicMock:
        attempts.append(1)
        if len(attempts) < 3:
            raise mod.OperationalError("connection refused")
        conn = MagicMock()
        cur = MagicMock()
        cur.__iter__.side_effect = lambda: iter([{"id": 1, "name": "Alice"}])
        conn.cursor.return_value = cur
        return conn

    with patch.object(SQLServerSource, "_connect", side_effect=connect):
        with patch("drt.destinations.retry.time.sleep"):
            rows = list(SQLServerSource().extract("SELECT * FROM users", _profile()))

    assert rows == [{"id": 1, "name": "Alice"}]
    assert len(attempts) == 3


def test_extract_does_not_retry_permanent_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    from unittest.mock import patch

    mod = _install_fake_pymssql(monkeypatch)
    attempts: list[int] = []

    def connect(_config: object) -> MagicMock:
        attempts.append(1)
        raise mod.ProgrammingError("Invalid object name 'nope'")

    with patch.object(SQLServerSource, "_connect", side_effect=connect):
        with patch("drt.destinations.retry.time.sleep") as sleep:
            with pytest.raises(mod.ProgrammingError):
                list(SQLServerSource().extract("SELECT * FROM nope", _profile()))

    assert len(attempts) == 1
    sleep.assert_not_called()


def test_a_mid_stream_failure_is_no_longer_retried_after_streaming(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Scope boundary (#766) — and a deliberate weakening from #765.

    Before streaming, this source materialised the whole result set with
    ``list(cur.fetchall())`` *inside* the retried closure. A driver that failed
    part-way through producing rows therefore failed before anything was
    yielded, and the retry could safely re-run the query: no row had reached
    the destination, so nothing could be duplicated. That made SQL Server
    strictly safer than Postgres/MySQL here, as an accident of buffering.

    Streaming removes that accident. Rows now leave the generator as they
    arrive, so a failure part-way through escapes to the caller with earlier
    rows already loaded downstream — exactly the documented #766 boundary that
    every other SQL source has always had. Re-running would duplicate them.

    This is a real behaviour change, and it is the price of not holding the
    result set in memory. It is pinned here so nobody reads the old docstring
    and assumes the stronger guarantee still holds.
    """
    mod = _install_fake_pymssql(monkeypatch)
    attempts: list[int] = []

    def connect(_config: object) -> MagicMock:
        attempts.append(1)

        def exploding_rows():
            yield {"id": 1}
            raise mod.OperationalError("connection reset")

        conn = MagicMock()
        cur = conn.cursor.return_value
        cur.__iter__.side_effect = exploding_rows
        return conn

    with patch.object(SQLServerSource, "_connect", side_effect=connect):
        with patch("drt.destinations.retry.time.sleep"):
            gen = SQLServerSource().extract("SELECT * FROM users", _profile())
            assert next(gen) == {"id": 1}
            with pytest.raises(mod.OperationalError):
                next(gen)

    assert len(attempts) == 1, "the failure escaped mid-stream, so it must not be retried"


def test_extract_does_not_retry_after_a_row_is_yielded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Once iteration has handed a row out, nothing downstream is retried.

    The retried closure has already returned by then, so a consumer-side
    failure propagates without re-running the query.
    """
    from unittest.mock import MagicMock, patch

    mod = _install_fake_pymssql(monkeypatch)
    attempts: list[int] = []

    def connect(_config: object) -> MagicMock:
        attempts.append(1)
        conn = MagicMock()
        cur = MagicMock()
        cur.__iter__.side_effect = lambda: iter([{"id": 1}, {"id": 2}])
        conn.cursor.return_value = cur
        return conn

    with patch.object(SQLServerSource, "_connect", side_effect=connect):
        with patch("drt.destinations.retry.time.sleep"):
            gen = SQLServerSource().extract("SELECT * FROM users", _profile())
            assert next(gen) == {"id": 1}
            # A transient error raised by the consumer after the first row is
            # not the source's to retry — the query already completed.
            with pytest.raises(mod.OperationalError):
                gen.throw(mod.OperationalError("connection reset"))

    assert len(attempts) == 1


class TestStreamingExtraction:
    """#765: SQL Server iterates the cursor instead of calling fetchall().

    Measured on SQL Server 2022, 300k rows x ~200B, fresh process:
    +151.2 MB RSS before, +39.3 MB after (through ``extract()`` itself).
    """

    def _conn(self, rows):
        conn = MagicMock()
        # The cursor must be conn's own child mock so its calls land in
        # conn.mock_calls — that ledger is how close ordering is observed.
        cur = conn.cursor.return_value
        cur.__iter__.side_effect = lambda: iter(rows)
        return conn

    def test_does_not_call_fetchall(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_fake_pymssql(monkeypatch)
        conn = self._conn([{"id": 1}])
        with patch.object(SQLServerSource, "_connect", return_value=conn):
            list(SQLServerSource().extract("SELECT 1", _profile()))

        conn.cursor.return_value.fetchall.assert_not_called()

    def test_arraysize_comes_from_fetch_size(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_fake_pymssql(monkeypatch)
        conn = self._conn([{"id": 1}])
        with patch.object(SQLServerSource, "_connect", return_value=conn):
            list(SQLServerSource().extract("SELECT 1", _profile(fetch_size=2500)))

        assert conn.cursor.return_value.arraysize == 2500

    def test_cursor_is_closed_before_the_connection(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_fake_pymssql(monkeypatch)
        conn = self._conn([{"id": 1}])
        with patch.object(SQLServerSource, "_connect", return_value=conn):
            list(SQLServerSource().extract("SELECT 1", _profile()))

        names = [c[0] for c in conn.mock_calls]
        assert "cursor().close" in names, "the cursor was never closed"
        assert names.index("cursor().close") < names.index("close"), (
            f"closed in the wrong order: {names}"
        )

    def test_connection_closes_when_the_generator_is_abandoned(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`--limit` / `--fail-fast` stop consuming mid-stream (#775/#774)."""
        _install_fake_pymssql(monkeypatch)
        conn = self._conn([{"id": i} for i in range(100)])
        with patch.object(SQLServerSource, "_connect", return_value=conn):
            gen = SQLServerSource().extract("SELECT 1", _profile())
            next(gen)
            gen.close()

        names = [c[0] for c in conn.mock_calls]
        assert "cursor().close" in names, "the cursor leaked on abandonment"
        assert names.index("cursor().close") < names.index("close")

    def test_empty_result_yields_nothing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_fake_pymssql(monkeypatch)
        conn = self._conn([])
        with patch.object(SQLServerSource, "_connect", return_value=conn):
            assert list(SQLServerSource().extract("SELECT 1", _profile())) == []
