"""Unit tests for ClickHouse source.

Uses a mock clickhouse-connect client — no real database required.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.credentials import ClickHouseProfile
from drt.sources.clickhouse import ClickHouseSource

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _config(**overrides: Any) -> ClickHouseProfile:
    defaults: dict[str, Any] = {
        "type": "clickhouse",
        "host": "localhost",
        "port": 8123,
        "database": "default",
        "user": "default",
        "password": "testpassword",
    }
    defaults.update(overrides)
    return ClickHouseProfile(**defaults)


def _fake_client() -> MagicMock:
    client = MagicMock()
    return client


def _streaming_client(rows, columns=("id", "name")) -> MagicMock:
    """A client whose query_rows_stream() behaves like the real one.

    The stream is a context manager, iterable, and exposes column names on
    ``.source.column_names`` *before* iteration starts.
    """
    client = MagicMock()
    stream = MagicMock()
    stream.source.column_names = columns
    stream.__enter__.return_value = stream
    stream.__exit__.return_value = False
    stream.__iter__.side_effect = lambda: iter(rows)
    client.query_rows_stream.return_value = stream
    return client


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestClickHouseSource:
    def test_extract_returns_rows(self) -> None:
        source = ClickHouseSource()
        config = _config()

        # Mock client and its query result
        # Since #765 rows come from query_rows_stream(), not query().
        mock_client = _streaming_client([(1, "Alice"), (2, "Bob")])

        with patch.object(ClickHouseSource, "_connect", return_value=mock_client):
            results = list(source.extract("SELECT * FROM users", config))

        assert len(results) == 2
        assert results[0] == {"id": 1, "name": "Alice"}
        assert results[1] == {"id": 2, "name": "Bob"}
        mock_client.close.assert_called_once()

    def test_test_connection_success(self) -> None:
        source = ClickHouseSource()
        config = _config()

        mock_client = _fake_client()
        with patch.object(ClickHouseSource, "_connect", return_value=mock_client):
            assert source.test_connection(config) is True

        mock_client.query.assert_called_with("SELECT 1")
        mock_client.close.assert_called_once()

    def test_test_connection_failure(self) -> None:
        source = ClickHouseSource()
        config = _config()

        with patch.object(ClickHouseSource, "_connect", side_effect=Exception("Connection failed")):
            assert source.test_connection(config) is False

    def test_connect_import_error(self) -> None:
        source = ClickHouseSource()
        config = _config()

        with patch("builtins.__import__", side_effect=ImportError):
            with pytest.raises(ImportError, match="ClickHouse support requires"):
                source._connect(config)

    def test_connect_parameters(self) -> None:
        source = ClickHouseSource()
        config = _config(user="analyst", database="prod")

        mock_module = MagicMock()
        with patch.dict("sys.modules", {"clickhouse_connect": mock_module}):
            source._connect(config)

            mock_module.get_client.assert_called_once_with(
                host="localhost",
                port=8123,
                database="prod",
                username="analyst",
                password="testpassword",
            )


# ---------------------------------------------------------------------------
# Transient-failure retry (#766)
# ---------------------------------------------------------------------------


def _install_fake_ch_exceptions(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Provide ``clickhouse_connect.driver.exceptions`` for classification tests.

    clickhouse-connect is an optional extra and is not installed in the
    default dev environment. The stub mirrors its real hierarchy (verified
    against clickhouse_connect/driver/exceptions.py): ClickHouseError at the
    root, Error below it, then InterfaceError and DatabaseError, with
    OperationalError / ProgrammingError / DataError / IntegrityError /
    InternalError / NotSupportedError as siblings under DatabaseError, and
    StreamClosedError under ProgrammingError.
    """
    import builtins
    import sys
    import types

    class ClickHouseError(Exception):
        pass

    class Error(ClickHouseError):
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

    class StreamClosedError(ProgrammingError):
        pass

    # Added in #862 once the pin enumerated the real module instead of a
    # hand-kept list, which is exactly how their absence surfaced.
    # StreamFailureError and StreamCompleteException descend from plain
    # Exception rather than DatabaseError — worth mirroring precisely, since
    # re-parenting either under OperationalError is what would silently start
    # retrying a stream failure.
    class InternalError(DatabaseError):
        pass

    class NotSupportedError(DatabaseError):
        pass

    class StreamCompleteException(Exception):
        pass

    class StreamFailureError(Exception):
        pass

    # The driver's Warning subclasses the *builtin* Warning as well as its own
    # base. Referenced via `builtins` because defining a class named Warning in
    # this scope makes the bare name local.
    class Warning(builtins.Warning, ClickHouseError):  # noqa: A001 - mirrors the driver
        pass

    exc_mod = types.ModuleType("clickhouse_connect.driver.exceptions")
    for cls in (
        ClickHouseError,
        Error,
        InterfaceError,
        DatabaseError,
        OperationalError,
        ProgrammingError,
        DataError,
        IntegrityError,
        StreamClosedError,
        InternalError,
        NotSupportedError,
        StreamCompleteException,
        StreamFailureError,
        Warning,
    ):
        setattr(exc_mod, cls.__name__, cls)

    root = types.ModuleType("clickhouse_connect")
    driver = types.ModuleType("clickhouse_connect.driver")
    driver.exceptions = exc_mod  # type: ignore[attr-defined]
    root.driver = driver  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "clickhouse_connect", root)
    monkeypatch.setitem(sys.modules, "clickhouse_connect.driver", driver)
    monkeypatch.setitem(sys.modules, "clickhouse_connect.driver.exceptions", exc_mod)
    return exc_mod


class TestClickHouseTransientClassification:
    @pytest.mark.parametrize("exc_name", ["OperationalError", "InterfaceError"])
    def test_transient_errors(self, monkeypatch: pytest.MonkeyPatch, exc_name: str) -> None:
        mod = _install_fake_ch_exceptions(monkeypatch)
        assert ClickHouseSource()._is_transient(getattr(mod, exc_name)("disconnect")) is True

    @pytest.mark.parametrize(
        "exc_name",
        ["ProgrammingError", "DataError", "IntegrityError", "DatabaseError", "ClickHouseError"],
    )
    def test_permanent_errors(self, monkeypatch: pytest.MonkeyPatch, exc_name: str) -> None:
        mod = _install_fake_ch_exceptions(monkeypatch)
        assert ClickHouseSource()._is_transient(getattr(mod, exc_name)("nope")) is False

    def test_stream_closed_error_is_permanent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """StreamClosedError subclasses ProgrammingError, so it must not retry."""
        mod = _install_fake_ch_exceptions(monkeypatch)
        assert ClickHouseSource()._is_transient(mod.StreamClosedError("closed")) is False

    def test_httpx_transport_error_needs_no_classification(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """ClickHouse's HTTP interface can leak httpx exceptions.

        `_is_transient` returns False for them and that is correct:
        `with_retry` catches `httpx.TransportError` natively, and `retry_on`
        is additive to that path rather than a replacement for it.
        """
        import httpx

        _install_fake_ch_exceptions(monkeypatch)
        assert ClickHouseSource()._is_transient(httpx.ConnectError("boom")) is False


class TestClickHouseSourceRetry:
    def test_transient_failure_is_retried_then_succeeds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mod = _install_fake_ch_exceptions(monkeypatch)
        attempts: list[int] = []

        def connect(_config: Any) -> MagicMock:
            attempts.append(1)
            if len(attempts) < 3:
                raise mod.OperationalError("unexpected disconnect")
            return _streaming_client([(1,)], columns=("id",))

        with patch.object(ClickHouseSource, "_connect", side_effect=connect):
            with patch("drt.destinations.retry.time.sleep"):
                rows = list(ClickHouseSource().extract("SELECT id FROM t", _config()))

        assert rows == [{"id": 1}]
        assert len(attempts) == 3

    def test_httpx_transport_error_is_retried_by_the_builtin_path(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The HTTP interface case, end to end."""
        import httpx

        _install_fake_ch_exceptions(monkeypatch)
        attempts: list[int] = []

        def connect(_config: Any) -> MagicMock:
            attempts.append(1)
            if len(attempts) < 2:
                raise httpx.ConnectError("connection refused")
            return _streaming_client([(7,)], columns=("id",))

        with patch.object(ClickHouseSource, "_connect", side_effect=connect):
            with patch("drt.destinations.retry.time.sleep"):
                rows = list(ClickHouseSource().extract("SELECT id FROM t", _config()))

        assert rows == [{"id": 7}]
        assert len(attempts) == 2

    def test_permanent_failure_is_not_retried(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mod = _install_fake_ch_exceptions(monkeypatch)
        attempts: list[int] = []

        def connect(_config: Any) -> MagicMock:
            attempts.append(1)
            raise mod.ProgrammingError("Table default.nope doesn't exist")

        with patch.object(ClickHouseSource, "_connect", side_effect=connect):
            with patch("drt.destinations.retry.time.sleep") as sleep:
                with pytest.raises(mod.ProgrammingError):
                    list(ClickHouseSource().extract("SELECT * FROM nope", _config()))

        assert len(attempts) == 1
        sleep.assert_not_called()

    def test_no_retry_once_a_row_has_been_yielded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Scope boundary (#766): a yielded row cannot be un-sent."""
        mod = _install_fake_ch_exceptions(monkeypatch)
        attempts: list[int] = []

        def connect(_config: Any) -> MagicMock:
            attempts.append(1)
            return _streaming_client([(1,), (2,)], columns=("id",))

        with patch.object(ClickHouseSource, "_connect", side_effect=connect):
            with patch("drt.destinations.retry.time.sleep"):
                gen = ClickHouseSource().extract("SELECT id FROM t", _config())
                assert next(gen) == {"id": 1}
                with pytest.raises(mod.OperationalError):
                    gen.throw(mod.OperationalError("connection reset"))

        assert len(attempts) == 1


class TestStreamingExtraction:
    """#765 slice 3: ClickHouse streams via query_rows_stream().

    Measured on a live ClickHouse 24, 300k rows x ~200B, each variant in a
    fresh process: ``client.query()`` peaked at +224 MB RSS (it materialises
    every row into ``result.result_rows``), ``query_rows_stream()`` at
    +149 MB. The remainder is clickhouse-connect buffering the HTTP response
    internally, not drt holding rows — a far smaller gap than the Postgres or
    MySQL legs, and deliberately reported as-is.
    """

    def test_uses_query_rows_stream_not_query(self) -> None:
        client = _streaming_client([(1, "Alice"), (2, "Bob")])

        with patch.object(ClickHouseSource, "_connect", return_value=client):
            rows = list(ClickHouseSource().extract("SELECT 1", _config()))

        assert rows == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        client.query_rows_stream.assert_called_once()
        client.query.assert_not_called()

    def test_client_closes_after_full_iteration(self) -> None:
        client = _streaming_client([(1, "Alice")])

        with patch.object(ClickHouseSource, "_connect", return_value=client):
            list(ClickHouseSource().extract("SELECT 1", _config()))

        client.close.assert_called_once()

    def test_client_closes_when_the_generator_is_abandoned(self) -> None:
        """`--limit` / `--fail-fast` stop consuming mid-stream (#775/#774)."""
        client = _streaming_client([(i, "x") for i in range(100)])

        with patch.object(ClickHouseSource, "_connect", return_value=client):
            gen = ClickHouseSource().extract("SELECT 1", _config())
            next(gen)
            gen.close()  # what GC does to an abandoned generator

        client.close.assert_called_once()
        # The stream's own __exit__ must run too, or the HTTP response is left
        # unconsumed on a client that is about to be closed under it.
        client.query_rows_stream.return_value.__exit__.assert_called_once()

    def test_empty_result_yields_nothing(self) -> None:
        """A live server returns an *empty* column_names tuple for an empty
        result — the ClickHouse counterpart of psycopg2's description=None,
        and the same trap: nothing may assume columns are present."""
        client = _streaming_client([], columns=())

        with patch.object(ClickHouseSource, "_connect", return_value=client):
            assert list(ClickHouseSource().extract("SELECT 1", _config())) == []

    def test_failed_attempt_closes_its_own_client(self) -> None:
        """A retried attempt must not leak the client it opened."""
        clients = []

        def connect(_config):
            client = _streaming_client([(1, "Alice")])
            if not clients:
                client.query_rows_stream.side_effect = RuntimeError("boom")
            clients.append(client)
            return client

        with patch.object(ClickHouseSource, "_connect", side_effect=connect):
            with pytest.raises(RuntimeError):
                list(ClickHouseSource().extract("SELECT 1", _config()))

        assert len(clients) == 1
        clients[0].close.assert_called_once()


def test_the_fake_exception_hierarchy_matches_the_real_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Compare the double against the real driver, class by class.

    Generalised from #861: a Databricks double had ``RequestError`` inheriting
    from the wrong base, and that single difference made an auth-retry bug
    structurally untestable — the mocked suite stayed green while a real 401
    was retried three times. Every optional-driver source is tested against a
    hand-written double, so a double that drifts produces confidently green
    tests for broken code.

    Asserting the *real* hierarchy alone would not catch that: the double is
    what the other tests actually run against, so the two have to be compared
    directly.

    The class list is **enumerated from the real module**, not hardcoded
    (@Muawiya-contact on #862). A literal list is the same drift surface one
    level up: it silently stops covering anything the driver adds, and the
    dangerous case is a *new* class re-parented under ``OperationalError``,
    which would start being retried with the pin unable to see it. Enumerating
    means a new driver exception is covered the day it appears.

    This runs for real in CI — ``ci.yml`` installs the ``clickhouse`` extra
    precisely so these suites are not silently skipped.
    """
    real = pytest.importorskip("clickhouse_connect.driver.exceptions")
    fake = _install_fake_ch_exceptions(monkeypatch)

    # Every exception class the real module defines, not a hand-kept list.
    real_names = sorted(
        n
        for n, obj in vars(real).items()
        if isinstance(obj, type) and issubclass(obj, BaseException) and not n.startswith("_")
    )
    assert real_names, "found no exception classes — did the module move?"

    missing = [n for n in real_names if getattr(fake, n, None) is None]
    assert not missing, (
        f"the double is missing {missing} — the driver defines exception classes "
        f"the double does not, so anything raising them is untested here"
    )

    for name in real_names:
        real_cls, fake_cls = getattr(real, name), getattr(fake, name)
        real_bases = [b.__name__ for b in real_cls.__mro__[1:] if b is not object]
        fake_bases = [b.__name__ for b in fake_cls.__mro__[1:] if b is not object]
        assert fake_bases == real_bases, (
            f"the double's {name} has bases {fake_bases} but the driver says "
            f"{real_bases} — every classification test here is running against "
            f"a hierarchy the driver does not have"
        )
