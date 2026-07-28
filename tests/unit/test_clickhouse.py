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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestClickHouseSource:
    def test_extract_returns_rows(self) -> None:
        source = ClickHouseSource()
        config = _config()

        # Mock client and its query result
        mock_client = _fake_client()
        mock_result = MagicMock()
        mock_result.column_names = ["id", "name"]
        mock_result.result_rows = [(1, "Alice"), (2, "Bob")]
        mock_client.query.return_value = mock_result

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
            client = MagicMock()
            result = MagicMock()
            result.column_names = ["id"]
            result.result_rows = [(1,)]
            client.query.return_value = result
            return client

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
            client = MagicMock()
            result = MagicMock()
            result.column_names = ["id"]
            result.result_rows = [(7,)]
            client.query.return_value = result
            return client

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
            client = MagicMock()
            result = MagicMock()
            result.column_names = ["id"]
            result.result_rows = [(1,), (2,)]
            client.query.return_value = result
            return client

        with patch.object(ClickHouseSource, "_connect", side_effect=connect):
            with patch("drt.destinations.retry.time.sleep"):
                gen = ClickHouseSource().extract("SELECT id FROM t", _config())
                assert next(gen) == {"id": 1}
                with pytest.raises(mod.OperationalError):
                    gen.throw(mod.OperationalError("connection reset"))

        assert len(attempts) == 1
