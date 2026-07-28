"""Tests for Databricks SQL Warehouse source."""

from __future__ import annotations

import pytest

from drt.config.credentials import DatabricksProfile
from drt.sources.base import Source
from drt.sources.databricks import DatabricksSource


def _profile(**overrides: object) -> DatabricksProfile:
    defaults: dict = {
        "type": "databricks",
        "server_hostname": "dbc-xxx.cloud.databricks.com",
        "http_path": "/sql/1.0/warehouses/abc",
        "access_token_env": "DATABRICKS_TOKEN",
        "schema": "default",
    }
    return DatabricksProfile(**{**defaults, **overrides})


def test_implements_source_protocol() -> None:
    assert isinstance(DatabricksSource(), Source)


def test_profile_describe_without_catalog() -> None:
    p = _profile()
    assert "default" in p.describe()
    assert p.describe().startswith("databricks")


def test_profile_describe_with_catalog() -> None:
    p = _profile(catalog="main", schema="analytics")
    assert "main.analytics" in p.describe()


def test_missing_token_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    src = DatabricksSource()
    with pytest.raises(ValueError, match="access_token"):
        # Force connection attempt by iterating (connection is lazy)
        # We need to actually call _connect to hit the token check
        src._connect(_profile())


def test_connection_import_error_handled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Confirm ImportError propagates when databricks-sql is not installed."""
    import sys

    # Simulate missing databricks module
    monkeypatch.setitem(sys.modules, "databricks", None)
    src = DatabricksSource()
    # _connect should raise ImportError when it tries to import
    # Note: this only hits if databricks-sql-connector isn't installed
    # Skip if it IS installed
    try:
        from databricks import sql  # noqa: F401

        pytest.skip("databricks-sql-connector is installed locally")
    except ImportError:
        monkeypatch.setenv("DATABRICKS_TOKEN", "fake-token")
        with pytest.raises(ImportError, match="drt-core\\[databricks\\]"):
            src._connect(_profile())


# ---------------------------------------------------------------------------
# Transient-failure retry (#766)
# ---------------------------------------------------------------------------


def _install_fake_dbsql_exc(monkeypatch: pytest.MonkeyPatch) -> object:
    """Provide ``databricks.sql.exc`` so classification is testable.

    ``databricks-sql-connector`` is an optional extra and is not installed in
    the default dev environment, so the exception classes are recreated with
    the driver's real shape: OperationalError and ProgrammingError as PEP 249
    siblings under DatabaseError, and RequestError outside that tree under the
    driver's own Error base.
    """
    import sys
    import types

    class Error(Exception):
        pass

    class DatabaseError(Error):
        pass

    class OperationalError(DatabaseError):
        pass

    class ProgrammingError(DatabaseError):
        pass

    class NotSupportedError(DatabaseError):
        pass

    class RequestError(Error):
        pass

    exc_mod = types.ModuleType("databricks.sql.exc")
    for cls in (
        Error,
        DatabaseError,
        OperationalError,
        ProgrammingError,
        NotSupportedError,
        RequestError,
    ):
        setattr(exc_mod, cls.__name__, cls)

    databricks_mod = types.ModuleType("databricks")
    sql_mod = types.ModuleType("databricks.sql")
    sql_mod.exc = exc_mod  # type: ignore[attr-defined]
    databricks_mod.sql = sql_mod  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "databricks", databricks_mod)
    monkeypatch.setitem(sys.modules, "databricks.sql", sql_mod)
    monkeypatch.setitem(sys.modules, "databricks.sql.exc", exc_mod)
    return exc_mod


def test_is_transient_operational_error(monkeypatch: pytest.MonkeyPatch) -> None:
    exc_mod = _install_fake_dbsql_exc(monkeypatch)
    assert DatabricksSource()._is_transient(exc_mod.OperationalError("service unavailable")) is True


def test_is_transient_request_error_cold_start(monkeypatch: pytest.MonkeyPatch) -> None:
    """A stopped SQL warehouse resuming — the #654 cold-start case."""
    exc_mod = _install_fake_dbsql_exc(monkeypatch)
    exc = exc_mod.RequestError("Warehouse is starting")
    assert DatabricksSource()._is_transient(exc) is True


@pytest.mark.parametrize(
    "exc_name", ["ProgrammingError", "DatabaseError", "NotSupportedError"]
)
def test_is_transient_false_for_permanent(
    monkeypatch: pytest.MonkeyPatch, exc_name: str
) -> None:
    exc_mod = _install_fake_dbsql_exc(monkeypatch)
    exc = getattr(exc_mod, exc_name)("nope")
    assert DatabricksSource()._is_transient(exc) is False


def test_is_transient_false_for_unrelated_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_dbsql_exc(monkeypatch)
    assert DatabricksSource()._is_transient(ValueError("unrelated")) is False


def test_extract_retries_cold_start(monkeypatch: pytest.MonkeyPatch) -> None:
    """Warehouse cold start: two RequestErrors, then rows come through."""
    from unittest.mock import MagicMock, patch

    exc_mod = _install_fake_dbsql_exc(monkeypatch)
    attempts: list[int] = []

    def connect(_config: object) -> MagicMock:
        attempts.append(1)
        if len(attempts) < 3:
            raise exc_mod.RequestError("Warehouse is starting")
        conn = MagicMock()
        cur = MagicMock()
        cur.description = [("id",)]
        cur.fetchall.return_value = [(1,)]
        conn.cursor.return_value = cur
        return conn

    with patch.object(DatabricksSource, "_connect", side_effect=connect):
        with patch("drt.destinations.retry.time.sleep"):
            rows = list(DatabricksSource().extract("SELECT id FROM t", _profile()))

    assert rows == [{"id": 1}]
    assert len(attempts) == 3


def test_extract_does_not_retry_bad_sql(monkeypatch: pytest.MonkeyPatch) -> None:
    from unittest.mock import MagicMock, patch

    exc_mod = _install_fake_dbsql_exc(monkeypatch)
    attempts: list[int] = []

    def connect(_config: object) -> MagicMock:
        attempts.append(1)
        raise exc_mod.ProgrammingError("Table or view not found")

    with patch.object(DatabricksSource, "_connect", side_effect=connect):
        with patch("drt.destinations.retry.time.sleep") as sleep:
            with pytest.raises(exc_mod.ProgrammingError):
                list(DatabricksSource().extract("SELECT * FROM nope", _profile()))

    assert len(attempts) == 1
    sleep.assert_not_called()


def test_extract_does_not_retry_after_first_row(monkeypatch: pytest.MonkeyPatch) -> None:
    """Scope boundary (#766): a yielded row cannot be un-sent."""
    from unittest.mock import MagicMock, patch

    exc_mod = _install_fake_dbsql_exc(monkeypatch)
    attempts: list[int] = []

    def connect(_config: object) -> MagicMock:
        attempts.append(1)

        def exploding_rows():
            yield (1,)
            raise exc_mod.OperationalError("connection reset")

        conn = MagicMock()
        cur = MagicMock()
        cur.description = [("id",)]
        cur.fetchall.return_value = exploding_rows()
        conn.cursor.return_value = cur
        return conn

    with patch.object(DatabricksSource, "_connect", side_effect=connect):
        with patch("drt.destinations.retry.time.sleep"):
            gen = DatabricksSource().extract("SELECT id FROM t", _profile())
            assert next(gen) == {"id": 1}
            with pytest.raises(exc_mod.OperationalError):
                next(gen)

    assert len(attempts) == 1
