"""Tests for SQL Server source."""

from __future__ import annotations

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
