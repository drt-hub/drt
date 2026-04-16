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
