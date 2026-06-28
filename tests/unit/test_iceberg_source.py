"""Unit tests for the Apache Iceberg source.

`pyiceberg` and DuckDB are mocked via sys.modules injection, so these run
without either installed.
"""

from __future__ import annotations

import sys
from typing import Any
from unittest.mock import MagicMock

import pytest

from drt.config.credentials import IcebergProfile, load_profile, save_profile
from drt.sources.base import Source
from drt.sources.iceberg import IcebergSource, _catalog_properties


def _mock_libs(monkeypatch: pytest.MonkeyPatch, rows: list[tuple], cols: list[str]):
    arrow = object()
    scan = MagicMock()
    scan.to_arrow.return_value = arrow
    table = MagicMock()
    table.scan.return_value = scan
    catalog = MagicMock()
    catalog.load_table.return_value = table
    pyiceberg_catalog = MagicMock()
    pyiceberg_catalog.load_catalog.return_value = catalog
    monkeypatch.setitem(sys.modules, "pyiceberg", MagicMock())
    monkeypatch.setitem(sys.modules, "pyiceberg.catalog", pyiceberg_catalog)

    result = MagicMock()
    result.description = [(c,) for c in cols]
    result.fetchall.return_value = rows
    conn = MagicMock()
    conn.execute.return_value = result
    duckdb_mod = MagicMock()
    duckdb_mod.connect.return_value = conn
    monkeypatch.setitem(sys.modules, "duckdb", duckdb_mod)
    return pyiceberg_catalog, catalog, conn, arrow


def test_implements_source_protocol() -> None:
    assert isinstance(IcebergSource(), Source)


def test_describe() -> None:
    p = IcebergProfile(type="iceberg", table="analytics.users")
    assert "iceberg" in p.describe()
    assert "analytics.users" in p.describe()


def test_catalog_properties_maps_uri_warehouse_and_env() -> None:
    import os

    os.environ["AWS_KEY"] = "secret123"
    try:
        props = _catalog_properties(
            IcebergProfile(
                type="iceberg",
                table="ns.t",
                catalog_uri="https://cat/api",
                warehouse="s3://b/wh",
                properties={"s3.access-key-id_ENV": "AWS_KEY", "plain": "v"},
            )
        )
    finally:
        del os.environ["AWS_KEY"]
    assert props == {
        "s3.access-key-id": "secret123",
        "plain": "v",
        "uri": "https://cat/api",
        "warehouse": "s3://b/wh",
    }


def test_extract_raises_without_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "pyiceberg.catalog", None)
    src = IcebergSource()
    cfg = IcebergProfile(type="iceberg", table="ns.users")
    with pytest.raises(ImportError, match=r"drt-core\[iceberg\]"):
        list(src.extract("SELECT 1", cfg))


def test_extract_registers_table_part_and_yields_dicts(monkeypatch: pytest.MonkeyPatch) -> None:
    pyiceberg_catalog, catalog, conn, arrow = _mock_libs(
        monkeypatch, rows=[(1, "a@x.com")], cols=["id", "email"]
    )
    cfg = IcebergProfile(type="iceberg", table="analytics.users", catalog_name="prod")
    rows = list(IcebergSource().extract("SELECT id, email FROM users", cfg))

    assert rows == [{"id": 1, "email": "a@x.com"}]
    pyiceberg_catalog.load_catalog.assert_called_once_with("prod")
    catalog.load_table.assert_called_once_with("analytics.users")
    conn.register.assert_called_once_with("users", arrow)  # table part only
    conn.execute.assert_called_once_with("SELECT id, email FROM users")
    conn.close.assert_called_once()


def test_profile_round_trip(tmp_path: Any) -> None:
    p = IcebergProfile(
        type="iceberg",
        table="analytics.users",
        catalog_uri="https://cat/api",
        warehouse="s3://b/wh",
        catalog_name="prod",
        properties={"s3.region": "us-east-1"},
    )
    save_profile("ice_test", p, config_dir=tmp_path)
    loaded = load_profile("ice_test", config_dir=tmp_path)
    assert isinstance(loaded, IcebergProfile)
    assert loaded.table == "analytics.users"
    assert loaded.catalog_uri == "https://cat/api"
    assert loaded.warehouse == "s3://b/wh"
    assert loaded.catalog_name == "prod"
    assert loaded.properties == {"s3.region": "us-east-1"}


def test_load_profile_requires_table(tmp_path: Any) -> None:
    (tmp_path / "profiles.yml").write_text("bad:\n  type: iceberg\n")
    with pytest.raises(ValueError, match="table"):
        load_profile("bad", config_dir=tmp_path)


def test_connection_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    catalog = MagicMock()
    pyiceberg_catalog = MagicMock()
    pyiceberg_catalog.load_catalog.return_value = catalog
    monkeypatch.setitem(sys.modules, "pyiceberg", MagicMock())
    monkeypatch.setitem(sys.modules, "pyiceberg.catalog", pyiceberg_catalog)
    cfg = IcebergProfile(type="iceberg", table="ns.users")
    assert IcebergSource().test_connection(cfg) is True
    catalog.load_table.assert_called_once_with("ns.users")


def test_connection_false_on_error(monkeypatch: pytest.MonkeyPatch) -> None:
    pyiceberg_catalog = MagicMock()
    pyiceberg_catalog.load_catalog.side_effect = RuntimeError("no catalog")
    monkeypatch.setitem(sys.modules, "pyiceberg", MagicMock())
    monkeypatch.setitem(sys.modules, "pyiceberg.catalog", pyiceberg_catalog)
    cfg = IcebergProfile(type="iceberg", table="ns.users")
    assert IcebergSource().test_connection(cfg) is False


def test_connection_false_without_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "pyiceberg.catalog", None)
    cfg = IcebergProfile(type="iceberg", table="ns.users")
    assert IcebergSource().test_connection(cfg) is False
