"""Unit tests for the Delta Lake source.

The `deltalake` extra and DuckDB are mocked via sys.modules injection, so these
run without either installed.
"""

from __future__ import annotations

import sys
from typing import Any
from unittest.mock import MagicMock

import pytest

from drt.config.credentials import DeltaLakeProfile, load_profile, save_profile
from drt.sources.base import Source
from drt.sources.deltalake import DeltaLakeSource, _table_name


def _mock_libs(monkeypatch: pytest.MonkeyPatch, rows: list[tuple], cols: list[str]):
    arrow = object()
    dt = MagicMock()
    dt.to_pyarrow_table.return_value = arrow
    deltalake_mod = MagicMock()
    deltalake_mod.DeltaTable.return_value = dt
    monkeypatch.setitem(sys.modules, "deltalake", deltalake_mod)

    result = MagicMock()
    result.description = [(c,) for c in cols]
    result.fetchall.return_value = rows
    conn = MagicMock()
    conn.execute.return_value = result
    duckdb_mod = MagicMock()
    duckdb_mod.connect.return_value = conn
    monkeypatch.setitem(sys.modules, "duckdb", duckdb_mod)
    return deltalake_mod, conn, arrow


def test_implements_source_protocol() -> None:
    assert isinstance(DeltaLakeSource(), Source)


def test_describe() -> None:
    p = DeltaLakeProfile(type="deltalake", location="s3://b/delta/users")
    assert "deltalake" in p.describe()
    assert "s3://b/delta/users" in p.describe()


def test_table_name_default_is_last_path_segment() -> None:
    assert _table_name(DeltaLakeProfile(type="deltalake", location="s3://b/delta/users")) == "users"
    assert _table_name(DeltaLakeProfile(type="deltalake", location="/data/orders/")) == "orders"
    assert _table_name(DeltaLakeProfile(type="deltalake", location="/x", table="t")) == "t"


def test_extract_raises_without_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "deltalake", None)
    src = DeltaLakeSource()
    cfg = DeltaLakeProfile(type="deltalake", location="/x/users")
    with pytest.raises(ImportError, match=r"drt-core\[deltalake\]"):
        list(src.extract("SELECT 1", cfg))


def test_extract_registers_table_and_yields_dicts(monkeypatch: pytest.MonkeyPatch) -> None:
    deltalake_mod, conn, arrow = _mock_libs(
        monkeypatch, rows=[(1, "a@x.com"), (2, "b@x.com")], cols=["id", "email"]
    )
    cfg = DeltaLakeProfile(type="deltalake", location="s3://b/delta/users")
    rows = list(DeltaLakeSource().extract("SELECT id, email FROM users", cfg))

    assert rows == [{"id": 1, "email": "a@x.com"}, {"id": 2, "email": "b@x.com"}]
    deltalake_mod.DeltaTable.assert_called_once_with("s3://b/delta/users", storage_options=None)
    conn.register.assert_called_once_with("users", arrow)
    conn.execute.assert_called_once_with("SELECT id, email FROM users")
    conn.close.assert_called_once()


def test_storage_options_env_resolved(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_KEY", "secret123")
    deltalake_mod, _conn, _arrow = _mock_libs(monkeypatch, rows=[], cols=[])
    cfg = DeltaLakeProfile(
        type="deltalake",
        location="/x/users",
        storage_options={"AWS_ACCESS_KEY_ID_ENV": "AWS_KEY", "region": "us-east-1"},
    )
    list(DeltaLakeSource().extract("SELECT 1", cfg))
    deltalake_mod.DeltaTable.assert_called_once_with(
        "/x/users", storage_options={"AWS_ACCESS_KEY_ID": "secret123", "region": "us-east-1"}
    )


def test_profile_round_trip(tmp_path: Any) -> None:
    p = DeltaLakeProfile(
        type="deltalake",
        location="s3://b/delta/users",
        table="users",
        storage_options={"AWS_ACCESS_KEY_ID_ENV": "AWS_KEY"},
    )
    save_profile("delta_test", p, config_dir=tmp_path)
    loaded = load_profile("delta_test", config_dir=tmp_path)
    assert isinstance(loaded, DeltaLakeProfile)
    assert loaded.location == "s3://b/delta/users"
    assert loaded.table == "users"
    assert loaded.storage_options == {"AWS_ACCESS_KEY_ID_ENV": "AWS_KEY"}


def test_load_profile_requires_location(tmp_path: Any) -> None:
    (tmp_path / "profiles.yml").write_text("bad:\n  type: deltalake\n")
    with pytest.raises(ValueError, match="location"):
        load_profile("bad", config_dir=tmp_path)


def test_connection_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    dt = MagicMock()
    dt.version.return_value = 3
    deltalake_mod = MagicMock()
    deltalake_mod.DeltaTable.return_value = dt
    monkeypatch.setitem(sys.modules, "deltalake", deltalake_mod)
    cfg = DeltaLakeProfile(type="deltalake", location="/x/users")
    assert DeltaLakeSource().test_connection(cfg) is True
    deltalake_mod.DeltaTable.assert_called_once_with("/x/users", storage_options=None)


def test_connection_false_when_unreachable(monkeypatch: pytest.MonkeyPatch) -> None:
    deltalake_mod = MagicMock()
    deltalake_mod.DeltaTable.side_effect = RuntimeError("table not found")
    monkeypatch.setitem(sys.modules, "deltalake", deltalake_mod)
    cfg = DeltaLakeProfile(type="deltalake", location="/x/missing")
    assert DeltaLakeSource().test_connection(cfg) is False


def test_connection_false_without_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "deltalake", None)
    cfg = DeltaLakeProfile(type="deltalake", location="/x/users")
    assert DeltaLakeSource().test_connection(cfg) is False
