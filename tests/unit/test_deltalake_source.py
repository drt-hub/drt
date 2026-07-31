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
    # A *dataset*, not a table: since #679 the source registers the lazy
    # dataset so DuckDB pushes filters and column selection into the scan.
    arrow = object()
    dt = MagicMock()
    dt.to_pyarrow_dataset.return_value = arrow
    deltalake_mod = MagicMock()
    deltalake_mod.DeltaTable.return_value = dt
    monkeypatch.setitem(sys.modules, "deltalake", deltalake_mod)

    result = MagicMock()
    result.description = [(c,) for c in cols]
    # Batched since #765 — one full batch then empty terminates the loop.
    result.fetchmany.side_effect = [rows, []]
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


def test_registers_a_lazy_dataset_not_a_materialised_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#679: the whole point is that the table is never fully read.

    ``to_pyarrow_table()`` pulled every row into memory before DuckDB saw the
    query, so a model's incremental WHERE reduced nothing. Measured on a 300k
    row table (~200B rows), fresh process, selecting 2 of 3 columns and ~1/9th
    of the rows: +244 MB RSS before, +120 MB after.
    """
    deltalake_mod, conn, dataset = _mock_libs(monkeypatch, rows=[(1,)], cols=["id"])
    cfg = DeltaLakeProfile(type="deltalake", location="/tmp/d", table="t")

    list(DeltaLakeSource().extract("SELECT id FROM t", cfg))

    dt = deltalake_mod.DeltaTable.return_value
    dt.to_pyarrow_table.assert_not_called()
    dt.to_pyarrow_dataset.assert_called_once()
    conn.register.assert_called_once_with("t", dataset)


def test_result_is_fetched_in_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    """The result set is not materialised either (#765)."""
    _mod, conn, _ds = _mock_libs(monkeypatch, rows=[(1,), (2,)], cols=["id"])
    cfg = DeltaLakeProfile(type="deltalake", location="/tmp/d", table="t")

    rows = list(DeltaLakeSource().extract("SELECT id FROM t", cfg))

    assert rows == [{"id": 1}, {"id": 2}]
    conn.execute.return_value.fetchall.assert_not_called()


@pytest.mark.parametrize("query", ["SELECT count(*) FROM t a JOIN t b ON a.id = b.id"])
def test_a_two_pass_query_is_still_correct(query: str, tmp_path: Any) -> None:
    """Guards the trap that ruled out `to_arrow_batch_reader()`.

    A batch reader is single-pass: DuckDB drains it on the first scan and the
    second sees nothing, so a self-join returns **0 rather than raising** —
    silently wrong results, which is worse than a failure. A dataset can be
    scanned repeatedly. Verified against a real Delta table rather than a mock,
    since the mock cannot reproduce exhaustion.
    """
    deltalake = pytest.importorskip("deltalake")
    pa = pytest.importorskip("pyarrow")
    pytest.importorskip("duckdb")

    loc = str(tmp_path / "t")
    deltalake.write_deltalake(loc, pa.table({"id": pa.array(range(100))}))
    cfg = DeltaLakeProfile(type="deltalake", location=loc, table="t")

    rows = list(DeltaLakeSource().extract(query, cfg))

    assert rows[0]["count_star()"] == 100, "the table was drained by the first scan"
