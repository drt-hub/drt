"""Tests for DuckDB source connector.

Mirrors the SQLite test surface (extract / empty / column names / connection /
invalid config) and adds DuckDB-specific type coverage: BIGINT, DOUBLE,
DATE, TIMESTAMP, NULL.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from unittest.mock import patch

import pytest

duckdb = pytest.importorskip("duckdb")

from drt.config.credentials import DuckDBProfile  # noqa: E402
from drt.sources.duckdb import DuckDBSource  # noqa: E402


@pytest.fixture
def seeded_db(tmp_path: Path) -> str:
    """Path to a DuckDB file with a small `users` table seeded."""
    db_path = str(tmp_path / "users.duckdb")
    conn = duckdb.connect(db_path)
    try:
        conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)")
        conn.executemany(
            "INSERT INTO users VALUES (?, ?, ?)",
            [(1, "Alice", 30), (2, "Bob", 25), (3, "Charlie", 35)],
        )
    finally:
        conn.close()
    return db_path


@pytest.fixture
def source() -> DuckDBSource:
    return DuckDBSource()


@pytest.fixture
def profile(seeded_db: str) -> DuckDBProfile:
    return DuckDBProfile(type="duckdb", database=seeded_db)


def test_extract_returns_all_rows(source: DuckDBSource, profile: DuckDBProfile) -> None:
    rows = list(source.extract("SELECT id, name, age FROM users ORDER BY id", profile))

    assert len(rows) == 3
    assert rows[0] == {"id": 1, "name": "Alice", "age": 30}
    assert rows[1] == {"id": 2, "name": "Bob", "age": 25}
    assert rows[2] == {"id": 3, "name": "Charlie", "age": 35}


def test_extract_empty_result(source: DuckDBSource, profile: DuckDBProfile) -> None:
    rows = list(source.extract("SELECT * FROM users WHERE age > 100", profile))

    assert rows == []


def test_extract_column_names(source: DuckDBSource, profile: DuckDBProfile) -> None:
    row = next(source.extract("SELECT id, name FROM users LIMIT 1", profile))

    assert set(row.keys()) == {"id", "name"}


def test_extract_null_values(source: DuckDBSource, tmp_path: Path) -> None:
    db_path = str(tmp_path / "nulls.duckdb")
    conn = duckdb.connect(db_path)
    try:
        conn.execute("CREATE TABLE t (id INTEGER, label VARCHAR)")
        conn.execute("INSERT INTO t VALUES (1, NULL), (2, 'present')")
    finally:
        conn.close()

    profile = DuckDBProfile(type="duckdb", database=db_path)
    rows = list(source.extract("SELECT id, label FROM t ORDER BY id", profile))

    assert rows == [{"id": 1, "label": None}, {"id": 2, "label": "present"}]


def test_extract_numeric_extremes(source: DuckDBSource, tmp_path: Path) -> None:
    db_path = str(tmp_path / "nums.duckdb")
    conn = duckdb.connect(db_path)
    try:
        conn.execute("CREATE TABLE t (big BIGINT, dbl DOUBLE)")
        # BIGINT range: -9223372036854775808 .. 9223372036854775807
        conn.execute("INSERT INTO t VALUES (9223372036854775807, 1.7976931348623157e308)")
        conn.execute("INSERT INTO t VALUES (-9223372036854775808, -1.7976931348623157e308)")
    finally:
        conn.close()

    profile = DuckDBProfile(type="duckdb", database=db_path)
    rows = list(source.extract("SELECT big, dbl FROM t ORDER BY big", profile))

    assert rows[0]["big"] == -9223372036854775808
    assert rows[1]["big"] == 9223372036854775807
    assert rows[0]["dbl"] == -1.7976931348623157e308
    assert rows[1]["dbl"] == 1.7976931348623157e308


def test_extract_date_and_timestamp(source: DuckDBSource, tmp_path: Path) -> None:
    db_path = str(tmp_path / "times.duckdb")
    conn = duckdb.connect(db_path)
    try:
        conn.execute("CREATE TABLE t (d DATE, ts TIMESTAMP)")
        conn.execute("INSERT INTO t VALUES (DATE '2026-05-24', TIMESTAMP '2026-05-24 12:00:00')")
    finally:
        conn.close()

    profile = DuckDBProfile(type="duckdb", database=db_path)
    row = next(source.extract("SELECT d, ts FROM t", profile))

    assert row["d"] == dt.date(2026, 5, 24)
    assert row["ts"] == dt.datetime(2026, 5, 24, 12, 0, 0)


def test_test_connection_success(source: DuckDBSource, profile: DuckDBProfile) -> None:
    assert source.test_connection(profile) is True


def test_test_connection_failure_on_bad_path(source: DuckDBSource) -> None:
    bad = DuckDBProfile(type="duckdb", database="/nonexistent/dir/db.duckdb")
    assert source.test_connection(bad) is False


def test_invalid_config_type_rejected(source: DuckDBSource) -> None:
    class FakeConfig:
        database = ":memory:"

    # NOTE: current impl uses `assert isinstance(...)`, which raises
    # AssertionError (and is suppressed under `python -O`). Tracked as a
    # follow-up — see PR description. Mirror current behavior for now.
    with pytest.raises(AssertionError):
        list(source.extract("SELECT 1", FakeConfig()))  # type: ignore[arg-type]


class TestStreamingExtraction:
    """#765: DuckDB fetches in batches instead of materialising everything.

    "It's a local file" is not the same as "it's free" — the cost being
    removed is holding every row as a Python object, which a local file incurs
    just as readily as a remote warehouse. Measured on 300k rows of ~200B in a
    fresh process: +150.9 MB RSS for ``fetchall()``, +42.2 MB batched.
    """

    def test_does_not_call_fetchall(self, tmp_path):
        db = str(tmp_path / "t.duckdb")
        duckdb = pytest.importorskip("duckdb")
        con = duckdb.connect(db)
        con.execute("CREATE TABLE t AS SELECT 1 AS id, 'a' AS name")
        con.close()

        with patch("duckdb.connect") as connect:
            result = connect.return_value.execute.return_value
            result.description = [("id",), ("name",)]
            result.fetchmany.side_effect = [[(1, "a")], []]

            rows = list(DuckDBSource().extract("SELECT * FROM t", DuckDBProfile(
                type="duckdb", database=db)))

        assert rows == [{"id": 1, "name": "a"}]
        result.fetchall.assert_not_called()

    def test_fetchmany_uses_fetch_size(self, tmp_path):
        db = str(tmp_path / "t.duckdb")
        with patch("duckdb.connect") as connect:
            result = connect.return_value.execute.return_value
            result.description = [("id",)]
            result.fetchmany.side_effect = [[(1,)], []]

            list(DuckDBSource().extract("SELECT 1", DuckDBProfile(
                type="duckdb", database=db, fetch_size=250)))

        assert result.fetchmany.call_args_list[0].args[0] == 250

    def test_batches_are_concatenated_in_order(self, tmp_path):
        """Multiple batches must join seamlessly — the boundary is where a
        naive loop drops or repeats a row."""
        db = str(tmp_path / "t.duckdb")
        with patch("duckdb.connect") as connect:
            result = connect.return_value.execute.return_value
            result.description = [("id",)]
            result.fetchmany.side_effect = [[(1,), (2,)], [(3,)], []]

            rows = list(DuckDBSource().extract("SELECT 1", DuckDBProfile(
                type="duckdb", database=db)))

        assert [r["id"] for r in rows] == [1, 2, 3]

    def test_empty_result_yields_nothing(self, tmp_path):
        db = str(tmp_path / "t.duckdb")
        with patch("duckdb.connect") as connect:
            result = connect.return_value.execute.return_value
            result.description = [("id",)]
            result.fetchmany.side_effect = [[]]

            assert list(DuckDBSource().extract("SELECT 1", DuckDBProfile(
                type="duckdb", database=db))) == []

    def test_real_file_roundtrip(self, tmp_path):
        """End to end against a real DuckDB file, spanning several batches."""
        duckdb = pytest.importorskip("duckdb")
        db = str(tmp_path / "t.duckdb")
        con = duckdb.connect(db)
        con.execute("CREATE TABLE t AS SELECT i AS id FROM range(2500) s(i)")
        con.close()

        profile = DuckDBProfile(type="duckdb", database=db, fetch_size=100)
        rows = list(DuckDBSource().extract("SELECT id FROM t ORDER BY id", profile))

        assert [r["id"] for r in rows] == list(range(2500))
