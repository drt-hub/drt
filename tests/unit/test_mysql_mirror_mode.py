"""Unit tests for ``sync.mode: mirror`` on the MySQL destination (#340 Step 2).

Mirror mode upserts source rows like ``full`` mode, then in the
``finalize_sync`` end-of-sync hook issues a single DELETE that removes
destination rows whose ``upsert_key`` is not in the set seen across
all batches.

Strategy under test: application-side diff (collect upsert_key tuples
in memory, then ``DELETE WHERE key NOT IN (collected)``). Memory-bound
to the source key cardinality. The temp-table strategy is a planned
follow-up for tables larger than a few million rows.

pymysql does not auto-expand tuple-of-tuples like psycopg2, so the
DELETE is built with explicit ``%s`` placeholders — these tests verify
that shape directly via the captured ``cur.execute`` call.

These tests mock ``pymysql`` connections — no real MySQL needed.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pymysql")

from typing import Any
from unittest.mock import MagicMock, patch

from drt.config.models import MySQLDestinationConfig, SyncOptions
from drt.destinations.mysql import MySQLDestination


def _options(**kwargs: Any) -> SyncOptions:
    defaults: dict[str, Any] = {"mode": "mirror"}
    defaults.update(kwargs)
    return SyncOptions(**defaults)


def _config(**overrides: Any) -> MySQLDestinationConfig:
    defaults: dict[str, Any] = {
        "type": "mysql",
        "host": "localhost",
        "dbname": "testdb",
        "user": "testuser",
        "password": "testpass",
        "table": "scores",
        "upsert_key": ["id"],
    }
    defaults.update(overrides)
    return MySQLDestinationConfig(**defaults)


def _fake_connection() -> MagicMock:
    conn = MagicMock()
    conn.cursor.return_value = MagicMock()
    return conn


# ---------------------------------------------------------------------------
# SyncOptions schema
# ---------------------------------------------------------------------------


def test_sync_options_accepts_mirror_mode() -> None:
    """``mode: mirror`` is a valid SyncOptions value (#340)."""
    opts = SyncOptions(mode="mirror")
    assert opts.mode == "mirror"


# ---------------------------------------------------------------------------
# Single-column upsert_key
# ---------------------------------------------------------------------------


def test_mirror_accumulates_keys_across_batches() -> None:
    """``_mirror_keys`` collects the upsert_key tuple from every loaded record."""
    dest = MySQLDestination()
    conn = _fake_connection()
    config = _config()
    opts = _options()

    with patch.object(MySQLDestination, "_connect", return_value=conn):
        dest.load(
            [{"id": 1, "score": 100}, {"id": 2, "score": 200}],
            config,
            opts,
        )
        dest.load(
            [{"id": 3, "score": 300}],
            config,
            opts,
        )

    assert dest._mirror_keys == [(1,), (2,), (3,)]


def test_finalize_mirror_issues_delete_with_collected_keys() -> None:
    """``finalize_sync`` runs ``DELETE WHERE id NOT IN (%s, %s)``."""
    dest = MySQLDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    config = _config()
    opts = _options()

    with patch.object(MySQLDestination, "_connect", return_value=load_conn):
        dest.load(
            [{"id": 1, "score": 100}, {"id": 2, "score": 200}],
            config,
            opts,
        )

    with patch.object(MySQLDestination, "_connect", return_value=finalize_conn):
        result = dest.finalize_sync(config, opts)

    # finalize_sync returns SyncResult on success
    assert result is not None
    assert result.success == 0
    assert result.failed == 0
    # DELETE was executed exactly once
    cur = finalize_conn.cursor.return_value
    assert cur.execute.call_count == 1
    stmt, params = cur.execute.call_args[0]
    # Single column form: flat list of values, two placeholders
    assert "NOT IN (%s, %s)" in stmt
    assert "`scores`" in stmt
    assert "`id`" in stmt
    assert set(params) == {1, 2}
    # commit ran
    finalize_conn.commit.assert_called_once()


def test_finalize_mirror_dedupes_overlapping_batches() -> None:
    """If two batches both contain id=1, the DELETE NOT IN list lists it once."""
    dest = MySQLDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    config = _config()
    opts = _options()

    with patch.object(MySQLDestination, "_connect", return_value=load_conn):
        dest.load([{"id": 1, "score": 100}], config, opts)
        dest.load([{"id": 1, "score": 999}], config, opts)
        dest.load([{"id": 2, "score": 200}], config, opts)

    with patch.object(MySQLDestination, "_connect", return_value=finalize_conn):
        dest.finalize_sync(config, opts)

    cur = finalize_conn.cursor.return_value
    stmt, params = cur.execute.call_args[0]
    # Two unique keys, even though id=1 came in twice
    assert sorted(params) == [1, 2]
    assert stmt.count("%s") == 2


def test_finalize_mirror_quotes_schema_qualified_table() -> None:
    """A ``schema.table`` config emits ``\\`schema\\`.\\`table\\``` in the DELETE."""
    dest = MySQLDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    config = _config(table="reporting.scores")
    opts = _options()

    with patch.object(MySQLDestination, "_connect", return_value=load_conn):
        dest.load([{"id": 1, "score": 100}], config, opts)
    with patch.object(MySQLDestination, "_connect", return_value=finalize_conn):
        dest.finalize_sync(config, opts)

    cur = finalize_conn.cursor.return_value
    stmt, _params = cur.execute.call_args[0]
    assert "`reporting`.`scores`" in stmt


# ---------------------------------------------------------------------------
# Composite upsert_key
# ---------------------------------------------------------------------------


def test_mirror_composite_key_accumulates_tuples() -> None:
    """Two-column upsert_key yields 2-tuples in ``_mirror_keys``."""
    dest = MySQLDestination()
    conn = _fake_connection()
    config = _config(upsert_key=["user_id", "session_id"])
    opts = _options()

    with patch.object(MySQLDestination, "_connect", return_value=conn):
        dest.load(
            [
                {"user_id": "a", "session_id": "x", "score": 1},
                {"user_id": "a", "session_id": "y", "score": 2},
                {"user_id": "b", "session_id": "x", "score": 3},
            ],
            config,
            opts,
        )

    assert dest._mirror_keys == [("a", "x"), ("a", "y"), ("b", "x")]


def test_finalize_mirror_composite_key_delete_shape() -> None:
    """Composite upsert_key → DELETE WHERE (c1, c2) NOT IN ((%s, %s), (%s, %s))."""
    dest = MySQLDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    config = _config(upsert_key=["user_id", "session_id"])
    opts = _options()

    with patch.object(MySQLDestination, "_connect", return_value=load_conn):
        dest.load(
            [
                {"user_id": "a", "session_id": "x", "score": 1},
                {"user_id": "b", "session_id": "y", "score": 2},
            ],
            config,
            opts,
        )

    with patch.object(MySQLDestination, "_connect", return_value=finalize_conn):
        dest.finalize_sync(config, opts)

    cur = finalize_conn.cursor.return_value
    stmt, params = cur.execute.call_args[0]
    # Composite form expands to flat list of values: (a, x, b, y) in some order
    assert "(`user_id`, `session_id`)" in stmt
    assert "NOT IN ((%s, %s), (%s, %s))" in stmt
    # Reconstruct the (k1, k2) tuples from the flat param list to check content
    pairs = {(params[i], params[i + 1]) for i in range(0, len(params), 2)}
    assert pairs == {("a", "x"), ("b", "y")}


# ---------------------------------------------------------------------------
# Safety paths
# ---------------------------------------------------------------------------


def test_finalize_mirror_skips_when_no_keys_observed() -> None:
    """No batch ever delivered records → finalize returns None, no DELETE.

    Prevents a transient empty source from silently wiping the destination.
    """
    dest = MySQLDestination()
    finalize_conn = _fake_connection()
    config = _config()
    opts = _options()

    # No load() called; _mirror_keys is still None.
    with patch.object(MySQLDestination, "_connect", return_value=finalize_conn):
        result = dest.finalize_sync(config, opts)

    assert result is None
    finalize_conn.cursor.assert_not_called()


def test_finalize_mirror_resets_state_after_run() -> None:
    """After finalize, ``_mirror_keys`` is cleared so a re-run starts fresh."""
    dest = MySQLDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    config = _config()
    opts = _options()

    with patch.object(MySQLDestination, "_connect", return_value=load_conn):
        dest.load([{"id": 1, "score": 100}], config, opts)
    with patch.object(MySQLDestination, "_connect", return_value=finalize_conn):
        dest.finalize_sync(config, opts)

    assert dest._mirror_keys is None


def test_mirror_raises_when_upsert_key_missing() -> None:
    """Mirror mode without ``upsert_key`` is a config error surfaced at load."""
    dest = MySQLDestination()
    conn = _fake_connection()
    # MySQLDestinationConfig requires upsert_key, but [] is permitted by
    # the type — guard against it explicitly in load().
    config = _config(upsert_key=[])
    opts = _options()

    with patch.object(MySQLDestination, "_connect", return_value=conn):
        with pytest.raises(ValueError, match="mirror requires destination.upsert_key"):
            dest.load([{"id": 1, "score": 100}], config, opts)


def test_finalize_sync_swap_still_works_when_mode_not_mirror() -> None:
    """The mirror branch must not break the existing swap-finalize path."""
    dest = MySQLDestination()
    dest._swap_shadow_created = True
    dest._swap_table = "scores"

    conn = _fake_connection()
    config = _config()
    swap_opts = SyncOptions(mode="replace", replace_strategy="swap")

    with patch.object(MySQLDestination, "_connect", return_value=conn):
        result = dest.finalize_sync(config, swap_opts)

    # Swap returned a SyncResult and cleared state
    assert result is not None
    assert dest._swap_shadow_created is False
    assert dest._swap_table is None
