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
from drt.destinations.base import SyncResult
from drt.destinations.mysql import MySQLDestination
from drt.destinations.row_errors import RowError


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


def test_mirror_excludes_failed_record_keys_from_accumulation() -> None:
    """Records whose batch_index appears in row_errors are skipped from ``_mirror_keys``.

    Only successfully-loaded keys count as "source state" — otherwise a
    transient row-level failure could cause the finalize DELETE to wipe a
    row that actually exists in the source.
    """
    dest = MySQLDestination()
    conn = _fake_connection()
    config = _config()
    opts = _options()

    # Force _load_upsert to report record at batch_index=1 as failed,
    # leaving indices 0 and 2 as successful.
    canned_result = SyncResult(
        success=2,
        failed=1,
        row_errors=[
            RowError(
                batch_index=1,
                record_preview='{"id": 2}',
                http_status=None,
                error_message="forced for test",
            )
        ],
    )

    with patch.object(MySQLDestination, "_connect", return_value=conn), patch.object(
        MySQLDestination, "_load_upsert", return_value=canned_result
    ):
        dest.load(
            [
                {"id": 1, "score": 100},
                {"id": 2, "score": 200},
                {"id": 3, "score": 300},
            ],
            config,
            opts,
        )

    # id=2 was the failed record; mirror_keys must contain only 1 and 3.
    assert dest._mirror_keys == [(1,), (3,)]


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


# ---------------------------------------------------------------------------
# mirror.strategy: tracked (#686)
# ---------------------------------------------------------------------------


def _tracked_options() -> SyncOptions:
    opts = _options(mirror={"strategy": "tracked"})
    opts._sync_name = "scores_sync"
    return opts


def test_tracked_first_run_baselines_without_deleting_mysql() -> None:
    """No prior state: keys inserted into _drt_synced_keys, target untouched."""
    dest = MySQLDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    cur = finalize_conn.cursor.return_value
    cur.fetchall.return_value = []

    with patch.object(MySQLDestination, "_connect", return_value=load_conn):
        dest.load([{"id": 1}, {"id": 2}], _config(), _tracked_options())
    with patch.object(MySQLDestination, "_connect", return_value=finalize_conn):
        result = dest.finalize_sync(_config(), _tracked_options())

    assert result is not None
    for call in cur.execute.call_args_list:
        stmt = str(call.args[0])
        if "DELETE" in stmt:
            assert "`scores`" not in stmt
    rows = cur.executemany.call_args.args[1]
    assert [r[0] for r in rows] == ["scores_sync", "scores_sync"]
    finalize_conn.commit.assert_called_once()


def test_tracked_second_run_deletes_only_stale_tracked_keys_mysql() -> None:
    """prev={1,2,3}, current={1,2} -> DELETE `scores` WHERE `id` IN (%s) w/ [3]."""
    from drt.destinations._mirror_state import key_hash, key_json

    dest = MySQLDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    cur = finalize_conn.cursor.return_value
    cur.fetchall.return_value = [
        (key_hash((k,)), key_json((k,))) for k in (1, 2, 3)
    ]

    with patch.object(MySQLDestination, "_connect", return_value=load_conn):
        dest.load([{"id": 1}, {"id": 2}], _config(), _tracked_options())
    with patch.object(MySQLDestination, "_connect", return_value=finalize_conn):
        dest.finalize_sync(_config(), _tracked_options())

    target_deletes = [
        c
        for c in cur.execute.call_args_list
        if "DELETE" in str(c.args[0]) and "`scores`" in str(c.args[0])
    ]
    assert len(target_deletes) == 1
    stmt, params = target_deletes[0].args
    assert "IN (%s)" in stmt and "NOT IN" not in stmt
    assert params == [3]
    finalize_conn.commit.assert_called_once()


def test_tracked_composite_key_flattens_params_mysql() -> None:
    """Composite key -> (`c1`, `c2`) IN ((%s, %s)) with flattened params."""
    from drt.destinations._mirror_state import key_hash, key_json

    dest = MySQLDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    cur = finalize_conn.cursor.return_value
    cur.fetchall.return_value = [
        (key_hash((1, "a")), key_json((1, "a"))),
        (key_hash((2, "b")), key_json((2, "b"))),
    ]
    config = _config(upsert_key=["tenant_id", "user_id"])

    with patch.object(MySQLDestination, "_connect", return_value=load_conn):
        dest.load(
            [{"tenant_id": 1, "user_id": "a"}], config, _tracked_options()
        )
    with patch.object(MySQLDestination, "_connect", return_value=finalize_conn):
        dest.finalize_sync(config, _tracked_options())

    target_deletes = [
        c
        for c in cur.execute.call_args_list
        if "DELETE" in str(c.args[0]) and "`scores`" in str(c.args[0])
    ]
    assert len(target_deletes) == 1
    stmt, params = target_deletes[0].args
    assert "(`tenant_id`, `user_id`) IN ((%s, %s))" in stmt
    assert params == [2, "b"]


def test_tracked_empty_source_is_noop_mysql() -> None:
    """No batches observed -> finalize no-op, baseline preserved."""
    dest = MySQLDestination()
    finalize_conn = _fake_connection()

    with patch.object(MySQLDestination, "_connect", return_value=finalize_conn):
        result = dest.finalize_sync(_config(), _tracked_options())

    assert result is None
    finalize_conn.cursor.return_value.execute.assert_not_called()


def test_tracked_state_table_in_target_database_mysql() -> None:
    """Qualified target `mydb.scores` -> state table `mydb`.`_drt_synced_keys`."""
    dest = MySQLDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    cur = finalize_conn.cursor.return_value
    cur.fetchall.return_value = []
    config = _config(table="mydb.scores")

    with patch.object(MySQLDestination, "_connect", return_value=load_conn):
        dest.load([{"id": 1}], config, _tracked_options())
    with patch.object(MySQLDestination, "_connect", return_value=finalize_conn):
        dest.finalize_sync(config, _tracked_options())

    executed = " | ".join(str(c.args[0]) for c in cur.execute.call_args_list)
    assert "`mydb`.`_drt_synced_keys`" in executed


def test_tracked_creates_state_table_when_absent_mysql() -> None:
    """information_schema COUNT -> 0: the state table is created (lazy default)."""
    dest = MySQLDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    cur = finalize_conn.cursor.return_value
    cur.fetchone.return_value = (0,)  # existence probe: table absent
    cur.fetchall.return_value = []

    with patch.object(MySQLDestination, "_connect", return_value=load_conn):
        dest.load([{"id": 1}], _config(), _tracked_options())
    with patch.object(MySQLDestination, "_connect", return_value=finalize_conn):
        dest.finalize_sync(_config(), _tracked_options())

    assert any(
        "CREATE TABLE" in str(c.args[0]) for c in cur.execute.call_args_list
    )
    # existence is probed via information_schema, not blind DDL
    assert any(
        "information_schema.tables" in str(c.args[0])
        for c in cur.execute.call_args_list
    )


def test_tracked_skips_create_when_state_table_preprovisioned_mysql() -> None:
    """information_schema COUNT -> 1: no CREATE, so a no-DDL user can run (#695).

    MySQL checks the CREATE privilege before the ``IF NOT EXISTS`` existence
    check, so the statement must not be emitted at all when the table exists.
    """
    dest = MySQLDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    cur = finalize_conn.cursor.return_value
    cur.fetchone.return_value = (1,)  # existence probe: already exists
    cur.fetchall.return_value = []

    with patch.object(MySQLDestination, "_connect", return_value=load_conn):
        dest.load([{"id": 1}], _config(), _tracked_options())
    with patch.object(MySQLDestination, "_connect", return_value=finalize_conn):
        dest.finalize_sync(_config(), _tracked_options())

    assert not any(
        "CREATE TABLE" in str(c.args[0]) for c in cur.execute.call_args_list
    )
    assert any(
        "_drt_synced_keys" in str(c.args[0]) for c in cur.execute.call_args_list
    )


# ---------------------------------------------------------------------------
# mirror.scope (#687)
# ---------------------------------------------------------------------------


def _scoped_options() -> SyncOptions:
    return _options(mirror={"scope": ["parent_id"]})


def test_scope_missing_column_fails_fast_mysql() -> None:
    """A scope column absent from the model output is a config error at load."""
    dest = MySQLDestination()
    conn = _fake_connection()

    with patch.object(MySQLDestination, "_connect", return_value=conn):
        with pytest.raises(ValueError, match="parent_id"):
            dest.load(
                [{"id": 1, "score": 100}],
                _config(upsert_key=["id"]),
                _scoped_options(),
            )


def test_scoped_mirror_deletes_within_observed_parents_only_mysql() -> None:
    """DELETE gains `` `parent_id` IN (...) AND `id` NOT IN (...) `` w/ params."""
    dest = MySQLDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    config = _config(upsert_key=["id"])
    opts = _scoped_options()

    with patch.object(MySQLDestination, "_connect", return_value=load_conn):
        dest.load(
            [
                {"id": 1, "parent_id": 10},
                {"id": 2, "parent_id": 10},
                {"id": 3, "parent_id": 20},
            ],
            config,
            opts,
        )
    with patch.object(MySQLDestination, "_connect", return_value=finalize_conn):
        result = dest.finalize_sync(config, opts)

    assert result is not None
    cur = finalize_conn.cursor.return_value
    assert cur.execute.call_count == 1
    stmt, params = cur.execute.call_args.args
    assert "`parent_id` IN (%s, %s)" in stmt
    assert "NOT IN (%s, %s, %s)" in stmt
    # scope params first, then key params
    assert set(params[:2]) == {10, 20}
    assert set(params[2:]) == {1, 2, 3}


def test_scoped_mirror_composite_scope_flattens_params_mysql() -> None:
    """Composite scope -> (`t`, `p`) IN ((%s, %s)) with flattened params."""
    dest = MySQLDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    config = _config(upsert_key=["id"])
    opts = _options(mirror={"scope": ["tenant_id", "parent_id"]})

    with patch.object(MySQLDestination, "_connect", return_value=load_conn):
        dest.load(
            [{"id": 1, "tenant_id": "t1", "parent_id": 10}],
            config,
            opts,
        )
    with patch.object(MySQLDestination, "_connect", return_value=finalize_conn):
        dest.finalize_sync(config, opts)

    cur = finalize_conn.cursor.return_value
    stmt, params = cur.execute.call_args.args
    assert "(`tenant_id`, `parent_id`) IN ((%s, %s))" in stmt
    assert params[:2] == ["t1", 10]
