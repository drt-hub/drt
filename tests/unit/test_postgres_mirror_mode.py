"""Unit tests for ``sync.mode: mirror`` on the Postgres destination (#340).

Mirror mode upserts source rows like ``full`` mode, then in the
``finalize_sync`` end-of-sync hook issues a single DELETE that removes
destination rows whose ``upsert_key`` is not in the set seen across
all batches.

Strategy under test: application-side diff (collect upsert_key tuples
in memory, then ``DELETE WHERE key NOT IN (collected)``). Memory-bound
to the source key cardinality. The temp-table strategy is a planned
follow-up for tables larger than a few million rows.

These tests mock ``psycopg2`` connections — no real PostgreSQL needed.
The contract under test is: did the destination issue the right DELETE
SQL with the right parameter shape, given a series of batches?
"""

from __future__ import annotations

import pytest

pytest.importorskip("psycopg2.sql")

from typing import Any
from unittest.mock import MagicMock, patch

from drt.config.models import PostgresDestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.postgres import PostgresDestination
from drt.destinations.row_errors import RowError


def _options(**kwargs: Any) -> SyncOptions:
    defaults: dict[str, Any] = {"mode": "mirror"}
    defaults.update(kwargs)
    return SyncOptions(**defaults)


def _config(**overrides: Any) -> PostgresDestinationConfig:
    defaults: dict[str, Any] = {
        "type": "postgres",
        "host": "localhost",
        "dbname": "testdb",
        "user": "testuser",
        "password": "testpass",
        "table": "public.scores",
        "upsert_key": ["id"],
    }
    defaults.update(overrides)
    return PostgresDestinationConfig(**defaults)


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
    dest = PostgresDestination()
    conn = _fake_connection()
    config = _config()
    opts = _options()

    with patch.object(PostgresDestination, "_connect", return_value=conn):
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
    """``finalize_sync`` runs ``DELETE WHERE id NOT IN (collected)``."""
    dest = PostgresDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    config = _config()
    opts = _options()

    with patch.object(PostgresDestination, "_connect", return_value=load_conn):
        dest.load(
            [{"id": 1, "score": 100}, {"id": 2, "score": 200}],
            config,
            opts,
        )

    with patch.object(PostgresDestination, "_connect", return_value=finalize_conn):
        result = dest.finalize_sync(config, opts)

    # finalize_sync returns SyncResult on success
    assert result is not None
    assert result.success == 0
    assert result.failed == 0
    # DELETE was executed exactly once
    cur = finalize_conn.cursor.return_value
    assert cur.execute.call_count == 1
    # Params is the dedup'd set of single-element tuples flattened: (1, 2)
    _stmt, params = cur.execute.call_args[0]
    assert set(params[0]) == {1, 2}
    # commit ran
    finalize_conn.commit.assert_called_once()


def test_finalize_mirror_dedupes_overlapping_batches() -> None:
    """If two batches both contain id=1, the DELETE NOT IN list lists it once."""
    dest = PostgresDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    config = _config()
    opts = _options()

    with patch.object(PostgresDestination, "_connect", return_value=load_conn):
        dest.load([{"id": 1, "score": 100}], config, opts)
        dest.load([{"id": 1, "score": 999}], config, opts)
        dest.load([{"id": 2, "score": 200}], config, opts)

    with patch.object(PostgresDestination, "_connect", return_value=finalize_conn):
        dest.finalize_sync(config, opts)

    cur = finalize_conn.cursor.return_value
    _stmt, params = cur.execute.call_args[0]
    # Two unique keys, even though id=1 came in twice
    assert sorted(params[0]) == [1, 2]


# ---------------------------------------------------------------------------
# Composite upsert_key
# ---------------------------------------------------------------------------


def test_mirror_composite_key_accumulates_tuples() -> None:
    """Two-column upsert_key yields 2-tuples in ``_mirror_keys``."""
    dest = PostgresDestination()
    conn = _fake_connection()
    config = _config(upsert_key=["user_id", "session_id"])
    opts = _options()

    with patch.object(PostgresDestination, "_connect", return_value=conn):
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
    """Composite upsert_key → DELETE WHERE (c1, c2) NOT IN ((v1a, v2a), ...)."""
    dest = PostgresDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    config = _config(upsert_key=["user_id", "session_id"])
    opts = _options()

    with patch.object(PostgresDestination, "_connect", return_value=load_conn):
        dest.load(
            [
                {"user_id": "a", "session_id": "x", "score": 1},
                {"user_id": "b", "session_id": "y", "score": 2},
            ],
            config,
            opts,
        )

    with patch.object(PostgresDestination, "_connect", return_value=finalize_conn):
        dest.finalize_sync(config, opts)

    cur = finalize_conn.cursor.return_value
    _stmt, params = cur.execute.call_args[0]
    # Tuple of tuples — psycopg2 expands to ((a, x), (b, y))
    assert set(params[0]) == {("a", "x"), ("b", "y")}


# ---------------------------------------------------------------------------
# Safety paths
# ---------------------------------------------------------------------------


def test_finalize_mirror_skips_when_no_keys_observed() -> None:
    """No batch ever delivered records → finalize returns None, no DELETE.

    Prevents a transient empty source from silently wiping the destination.
    """
    dest = PostgresDestination()
    finalize_conn = _fake_connection()
    config = _config()
    opts = _options()

    # No load() called; _mirror_keys is still None.
    with patch.object(PostgresDestination, "_connect", return_value=finalize_conn):
        result = dest.finalize_sync(config, opts)

    assert result is None
    finalize_conn.cursor.assert_not_called()


def test_finalize_mirror_resets_state_after_run() -> None:
    """After finalize, ``_mirror_keys`` is cleared so a re-run starts fresh."""
    dest = PostgresDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    config = _config()
    opts = _options()

    with patch.object(PostgresDestination, "_connect", return_value=load_conn):
        dest.load([{"id": 1, "score": 100}], config, opts)
    with patch.object(PostgresDestination, "_connect", return_value=finalize_conn):
        dest.finalize_sync(config, opts)

    assert dest._mirror_keys is None


def test_mirror_raises_when_upsert_key_missing() -> None:
    """Mirror mode without ``upsert_key`` is a config error surfaced at load."""
    dest = PostgresDestination()
    conn = _fake_connection()
    # PostgresDestinationConfig requires upsert_key, but [] is permitted by
    # the type — guard against it explicitly in load().
    config = _config(upsert_key=[])
    opts = _options()

    with patch.object(PostgresDestination, "_connect", return_value=conn):
        with pytest.raises(ValueError, match="mirror requires destination.upsert_key"):
            dest.load([{"id": 1, "score": 100}], config, opts)


def test_mirror_excludes_failed_record_keys_from_accumulation() -> None:
    """Records whose batch_index appears in row_errors are skipped from ``_mirror_keys``.

    Only successfully-loaded keys count as "source state" — otherwise a
    transient row-level failure could cause the finalize DELETE to wipe a
    row that actually exists in the source. Parity backfill for the
    branch added in #596; sibling test on the MySQL side ships in
    ``tests/unit/test_mysql_mirror_mode.py``.
    """
    dest = PostgresDestination()
    conn = _fake_connection()
    config = _config()
    opts = _options()

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

    with patch.object(PostgresDestination, "_connect", return_value=conn), patch.object(
        PostgresDestination, "_load_upsert", return_value=canned_result
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

    assert dest._mirror_keys == [(1,), (3,)]


def test_finalize_sync_swap_still_works_when_mode_not_mirror() -> None:
    """The mirror branch must not break the existing swap-finalize path."""
    dest = PostgresDestination()
    dest._swap_shadow_created = True
    dest._swap_table = "public.scores"

    conn = _fake_connection()
    config = _config()
    swap_opts = SyncOptions(mode="replace", replace_strategy="swap")

    with patch.object(PostgresDestination, "_connect", return_value=conn):
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


def _executed_sql(cur: MagicMock) -> str:
    """Concatenated repr of every execute/executemany statement."""
    calls = cur.execute.call_args_list + cur.executemany.call_args_list
    return " | ".join(str(c.args[0]) for c in calls)


def test_tracked_first_run_baselines_without_deleting() -> None:
    """No prior state: current keys are inserted, the target sees no DELETE."""
    dest = PostgresDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    cur = finalize_conn.cursor.return_value
    cur.fetchall.return_value = []  # state read: empty -> baseline

    with patch.object(PostgresDestination, "_connect", return_value=load_conn):
        dest.load([{"id": 1}, {"id": 2}], _config(), _tracked_options())
    with patch.object(PostgresDestination, "_connect", return_value=finalize_conn):
        result = dest.finalize_sync(_config(), _tracked_options())

    assert result is not None
    executed = _executed_sql(cur)
    assert "_drt_synced_keys" in executed
    # the only DELETE statements target the state table, never 'scores'
    for call in cur.execute.call_args_list:
        stmt = str(call.args[0])
        if "DELETE" in stmt:
            assert "scores" not in stmt
    # state rewrite recorded both current keys
    rows = cur.executemany.call_args.args[1]
    assert [r[0] for r in rows] == ["scores_sync", "scores_sync"]
    finalize_conn.commit.assert_called_once()


def test_tracked_second_run_deletes_only_stale_tracked_keys() -> None:
    """prev={1,2,3}, current={1,2} -> DELETE scores WHERE id IN ((3,)) only."""
    from drt.destinations._mirror_state import key_hash, key_json

    dest = PostgresDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    cur = finalize_conn.cursor.return_value
    cur.fetchall.return_value = [
        (key_hash((k,)), key_json((k,))) for k in (1, 2, 3)
    ]

    with patch.object(PostgresDestination, "_connect", return_value=load_conn):
        dest.load([{"id": 1}, {"id": 2}], _config(), _tracked_options())
    with patch.object(PostgresDestination, "_connect", return_value=finalize_conn):
        dest.finalize_sync(_config(), _tracked_options())

    target_deletes = [
        c
        for c in cur.execute.call_args_list
        if "DELETE" in str(c.args[0]) and "scores" in str(c.args[0])
    ]
    assert len(target_deletes) == 1
    assert target_deletes[0].args[1] == ((3,),)
    finalize_conn.commit.assert_called_once()


def test_tracked_second_run_all_keys_still_present_deletes_nothing() -> None:
    """prev == current -> no target DELETE, state simply rewritten."""
    from drt.destinations._mirror_state import key_hash, key_json

    dest = PostgresDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    cur = finalize_conn.cursor.return_value
    cur.fetchall.return_value = [
        (key_hash((k,)), key_json((k,))) for k in (1, 2)
    ]

    with patch.object(PostgresDestination, "_connect", return_value=load_conn):
        dest.load([{"id": 1}, {"id": 2}], _config(), _tracked_options())
    with patch.object(PostgresDestination, "_connect", return_value=finalize_conn):
        dest.finalize_sync(_config(), _tracked_options())

    for call in cur.execute.call_args_list:
        stmt = str(call.args[0])
        if "DELETE" in stmt:
            assert "scores" not in stmt


def test_tracked_empty_source_keeps_state_and_deletes_nothing() -> None:
    """No batches observed -> finalize is a no-op (baseline preserved)."""
    dest = PostgresDestination()
    finalize_conn = _fake_connection()

    with patch.object(PostgresDestination, "_connect", return_value=finalize_conn):
        result = dest.finalize_sync(_config(), _tracked_options())

    assert result is None
    finalize_conn.cursor.return_value.execute.assert_not_called()


def test_tracked_composite_key_uses_tuple_in_form() -> None:
    """Composite upsert_key -> DELETE WHERE (c1, c2) IN %s with tuple params."""
    from drt.destinations._mirror_state import key_hash, key_json

    dest = PostgresDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    cur = finalize_conn.cursor.return_value
    cur.fetchall.return_value = [
        (key_hash((1, "a")), key_json((1, "a"))),
        (key_hash((2, "b")), key_json((2, "b"))),
    ]
    config = _config(upsert_key=["tenant_id", "user_id"])

    with patch.object(PostgresDestination, "_connect", return_value=load_conn):
        dest.load(
            [{"tenant_id": 1, "user_id": "a"}], config, _tracked_options()
        )
    with patch.object(PostgresDestination, "_connect", return_value=finalize_conn):
        dest.finalize_sync(config, _tracked_options())

    target_deletes = [
        c
        for c in cur.execute.call_args_list
        if "DELETE" in str(c.args[0]) and "scores" in str(c.args[0])
    ]
    assert len(target_deletes) == 1
    assert target_deletes[0].args[1] == (((2, "b"),),)


def test_tracked_baseline_logs_warning(caplog: pytest.LogCaptureFixture) -> None:
    """First run / lost state must be loudly visible, not silent."""
    dest = PostgresDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    finalize_conn.cursor.return_value.fetchall.return_value = []

    with patch.object(PostgresDestination, "_connect", return_value=load_conn):
        dest.load([{"id": 1}], _config(), _tracked_options())
    with (
        patch.object(PostgresDestination, "_connect", return_value=finalize_conn),
        caplog.at_level("WARNING"),
    ):
        dest.finalize_sync(_config(), _tracked_options())

    assert any("baselin" in r.message.lower() for r in caplog.records)


def test_tracked_creates_state_table_when_absent() -> None:
    """to_regclass -> NULL: the state table is created (lazy-create default)."""
    dest = PostgresDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    cur = finalize_conn.cursor.return_value
    cur.fetchone.return_value = (None,)  # existence probe: table absent
    cur.fetchall.return_value = []

    with patch.object(PostgresDestination, "_connect", return_value=load_conn):
        dest.load([{"id": 1}], _config(), _tracked_options())
    with patch.object(PostgresDestination, "_connect", return_value=finalize_conn):
        dest.finalize_sync(_config(), _tracked_options())

    assert any(
        "CREATE TABLE" in str(c.args[0]) for c in cur.execute.call_args_list
    )


def test_tracked_skips_create_when_state_table_preprovisioned() -> None:
    """to_regclass -> non-NULL: no CREATE, so a no-DDL user can run (#695)."""
    dest = PostgresDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    cur = finalize_conn.cursor.return_value
    cur.fetchone.return_value = ("public._drt_synced_keys",)  # already exists
    cur.fetchall.return_value = []

    with patch.object(PostgresDestination, "_connect", return_value=load_conn):
        dest.load([{"id": 1}], _config(), _tracked_options())
    with patch.object(PostgresDestination, "_connect", return_value=finalize_conn):
        dest.finalize_sync(_config(), _tracked_options())

    assert not any(
        "CREATE TABLE" in str(c.args[0]) for c in cur.execute.call_args_list
    )
    # the sync still functions: state is read and rewritten
    assert any(
        "_drt_synced_keys" in str(c.args[0]) for c in cur.execute.call_args_list
    )


# ---------------------------------------------------------------------------
# mirror.scope (#687)
# ---------------------------------------------------------------------------


def _scoped_options() -> SyncOptions:
    return _options(mirror={"scope": ["parent_id"]})


def test_scope_missing_column_fails_fast() -> None:
    """A scope column absent from the model output is a config error at load."""
    dest = PostgresDestination()
    conn = _fake_connection()

    with patch.object(PostgresDestination, "_connect", return_value=conn):
        with pytest.raises(ValueError, match="parent_id"):
            dest.load(
                [{"id": 1, "score": 100}],
                _config(upsert_key=["id"]),
                _scoped_options(),
            )


def test_scoped_mirror_deletes_within_observed_parents_only() -> None:
    """DELETE gains `scope IN %s AND key NOT IN %s` with observed values."""
    dest = PostgresDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    config = _config(upsert_key=["id"])
    opts = _scoped_options()

    with patch.object(PostgresDestination, "_connect", return_value=load_conn):
        dest.load(
            [
                {"id": 1, "parent_id": 10},
                {"id": 2, "parent_id": 10},
                {"id": 3, "parent_id": 20},
            ],
            config,
            opts,
        )
    with patch.object(PostgresDestination, "_connect", return_value=finalize_conn):
        result = dest.finalize_sync(config, opts)

    assert result is not None
    cur = finalize_conn.cursor.return_value
    assert cur.execute.call_count == 1
    stmt, params = cur.execute.call_args.args
    stmt_s = str(stmt)
    assert "IN" in stmt_s and "NOT IN" in stmt_s
    scopes, keys = params
    assert set(scopes) == {10, 20}
    assert set(keys) == {1, 2, 3}


def test_scoped_mirror_composite_scope_uses_tuple_form() -> None:
    """Composite scope -> (s1, s2) IN %s with tuple-of-tuples params."""
    dest = PostgresDestination()
    load_conn = _fake_connection()
    finalize_conn = _fake_connection()
    config = _config(upsert_key=["id"])
    opts = _options(mirror={"scope": ["tenant_id", "parent_id"]})

    with patch.object(PostgresDestination, "_connect", return_value=load_conn):
        dest.load(
            [{"id": 1, "tenant_id": "t1", "parent_id": 10}],
            config,
            opts,
        )
    with patch.object(PostgresDestination, "_connect", return_value=finalize_conn):
        dest.finalize_sync(config, opts)

    cur = finalize_conn.cursor.return_value
    _stmt, params = cur.execute.call_args.args
    scopes, _keys = params
    assert scopes == (("t1", 10),)


def test_scoped_mirror_empty_source_still_skips_delete() -> None:
    """The #340 empty-source guard applies to scoped mirror unchanged."""
    dest = PostgresDestination()
    finalize_conn = _fake_connection()

    with patch.object(PostgresDestination, "_connect", return_value=finalize_conn):
        assert dest.finalize_sync(_config(), _scoped_options()) is None

    finalize_conn.cursor.return_value.execute.assert_not_called()
