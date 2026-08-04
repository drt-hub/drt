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

import json

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


def _state_conn(
    raw_diff: list[tuple[str, str]] | None = None,
    to_insert: list[tuple[str, str]] | None = None,
    previous_exists: bool = True,
    table_exists: bool = True,
    scope_key_of: dict[str, str | None] | None = None,
) -> MagicMock:
    """A finalize-side connection whose cursor answers the three distinct
    reads #694 part 2 introduced, dispatched by the most recent ``execute()``
    call's SQL text (mirroring the dialect-agnostic double in
    ``test_sql_base.py``):

    - the state-table existence probe (``to_regclass`` — a single-value
      ``fetchone``, matched by exclusion since it's the only ``fetchone``
      call whose SQL doesn't contain ``LIMIT 1``)
    - the #694 part 2 baseline-existence probe (``... LIMIT 1``, another
      ``fetchone``) — ``previous_exists``
    - the SQL-side diff (``previous - current`` via ``NOT EXISTS`` against
      the staged current keys, ``SELECT s.key_hash`` prefix) — ``raw_diff``
    - the genuinely-new-keys probe (``current - previous``, ``SELECT
      c.key_hash`` prefix) — ``to_insert``
    """
    conn = _fake_connection()
    cur = conn.cursor.return_value
    scope_key_of = scope_key_of or {}

    def fetchone_side_effect() -> Any:
        sql = str(cur.execute.call_args.args[0])
        if "LIMIT 1" in sql:
            return (1,) if previous_exists else None
        return (1,) if table_exists else None

    def fetchall_side_effect() -> list[tuple[Any, ...]]:
        sql = str(cur.execute.call_args.args[0])
        if "SELECT s.key_hash" in sql:
            # #890: a scoped run asks for scope_key as a third column so it can
            # spot rows written before that column existed. Model the projection
            # actually requested — a fake that always returns two columns would
            # hide a real shape mismatch.
            rows = list(raw_diff or [])
            if "s.scope_key" in sql:
                return [(h, kj, scope_key_of.get(h)) for h, kj in rows]
            return rows
        if "SELECT c.key_hash" in sql:
            return list(to_insert or [])
        return []

    cur.fetchone.side_effect = fetchone_side_effect
    cur.fetchall.side_effect = fetchall_side_effect
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
    from drt.destinations._mirror_state import key_hash, key_json

    dest = PostgresDestination()
    load_conn = _fake_connection()
    finalize_conn = _state_conn(
        raw_diff=[],
        to_insert=[(key_hash((k,)), key_json((k,))) for k in (1, 2)],
        previous_exists=False,
    )
    cur = finalize_conn.cursor.return_value

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
    finalize_conn = _state_conn(
        raw_diff=[(key_hash((3,)), key_json((3,)))], to_insert=[]
    )
    cur = finalize_conn.cursor.return_value

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
    dest = PostgresDestination()
    load_conn = _fake_connection()
    finalize_conn = _state_conn(raw_diff=[], to_insert=[])
    cur = finalize_conn.cursor.return_value

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
    finalize_conn = _state_conn(
        raw_diff=[(key_hash((2, "b")), key_json((2, "b")))], to_insert=[]
    )
    cur = finalize_conn.cursor.return_value
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
    from drt.destinations._mirror_state import key_hash, key_json

    dest = PostgresDestination()
    load_conn = _fake_connection()
    finalize_conn = _state_conn(
        raw_diff=[],
        to_insert=[(key_hash((1,)), key_json((1,)))],
        previous_exists=False,
    )

    with patch.object(PostgresDestination, "_connect", return_value=load_conn):
        dest.load([{"id": 1}], _config(), _tracked_options())
    with (
        patch.object(PostgresDestination, "_connect", return_value=finalize_conn),
        caplog.at_level("WARNING"),
    ):
        dest.finalize_sync(_config(), _tracked_options())

    assert any("baselin" in r.message.lower() for r in caplog.records)


# ---------------------------------------------------------------------------
# mirror.scope + strategy: tracked (#694)
# ---------------------------------------------------------------------------


def _tracked_scoped_options(scope: list[str] = ["parent_id"]) -> SyncOptions:
    opts = _options(mirror={"strategy": "tracked", "scope": scope})
    opts._sync_name = "scores_sync"
    return opts


def test_tracked_scoped_deletes_only_stale_keys_within_observed_scope() -> None:
    """Prior state has parent 1: {(1,"a"),(1,"b")} and parent 2: {(2,"x")}.
    This run only touches parent 1 with just (1,"a") -> (1,"b") is stale and
    deleted; (2,"x") is under a parent this run never saw and must survive."""
    from drt.destinations._mirror_state import key_hash, key_json

    dest = PostgresDestination()
    load_conn = _fake_connection()
    finalize_conn = _state_conn(
        raw_diff=[(key_hash(k), key_json(k)) for k in ((1, "b"), (2, "x"))],
        to_insert=[],  # (1,"a") is already tracked
    )
    cur = finalize_conn.cursor.return_value
    config = _config(upsert_key=["parent_id", "id"])

    with patch.object(PostgresDestination, "_connect", return_value=load_conn):
        dest.load([{"parent_id": 1, "id": "a"}], config, _tracked_scoped_options())
    with patch.object(PostgresDestination, "_connect", return_value=finalize_conn):
        dest.finalize_sync(config, _tracked_scoped_options())

    target_deletes = [
        c
        for c in cur.execute.call_args_list
        if "DELETE" in str(c.args[0]) and "scores" in str(c.args[0])
    ]
    assert len(target_deletes) == 1
    assert target_deletes[0].args[1] == (((1, "b"),),)


def test_tracked_scoped_rewrite_preserves_out_of_scope_state() -> None:
    """(2,"x") is under a parent this run never observed — #694 part 2 never
    reads or rewrites it at all (not even to reinsert it unchanged): it's
    simply never a candidate for either the diff-delete or the insert-new
    query, so it's absent from every state-table executemany call."""
    from drt.destinations._mirror_state import key_hash, key_json

    dest = PostgresDestination()
    load_conn = _fake_connection()
    finalize_conn = _state_conn(
        raw_diff=[(key_hash(k), key_json(k)) for k in ((1, "b"), (2, "x"))],
        to_insert=[],
    )
    cur = finalize_conn.cursor.return_value
    config = _config(upsert_key=["parent_id", "id"])

    with patch.object(PostgresDestination, "_connect", return_value=load_conn):
        dest.load([{"parent_id": 1, "id": "a"}], config, _tracked_scoped_options())
    with patch.object(PostgresDestination, "_connect", return_value=finalize_conn):
        dest.finalize_sync(config, _tracked_scoped_options())

    state_delete_calls = [
        c
        for c in cur.executemany.call_args_list
        if "DELETE" in str(c.args[0]) and "scores" not in str(c.args[0])
    ]
    assert len(state_delete_calls) == 1
    deleted_hashes = {row[1] for row in state_delete_calls[0].args[1]}
    assert deleted_hashes == {key_hash((1, "b"))}
    # #694 part 2 pinned that an out-of-scope row is never touched at all. #890
    # narrows that: it may be touched *once*, by the scope backfill, and only
    # while its scope columns are still NULL. It is still never deleted and
    # never re-inserted, and once healed it is filtered out in SQL and never
    # read again. Asserting the shape rather than dropping the check, so a
    # future change that starts deleting or rewriting it still fails here.
    touched = [
        c for c in cur.executemany.call_args_list if key_hash((2, "x")) in str(c.args[1])
    ]
    assert len(touched) <= 1
    for call in touched:
        assert str(call.args[0]).startswith("UPDATE") or "SET scope_spec" in str(call.args[0])


def test_tracked_scoped_first_touch_of_a_scope_is_not_a_baseline_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Prior state exists, just not for the parent this run touches — not
    the same as never having run before, so no baseline warning and no
    target delete (nothing tracked here yet to diff against)."""
    from drt.destinations._mirror_state import key_hash, key_json

    dest = PostgresDestination()
    load_conn = _fake_connection()
    finalize_conn = _state_conn(
        raw_diff=[(key_hash((2, "x")), key_json((2, "x")))],
        to_insert=[(key_hash((1, "a")), key_json((1, "a")))],
    )
    cur = finalize_conn.cursor.return_value
    config = _config(upsert_key=["parent_id", "id"])

    with patch.object(PostgresDestination, "_connect", return_value=load_conn):
        dest.load([{"parent_id": 1, "id": "a"}], config, _tracked_scoped_options())
    with (
        patch.object(PostgresDestination, "_connect", return_value=finalize_conn),
        caplog.at_level("WARNING"),
    ):
        dest.finalize_sync(config, _tracked_scoped_options())

    assert not any("baselin" in r.message.lower() for r in caplog.records)
    for call in cur.execute.call_args_list:
        stmt = str(call.args[0])
        if "DELETE" in stmt:
            assert "scores" not in stmt
    # only the newly-observed (1,"a") is inserted; (2,"x") is untouched
    insert_calls = [
        c
        for c in cur.executemany.call_args_list
        if "INSERT" in str(c.args[0]) and "_drt_synced_keys" in str(c.args[0])
    ]
    assert len(insert_calls) == 1
    persisted_keys = {tuple(json.loads(r[2])) for r in insert_calls[0].args[1]}
    assert persisted_keys == {(1, "a")}


def test_tracked_scoped_genuinely_no_prior_state_still_warns_baseline(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """No prior state at all (not just none in scope) -> still the ordinary
    #686 baseline warning, unaffected by scope being configured."""
    from drt.destinations._mirror_state import key_hash, key_json

    dest = PostgresDestination()
    load_conn = _fake_connection()
    finalize_conn = _state_conn(
        raw_diff=[],
        to_insert=[(key_hash((1, "a")), key_json((1, "a")))],
        previous_exists=False,
    )
    config = _config(upsert_key=["parent_id", "id"])

    with patch.object(PostgresDestination, "_connect", return_value=load_conn):
        dest.load([{"parent_id": 1, "id": "a"}], config, _tracked_scoped_options())
    with (
        patch.object(PostgresDestination, "_connect", return_value=finalize_conn),
        caplog.at_level("WARNING"),
    ):
        dest.finalize_sync(config, _tracked_scoped_options())

    assert any("baselin" in r.message.lower() for r in caplog.records)


def test_tracked_scoped_composite_scope_columns() -> None:
    """A two-column scope (e.g. tenant_id + parent_id) derives correctly
    from a three-column upsert_key."""
    from drt.destinations._mirror_state import key_hash, key_json

    dest = PostgresDestination()
    load_conn = _fake_connection()
    finalize_conn = _state_conn(
        raw_diff=[
            (key_hash(k), key_json(k)) for k in ((1, 1, "b"), (1, 2, "x"))
        ],
        to_insert=[],
    )
    cur = finalize_conn.cursor.return_value
    config = _config(upsert_key=["tenant_id", "parent_id", "id"])
    opts = _tracked_scoped_options(scope=["tenant_id", "parent_id"])

    with patch.object(PostgresDestination, "_connect", return_value=load_conn):
        dest.load([{"tenant_id": 1, "parent_id": 1, "id": "a"}], config, opts)
    with patch.object(PostgresDestination, "_connect", return_value=finalize_conn):
        dest.finalize_sync(config, opts)

    target_deletes = [
        c
        for c in cur.execute.call_args_list
        if "DELETE" in str(c.args[0]) and "scores" in str(c.args[0])
    ]
    assert len(target_deletes) == 1
    assert target_deletes[0].args[1] == (((1, 1, "b"),),)


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


# ---------------------------------------------------------------------------
# tracked-mirror state reset (#776)
# ---------------------------------------------------------------------------


class TestResetTrackedState:
    """`drt state reset --tracked-mirror` clears one sync's tracked keys.

    This is the most dangerous of the three reset levels, and the only one
    that writes to the *destination*. Re-baselining means the next mirror pass
    treats whatever is in the target as drt's own — so rows the application
    wrote become deletion candidates, which is the exact risk #686 exists to
    prevent. Hence: scoped to one sync, never touches the target table, and
    reports whether it actually removed anything.
    """

    def test_deletes_only_this_syncs_rows(self) -> None:
        dest = PostgresDestination()
        conn = _fake_connection()
        cur = conn.cursor.return_value
        cur.rowcount = 3

        with patch.object(PostgresDestination, "_connect", return_value=conn):
            removed = dest.reset_tracked_state(_config(), "scores_sync")

        executed = _executed_sql(cur)
        assert "_drt_synced_keys" in executed
        assert "DELETE" in executed.upper()
        # the sync name is bound, never interpolated
        delete_calls = [c for c in cur.execute.call_args_list if "DELETE" in str(c.args[0]).upper()]
        assert delete_calls, "no DELETE was issued"
        assert delete_calls[-1].args[1] == ("scores_sync",)
        assert removed == 3
        conn.commit.assert_called_once()

    def test_never_touches_the_target_table(self) -> None:
        """The one thing this must never do is delete user data."""
        dest = PostgresDestination()
        conn = _fake_connection()
        cur = conn.cursor.return_value
        cur.rowcount = 0

        with patch.object(PostgresDestination, "_connect", return_value=conn):
            dest.reset_tracked_state(_config(), "scores_sync")

        for call in cur.execute.call_args_list:
            stmt = str(call.args[0])
            if "DELETE" in stmt.upper():
                assert "_drt_synced_keys" in stmt, f"DELETE hit a non-state table: {stmt}"

    def test_missing_state_table_is_a_noop(self) -> None:
        """Resetting a sync that never ran tracked mirror must not error, and
        must not create the table just to empty it."""
        dest = PostgresDestination()
        conn = _fake_connection()

        with patch.object(PostgresDestination, "_connect", return_value=conn):
            with patch.object(PostgresDestination, "_state_table_exists", return_value=False):
                removed = dest.reset_tracked_state(_config(), "scores_sync")

        assert removed == 0
        executed = _executed_sql(conn.cursor.return_value)
        assert "CREATE TABLE" not in executed.upper()

    def test_connection_is_closed_on_failure(self) -> None:
        dest = PostgresDestination()
        conn = _fake_connection()
        conn.cursor.return_value.execute.side_effect = RuntimeError("boom")

        with patch.object(PostgresDestination, "_connect", return_value=conn):
            with pytest.raises(RuntimeError):
                dest.reset_tracked_state(_config(), "scores_sync")

        conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# #890 — scope-aware SQL diff
#
# The Python scope filter stays authoritative. Everything below is about the
# SQL predicate being a purely *coarse* pre-filter: it may hand Python more
# rows than necessary, it may never hand it fewer.
# ---------------------------------------------------------------------------


def _scoped_diff_call(cur: MagicMock) -> Any:
    """The tracked-mirror diff SELECT (``previous - current``)."""
    return next(c for c in cur.execute.call_args_list if "SELECT s.key_hash" in str(c.args[0]))


def _run_tracked_scoped(dest: Any, cur_conn: MagicMock, load_conn: MagicMock) -> None:
    opts = _tracked_scoped_options()
    with patch.object(PostgresDestination, "_connect", return_value=load_conn):
        dest.load([{"id": "a", "parent_id": 1}], _config(upsert_key=["parent_id", "id"]), opts)
    with patch.object(PostgresDestination, "_connect", return_value=cur_conn):
        dest.finalize_sync(_config(upsert_key=["parent_id", "id"]), opts)


def test_scoped_diff_is_narrowed_in_sql_when_columns_exist() -> None:
    dest = PostgresDestination()
    conn = _state_conn(raw_diff=[], to_insert=[])
    cur = conn.cursor.return_value

    with patch.object(PostgresDestination, "_state_scope_columns_exist", return_value=True):
        _run_tracked_scoped(dest, conn, _fake_connection())

    call = _scoped_diff_call(cur)
    sql, params = str(call.args[0]), call.args[1]
    assert "s.scope_key IN" in sql
    # the observed scope travels as a bound parameter, never interpolated
    assert '[1]' in params


def test_scoped_diff_lets_rows_from_another_scope_spec_through() -> None:
    """The branch that keeps the coarse filter honest.

    A row written while ``mirror.scope`` was something else carries a
    ``scope_key`` no current observed scope can match. Without the
    ``scope_spec <>`` escape it would drop out of the diff and stop being a
    deletion candidate — silently, and for good. Today's code re-derives scope
    from config every run and has no such failure; this preserves that.
    """
    dest = PostgresDestination()
    conn = _state_conn(raw_diff=[], to_insert=[])
    cur = conn.cursor.return_value

    with patch.object(PostgresDestination, "_state_scope_columns_exist", return_value=True):
        _run_tracked_scoped(dest, conn, _fake_connection())

    sql = str(_scoped_diff_call(cur).args[0])
    assert "s.scope_key IS NULL" in sql
    assert "s.scope_spec <> %s" in sql
    # the spec bound is the one this run is configured with
    assert '["parent_id"]' in _scoped_diff_call(cur).args[1]


def test_scoped_diff_falls_back_when_alter_is_refused() -> None:
    """No ALTER privilege (#695 family) is a supported state, not an error."""
    dest = PostgresDestination()
    conn = _state_conn(raw_diff=[], to_insert=[])
    cur = conn.cursor.return_value

    with (
        patch.object(PostgresDestination, "_state_scope_columns_exist", return_value=False),
        patch.object(
            PostgresDestination,
            "_add_state_scope_columns",
            side_effect=Exception("permission denied for table _drt_synced_keys"),
        ),
    ):
        _run_tracked_scoped(dest, conn, _fake_connection())

    sql = str(_scoped_diff_call(cur).args[0])
    assert "scope_key" not in sql  # ran, just without the optimisation
    assert any("ROLLBACK TO SAVEPOINT" in str(c.args[0]) for c in cur.execute.call_args_list)


def test_unscoped_tracked_never_probes_or_alters_scope_columns() -> None:
    """An unscoped sync gains nothing here and must not pay a probe, let alone DDL."""
    dest = PostgresDestination()
    conn = _state_conn(raw_diff=[], to_insert=[])
    load_conn = _fake_connection()

    with (
        patch.object(PostgresDestination, "_state_scope_columns_exist") as probe,
        patch.object(PostgresDestination, "_add_state_scope_columns") as alter,
        patch.object(PostgresDestination, "_connect", return_value=load_conn),
    ):
        dest.load([{"id": 1}], _config(), _tracked_options())
        with patch.object(PostgresDestination, "_connect", return_value=conn):
            dest.finalize_sync(_config(), _tracked_options())

    probe.assert_not_called()
    alter.assert_not_called()


def test_scoped_insert_records_spec_alongside_the_scope_value() -> None:
    from drt.destinations._mirror_state import key_hash, key_json

    key = (1, "a")
    dest = PostgresDestination()
    conn = _state_conn(raw_diff=[], to_insert=[(key_hash(key), key_json(key))])
    cur = conn.cursor.return_value

    with patch.object(PostgresDestination, "_state_scope_columns_exist", return_value=True):
        _run_tracked_scoped(dest, conn, _fake_connection())

    # the state-table insert, not the staging one that precedes it
    insert = next(
        c
        for c in cur.executemany.call_args_list
        if "INSERT INTO" in str(c.args[0]) and "_drt_synced_keys" in str(c.args[0])
    )
    assert "scope_spec" in str(insert.args[0])
    assert insert.args[1][0][3:] == ('["parent_id"]', "[1]")


def test_scope_backfill_heals_pre_890_rows_but_not_doomed_ones() -> None:
    """Rows tracked before the scope columns existed get healed in place.

    Without this they stay NULL forever: #694 part 2 deliberately never
    rewrites an already-tracked row, so there is no write for the new columns
    to ride along with, and on an upgraded state table the SQL filter would
    never engage at all. Caught by running against a real server, not by a
    mock — see #890.
    """
    from drt.destinations._mirror_state import key_hash, key_json

    survivor, doomed = (2, "other-scope"), (1, "stale")
    conn = _state_conn(
        raw_diff=[(key_hash(k), key_json(k)) for k in (survivor, doomed)],
        to_insert=[],
        scope_key_of={key_hash(survivor): None, key_hash(doomed): None},
    )
    cur = conn.cursor.return_value

    with patch.object(PostgresDestination, "_state_scope_columns_exist", return_value=True):
        _run_tracked_scoped(PostgresDestination(), conn, _fake_connection())

    updates = [c for c in cur.executemany.call_args_list if "SET scope_spec" in str(c.args[0])]
    assert len(updates) == 1
    healed = {row[3] for row in updates[0].args[1]}

    assert key_hash(survivor) in healed
    # the doomed row is deleted two statements later — healing it is pure waste
    assert key_hash(doomed) not in healed
    assert updates[0].args[1][0][:2] == ('["parent_id"]', "[2]")


def test_scope_backfill_is_capped_per_run() -> None:
    """Bounded on purpose: expand/contract says backfill in batches, and a sync
    run is the hot path. A big state table converges over a few runs rather
    than one run paying for the whole history."""
    from drt.destinations._mirror_state import SCOPE_BACKFILL_PER_RUN, key_hash, key_json

    keys = [(9, f"k{i}") for i in range(SCOPE_BACKFILL_PER_RUN + 25)]
    conn = _state_conn(
        raw_diff=[(key_hash(k), key_json(k)) for k in keys],
        to_insert=[],
        scope_key_of={key_hash(k): None for k in keys},
    )
    cur = conn.cursor.return_value

    with patch.object(PostgresDestination, "_state_scope_columns_exist", return_value=True):
        _run_tracked_scoped(PostgresDestination(), conn, _fake_connection())

    updates = [c for c in cur.executemany.call_args_list if "SET scope_spec" in str(c.args[0])]
    assert len(updates[0].args[1]) == SCOPE_BACKFILL_PER_RUN
