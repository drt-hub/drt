"""Unit tests for ``sync.mode: mirror`` on the Snowflake destination (#340 Step 4).

Mirror mode forces the MERGE write path (regardless of ``config.mode``),
then in the ``finalize_sync`` end-of-sync hook issues a single DELETE
that removes destination rows whose ``upsert_key`` is not in the set
seen across all batches.

Strategy under test: application-side diff (collect upsert_key tuples
in memory, then ``DELETE FROM ... WHERE key NOT IN (collected)``). The
Snowflake connector uses ``%s`` placeholders (same family as psycopg2
/ pymysql), but Snowflake SQL does not auto-expand a tuple-of-tuples —
so the placeholder list is built explicitly. Same shape as the MySQL
Step 2 implementation.

These tests inject mock ``snowflake.connector`` modules via
``sys.modules`` — no real Snowflake account or
``snowflake-connector-python`` install required (matches the pattern in
``test_snowflake_destination.py``).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.models import SnowflakeDestinationConfig, SyncOptions
from drt.destinations.snowflake import SnowflakeDestination


def _options(**kwargs: Any) -> SyncOptions:
    defaults: dict[str, Any] = {"mode": "mirror"}
    defaults.update(kwargs)
    return SyncOptions(**defaults)


def _config(**overrides: Any) -> SnowflakeDestinationConfig:
    defaults: dict[str, Any] = {
        "type": "snowflake",
        "account_env": "SF_ACCOUNT",
        "user_env": "SF_USER",
        "password_env": "SF_PASSWORD",
        "database": "ANALYTICS",
        "schema": "PUBLIC",
        "table": "USER_SCORES",
        "warehouse": "COMPUTE_WH",
        "upsert_key": ["id"],
        # Isolate from Layer-3 introspection (#317) — asserts exact SQL ordering.
        "introspect_schema": False,
    }
    defaults.update(overrides)
    return SnowflakeDestinationConfig.model_validate(defaults)


def _set_creds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SF_ACCOUNT", "acct.us-east-1")
    monkeypatch.setenv("SF_USER", "test_user")
    monkeypatch.setenv("SF_PASSWORD", "test_pass")


def _fake_conn() -> MagicMock:
    """Fake snowflake.connector connection with a context-managed cursor."""
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False
    conn._cur = cur  # for assertions
    return conn


def _configure_state_cur(
    conn: MagicMock,
    raw_diff: list[tuple[str, str]] | None = None,
    to_insert: list[tuple[str, str]] | None = None,
    previous_exists: bool = True,
    table_exists: bool = True,
    scope_key_of: dict[str, str | None] | None = None,
) -> None:
    """Configure a ``_fake_conn()``'s cursor to answer the #694 part 2 reads,
    dispatched by the most recent ``execute()`` call's SQL text — see the
    Postgres test file's twin helper for the full rationale. Also covers
    the ``SHOW TABLES`` existence probe (Snowflake's own ``fetchall``-based
    check, unlike Postgres/MySQL's ``fetchone``-based one)."""
    cur = conn._cur
    scope_key_of = scope_key_of or {}

    def fetchone_side_effect() -> Any:
        sql = cur.execute.call_args.args[0] if cur.execute.call_args.args else ""
        if "LIMIT 1" in sql:
            return (1,) if previous_exists else None
        return None

    def fetchall_side_effect() -> list[tuple[str, str]]:
        sql = cur.execute.call_args.args[0] if cur.execute.call_args.args else ""
        if "SHOW TABLES" in sql:
            return [("_DRT_SYNCED_KEYS",)] if table_exists else []
        if "SELECT s.key_hash" in sql:
            # #890: model the projection actually asked for — a scoped run adds
            # scope_key as a third column so pre-#890 rows can be spotted.
            rows = list(raw_diff or [])
            if "s.scope_key" in sql:
                return [(h, kj, scope_key_of.get(h)) for h, kj in rows]
            return rows
        if "SELECT c.key_hash" in sql:
            return list(to_insert or [])
        return []

    cur.fetchone.side_effect = fetchone_side_effect
    cur.fetchall.side_effect = fetchall_side_effect


def _mocked_snowflake_modules(conn: MagicMock | None = None) -> dict[str, MagicMock]:
    """Build sys.modules entries that satisfy ``import snowflake.connector``."""
    mock_module = MagicMock()
    mock_connector = MagicMock()
    if conn is not None:
        mock_connector.connect.return_value = conn
    mock_module.connector = mock_connector
    return {"snowflake": mock_module, "snowflake.connector": mock_connector}


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


def test_mirror_accumulates_keys_across_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    """``_mirror_keys`` collects the upsert_key tuple from every loaded record."""
    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    conn = _fake_conn()
    modules = _mocked_snowflake_modules(conn)
    config = _config()
    opts = _options()

    with patch.dict("sys.modules", modules):
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


def test_mirror_forces_merge_path_even_when_config_mode_is_insert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``sync.mode: mirror`` overrides ``config.mode: insert`` — MERGE runs.

    Mirror mode semantically requires upsert; users shouldn't have to
    also set ``config.mode: merge``. Verify the MERGE branch ran (CREATE
    TEMP TABLE + MERGE INTO).
    """
    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    conn = _fake_conn()
    modules = _mocked_snowflake_modules(conn)
    config = _config(mode="insert")  # explicit insert — should be overridden
    opts = _options()

    with patch.dict("sys.modules", modules):
        dest.load([{"id": 1, "score": 100}], config, opts)

    sqls = [
        (call.args[0] if call.args else "")
        for call in conn._cur.execute.call_args_list
    ]
    assert any("CREATE TEMP TABLE" in s for s in sqls)
    assert any("MERGE INTO ANALYTICS.PUBLIC.USER_SCORES" in s for s in sqls)


def test_finalize_mirror_issues_delete_with_collected_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``finalize_sync`` runs ``DELETE WHERE id NOT IN (%s, %s)``."""
    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    load_conn = _fake_conn()
    finalize_conn = _fake_conn()
    config = _config()
    opts = _options()

    with patch.dict("sys.modules", _mocked_snowflake_modules(load_conn)):
        dest.load(
            [{"id": 1, "score": 100}, {"id": 2, "score": 200}],
            config,
            opts,
        )

    with patch.dict("sys.modules", _mocked_snowflake_modules(finalize_conn)):
        result = dest.finalize_sync(config, opts)

    assert result is not None
    assert result.success == 0
    assert result.failed == 0

    # DELETE was the (only) statement run on the finalize cursor
    delete_calls = [
        call
        for call in finalize_conn._cur.execute.call_args_list
        if "DELETE FROM" in (call.args[0] if call.args else "")
    ]
    assert len(delete_calls) == 1
    stmt = delete_calls[0].args[0]
    params = delete_calls[0].args[1]
    assert "DELETE FROM ANALYTICS.PUBLIC.USER_SCORES" in stmt
    assert "id NOT IN (%s, %s)" in stmt
    assert set(params) == {1, 2}


def test_finalize_mirror_dedupes_overlapping_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If two batches both contain id=1, the DELETE NOT IN list lists it once."""
    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    load_conn = _fake_conn()
    finalize_conn = _fake_conn()
    config = _config()
    opts = _options()

    with patch.dict("sys.modules", _mocked_snowflake_modules(load_conn)):
        dest.load([{"id": 1, "score": 100}], config, opts)
        dest.load([{"id": 1, "score": 999}], config, opts)
        dest.load([{"id": 2, "score": 200}], config, opts)

    with patch.dict("sys.modules", _mocked_snowflake_modules(finalize_conn)):
        dest.finalize_sync(config, opts)

    delete_call = next(
        call
        for call in finalize_conn._cur.execute.call_args_list
        if "DELETE FROM" in (call.args[0] if call.args else "")
    )
    stmt = delete_call.args[0]
    params = delete_call.args[1]
    # Two unique keys, even though id=1 came in twice
    assert sorted(params) == [1, 2]
    assert stmt.count("%s") == 2


# ---------------------------------------------------------------------------
# Composite upsert_key
# ---------------------------------------------------------------------------


def test_mirror_composite_key_accumulates_tuples(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two-column upsert_key yields 2-tuples in ``_mirror_keys``."""
    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    conn = _fake_conn()
    modules = _mocked_snowflake_modules(conn)
    config = _config(upsert_key=["user_id", "session_id"])
    opts = _options()

    with patch.dict("sys.modules", modules):
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


def test_finalize_mirror_composite_key_delete_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Composite upsert_key → DELETE WHERE (c1, c2) NOT IN ((%s, %s), (%s, %s))."""
    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    load_conn = _fake_conn()
    finalize_conn = _fake_conn()
    config = _config(upsert_key=["user_id", "session_id"])
    opts = _options()

    with patch.dict("sys.modules", _mocked_snowflake_modules(load_conn)):
        dest.load(
            [
                {"user_id": "a", "session_id": "x", "score": 1},
                {"user_id": "b", "session_id": "y", "score": 2},
            ],
            config,
            opts,
        )

    with patch.dict("sys.modules", _mocked_snowflake_modules(finalize_conn)):
        dest.finalize_sync(config, opts)

    delete_call = next(
        call
        for call in finalize_conn._cur.execute.call_args_list
        if "DELETE FROM" in (call.args[0] if call.args else "")
    )
    stmt = delete_call.args[0]
    params = delete_call.args[1]
    assert "(user_id, session_id)" in stmt
    assert "NOT IN ((%s, %s), (%s, %s))" in stmt
    pairs = {(params[i], params[i + 1]) for i in range(0, len(params), 2)}
    assert pairs == {("a", "x"), ("b", "y")}


# ---------------------------------------------------------------------------
# Safety paths
# ---------------------------------------------------------------------------


def test_finalize_mirror_skips_when_no_keys_observed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No batch ever delivered records → finalize returns None, no DELETE.

    Prevents a transient empty source from silently wiping the destination.
    No connection is opened because finalize bails out on
    ``_mirror_keys`` being empty/None before reaching the connector.
    """
    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    config = _config()
    opts = _options()

    # No load() called; _mirror_keys is still None.
    # No sys.modules patch: if finalize tried to connect it would fail.
    result = dest.finalize_sync(config, opts)
    assert result is None


def test_finalize_mirror_resets_state_after_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After finalize, ``_mirror_keys`` is cleared so a re-run starts fresh."""
    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    load_conn = _fake_conn()
    finalize_conn = _fake_conn()
    config = _config()
    opts = _options()

    with patch.dict("sys.modules", _mocked_snowflake_modules(load_conn)):
        dest.load([{"id": 1, "score": 100}], config, opts)
    with patch.dict("sys.modules", _mocked_snowflake_modules(finalize_conn)):
        dest.finalize_sync(config, opts)

    assert dest._mirror_keys is None


def test_mirror_raises_when_upsert_key_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mirror mode without ``upsert_key`` is a config error surfaced at load.

    Validated BEFORE any INSERT / MERGE — fail-fast.
    """
    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    conn = _fake_conn()
    config = _config(upsert_key=None)
    opts = _options()

    with patch.dict("sys.modules", _mocked_snowflake_modules(conn)):
        with pytest.raises(ValueError, match="mirror requires destination.upsert_key"):
            dest.load([{"id": 1, "score": 100}], config, opts)

    # No INSERT / CREATE TEMP TABLE / MERGE ran on the destination
    conn._cur.execute.assert_not_called()


def test_mirror_excludes_failed_record_keys_from_accumulation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Records whose batch_index appears in row_errors are skipped from ``_mirror_keys``.

    Only successfully-staged keys count as "source state" — same shape as
    Postgres / MySQL / ClickHouse Step 1-3. The Snowflake merge path
    records row_errors for failures during the staging INSERT loop.
    """
    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    conn = _fake_conn()
    config = _config()
    opts = _options(on_error="skip")

    # Force the SECOND INSERT into the staging table to fail. The first
    # cur.execute is CREATE TEMP TABLE — let that succeed. Then alternate
    # success / fail / success on the INSERTs.
    call_counter = {"n": 0}

    def _execute_with_one_insert_failure(*args: Any, **_kwargs: Any) -> None:
        call_counter["n"] += 1
        sql = args[0] if args else ""
        # CREATE TEMP TABLE = call 1 — succeed
        # INSERT INTO TMP_... call 2 (record idx 0) — succeed
        # INSERT INTO TMP_... call 3 (record idx 1) — fail
        # INSERT INTO TMP_... call 4 (record idx 2) — succeed
        # MERGE INTO ...        call 5 — succeed
        if call_counter["n"] == 3 and "INSERT INTO TMP_" in sql:
            raise RuntimeError("forced for test")

    conn._cur.execute.side_effect = _execute_with_one_insert_failure

    with patch.dict("sys.modules", _mocked_snowflake_modules(conn)):
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


def test_finalize_sync_returns_none_for_non_mirror_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Snowflake has no swap-replace finalize — non-mirror modes return None.

    No connection is opened for non-mirror finalize; the destination
    short-circuits before reaching the connector.
    """
    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    config = _config()
    insert_opts = SyncOptions(mode="full")  # any non-mirror

    # No sys.modules patch: any connect attempt would raise.
    result = dest.finalize_sync(config, insert_opts)
    assert result is None


def test_tracked_strategy_accepted_on_snowflake(monkeypatch: pytest.MonkeyPatch) -> None:
    """``mirror.strategy: tracked`` (#692) is now supported on Snowflake."""
    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    conn = _fake_conn()
    conn._cur.fetchall.return_value = []  # SHOW TABLES + state SELECT, both empty
    opts = _options(mirror={"strategy": "tracked"})

    with patch.dict("sys.modules", _mocked_snowflake_modules(conn)):
        result = dest.load([{"id": 1, "score": 100}], _config(), opts)

    assert result.failed == 0


def test_scope_accepted_on_snowflake(monkeypatch: pytest.MonkeyPatch) -> None:
    """``mirror.scope`` (#692, destination strategy) is now supported."""
    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    conn = _fake_conn()
    opts = _options(mirror={"scope": ["parent_id"]})

    with patch.dict("sys.modules", _mocked_snowflake_modules(conn)):
        result = dest.load([{"id": 1, "parent_id": 10}], _config(), opts)

    assert result.failed == 0


def test_scope_missing_column_fails_fast_on_snowflake(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    conn = _fake_conn()
    opts = _options(mirror={"scope": ["parent_id"]})

    with patch.dict("sys.modules", _mocked_snowflake_modules(conn)):
        with pytest.raises(ValueError, match="mirror.scope columns missing"):
            dest.load([{"id": 1}], _config(), opts)


def test_scoped_mirror_deletes_within_observed_parents_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Destination-strategy scope: the DELETE only ever considers rows under
    parents this run actually observed."""
    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    load_conn = _fake_conn()
    finalize_conn = _fake_conn()
    config = _config(upsert_key=["parent_id", "id"])
    opts = _options(mirror={"scope": ["parent_id"]})

    with patch.dict("sys.modules", _mocked_snowflake_modules(load_conn)):
        dest.load([{"parent_id": 1, "id": "a", "score": 1}], config, opts)

    with patch.dict("sys.modules", _mocked_snowflake_modules(finalize_conn)):
        dest.finalize_sync(config, opts)

    delete_call = next(
        call
        for call in finalize_conn._cur.execute.call_args_list
        if "DELETE FROM" in (call.args[0] if call.args else "")
    )
    stmt, params = delete_call.args
    assert "parent_id IN" in stmt
    assert params == [1, 1, "a"]


def test_scoped_mirror_composite_scope_uses_tuple_form(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Multi-column scope -> the composite `(c1, c2) IN ((%s, %s), ...)`
    branch of the scope clause, not just the single-column form above."""
    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    load_conn = _fake_conn()
    finalize_conn = _fake_conn()
    config = _config(upsert_key=["tenant_id", "parent_id", "id"])
    opts = _options(mirror={"scope": ["tenant_id", "parent_id"]})

    with patch.dict("sys.modules", _mocked_snowflake_modules(load_conn)):
        dest.load([{"tenant_id": 1, "parent_id": 1, "id": "a", "score": 1}], config, opts)

    with patch.dict("sys.modules", _mocked_snowflake_modules(finalize_conn)):
        dest.finalize_sync(config, opts)

    delete_call = next(
        call
        for call in finalize_conn._cur.execute.call_args_list
        if "DELETE FROM" in (call.args[0] if call.args else "")
    )
    stmt, params = delete_call.args
    assert "(tenant_id, parent_id) IN ((%s, %s))" in stmt
    assert params == [1, 1, 1, 1, "a"]


def test_scope_rejected_with_tracked_when_not_subset_of_upsert_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#694's composition constraint applies on Snowflake too."""
    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    conn = _fake_conn()
    opts = _options(mirror={"strategy": "tracked", "scope": ["parent_id"]})

    with patch.dict("sys.modules", _mocked_snowflake_modules(conn)):
        with pytest.raises(ValueError, match="mirror.scope columns must be part of"):
            dest.load([{"id": 1, "parent_id": 10}], _config(upsert_key=["id"]), opts)


# ---------------------------------------------------------------------------
# mirror.strategy: tracked (#692, mirroring Postgres/MySQL's #686)
# ---------------------------------------------------------------------------


def _tracked_options() -> SyncOptions:
    opts = _options(mirror={"strategy": "tracked"})
    opts._sync_name = "scores_sync"
    return opts


def test_tracked_first_run_baselines_without_deleting_snowflake(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from drt.destinations._mirror_state import key_hash, key_json

    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    load_conn = _fake_conn()
    finalize_conn = _fake_conn()
    _configure_state_cur(
        finalize_conn,
        raw_diff=[],
        to_insert=[(key_hash((k,)), key_json((k,))) for k in (1, 2)],
        previous_exists=False,
        table_exists=False,
    )

    with patch.dict("sys.modules", _mocked_snowflake_modules(load_conn)):
        dest.load([{"id": 1}, {"id": 2}], _config(), _tracked_options())
    with patch.dict("sys.modules", _mocked_snowflake_modules(finalize_conn)):
        result = dest.finalize_sync(_config(), _tracked_options())

    assert result is not None
    for call in finalize_conn._cur.execute.call_args_list:
        stmt = call.args[0] if call.args else ""
        if "DELETE FROM" in stmt:
            assert "USER_SCORES" not in stmt
    rows = finalize_conn._cur.executemany.call_args.args[1]
    assert [r[0] for r in rows] == ["scores_sync", "scores_sync"]


def test_tracked_second_run_deletes_only_stale_tracked_keys_snowflake(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """prev={1,2,3}, current={1,2} -> DELETE USER_SCORES WHERE id IN (%s) w/ [3]."""
    from drt.destinations._mirror_state import key_hash, key_json

    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    load_conn = _fake_conn()
    finalize_conn = _fake_conn()
    _configure_state_cur(
        finalize_conn,
        raw_diff=[(key_hash((3,)), key_json((3,)))],
        to_insert=[],
    )

    with patch.dict("sys.modules", _mocked_snowflake_modules(load_conn)):
        dest.load([{"id": 1}, {"id": 2}], _config(), _tracked_options())
    with patch.dict("sys.modules", _mocked_snowflake_modules(finalize_conn)):
        dest.finalize_sync(_config(), _tracked_options())

    target_deletes = [
        call
        for call in finalize_conn._cur.execute.call_args_list
        if "DELETE FROM" in (call.args[0] if call.args else "")
        and "USER_SCORES" in call.args[0]
    ]
    assert len(target_deletes) == 1
    stmt, params = target_deletes[0].args
    assert "IN (%s)" in stmt and "NOT IN" not in stmt
    assert params == [3]


def test_tracked_stages_current_keys_before_diffing_snowflake(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#694 part 2: current's keys are staged into a scratch TEMPORARY table
    before the diff query runs, and the scratch table is dropped at the end."""
    from drt.destinations._mirror_state import key_hash, key_json

    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    load_conn = _fake_conn()
    finalize_conn = _fake_conn()
    _configure_state_cur(finalize_conn)

    with patch.dict("sys.modules", _mocked_snowflake_modules(load_conn)):
        dest.load([{"id": 1}, {"id": 2}], _config(), _tracked_options())
    with patch.dict("sys.modules", _mocked_snowflake_modules(finalize_conn)):
        dest.finalize_sync(_config(), _tracked_options())

    calls = finalize_conn._cur.execute.call_args_list
    create_idx = next(
        i for i, c in enumerate(calls) if "CREATE TEMPORARY TABLE" in c.args[0]
    )
    diff_idx = next(i for i, c in enumerate(calls) if c.args[0].startswith("SELECT s.key_hash"))
    drop_idx = next(i for i, c in enumerate(calls) if c.args[0].startswith("DROP TABLE"))
    assert create_idx < diff_idx < drop_idx
    stage_insert = next(
        c
        for c in finalize_conn._cur.executemany.call_args_list
        if "INSERT INTO" in c.args[0] and "DIFF_KEYS" in c.args[0].upper()
    )
    for k in (1, 2):
        assert (key_hash((k,)), key_json((k,))) in stage_insert.args[1]


def test_tracked_inserts_only_genuinely_new_keys_snowflake(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A current key already tracked under the same hash never needs
    rewriting — only rows the new-keys query actually returns get inserted."""
    from drt.destinations._mirror_state import key_hash, key_json

    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    load_conn = _fake_conn()
    finalize_conn = _fake_conn()
    _configure_state_cur(
        finalize_conn, raw_diff=[], to_insert=[(key_hash((2,)), key_json((2,)))]
    )

    with patch.dict("sys.modules", _mocked_snowflake_modules(load_conn)):
        dest.load([{"id": 1}, {"id": 2}], _config(), _tracked_options())
    with patch.dict("sys.modules", _mocked_snowflake_modules(finalize_conn)):
        dest.finalize_sync(_config(), _tracked_options())

    insert_calls = [
        c
        for c in finalize_conn._cur.executemany.call_args_list
        if "INSERT INTO" in c.args[0] and "USER_SCORES" not in c.args[0]
        and "DIFF_KEYS" not in c.args[0].upper()
    ]
    assert len(insert_calls) == 1
    rows = insert_calls[0].args[1]
    assert rows == [("scores_sync", key_hash((2,)), key_json((2,)))]


def test_tracked_empty_source_is_noop_snowflake(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    finalize_conn = _fake_conn()

    with patch.dict("sys.modules", _mocked_snowflake_modules(finalize_conn)):
        result = dest.finalize_sync(_config(), _tracked_options())

    assert result is None
    finalize_conn._cur.execute.assert_not_called()


def test_tracked_baseline_logs_warning_snowflake(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from drt.destinations._mirror_state import key_hash, key_json

    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    load_conn = _fake_conn()
    finalize_conn = _fake_conn()
    _configure_state_cur(
        finalize_conn,
        raw_diff=[],
        to_insert=[(key_hash((1,)), key_json((1,)))],
        previous_exists=False,
        table_exists=False,
    )

    with patch.dict("sys.modules", _mocked_snowflake_modules(load_conn)):
        dest.load([{"id": 1}], _config(), _tracked_options())
    with (
        patch.dict("sys.modules", _mocked_snowflake_modules(finalize_conn)),
        caplog.at_level("WARNING"),
    ):
        dest.finalize_sync(_config(), _tracked_options())

    assert any("baselin" in r.message.lower() for r in caplog.records)


# ---------------------------------------------------------------------------
# mirror.scope + strategy: tracked (#694, extended to Snowflake by #692)
# ---------------------------------------------------------------------------


def _tracked_scoped_options(scope: list[str] = ["parent_id"]) -> SyncOptions:
    opts = _options(mirror={"strategy": "tracked", "scope": scope})
    opts._sync_name = "scores_sync"
    return opts


def test_tracked_scoped_deletes_only_stale_keys_within_observed_scope_snowflake(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prior state has parent 1: {(1,"a"),(1,"b")} and parent 2: {(2,"x")}.
    This run only touches parent 1 with just (1,"a") -> (1,"b") is stale and
    deleted; (2,"x") is under a parent this run never saw and must survive
    (never appears in any executemany call — #694 part 2 never touches it)."""
    from drt.destinations._mirror_state import key_hash, key_json

    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    load_conn = _fake_conn()
    finalize_conn = _fake_conn()
    _configure_state_cur(
        finalize_conn,
        raw_diff=[(key_hash(k), key_json(k)) for k in ((1, "b"), (2, "x"))],
        to_insert=[],
    )
    config = _config(upsert_key=["parent_id", "id"])

    with patch.dict("sys.modules", _mocked_snowflake_modules(load_conn)):
        dest.load([{"parent_id": 1, "id": "a"}], config, _tracked_scoped_options())
    with patch.dict("sys.modules", _mocked_snowflake_modules(finalize_conn)):
        dest.finalize_sync(config, _tracked_scoped_options())

    target_deletes = [
        call
        for call in finalize_conn._cur.execute.call_args_list
        if "DELETE FROM" in (call.args[0] if call.args else "")
        and "USER_SCORES" in call.args[0]
    ]
    assert len(target_deletes) == 1
    _, params = target_deletes[0].args
    assert params == [1, "b"]

    state_delete_calls = [
        c
        for c in finalize_conn._cur.executemany.call_args_list
        if "DELETE FROM" in c.args[0] and "USER_SCORES" not in c.args[0]
    ]
    assert len(state_delete_calls) == 1
    deleted_hashes = {row[1] for row in state_delete_calls[0].args[1]}
    assert deleted_hashes == {key_hash((1, "b"))}
    # #694 part 2 pinned that an out-of-scope row is never touched at all. #890
    # narrows that: it may be touched *once*, by the scope backfill, and only
    # while its scope columns are still NULL. It is still never deleted and
    # never re-inserted, and once healed it is filtered out in SQL. Asserting
    # the shape rather than dropping the check, so a future change that starts
    # deleting or rewriting it still fails here.
    touched = [
        c
        for c in finalize_conn._cur.executemany.call_args_list
        if key_hash((2, "x")) in str(c.args[1])
    ]
    assert len(touched) <= 1
    for call in touched:
        assert "SET scope_spec" in str(call.args[0])


def test_tracked_scoped_first_touch_of_a_scope_is_not_a_baseline_warning_snowflake(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from drt.destinations._mirror_state import key_hash, key_json

    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    load_conn = _fake_conn()
    finalize_conn = _fake_conn()
    _configure_state_cur(
        finalize_conn,
        raw_diff=[(key_hash((2, "x")), key_json((2, "x")))],
        to_insert=[(key_hash((1, "a")), key_json((1, "a")))],
    )
    config = _config(upsert_key=["parent_id", "id"])

    with patch.dict("sys.modules", _mocked_snowflake_modules(load_conn)):
        dest.load([{"parent_id": 1, "id": "a"}], config, _tracked_scoped_options())
    with (
        patch.dict("sys.modules", _mocked_snowflake_modules(finalize_conn)),
        caplog.at_level("WARNING"),
    ):
        dest.finalize_sync(config, _tracked_scoped_options())

    assert not any("baselin" in r.message.lower() for r in caplog.records)
    for call in finalize_conn._cur.execute.call_args_list:
        stmt = call.args[0] if call.args else ""
        if "DELETE FROM" in stmt:
            assert "USER_SCORES" not in stmt


def test_tracked_scoped_genuinely_no_prior_state_still_warns_baseline_snowflake(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from drt.destinations._mirror_state import key_hash, key_json

    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    load_conn = _fake_conn()
    finalize_conn = _fake_conn()
    _configure_state_cur(
        finalize_conn,
        raw_diff=[],
        to_insert=[(key_hash((1, "a")), key_json((1, "a")))],
        previous_exists=False,
        table_exists=False,
    )
    config = _config(upsert_key=["parent_id", "id"])

    with patch.dict("sys.modules", _mocked_snowflake_modules(load_conn)):
        dest.load([{"parent_id": 1, "id": "a"}], config, _tracked_scoped_options())
    with (
        patch.dict("sys.modules", _mocked_snowflake_modules(finalize_conn)),
        caplog.at_level("WARNING"),
    ):
        dest.finalize_sync(config, _tracked_scoped_options())

    assert any("baselin" in r.message.lower() for r in caplog.records)


# ---------------------------------------------------------------------------
# #890 — scope-aware SQL diff (Snowflake leg; see #904 for the design)
# ---------------------------------------------------------------------------


def _scope_columns(conn: MagicMock, *, present: bool) -> None:
    """Answer the #890 column probe on top of the shared state-cursor fake."""
    cur = conn._cur
    inner = cur.fetchone.side_effect

    def fetchone(*a: Any, **k: Any) -> Any:
        sql = cur.execute.call_args.args[0] if cur.execute.call_args.args else ""
        if "information_schema.columns" in sql:
            return (2 if present else 0,)
        return inner()

    cur.fetchone.side_effect = fetchone


def _diff_call(conn: MagicMock) -> Any:
    return next(
        c
        for c in conn._cur.execute.call_args_list
        if c.args and str(c.args[0]).startswith("SELECT s.key_hash")
    )


def _run_scoped(monkeypatch: pytest.MonkeyPatch, finalize_conn: MagicMock) -> None:
    _set_creds(monkeypatch)
    dest = SnowflakeDestination()
    config = _config(upsert_key=["parent_id", "id"])
    with patch.dict("sys.modules", _mocked_snowflake_modules(_fake_conn())):
        dest.load([{"parent_id": 1, "id": "a"}], config, _tracked_scoped_options())
    with patch.dict("sys.modules", _mocked_snowflake_modules(finalize_conn)):
        dest.finalize_sync(config, _tracked_scoped_options())


def test_scoped_diff_is_narrowed_in_sql_snowflake(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _fake_conn()
    _configure_state_cur(conn, raw_diff=[], to_insert=[])
    _scope_columns(conn, present=True)

    _run_scoped(monkeypatch, conn)

    sql, params = _diff_call(conn).args
    assert "s.scope_key IN" in sql
    # both escape branches present — they are what keep this a coarse filter
    assert "s.scope_key IS NULL" in sql
    assert "s.scope_spec <> %s" in sql
    assert '["parent_id"]' in params and "[1]" in params


def test_scoped_diff_falls_back_when_alter_is_refused_snowflake(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No ALTER privilege is a supported state, not an error.

    Snowflake needs no SAVEPOINT for this the way Postgres/MySQL do — the
    connection autocommits, so a refused ALTER leaves nothing half-applied.
    """
    conn = _fake_conn()
    _configure_state_cur(conn, raw_diff=[], to_insert=[])
    _scope_columns(conn, present=False)
    real_execute = conn._cur.execute.side_effect

    def execute(sql: str, *a: Any, **k: Any) -> Any:
        if sql.startswith("ALTER TABLE") and "scope_spec" in sql:
            raise Exception("insufficient privileges on table _DRT_SYNCED_KEYS")
        return real_execute(sql, *a, **k) if real_execute else None

    conn._cur.execute.side_effect = execute

    _run_scoped(monkeypatch, conn)

    assert "scope_key" not in str(_diff_call(conn).args[0])


def test_unscoped_tracked_never_probes_scope_columns_snowflake(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_creds(monkeypatch)
    conn = _fake_conn()
    _configure_state_cur(conn, raw_diff=[], to_insert=[])
    dest = SnowflakeDestination()
    with patch.dict("sys.modules", _mocked_snowflake_modules(_fake_conn())):
        dest.load([{"id": 1}], _config(), _tracked_options())
    with patch.dict("sys.modules", _mocked_snowflake_modules(conn)):
        dest.finalize_sync(_config(), _tracked_options())

    assert not any(
        "information_schema.columns" in str(c.args[0])
        for c in conn._cur.execute.call_args_list
        if c.args
    )
