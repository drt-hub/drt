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
