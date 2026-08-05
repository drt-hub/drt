"""Unit tests for ``sync.mode: mirror`` on the ClickHouse destination (#340 Step 3).

Mirror mode INSERTs source rows like ``full`` mode, then in the
``finalize_sync`` end-of-sync hook issues an ``ALTER TABLE ... DELETE``
mutation that removes destination rows whose ``upsert_key`` is not in
the set seen across all batches.

Strategy under test: application-side diff (collect upsert_key tuples
in memory, then ``ALTER TABLE ... DELETE WHERE key NOT IN (collected)``
with ``mutations_sync=1``). Memory-bound to the source key cardinality
and the mutation rewrites parts — appropriate for small/medium reference
tables. The temp-table strategy is a planned follow-up for high-volume
tables.

clickhouse_connect supports native ``{name:Type}`` parameter binding
with ``Array(...)`` types, so unlike Postgres / MySQL we don't build
the placeholder list explicitly. Both column references and parameter
values are coerced with ``toString()`` so the comparison works regardless
of source column type.

These tests mock ``clickhouse_connect`` clients — no real ClickHouse
needed.
"""

from __future__ import annotations

import pytest

pytest.importorskip("clickhouse_connect")

from typing import Any
from unittest.mock import MagicMock, patch

from drt.config.models import ClickHouseDestinationConfig, SyncOptions
from drt.destinations.clickhouse import ClickHouseDestination


def _options(**kwargs: Any) -> SyncOptions:
    defaults: dict[str, Any] = {"mode": "mirror"}
    defaults.update(kwargs)
    return SyncOptions(**defaults)


def _config(**overrides: Any) -> ClickHouseDestinationConfig:
    defaults: dict[str, Any] = {
        "type": "clickhouse",
        "host": "localhost",
        "database": "default",
        "user": "default",
        "password": "",
        "table": "scores",
        "upsert_key": ["id"],
    }
    defaults.update(overrides)
    return ClickHouseDestinationConfig(**defaults)


def _fake_client() -> MagicMock:
    return MagicMock()


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
    dest = ClickHouseDestination()
    client = _fake_client()
    config = _config()
    opts = _options()

    with patch.object(ClickHouseDestination, "_connect", return_value=client):
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


def test_finalize_mirror_issues_alter_delete_with_collected_keys() -> None:
    """``finalize_sync`` runs ``ALTER TABLE ... DELETE WHERE id NOT IN {keys:Array(String)}``."""
    dest = ClickHouseDestination()
    load_client = _fake_client()
    finalize_client = _fake_client()
    config = _config()
    opts = _options()

    with patch.object(ClickHouseDestination, "_connect", return_value=load_client):
        dest.load(
            [{"id": 1, "score": 100}, {"id": 2, "score": 200}],
            config,
            opts,
        )

    with patch.object(ClickHouseDestination, "_connect", return_value=finalize_client):
        result = dest.finalize_sync(config, opts)

    assert result is not None
    assert result.success == 0
    assert result.failed == 0
    # ALTER TABLE DELETE was executed exactly once
    assert finalize_client.command.call_count == 1
    args, kwargs = finalize_client.command.call_args
    sql = args[0]
    assert "ALTER TABLE `scores` DELETE" in sql
    assert "toString(`id`) NOT IN {keys:Array(String)}" in sql
    # Parameters pass observed keys as strings
    assert set(kwargs["parameters"]["keys"]) == {"1", "2"}
    # Mutation must be synchronous so the call blocks until DELETE finishes
    assert kwargs["settings"] == {"mutations_sync": 1}


def test_finalize_mirror_dedupes_overlapping_batches() -> None:
    """If two batches both contain id=1, the DELETE NOT IN list lists it once."""
    dest = ClickHouseDestination()
    load_client = _fake_client()
    finalize_client = _fake_client()
    config = _config()
    opts = _options()

    with patch.object(ClickHouseDestination, "_connect", return_value=load_client):
        dest.load([{"id": 1, "score": 100}], config, opts)
        dest.load([{"id": 1, "score": 999}], config, opts)
        dest.load([{"id": 2, "score": 200}], config, opts)

    with patch.object(ClickHouseDestination, "_connect", return_value=finalize_client):
        dest.finalize_sync(config, opts)

    _args, kwargs = finalize_client.command.call_args
    keys = kwargs["parameters"]["keys"]
    # Two unique keys, even though id=1 came in twice
    assert sorted(keys) == ["1", "2"]


def test_finalize_mirror_quotes_database_qualified_table() -> None:
    """A ``db.table`` config emits ``\\`db\\`.\\`table\\``` in the DELETE."""
    dest = ClickHouseDestination()
    load_client = _fake_client()
    finalize_client = _fake_client()
    config = _config(table="analytics.scores")
    opts = _options()

    with patch.object(ClickHouseDestination, "_connect", return_value=load_client):
        dest.load([{"id": 1, "score": 100}], config, opts)
    with patch.object(ClickHouseDestination, "_connect", return_value=finalize_client):
        dest.finalize_sync(config, opts)

    args, _kwargs = finalize_client.command.call_args
    assert "ALTER TABLE `analytics`.`scores` DELETE" in args[0]


# ---------------------------------------------------------------------------
# Composite upsert_key
# ---------------------------------------------------------------------------


def test_mirror_composite_key_accumulates_tuples() -> None:
    """Two-column upsert_key yields 2-tuples in ``_mirror_keys``."""
    dest = ClickHouseDestination()
    client = _fake_client()
    config = _config(upsert_key=["user_id", "session_id"])
    opts = _options()

    with patch.object(ClickHouseDestination, "_connect", return_value=client):
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
    """Composite upsert_key → DELETE NOT IN {keys:Array(Tuple(String, String))}."""
    dest = ClickHouseDestination()
    load_client = _fake_client()
    finalize_client = _fake_client()
    config = _config(upsert_key=["user_id", "session_id"])
    opts = _options()

    with patch.object(ClickHouseDestination, "_connect", return_value=load_client):
        dest.load(
            [
                {"user_id": "a", "session_id": "x", "score": 1},
                {"user_id": "b", "session_id": "y", "score": 2},
            ],
            config,
            opts,
        )

    with patch.object(ClickHouseDestination, "_connect", return_value=finalize_client):
        dest.finalize_sync(config, opts)

    args, kwargs = finalize_client.command.call_args
    sql = args[0]
    assert "(toString(`user_id`), toString(`session_id`))" in sql
    assert "NOT IN {keys:Array(Tuple(String, String))}" in sql
    # keys is a list of stringified tuples
    pairs = set(kwargs["parameters"]["keys"])
    assert pairs == {("a", "x"), ("b", "y")}


# ---------------------------------------------------------------------------
# Safety paths
# ---------------------------------------------------------------------------


def test_finalize_mirror_skips_when_no_keys_observed() -> None:
    """No batch ever delivered records → finalize returns None, no DELETE.

    Prevents a transient empty source from silently wiping the destination.
    """
    dest = ClickHouseDestination()
    finalize_client = _fake_client()
    config = _config()
    opts = _options()

    # No load() called; _mirror_keys is still None.
    with patch.object(ClickHouseDestination, "_connect", return_value=finalize_client):
        result = dest.finalize_sync(config, opts)

    assert result is None
    finalize_client.command.assert_not_called()


def test_finalize_mirror_resets_state_after_run() -> None:
    """After finalize, ``_mirror_keys`` is cleared so a re-run starts fresh."""
    dest = ClickHouseDestination()
    load_client = _fake_client()
    finalize_client = _fake_client()
    config = _config()
    opts = _options()

    with patch.object(ClickHouseDestination, "_connect", return_value=load_client):
        dest.load([{"id": 1, "score": 100}], config, opts)
    with patch.object(ClickHouseDestination, "_connect", return_value=finalize_client):
        dest.finalize_sync(config, opts)

    assert dest._mirror_keys is None


def test_mirror_raises_when_upsert_key_missing() -> None:
    """Mirror mode without ``upsert_key`` is a config error surfaced at load.

    ClickHouseDestinationConfig.upsert_key is ``list[str] | None`` (unlike
    Postgres / MySQL where it's required at the config layer), so the
    runtime guard in ``load()`` is the only defence.
    """
    dest = ClickHouseDestination()
    client = _fake_client()
    config = _config(upsert_key=None)
    opts = _options()

    with patch.object(ClickHouseDestination, "_connect", return_value=client):
        with pytest.raises(ValueError, match="mirror requires destination.upsert_key"):
            dest.load([{"id": 1, "score": 100}], config, opts)

    # ValueError raised BEFORE any INSERT — table was never touched.
    client.insert.assert_not_called()


def test_mirror_excludes_failed_record_keys_from_accumulation() -> None:
    """Records whose batch_index appears in row_errors are skipped from ``_mirror_keys``.

    Only successfully-loaded keys count as "source state" — same shape as
    Postgres / MySQL Step 1+2.
    """
    dest = ClickHouseDestination()
    client = _fake_client()
    # Make the second insert raise so batch_index=1 ends up in row_errors.
    call_counter = {"n": 0}

    def _insert_with_one_failure(*_args: Any, **_kwargs: Any) -> None:
        call_counter["n"] += 1
        if call_counter["n"] == 2:
            raise RuntimeError("forced for test")

    client.insert.side_effect = _insert_with_one_failure
    config = _config()
    opts = _options(on_error="skip")

    with patch.object(ClickHouseDestination, "_connect", return_value=client):
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
    dest = ClickHouseDestination()
    dest._swap_shadow_created = True
    dest._swap_table = "scores"

    client = _fake_client()
    config = _config()
    swap_opts = SyncOptions(mode="replace", replace_strategy="swap")

    with patch.object(ClickHouseDestination, "_connect", return_value=client):
        result = dest.finalize_sync(config, swap_opts)

    assert result is not None
    assert dest._swap_shadow_created is False
    assert dest._swap_table is None
    # Existing path runs EXCHANGE TABLES + DROP TABLE
    commands = [c.args[0] for c in client.command.call_args_list]
    assert any("EXCHANGE TABLES" in cmd for cmd in commands)
    assert any("DROP TABLE" in cmd for cmd in commands)


def test_tracked_strategy_accepted_on_clickhouse() -> None:
    """``mirror.strategy: tracked`` (#692) is now supported on ClickHouse."""
    dest = ClickHouseDestination()
    client = _fake_client()
    opts = _options(mirror={"strategy": "tracked"})

    with patch.object(ClickHouseDestination, "_connect", return_value=client):
        result = dest.load([{"id": 1, "score": 100}], _config(), opts)

    assert result.failed == 0


def test_scope_accepted_on_clickhouse() -> None:
    """``mirror.scope`` (#692, destination strategy) is now supported."""
    dest = ClickHouseDestination()
    client = _fake_client()
    opts = _options(mirror={"scope": ["parent_id"]})

    with patch.object(ClickHouseDestination, "_connect", return_value=client):
        result = dest.load([{"id": 1, "parent_id": 10}], _config(), opts)

    assert result.failed == 0


def test_scope_missing_column_fails_fast_on_clickhouse() -> None:
    dest = ClickHouseDestination()
    client = _fake_client()
    opts = _options(mirror={"scope": ["parent_id"]})

    with patch.object(ClickHouseDestination, "_connect", return_value=client):
        with pytest.raises(ValueError, match="mirror.scope columns missing"):
            dest.load([{"id": 1}], _config(), opts)

    client.insert.assert_not_called()


def test_scoped_mirror_deletes_within_observed_parents_only_clickhouse() -> None:
    """Destination-strategy scope: the DELETE only ever considers rows under
    parents this run actually observed."""
    dest = ClickHouseDestination()
    load_client = _fake_client()
    finalize_client = _fake_client()
    config = _config(upsert_key=["parent_id", "id"])
    opts = _options(mirror={"scope": ["parent_id"]})

    with patch.object(ClickHouseDestination, "_connect", return_value=load_client):
        dest.load([{"parent_id": 1, "id": "a", "score": 1}], config, opts)
    with patch.object(ClickHouseDestination, "_connect", return_value=finalize_client):
        dest.finalize_sync(config, opts)

    args, kwargs = finalize_client.command.call_args
    sql = args[0]
    assert "toString(`parent_id`) IN {scope_keys:Array(String)} AND" in sql
    assert kwargs["parameters"]["scope_keys"] == ["1"]


def test_scoped_mirror_composite_scope_uses_tuple_form_clickhouse() -> None:
    dest = ClickHouseDestination()
    load_client = _fake_client()
    finalize_client = _fake_client()
    config = _config(upsert_key=["tenant_id", "parent_id", "id"])
    opts = _options(mirror={"scope": ["tenant_id", "parent_id"]})

    with patch.object(ClickHouseDestination, "_connect", return_value=load_client):
        dest.load([{"tenant_id": 1, "parent_id": 1, "id": "a", "score": 1}], config, opts)
    with patch.object(ClickHouseDestination, "_connect", return_value=finalize_client):
        dest.finalize_sync(config, opts)

    args, kwargs = finalize_client.command.call_args
    sql = args[0]
    assert (
        "(toString(`tenant_id`), toString(`parent_id`)) "
        "IN {scope_keys:Array(Tuple(String, String))} AND" in sql
    )
    assert kwargs["parameters"]["scope_keys"] == [("1", "1")]


def test_scope_rejected_with_tracked_when_not_subset_of_upsert_key_clickhouse() -> None:
    """#694's composition constraint applies on ClickHouse too."""
    dest = ClickHouseDestination()
    client = _fake_client()
    opts = _options(mirror={"strategy": "tracked", "scope": ["parent_id"]})

    with patch.object(ClickHouseDestination, "_connect", return_value=client):
        with pytest.raises(ValueError, match="mirror.scope columns must be part of"):
            dest.load([{"id": 1, "parent_id": 10}], _config(upsert_key=["id"]), opts)


# ---------------------------------------------------------------------------
# mirror.strategy: tracked (#692, mirroring Postgres/MySQL/Snowflake's #686)
# ---------------------------------------------------------------------------


def _tracked_options() -> SyncOptions:
    opts = _options(mirror={"strategy": "tracked"})
    opts._sync_name = "scores_sync"
    return opts


def _state_client(
    raw_diff: list[tuple[str, str]] | None = None,
    new_hashes: list[str] | None = None,
    previous_exists: bool = True,
    exists: bool = True,
) -> MagicMock:
    """A fake client wired for the #694 part 2 read path — ``EXISTS TABLE``,
    a baseline existence probe, the SQL-side diff, and the genuinely-new-
    keys probe all go through ``client.query()``, dispatched here by the
    query's SQL text since ``query()`` is now called up to four times per
    run (no staging table on ClickHouse — see the docstring in
    ``clickhouse.py`` for why: ``Array(String)`` parameters already hold
    the whole current-key-hash set as one bound value).

    ``raw_diff`` is what ``previous - current`` would have computed
    server-side; ``new_hashes`` is what ``current - previous`` would have.
    """
    client = _fake_client()

    def query_side_effect(sql: str, *args: Any, **kwargs: Any) -> MagicMock:
        if sql.startswith("EXISTS TABLE"):
            return MagicMock(result_rows=[(1 if exists else 0,)])
        if "LIMIT 1" in sql:
            return MagicMock(result_rows=[(1,)] if previous_exists else [])
        if sql.startswith("SELECT key_hash, key_json"):
            return MagicMock(result_rows=list(raw_diff or []))
        if sql.startswith("SELECT arrayJoin"):
            return MagicMock(result_rows=[(h,) for h in (new_hashes or [])])
        return MagicMock(result_rows=[])

    client.query.side_effect = query_side_effect
    return client


def test_tracked_creates_state_table_when_absent_clickhouse() -> None:
    """``EXISTS TABLE`` -> 0: the state table is created (lazy-create
    default, mirrors #695's pre-provisioning probe on the other dialects)."""
    dest = ClickHouseDestination()
    load_client = _fake_client()
    finalize_client = _state_client(exists=False)

    with patch.object(ClickHouseDestination, "_connect", return_value=load_client):
        dest.load([{"id": 1}], _config(), _tracked_options())
    with patch.object(ClickHouseDestination, "_connect", return_value=finalize_client):
        dest.finalize_sync(_config(), _tracked_options())

    create_calls = [
        call.args[0]
        for call in finalize_client.command.call_args_list
        if "CREATE TABLE" in (call.args[0] if call.args else "")
    ]
    assert len(create_calls) == 1
    assert "_drt_synced_keys" in create_calls[0]
    assert "ENGINE = MergeTree" in create_calls[0]


def test_tracked_skips_create_when_state_table_preprovisioned_clickhouse() -> None:
    """``EXISTS TABLE`` -> 1: no CREATE TABLE is issued."""
    dest = ClickHouseDestination()
    load_client = _fake_client()
    finalize_client = _state_client(exists=True)

    with patch.object(ClickHouseDestination, "_connect", return_value=load_client):
        dest.load([{"id": 1}], _config(), _tracked_options())
    with patch.object(ClickHouseDestination, "_connect", return_value=finalize_client):
        dest.finalize_sync(_config(), _tracked_options())

    assert not any(
        "CREATE TABLE" in (call.args[0] if call.args else "")
        for call in finalize_client.command.call_args_list
    )


def test_tracked_first_run_baselines_without_deleting_clickhouse() -> None:
    from drt.destinations._mirror_state import key_hash

    dest = ClickHouseDestination()
    load_client = _fake_client()
    finalize_client = _state_client(
        raw_diff=[],
        new_hashes=[key_hash((1,)), key_hash((2,))],
        previous_exists=False,
    )

    with patch.object(ClickHouseDestination, "_connect", return_value=load_client):
        dest.load([{"id": 1}, {"id": 2}], _config(), _tracked_options())
    with patch.object(ClickHouseDestination, "_connect", return_value=finalize_client):
        result = dest.finalize_sync(_config(), _tracked_options())

    assert result is not None
    for call in finalize_client.command.call_args_list:
        sql = call.args[0] if call.args else ""
        if "DELETE" in sql:
            assert "`scores`" not in sql
    insert_rows = finalize_client.insert.call_args.args[1]
    assert [r[0] for r in insert_rows] == ["scores_sync", "scores_sync"]


def test_tracked_second_run_deletes_only_stale_tracked_keys_clickhouse() -> None:
    """prev={1,2,3}, current={1,2} -> ALTER TABLE `scores` DELETE ... IN {keys:...} w/ ["3"]."""
    from drt.destinations._mirror_state import key_hash, key_json

    dest = ClickHouseDestination()
    load_client = _fake_client()
    finalize_client = _state_client(raw_diff=[(key_hash((3,)), key_json((3,)))])

    with patch.object(ClickHouseDestination, "_connect", return_value=load_client):
        dest.load([{"id": 1}, {"id": 2}], _config(), _tracked_options())
    with patch.object(ClickHouseDestination, "_connect", return_value=finalize_client):
        dest.finalize_sync(_config(), _tracked_options())

    target_deletes = [
        call
        for call in finalize_client.command.call_args_list
        if "DELETE" in (call.args[0] if call.args else "") and "`scores`" in call.args[0]
    ]
    assert len(target_deletes) == 1
    sql, kwargs = target_deletes[0].args[0], target_deletes[0].kwargs
    assert "IN {keys:Array(String)}" in sql and "NOT IN" not in sql
    assert kwargs["parameters"]["keys"] == ["3"]


def test_tracked_empty_source_is_noop_clickhouse() -> None:
    dest = ClickHouseDestination()
    finalize_client = _fake_client()

    with patch.object(ClickHouseDestination, "_connect", return_value=finalize_client):
        result = dest.finalize_sync(_config(), _tracked_options())

    assert result is None
    finalize_client.command.assert_not_called()


def test_tracked_baseline_logs_warning_clickhouse(caplog: pytest.LogCaptureFixture) -> None:
    from drt.destinations._mirror_state import key_hash

    dest = ClickHouseDestination()
    load_client = _fake_client()
    finalize_client = _state_client(raw_diff=[], new_hashes=[key_hash((1,))], previous_exists=False)

    with patch.object(ClickHouseDestination, "_connect", return_value=load_client):
        dest.load([{"id": 1}], _config(), _tracked_options())
    with (
        patch.object(ClickHouseDestination, "_connect", return_value=finalize_client),
        caplog.at_level("WARNING"),
    ):
        dest.finalize_sync(_config(), _tracked_options())

    assert any("baselin" in r.message.lower() for r in caplog.records)


# ---------------------------------------------------------------------------
# mirror.scope + strategy: tracked (#694, extended to ClickHouse by #692)
# ---------------------------------------------------------------------------


def _tracked_scoped_options(scope: list[str] = ["parent_id"]) -> SyncOptions:
    opts = _options(mirror={"strategy": "tracked", "scope": scope})
    opts._sync_name = "scores_sync"
    return opts


def test_tracked_scoped_deletes_only_stale_keys_within_observed_scope_clickhouse() -> None:
    """Prior state has parent 1: {(1,"a"),(1,"b")} and parent 2: {(2,"x")}.
    This run only touches parent 1 with just (1,"a") -> (1,"b") is stale and
    deleted; (2,"x") is under a parent this run never saw and must survive
    (#694 part 2: never read or rewritten at all — never even queried, let
    alone reinserted)."""
    from drt.destinations._mirror_state import key_hash, key_json

    dest = ClickHouseDestination()
    load_client = _fake_client()
    finalize_client = _state_client(
        raw_diff=[(key_hash(k), key_json(k)) for k in ((1, "b"), (2, "x"))],
        new_hashes=[],  # (1,"a") already tracked
    )
    config = _config(upsert_key=["parent_id", "id"])

    with patch.object(ClickHouseDestination, "_connect", return_value=load_client):
        dest.load([{"parent_id": 1, "id": "a"}], config, _tracked_scoped_options())
    with patch.object(ClickHouseDestination, "_connect", return_value=finalize_client):
        dest.finalize_sync(config, _tracked_scoped_options())

    target_deletes = [
        call
        for call in finalize_client.command.call_args_list
        if "DELETE" in (call.args[0] if call.args else "") and "`scores`" in call.args[0]
    ]
    assert len(target_deletes) == 1
    assert target_deletes[0].kwargs["parameters"]["keys"] == [("1", "b")]

    state_delete_calls = [
        call
        for call in finalize_client.command.call_args_list
        if "DELETE" in (call.args[0] if call.args else "") and "`scores`" not in call.args[0]
    ]
    assert len(state_delete_calls) == 1
    assert state_delete_calls[0].kwargs["parameters"]["hashes"] == [key_hash((1, "b"))]
    finalize_client.insert.assert_not_called()


def test_tracked_scoped_first_touch_of_a_scope_is_not_a_baseline_warning_clickhouse(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from drt.destinations._mirror_state import key_hash, key_json

    dest = ClickHouseDestination()
    load_client = _fake_client()
    finalize_client = _state_client(
        raw_diff=[(key_hash((2, "x")), key_json((2, "x")))],
        new_hashes=[key_hash((1, "a"))],
    )
    config = _config(upsert_key=["parent_id", "id"])

    with patch.object(ClickHouseDestination, "_connect", return_value=load_client):
        dest.load([{"parent_id": 1, "id": "a"}], config, _tracked_scoped_options())
    with (
        patch.object(ClickHouseDestination, "_connect", return_value=finalize_client),
        caplog.at_level("WARNING"),
    ):
        dest.finalize_sync(config, _tracked_scoped_options())

    assert not any("baselin" in r.message.lower() for r in caplog.records)
    for call in finalize_client.command.call_args_list:
        sql = call.args[0] if call.args else ""
        if "DELETE" in sql:
            assert "`scores`" not in sql
    insert_rows = finalize_client.insert.call_args.args[1]
    assert [(r[0], r[1]) for r in insert_rows] == [("scores_sync", key_hash((1, "a")))]


def test_tracked_scoped_genuinely_no_prior_state_still_warns_baseline_clickhouse(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from drt.destinations._mirror_state import key_hash

    dest = ClickHouseDestination()
    load_client = _fake_client()
    finalize_client = _state_client(
        raw_diff=[], new_hashes=[key_hash((1, "a"))], previous_exists=False
    )
    config = _config(upsert_key=["parent_id", "id"])

    with patch.object(ClickHouseDestination, "_connect", return_value=load_client):
        dest.load([{"parent_id": 1, "id": "a"}], config, _tracked_scoped_options())
    with (
        patch.object(ClickHouseDestination, "_connect", return_value=finalize_client),
        caplog.at_level("WARNING"),
    ):
        dest.finalize_sync(config, _tracked_scoped_options())

    assert any("baselin" in r.message.lower() for r in caplog.records)
