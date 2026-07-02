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


def test_tracked_strategy_rejected_on_clickhouse() -> None:
    """``mirror.strategy: tracked`` (#686) is Postgres/MySQL-only for now.

    Must fail fast rather than silently falling back to the destination
    diff, whose delete semantics are co-writer-unsafe.
    """
    dest = ClickHouseDestination()
    client = _fake_client()
    opts = _options(mirror={"strategy": "tracked"})

    with patch.object(ClickHouseDestination, "_connect", return_value=client):
        with pytest.raises(ValueError, match="tracked is not yet supported"):
            dest.load([{"id": 1, "score": 100}], _config(), opts)

    client.insert.assert_not_called()
