"""Run-time delete guards on ClickHouse (#1218 part 2).

``ALTER TABLE ... DELETE`` is an asynchronous mutation that reports no row
count, so the guard is a pre-flight ``count()`` over the mutation's own WHERE
predicate; a refusal therefore happens before anything is deleted.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.models import SyncOptions
from drt.destinations.clickhouse import ClickHouseDestination
from drt.engine.guards import GuardTripped

from .test_clickhouse_destination import _config, _options


class _Result:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.result_rows = rows


def _client(counts: list[int], stale: list[tuple[str, str]] | None = None, state: int = 0) -> Any:
    remaining = list(counts)
    client = MagicMock()
    client.queries = []

    def query(sql: str, parameters: Any = None, **_: Any) -> _Result:
        client.queries.append((sql, parameters))
        if sql.startswith("EXISTS TABLE") or sql.startswith("SELECT 1 FROM"):
            return _Result([(1,)])
        if "system.columns" in sql:
            return _Result([(2,)])
        if sql.startswith("SELECT key_hash, key_json FROM"):
            return _Result(stale or [])
        if sql.startswith("SELECT arrayJoin"):
            return _Result([])
        if sql.startswith("SELECT count() FROM") and "_drt_synced_keys" in sql:
            return _Result([(state,)])
        if sql.startswith("SELECT count()"):
            return _Result([(remaining.pop(0),)])
        return _Result([])

    client.query.side_effect = query
    return client


def _mutations(client: Any) -> list[str]:
    return [c.args[0] for c in client.command.call_args_list if " DELETE WHERE " in c.args[0]]


def _finalize(destination: ClickHouseDestination, client: Any, config: Any, options: Any) -> Any:
    with patch.object(ClickHouseDestination, "_connect", return_value=client):
        return destination.finalize_sync(config, options)


def _mirror(limits: dict[str, Any], **extra: Any) -> SyncOptions:
    return _options(mode="mirror", guards=limits, **extra)


def test_destination_refusal_is_before_the_mutation() -> None:
    destination = ClickHouseDestination()
    destination._mirror_keys = [(1,), (2,)]
    client = _client([3])
    config = _config(upsert_key=["id"])

    with pytest.raises(GuardTripped) as raised:
        _finalize(destination, client, config, _mirror({"max_deletes": 2}))

    assert raised.value.trips[0].observed == 3
    assert _mutations(client) == []
    sql, params = client.queries[0]
    assert sql == (
        "SELECT count() FROM `analytics_scores` WHERE toString(`id`) NOT IN {keys:Array(String)}"
    )
    assert sorted(params["keys"]) == ["1", "2"]
    assert destination._mirror_keys is None


def test_destination_pass_counts_the_scoped_baseline_only_for_a_percentage() -> None:
    destination = ClickHouseDestination()
    destination._mirror_keys = [("a", 1)]
    destination._mirror_scopes = {("a",)}
    client = _client([2, 4])
    config = _config(upsert_key=["tenant", "id"])
    options = _mirror({"max_delete_pct": 60}, mirror={"scope": ["tenant"]})

    _finalize(destination, client, config, options)

    counts = [q for q in client.queries if q[0].startswith("SELECT count()")]
    assert "toString(`tenant`) IN {scope_keys:Array(String)} AND" in counts[0][0]
    assert counts[1] == (
        "SELECT count() FROM `analytics_scores` WHERE toString(`tenant`) IN "
        "{scope_keys:Array(String)}",
        {"scope_keys": ["a"]},
    )
    assert len(_mutations(client)) == 1

    destination._mirror_keys = [(1,)]
    client = _client([1])
    _finalize(destination, client, _config(upsert_key=["id"]), _mirror({"max_deletes": 5}))
    assert len([q for q in client.queries if q[0].startswith("SELECT count()")]) == 1


def test_forced_trip_runs_the_mutation_and_is_recorded() -> None:
    destination = ClickHouseDestination()
    destination._mirror_keys = [(1,)]
    client = _client([3])
    options = _mirror({"max_deletes": 1})
    options._force_guards = True

    _finalize(destination, client, _config(upsert_key=["id"]), options)

    assert len(_mutations(client)) == 1
    assert [t.guard for t in options._forced_guard_trips] == ["max_deletes"]


def test_no_guards_means_no_count_queries() -> None:
    destination = ClickHouseDestination()
    destination._mirror_keys = [(1,)]
    client = _client([])

    _finalize(destination, client, _config(upsert_key=["id"]), _options(mode="mirror"))

    assert client.queries == []
    assert len(_mutations(client)) == 1


def _diff(limits: dict[str, Any]) -> SyncOptions:
    options = _options(
        mode="mirror",
        incremental_strategy="diff",
        mirror={"strategy": "diff"},
        guards=limits,
    )
    options._diff_removed_keys = [{"id": 1}, {"id": 2}]
    return options


@pytest.mark.parametrize("limits", [{"max_deletes": 1}, {"max_delete_pct": 90}])
def test_diff_refuses_before_the_mutation_and_percentage_fails_closed(limits: Any) -> None:
    destination = ClickHouseDestination()
    client = _client([2])

    with pytest.raises(GuardTripped):
        _finalize(destination, client, _config(upsert_key=["id"]), _diff(limits))

    assert _mutations(client) == []
    assert client.queries[0][0].startswith("SELECT count() FROM `analytics_scores` WHERE toString")
    assert " IN {keys:Array(String)}" in client.queries[0][0]


def _tracked(limits: dict[str, Any], **mirror: Any) -> SyncOptions:
    options = _options(mode="mirror", mirror={"strategy": "tracked", **mirror}, guards=limits)
    options._sync_name = "tracked_sync"
    return options


def test_tracked_unscoped_baseline_comes_from_state_and_refusal_leaves_state() -> None:
    destination = ClickHouseDestination()
    destination._mirror_keys = [(1,)]
    stale = [("h2", json.dumps([2])), ("h3", json.dumps([3]))]
    client = _client([2], stale=stale, state=4)

    with pytest.raises(GuardTripped) as raised:
        _finalize(destination, client, _config(upsert_key=["id"]), _tracked({"max_delete_pct": 10}))

    assert raised.value.trips[0].observed == 50.0
    assert _mutations(client) == []
    assert any(
        q[0].startswith("SELECT count() FROM `_drt_synced_keys` WHERE sync_name")
        for q in client.queries
    )
    assert not any("INSERT" in c.args[0] for c in client.command.call_args_list)


def test_tracked_scoped_percentage_fails_closed_without_a_state_denominator() -> None:
    destination = ClickHouseDestination()
    destination._mirror_keys = [("a", 1)]
    destination._mirror_scopes = {("a",)}
    stale = [("h2", json.dumps(["a", 2]))]
    client = _client([1], stale=stale, state=1000)

    with pytest.raises(GuardTripped):
        _finalize(
            destination,
            client,
            _config(upsert_key=["tenant", "id"]),
            _tracked({"max_delete_pct": 99}, scope=["tenant"]),
        )

    assert not any(
        q[0].startswith("SELECT count() FROM `_drt_synced_keys`") for q in client.queries
    )
    assert _mutations(client) == []


def _swap(client: Any, limits: dict[str, Any], config: Any, force: bool = False) -> Any:
    destination = ClickHouseDestination()
    destination._swap_shadow_created = True
    destination._swap_table = "analytics_scores"
    options = _options(mode="replace", replace_strategy="swap", guards=limits)
    options._force_guards = force
    try:
        return _finalize(destination, client, config, options), options
    finally:
        assert destination._swap_shadow_created is False


def test_swap_trip_never_exchanges_and_drops_the_shadow() -> None:
    client = _client([5, 3])
    with pytest.raises(GuardTripped):
        _swap(client, {"max_deletes": 1}, _config(upsert_key=["id"]))

    commands = [c.args[0] for c in client.command.call_args_list]
    assert not any(c.startswith("EXCHANGE") for c in commands)
    assert commands[-1] == "DROP TABLE IF EXISTS `analytics_scores__drt_swap`"
    assert client.queries[0][0] == "SELECT count() FROM `analytics_scores`"
    assert "NOT IN (SELECT" in client.queries[1][0]
    assert "(isNull(`id`), ifNull(toString(`id`), ''))" in client.queries[1][0]
    assert "__drt_null__" not in client.queries[1][0]  # no sentinel a real value could equal


def test_swap_force_exchanges_and_records_and_composite_is_null_safe() -> None:
    client = _client([5, 3])
    result, options = _swap(client, {"max_deletes": 1}, _config(upsert_key=["a", "b"]), force=True)
    assert result is not None
    assert any(c.args[0].startswith("EXCHANGE") for c in client.command.call_args_list)
    assert options._forced_guard_trips[0].guard == "max_deletes"
    assert client.queries[1][0].startswith(
        "SELECT count() FROM `analytics_scores` WHERE "
        "(isNull(`a`), ifNull(toString(`a`), ''), "
        "isNull(`b`), ifNull(toString(`b`), '')) NOT IN (SELECT "
    )


def test_keyless_swap_uses_the_documented_cardinality_fallback_and_survives_drop_failure() -> None:
    client = _client([5, 1])
    client.command.side_effect = RuntimeError("drop failed")
    with pytest.raises(GuardTripped) as raised:
        _swap(client, {"max_deletes": 2}, _config())
    assert raised.value.trips[0].observed == 4


@pytest.mark.parametrize("path", ["destination", "diff", "tracked", "swap"])
def test_client_close_failure_never_masks_a_guard_refusal(path: str) -> None:
    destination = ClickHouseDestination()
    config = _config(upsert_key=["id"])
    if path == "destination":
        destination._mirror_keys = [(1,), (2,)]
        options = _mirror({"max_deletes": 1})
        client = _client([3])
    elif path == "diff":
        options = _diff({"max_deletes": 1})
        client = _client([2])
    elif path == "tracked":
        destination._mirror_keys = [(1,)]
        options = _tracked({"max_deletes": 1})
        client = _client([2], stale=[("h2", json.dumps([2])), ("h3", json.dumps([3]))], state=4)
    else:
        destination._swap_shadow_created = True
        destination._swap_table = "analytics_scores"
        options = _options(mode="replace", replace_strategy="swap", guards={"max_deletes": 1})
        client = _client([5, 3])
    client.close.side_effect = RuntimeError("close failed")

    with pytest.raises(GuardTripped):
        _finalize(destination, client, config, options)

    client.close.assert_called_once()


def test_client_close_failure_still_surfaces_when_nothing_tripped() -> None:
    destination = ClickHouseDestination()
    destination._mirror_keys = [(1,)]
    client = _client([1])
    client.close.side_effect = RuntimeError("close failed")

    with pytest.raises(RuntimeError, match="close failed"):
        _finalize(destination, client, _config(upsert_key=["id"]), _mirror({"max_deletes": 5}))
