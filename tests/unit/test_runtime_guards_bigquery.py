"""Run-time delete guards on BigQuery (#1218 part 2): count first, re-check the DML job."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.destinations.bigquery import BigQueryDestination
from drt.engine.guards import GuardOutcomeCommitted, GuardTripped

from .test_bigquery_destination import (
    _config,
    _fake_client,
    _mocked_bq_modules,
    _options,
    _schema_field,
    _sqls,
    _stable_scratch_run_id,  # noqa: F401 — autouse fixture: deterministic scratch names
)

KEYS = "my-proj.analytics.user_scores__drt_mirror_keys_a1b2c3d4"


def _scripted(client: MagicMock, counts: list[int], affected: Any = 3) -> None:
    """``COUNT(*)`` queries pop ``counts``; a DELETE job reports ``affected`` rows."""
    remaining = list(counts)

    def query(sql: str, job_config: Any = None) -> MagicMock:
        job = MagicMock()
        if sql.startswith("SELECT COUNT(*)"):
            job.result.return_value = [(remaining.pop(0),)]
        elif sql.startswith("DELETE FROM"):
            job.num_dml_affected_rows = affected
        return job

    client.query.side_effect = query


def _mirror(limits: dict[str, Any], **extra: Any) -> Any:
    return _options(mode="mirror", guards=limits, **extra)


def _run_mirror(client: MagicMock, options: Any, config: Any, records: list[dict[str, Any]]) -> Any:
    destination = BigQueryDestination()
    with patch.dict("sys.modules", _mocked_bq_modules(client)):
        destination.load(records, config, options)
        return destination.finalize_sync(config, options)


def _deletes(client: MagicMock) -> list[str]:
    return [s for s in _sqls(client) if s.startswith("DELETE FROM")]


def test_destination_refusal_happens_before_delete_and_drops_staged_keys() -> None:
    client = _fake_client()
    _scripted(client, [3])
    config = _config(mode="insert", upsert_key=["id"])

    with pytest.raises(GuardTripped) as raised:
        _run_mirror(client, _mirror({"max_deletes": 2}), config, [{"id": 1}, {"id": 2}])

    assert raised.value.trips[0].observed == 3
    assert not isinstance(raised.value, GuardOutcomeCommitted)
    assert _deletes(client) == []
    count = next(s for s in _sqls(client) if s.startswith("SELECT COUNT(*)"))
    assert count.startswith(
        "SELECT COUNT(*) FROM `my-proj.analytics.user_scores` AS T WHERE NOT EXISTS"
    )
    client.delete_table.assert_any_call(KEYS, not_found_ok=True)


def test_destination_pass_counts_scoped_baseline_and_verifies_the_job() -> None:
    client = _fake_client()
    _scripted(client, [2, 4], affected=2)
    config = _config(mode="insert", upsert_key=["tenant", "id"])
    options = _mirror({"max_delete_pct": 60}, mirror={"scope": ["tenant"]})

    _run_mirror(client, options, config, [{"tenant": "a", "id": 1}])

    counts = [s for s in _sqls(client) if s.startswith("SELECT COUNT(*)")]
    assert "EXISTS (SELECT 1 FROM" in counts[0] and "TO_JSON_STRING(T.`tenant`)" in counts[0]
    assert counts[1].startswith(
        "SELECT COUNT(*) FROM `my-proj.analytics.user_scores` AS T WHERE EXISTS"
    )
    assert counts[1].count("NOT EXISTS") == 0
    assert len(_deletes(client)) == 1


def test_baseline_is_only_counted_for_a_percentage_guard() -> None:
    client = _fake_client()
    _scripted(client, [1], affected=1)
    config = _config(mode="insert", upsert_key=["id"])

    _run_mirror(client, _mirror({"max_deletes": 5}), config, [{"id": 1}])

    assert len([s for s in _sqls(client) if s.startswith("SELECT COUNT(*)")]) == 1


@pytest.mark.parametrize("affected", [None, -1, True, "7"])
def test_unreadable_job_row_count_is_reported_as_already_committed(affected: Any) -> None:
    client = _fake_client()
    _scripted(client, [1], affected=affected)
    config = _config(mode="insert", upsert_key=["id"])

    with pytest.raises(GuardOutcomeCommitted, match="could not be read back"):
        _run_mirror(client, _mirror({"max_deletes": 5}), config, [{"id": 1}])

    assert len(_deletes(client)) == 1
    client.delete_table.assert_any_call(KEYS, not_found_ok=True)


def test_late_larger_delete_is_reported_as_already_committed_unless_forced() -> None:
    client = _fake_client()
    _scripted(client, [1], affected=9)
    config = _config(mode="insert", upsert_key=["id"])

    with pytest.raises(GuardOutcomeCommitted, match="9 row"):
        _run_mirror(client, _mirror({"max_deletes": 1}), config, [{"id": 1}])

    client = _fake_client()
    _scripted(client, [2], affected=9)
    forced = _mirror({"max_deletes": 1})
    forced._force_guards = True
    _run_mirror(client, forced, config, [{"id": 1}])
    assert [(t.guard, t.observed) for t in forced._forced_guard_trips] == [("max_deletes", 9)]


def test_no_guards_means_no_count_queries() -> None:
    client = _fake_client()
    _scripted(client, [], affected=1)
    config = _config(mode="insert", upsert_key=["id"])

    _run_mirror(client, _options(mode="mirror"), config, [{"id": 1}])

    assert not any(s.startswith("SELECT COUNT(*)") for s in _sqls(client))


def _diff_options(limits: dict[str, Any]) -> Any:
    options = _options(
        mode="mirror",
        incremental_strategy="diff",
        mirror={"strategy": "diff"},
        guards=limits,
    )
    options._diff_removed_keys = [{"id": 1}, {"id": 2}]
    return options


def test_diff_refusal_is_before_delete_and_percentage_fails_closed() -> None:
    for limits in ({"max_deletes": 1}, {"max_delete_pct": 90}):
        client = _fake_client()
        client.get_table.return_value.schema = [_schema_field("id")]
        _scripted(client, [2])
        destination = BigQueryDestination()
        with patch.dict("sys.modules", _mocked_bq_modules(client)), pytest.raises(GuardTripped):
            destination.finalize_sync(_config(upsert_key=["id"]), _diff_options(limits))
        assert _deletes(client) == []
        client.delete_table.assert_any_call(
            f"{KEYS.rsplit('_', 1)[0]}_diff_a1b2c3d4", not_found_ok=True
        )


def test_diff_pass_deletes_and_verifies() -> None:
    client = _fake_client()
    client.get_table.return_value.schema = [_schema_field("id")]
    _scripted(client, [2], affected=2)
    destination = BigQueryDestination()
    with patch.dict("sys.modules", _mocked_bq_modules(client)):
        destination.finalize_sync(_config(upsert_key=["id"]), _diff_options({"max_deletes": 2}))
    assert len(_deletes(client)) == 1


def _swap(client: MagicMock, limits: dict[str, Any], config: Any, force: bool = False) -> Any:
    destination = BigQueryDestination()
    options = _options(mode="replace", replace_strategy="swap", guards=limits)
    options._force_guards = force
    with patch.dict("sys.modules", _mocked_bq_modules(client)):
        destination.load([{"id": 1}], config, options)
        try:
            return destination.finalize_sync(config, options), options
        finally:
            assert destination._swap_shadow_created is False


def test_swap_trip_never_copies_over_the_live_table_and_drops_the_shadow() -> None:
    client = _fake_client()
    _scripted(client, [5, 3])
    with pytest.raises(GuardTripped):
        _swap(client, {"max_deletes": 1}, _config(upsert_key=["id"]))

    counts = [s for s in _sqls(client) if s.startswith("SELECT COUNT(*)")]
    assert counts[0] == "SELECT COUNT(*) FROM `my-proj.analytics.user_scores`"
    assert "TO_JSON_STRING(L.`id`) = TO_JSON_STRING(S.`id`)" in counts[1]
    shadow = "my-proj.analytics.user_scores__drt_swap_a1b2c3d4"
    assert not any(
        c.args == (shadow, "my-proj.analytics.user_scores")
        for c in client.copy_table.call_args_list
    )
    client.delete_table.assert_any_call(shadow, not_found_ok=True)


def test_swap_force_copies_and_records_the_trip() -> None:
    client = _fake_client()
    _scripted(client, [5, 3])
    result, options = _swap(client, {"max_deletes": 1}, _config(upsert_key=["id"]), force=True)
    assert result is not None
    assert options._forced_guard_trips[0].guard == "max_deletes"
    assert any(
        c.args
        == ("my-proj.analytics.user_scores__drt_swap_a1b2c3d4", "my-proj.analytics.user_scores")
        for c in client.copy_table.call_args_list
    )


def test_keyless_swap_uses_the_documented_cardinality_fallback() -> None:
    client = _fake_client()
    _scripted(client, [5, 1])
    with pytest.raises(GuardTripped) as raised:
        _swap(client, {"max_deletes": 2}, _config())
    assert raised.value.trips[0].observed == 4


def test_staging_cleanup_failure_never_masks_a_guard_refusal() -> None:
    client = _fake_client()
    _scripted(client, [3])
    config = _config(mode="insert", upsert_key=["id"])
    options = _mirror({"max_deletes": 1})
    destination = BigQueryDestination()
    with patch.dict("sys.modules", _mocked_bq_modules(client)):
        destination.load([{"id": 1}], config, options)
        client.delete_table.side_effect = RuntimeError("cleanup failed")  # finalize only
        with pytest.raises(GuardTripped):
            destination.finalize_sync(config, options)

    client = _fake_client()
    client.get_table.return_value.schema = [_schema_field("id")]
    _scripted(client, [2])
    client.delete_table.side_effect = RuntimeError("cleanup failed")
    destination = BigQueryDestination()
    with patch.dict("sys.modules", _mocked_bq_modules(client)), pytest.raises(GuardTripped):
        destination.finalize_sync(_config(upsert_key=["id"]), _diff_options({"max_deletes": 1}))


def test_shadow_cleanup_failure_never_masks_a_swap_refusal_but_surfaces_otherwise() -> None:
    client = _fake_client()
    _scripted(client, [5, 3])
    client.delete_table.side_effect = RuntimeError("cleanup failed")
    with pytest.raises(GuardTripped):
        _swap(client, {"max_deletes": 1}, _config(upsert_key=["id"]))


def test_staging_cleanup_failure_still_surfaces_when_nothing_tripped() -> None:
    client = _fake_client()
    _scripted(client, [1], affected=1)
    config = _config(mode="insert", upsert_key=["id"])
    options = _mirror({"max_deletes": 5})
    destination = BigQueryDestination()
    with patch.dict("sys.modules", _mocked_bq_modules(client)):
        destination.load([{"id": 1}], config, options)
        client.delete_table.side_effect = RuntimeError("cleanup failed")
        with pytest.raises(RuntimeError, match="cleanup failed"):
            destination.finalize_sync(config, options)
