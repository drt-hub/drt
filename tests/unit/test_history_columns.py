"""Optional additive columns of the warehouse history tables (#1264) — shared helpers."""

from __future__ import annotations

import json
from typing import Any

import pytest

from drt.state._history_columns import (
    OPTIONAL_HISTORY_COLUMNS,
    column_list_sql,
    decode_optional,
    encode_optional,
    resolve_optional_columns,
)
from drt.state.history import HistoryEntry


def _entry(**kw: Any) -> HistoryEntry:
    return HistoryEntry(
        sync_name="s",
        started_at="t0",
        completed_at="t1",
        duration_seconds=1.0,
        status="success",
        records_synced=1,
        records_failed=0,
        **kw,
    )


def test_guards_is_the_first_optional_column_and_sql_fragment_follows_presence() -> None:
    assert OPTIONAL_HISTORY_COLUMNS[0] == "guards"
    assert column_list_sql(()) == ""
    assert column_list_sql(("guards",)) == ", guards"


def test_encode_is_json_text_or_null_in_present_order() -> None:
    guards = {"forced": True, "tripped": [{"guard": "max_deletes", "observed": 3}]}
    assert json.loads(encode_optional(_entry(guards=guards), ("guards",))[0] or "") == guards
    assert encode_optional(_entry(), ("guards",)) == [None]
    assert encode_optional(_entry(guards={"x": 1}), ()) == []


@pytest.mark.parametrize(
    ("cell", "expected"),
    [
        (None, {}),
        ("", {}),
        ('{"forced": true}', {"guards": {"forced": True}}),
        ({"forced": False}, {"guards": {"forced": False}}),  # a driver that already parsed JSON
    ],
)
def test_decode_handles_text_null_and_already_parsed_cells(cell: Any, expected: dict) -> None:
    assert decode_optional(("guards",), [cell]) == expected


def test_decode_ignores_non_json_text(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("WARNING"):
        assert decode_optional(("guards",), ["not json"]) == {}
    assert "non-JSON" in caplog.text


def test_a_read_never_adds_a_column() -> None:
    added: list[str] = []
    assert resolve_optional_columns(lambda: ["sync_name"], None, table="t") == ()
    assert resolve_optional_columns(lambda: ["GUARDS"], None, table="t") == ("guards",)
    assert added == []


def test_a_concurrent_add_that_wins_the_race_counts_as_present() -> None:
    calls = {"list": 0}

    def list_columns() -> list[str]:
        calls["list"] += 1
        return ["sync_name"] if calls["list"] == 1 else ["sync_name", "guards"]

    def add_column(name: str) -> None:
        raise RuntimeError("column already exists")  # another session added it first

    assert resolve_optional_columns(list_columns, add_column, table="t") == ("guards",)


def test_a_failed_add_and_a_failing_reprobe_degrade_to_legacy(
    caplog: pytest.LogCaptureFixture,
) -> None:
    calls = {"list": 0}

    def list_columns() -> list[str]:
        calls["list"] += 1
        if calls["list"] > 1:
            raise RuntimeError("probe broke")
        return ["sync_name"]

    def add_column(name: str) -> None:
        raise PermissionError("denied")

    with caplog.at_level("WARNING"):
        assert resolve_optional_columns(list_columns, add_column, table="t") == ()
    assert "could not be added" in caplog.text


def test_a_successful_add_is_reported_present_and_listing_failure_means_legacy(
    caplog: pytest.LogCaptureFixture,
) -> None:
    added: list[str] = []
    expected = ("guards", "plan_id", "approved_by")
    assert resolve_optional_columns(lambda: ["sync_name"], added.append, table="t") == expected
    assert added == list(expected)

    def broken() -> list[str]:
        raise RuntimeError("no information_schema access")

    with caplog.at_level("WARNING"):
        assert resolve_optional_columns(broken, added.append, table="t") == ()
    assert added == list(expected)  # nothing was altered when the probe itself failed
    assert "could not inspect columns" in caplog.text


def test_scalar_columns_are_plain_text_and_guards_stay_json() -> None:
    entry = HistoryEntry(
        sync_name="s",
        started_at="t0",
        completed_at="t1",
        duration_seconds=1.0,
        status="success",
        records_synced=1,
        records_failed=0,
        guards={"forced": True, "tripped": []},
        plan_id="sha256:abc",
        approved_by="alice",
    )
    present = ("guards", "plan_id", "approved_by")

    cells = encode_optional(entry, present)

    assert cells == ['{"forced": true, "tripped": []}', "sha256:abc", "alice"]
    assert decode_optional(present, cells) == {
        "guards": {"forced": True, "tripped": []},
        "plan_id": "sha256:abc",
        "approved_by": "alice",
    }
    # Plain text that merely looks like JSON stays a string for a scalar column.
    assert decode_optional(("approved_by",), ["123"]) == {"approved_by": "123"}
