"""Tests for the shared selection resolver (#771).

Grammar: bare name (glob-capable), tag:<pattern>, destination:<type>,
state:modified/state:new, "*"/"all"; repeated --select unions, --exclude
subtracts, definition order is preserved, and a select token matching nothing
is an error.
"""

from __future__ import annotations

import pytest

from drt.cli._selection import (
    SelectionError,
    complete_selector,
    matches,
    select_syncs,
)
from drt.cli._state_selection import StateDiff
from drt.config.models import SyncConfig


def _sync(name: str, tags: list[str] | None = None, dest_type: str = "rest_api") -> SyncConfig:
    destination: dict = {"type": dest_type, "url": "https://example.com"}
    if dest_type == "hubspot":
        destination = {"type": "hubspot", "object_type": "contacts", "token_env": "T"}
    return SyncConfig.model_validate(
        {
            "name": name,
            "tags": tags or [],
            "model": "ref('t')",
            "destination": destination,
            "sync": {"mode": "full"},
        }
    )


@pytest.fixture
def syncs() -> list[SyncConfig]:
    return [
        _sync("users_to_hubspot", tags=["crm", "nightly"], dest_type="hubspot"),
        _sync("users_backfill", tags=["crm"], dest_type="hubspot"),
        _sync("events_to_rest", tags=["ads"]),
    ]


# ---------------------------------------------------------------------------
# token matching
# ---------------------------------------------------------------------------


def test_bare_name_exact(syncs: list[SyncConfig]) -> None:
    assert [s.name for s in select_syncs(syncs, ["events_to_rest"])] == ["events_to_rest"]


def test_bare_name_glob(syncs: list[SyncConfig]) -> None:
    assert [s.name for s in select_syncs(syncs, ["users_*"])] == [
        "users_to_hubspot",
        "users_backfill",
    ]


def test_tag_selector(syncs: list[SyncConfig]) -> None:
    assert [s.name for s in select_syncs(syncs, ["tag:ads"])] == ["events_to_rest"]


def test_tag_selector_glob(syncs: list[SyncConfig]) -> None:
    assert [s.name for s in select_syncs(syncs, ["tag:night*"])] == ["users_to_hubspot"]


def test_destination_selector(syncs: list[SyncConfig]) -> None:
    assert [s.name for s in select_syncs(syncs, ["destination:hubspot"])] == [
        "users_to_hubspot",
        "users_backfill",
    ]


def test_state_selectors(syncs: list[SyncConfig]) -> None:
    state_diff = StateDiff(
        new=frozenset({"events_to_rest"}),
        modified=frozenset({"users_backfill", "events_to_rest"}),
    )

    assert [
        s.name for s in select_syncs(syncs, ["state:new"], state_diff=state_diff)
    ] == ["events_to_rest"]
    assert [
        s.name for s in select_syncs(syncs, ["state:modified"], state_diff=state_diff)
    ] == ["users_backfill", "events_to_rest"]


def test_star_and_all_sentinels(syncs: list[SyncConfig]) -> None:
    assert select_syncs(syncs, ["*"]) == syncs
    assert select_syncs(syncs, ["all"]) == syncs


def test_unknown_method_errors(syncs: list[SyncConfig]) -> None:
    with pytest.raises(SelectionError, match="Unknown selector method 'source:'") as exc_info:
        select_syncs(syncs, ["source:bigquery"])
    assert "state:" in str(exc_info.value)


def test_unknown_state_selector_has_distinct_error(syncs: list[SyncConfig]) -> None:
    with pytest.raises(SelectionError, match="Unknown state selector 'state:unmodified'") as exc:
        select_syncs(syncs, ["state:unmodified"])
    assert "state:modified" in str(exc.value)
    assert "state:new" in str(exc.value)
    assert "Unknown selector method" not in str(exc.value)


@pytest.mark.parametrize("token", ["state:modified", "state:new"])
def test_state_selector_without_diff_requires_baseline(
    syncs: list[SyncConfig], token: str
) -> None:
    with pytest.raises(SelectionError, match="requires --state"):
        select_syncs(syncs, [token])


# ---------------------------------------------------------------------------
# union / exclude / ordering
# ---------------------------------------------------------------------------


def test_repeated_select_unions_and_dedupes(syncs: list[SyncConfig]) -> None:
    selected = select_syncs(syncs, ["tag:crm", "users_to_hubspot", "tag:ads"])
    assert [s.name for s in selected] == [
        "users_to_hubspot",
        "users_backfill",
        "events_to_rest",
    ]  # definition order, no duplicates


def test_exclude_subtracts(syncs: list[SyncConfig]) -> None:
    selected = select_syncs(syncs, ["tag:crm"], exclude=["users_backfill"])
    assert [s.name for s in selected] == ["users_to_hubspot"]


def test_exclude_without_select_applies_to_all(syncs: list[SyncConfig]) -> None:
    selected = select_syncs(syncs, None, exclude=["destination:hubspot"])
    assert [s.name for s in selected] == ["events_to_rest"]


def test_exclude_token_matching_nothing_is_fine(syncs: list[SyncConfig]) -> None:
    assert select_syncs(syncs, None, exclude=["nope_*"]) == syncs


def test_exclude_can_empty_the_selection(syncs: list[SyncConfig]) -> None:
    assert select_syncs(syncs, ["tag:ads"], exclude=["*"]) == []


def test_state_selector_is_forwarded_to_exclude(syncs: list[SyncConfig]) -> None:
    state_diff = StateDiff(
        new=frozenset(),
        modified=frozenset({"users_to_hubspot", "events_to_rest"}),
    )
    selected = select_syncs(syncs, None, exclude=["state:modified"], state_diff=state_diff)
    assert [s.name for s in selected] == ["users_backfill"]


# ---------------------------------------------------------------------------
# no-match errors (message compatibility with the pre-#771 CLI)
# ---------------------------------------------------------------------------


def test_no_match_bare_name_message(syncs: list[SyncConfig]) -> None:
    with pytest.raises(SelectionError, match="No sync named 'ghost' found."):
        select_syncs(syncs, ["ghost"])


def test_no_match_tag_message(syncs: list[SyncConfig]) -> None:
    with pytest.raises(SelectionError, match="No syncs with tag 'ghost' found."):
        select_syncs(syncs, ["tag:ghost"])


def test_no_match_glob_message(syncs: list[SyncConfig]) -> None:
    with pytest.raises(SelectionError, match="No syncs matching 'ghost_\\*' found."):
        select_syncs(syncs, ["ghost_*"])


def test_no_match_destination_message(syncs: list[SyncConfig]) -> None:
    with pytest.raises(SelectionError, match="No syncs with destination 'slack' found."):
        select_syncs(syncs, ["destination:slack"])


@pytest.mark.parametrize(
    ("token", "message"),
    [
        ("state:modified", "No modified syncs found relative to the baseline manifest."),
        ("state:new", "No new syncs found relative to the baseline manifest."),
    ],
)
def test_no_match_state_message(syncs: list[SyncConfig], token: str, message: str) -> None:
    empty = StateDiff(new=frozenset(), modified=frozenset())
    with pytest.raises(SelectionError, match=message):
        select_syncs(syncs, [token], state_diff=empty)


# ---------------------------------------------------------------------------
# matches() direct + completion
# ---------------------------------------------------------------------------


def test_matches_direct(syncs: list[SyncConfig]) -> None:
    assert matches(syncs[0], "users_to_hubspot")
    assert matches(syncs[0], "tag:crm")
    assert matches(syncs[0], "destination:hub*")
    assert not matches(syncs[2], "destination:hubspot")


def test_matches_state_direct(syncs: list[SyncConfig]) -> None:
    state_diff = StateDiff(
        new=frozenset({"events_to_rest"}),
        modified=frozenset({"users_to_hubspot", "events_to_rest"}),
    )
    assert matches(syncs[2], "state:new", state_diff=state_diff)
    assert not matches(syncs[0], "state:new", state_diff=state_diff)
    assert matches(syncs[0], "state:modified", state_diff=state_diff)


def test_complete_selector_without_syncs_still_lists_state_selectors(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    assert complete_selector("") == ["state:modified", "state:new"]


def test_complete_selector_load_failure_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(_path) -> None:
        raise ValueError("bad YAML")

    monkeypatch.setattr("drt.config.parser.load_syncs", fail)
    assert complete_selector("") == []


def test_complete_selector_lists_names_tags_destinations(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "syncs").mkdir()
    (tmp_path / "syncs" / "a.yml").write_text(
        "name: users_sync\n"
        "tags: [crm]\n"
        "model: ref('users')\n"
        "destination: {type: rest_api, url: 'https://x'}\n"
        "sync: {mode: full}\n"
    )
    monkeypatch.chdir(tmp_path)
    values = complete_selector("")
    assert "users_sync" in values
    assert "tag:crm" in values
    assert "destination:rest_api" in values
    assert "state:modified" in values
    assert "state:new" in values
    assert complete_selector("tag:") == ["tag:crm"]
    assert complete_selector("state:") == ["state:modified", "state:new"]
