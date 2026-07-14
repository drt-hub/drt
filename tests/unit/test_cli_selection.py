"""Tests for the shared selection resolver (#771).

Grammar: bare name (glob-capable), tag:<pattern>, destination:<type>,
"*"/"all"; repeated --select unions, --exclude subtracts, definition order
is preserved, and a select token matching nothing is an error.
"""

from __future__ import annotations

import pytest

from drt.cli._selection import (
    SelectionError,
    complete_selector,
    matches,
    select_syncs,
)
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


def test_star_and_all_sentinels(syncs: list[SyncConfig]) -> None:
    assert select_syncs(syncs, ["*"]) == syncs
    assert select_syncs(syncs, ["all"]) == syncs


def test_unknown_method_errors(syncs: list[SyncConfig]) -> None:
    with pytest.raises(SelectionError, match="Unknown selector method 'source:'"):
        select_syncs(syncs, ["source:bigquery"])


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


# ---------------------------------------------------------------------------
# matches() direct + completion
# ---------------------------------------------------------------------------


def test_matches_direct(syncs: list[SyncConfig]) -> None:
    assert matches(syncs[0], "users_to_hubspot")
    assert matches(syncs[0], "tag:crm")
    assert matches(syncs[0], "destination:hub*")
    assert not matches(syncs[2], "destination:hubspot")


def test_complete_selector_outside_project_returns_empty(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
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
    assert complete_selector("tag:") == ["tag:crm"]
