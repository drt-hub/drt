"""Tests for baseline comparison backing ``state:`` selectors (#772)."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from drt.cli._selection import SelectionError
from drt.cli._state_selection import StateDiff, load_state_diff
from drt.config.fingerprint import sync_fingerprints
from drt.config.models import SyncConfig
from drt.docs.manifest import SCHEMA_VERSION, Manifest, Sync

# Deliberately hardcoded rather than imported from _state_selection: this
# fixes the "config_hash exists as of schema v3" fact independently of both
# the module under test and the live SCHEMA_VERSION constant, so a bug that
# quietly re-coupled the version check to the moving SCHEMA_VERSION constant
# (rejecting a perfectly good baseline once schema v4 ships for something
# unrelated to config_hash) fails this test even if SCHEMA_VERSION itself
# has moved past 3 by the time this runs.
_SCHEMA_VERSION_WITH_CONFIG_HASH = 3


def _sync(name: str) -> SyncConfig:
    return SyncConfig.model_validate(
        {
            "name": name,
            "model": "ref('source_table')",
            "destination": {"type": "rest_api", "url": "https://example.com"},
            "sync": {"mode": "full"},
        }
    )


def _write_sync(project_dir: Path, name: str) -> None:
    syncs_dir = project_dir / "syncs"
    syncs_dir.mkdir(exist_ok=True)
    (syncs_dir / f"{name}.yml").write_text(
        f"name: {name}\n"
        "model: ref('source_table')\n"
        "destination: {type: rest_api, url: 'https://example.com'}\n"
        "sync: {mode: full}\n"
    )


def _manifest_sync(name: str, config_hash: str | None) -> Sync:
    return Sync(
        name=name,
        source="default",
        destination="rest_api",
        mode="full",
        config_hash=config_hash,
    )


def _write_manifest(
    path: Path, syncs: list[Sync] | None = None, *, schema_version: int = SCHEMA_VERSION
) -> None:
    manifest = Manifest(
        schema_version=schema_version,
        drt_version="0.8.4",
        syncs=syncs or [],
    )
    path.write_text(json.dumps(manifest.to_dict()))


def _assert_everything_new(diff: StateDiff, names: set[str]) -> None:
    expected = frozenset(names)
    assert diff.new == expected
    assert diff.modified == expected


def test_missing_baseline_warns_and_treats_everything_as_new(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    current = [_sync("users"), _sync("events")]

    with caplog.at_level(logging.WARNING, logger="drt.cli._state_selection"):
        diff = load_state_diff(tmp_path / "missing.json", current, tmp_path)

    _assert_everything_new(diff, {"users", "events"})
    assert "treating every current sync as new" in caplog.text


def test_unreadable_baseline_warns_and_treats_everything_as_new(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    baseline = tmp_path / "baseline.json"
    baseline.mkdir()

    with caplog.at_level(logging.WARNING, logger="drt.cli._state_selection"):
        diff = load_state_diff(baseline, [_sync("users")], tmp_path)

    _assert_everything_new(diff, {"users"})
    assert "Could not load baseline manifest" in caplog.text


@pytest.mark.parametrize("contents", ["{bad json", "[]"])
def test_unparseable_baseline_warns_and_treats_everything_as_new(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, contents: str
) -> None:
    baseline = tmp_path / "baseline.json"
    baseline.write_text(contents)

    with caplog.at_level(logging.WARNING, logger="drt.cli._state_selection"):
        diff = load_state_diff(baseline, [_sync("users")], tmp_path)

    _assert_everything_new(diff, {"users"})
    assert "Could not load baseline manifest" in caplog.text


@pytest.mark.parametrize("bad_version", ['"3"', "null"])
def test_malformed_schema_version_warns_and_treats_everything_as_new(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, bad_version: str
) -> None:
    """Manifest.from_dict() has no type validation, so a hand-edited or
    corrupted baseline can carry a non-numeric schema_version. That must not
    crash with a raw TypeError out of the < comparison."""
    baseline = tmp_path / "baseline.json"
    baseline.write_text(f'{{"schema_version": {bad_version}, "drt_version": "0.8.4", "syncs": []}}')

    with caplog.at_level(logging.WARNING, logger="drt.cli._state_selection"):
        diff = load_state_diff(baseline, [_sync("users")], tmp_path)

    _assert_everything_new(diff, {"users"})
    assert "malformed schema_version" in caplog.text


def test_old_schema_baseline_raises_clear_selection_error(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.json"
    _write_manifest(baseline, schema_version=2)

    with pytest.raises(SelectionError, match="schema version 2") as exc_info:
        load_state_diff(baseline, [_sync("users")], tmp_path)

    assert "predates config_hash" in str(exc_info.value)
    assert "regenerate" in str(exc_info.value)


def test_baseline_at_or_above_config_hash_schema_version_is_accepted(tmp_path: Path) -> None:
    """A baseline should stay valid even if drt's live SCHEMA_VERSION moves on.

    Regression guard for comparing against the *live* SCHEMA_VERSION constant
    instead of a fixed "config_hash exists as of this version" threshold — that
    would start rejecting today's perfectly comparable v3 baselines the moment
    schema v4 ships for anything unrelated to config_hash.
    """
    _write_sync(tmp_path, "users")
    current_hash = sync_fingerprints(tmp_path)["users"]

    for schema_version in (_SCHEMA_VERSION_WITH_CONFIG_HASH, _SCHEMA_VERSION_WITH_CONFIG_HASH + 5):
        baseline = tmp_path / f"baseline-{schema_version}.json"
        _write_manifest(
            baseline, [_manifest_sync("users", current_hash)], schema_version=schema_version
        )

        diff = load_state_diff(baseline, [_sync("users")], tmp_path)

        assert diff == StateDiff(new=frozenset(), modified=frozenset())


def test_new_sync_is_new_and_modified(tmp_path: Path) -> None:
    _write_sync(tmp_path, "users")
    baseline = tmp_path / "baseline.json"
    _write_manifest(baseline)

    diff = load_state_diff(baseline, [_sync("users")], tmp_path)

    assert diff.new == frozenset({"users"})
    assert diff.modified == frozenset({"users"})


def test_changed_hash_sync_is_modified_but_not_new(tmp_path: Path) -> None:
    _write_sync(tmp_path, "users")
    baseline = tmp_path / "baseline.json"
    _write_manifest(baseline, [_manifest_sync("users", "old-hash")])

    diff = load_state_diff(baseline, [_sync("users")], tmp_path)

    assert diff.new == frozenset()
    assert diff.modified == frozenset({"users"})


def test_unchanged_sync_is_in_neither_set(tmp_path: Path) -> None:
    _write_sync(tmp_path, "users")
    current_hash = sync_fingerprints(tmp_path)["users"]
    baseline = tmp_path / "baseline.json"
    _write_manifest(baseline, [_manifest_sync("users", current_hash)])

    diff = load_state_diff(baseline, [_sync("users")], tmp_path)

    assert diff == StateDiff(new=frozenset(), modified=frozenset())


def test_none_baseline_hash_is_modified_not_unchanged(tmp_path: Path) -> None:
    _write_sync(tmp_path, "users")
    baseline = tmp_path / "baseline.json"
    _write_manifest(baseline, [_manifest_sync("users", None)])

    diff = load_state_diff(baseline, [_sync("users")], tmp_path)

    assert diff.new == frozenset()
    assert diff.modified == frozenset({"users"})


def test_missing_current_hash_is_modified_not_unchanged(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.json"
    _write_manifest(baseline, [_manifest_sync("users", "known-baseline-hash")])

    diff = load_state_diff(baseline, [_sync("users")], tmp_path)

    assert diff.new == frozenset()
    assert diff.modified == frozenset({"users"})


def test_deleted_baseline_sync_is_ignored(tmp_path: Path) -> None:
    _write_sync(tmp_path, "users")
    current_hash = sync_fingerprints(tmp_path)["users"]
    baseline = tmp_path / "baseline.json"
    _write_manifest(
        baseline,
        [
            _manifest_sync("users", current_hash),
            _manifest_sync("deleted_sync", "old-hash"),
        ],
    )

    diff = load_state_diff(baseline, [_sync("users")], tmp_path)

    assert diff == StateDiff(new=frozenset(), modified=frozenset())
