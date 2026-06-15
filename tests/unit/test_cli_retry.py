"""Tests for ``drt retry`` — Dead Letter Queue replay command (#278)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

import drt.cli._helpers as helpers
from drt.cli.main import app
from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError
from drt.state.dlq import DeadLetter, DlqStore

runner = CliRunner()


class _FakeDestination:
    """Replays records; fails any whose ``id`` is in ``fail_ids`` (per batch)."""

    def __init__(self, fail_ids: set[int]) -> None:
        self.fail_ids = fail_ids
        self.calls: list[list[dict]] = []

    def load(self, records, config, sync_options):  # type: ignore[no-untyped-def]
        self.calls.append(records)
        result = SyncResult()
        for i, rec in enumerate(records):
            if rec.get("id") in self.fail_ids:
                result.failed += 1
                result.row_errors.append(
                    RowError(
                        batch_index=i,
                        record_preview=str(rec)[:200],
                        http_status=503,
                        error_message="still failing",
                    )
                )
            else:
                result.success += 1
        return result


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "drt_project.yml").write_text(
        yaml.dump({"name": "t", "version": "0.1", "profile": "default"})
    )
    (tmp_path / "syncs").mkdir()
    (tmp_path / "syncs" / "post_users.yml").write_text(
        yaml.dump(
            {
                "name": "post_users",
                "model": "ref('users')",
                "destination": {"type": "rest_api", "url": "https://example.com"},
                "sync": {"batch_size": 2, "dlq": {"enabled": True}},
            }
        )
    )
    return tmp_path


def _seed(tmp_path: Path, ids: list[int]) -> DlqStore:
    store = DlqStore(tmp_path)
    store.append(
        "post_users",
        [DeadLetter(record={"id": i}, error_message="boom") for i in ids],
    )
    return store


def _patch_dest(monkeypatch: pytest.MonkeyPatch, dest: _FakeDestination) -> None:
    monkeypatch.setattr(helpers, "get_destination", lambda sync: dest)


def test_retry_empty_queue_is_friendly(project: Path) -> None:
    result = runner.invoke(app, ["retry", "post_users"])
    assert result.exit_code == 0
    assert "empty" in result.output.lower()


def test_retry_unknown_sync_exits_1(project: Path) -> None:
    result = runner.invoke(app, ["retry", "nope"])
    assert result.exit_code == 1
    assert "No sync named 'nope'" in result.output


def test_retry_all_success_drains_queue(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _seed(project, [1, 2, 3])
    dest = _FakeDestination(fail_ids=set())
    _patch_dest(monkeypatch, dest)

    result = runner.invoke(app, ["retry", "post_users"])

    assert result.exit_code == 0
    assert "3 succeeded, 0 still failing" in result.output
    assert store.depth("post_users") == 0
    # batch_size=2 → two load() calls (2 + 1).
    assert [len(c) for c in dest.calls] == [2, 1]


def test_retry_partial_keeps_failures_and_bumps_attempts(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _seed(project, [1, 2, 3])
    _patch_dest(monkeypatch, _FakeDestination(fail_ids={2}))

    result = runner.invoke(app, ["retry", "post_users"])

    assert result.exit_code == 0
    assert "2 succeeded, 1 still failing" in result.output
    remaining = store.read("post_users")
    assert [e.record["id"] for e in remaining] == [2]
    assert remaining[0].attempts == 2  # bumped from 1
    assert remaining[0].error_message == "still failing"


def test_retry_dry_run_sends_nothing(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _seed(project, [1, 2])
    dest = _FakeDestination(fail_ids=set())
    _patch_dest(monkeypatch, dest)

    result = runner.invoke(app, ["retry", "post_users", "--dry-run"])

    assert result.exit_code == 0
    assert "Would retry 2 of 2" in result.output
    assert dest.calls == []  # nothing sent
    assert store.depth("post_users") == 2  # queue untouched


def test_retry_dry_run_with_limit_notes_untouched(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _seed(project, [1, 2, 3])
    dest = _FakeDestination(fail_ids=set())
    _patch_dest(monkeypatch, dest)

    result = runner.invoke(app, ["retry", "post_users", "--dry-run", "--limit", "1"])

    assert result.exit_code == 0
    assert "Would retry 1 of 3" in result.output
    assert "2 record(s) left untouched" in result.output
    assert dest.calls == []
    assert store.depth("post_users") == 3  # queue untouched


def test_retry_clear_empties_without_sending(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _seed(project, [1, 2, 3])
    dest = _FakeDestination(fail_ids={1, 2, 3})
    _patch_dest(monkeypatch, dest)

    result = runner.invoke(app, ["retry", "post_users", "--clear"])

    assert result.exit_code == 0
    assert "Cleared 3 record(s)" in result.output
    assert dest.calls == []
    assert store.depth("post_users") == 0


def test_retry_limit_only_replays_oldest_n(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _seed(project, [1, 2, 3, 4])
    dest = _FakeDestination(fail_ids=set())
    _patch_dest(monkeypatch, dest)

    result = runner.invoke(app, ["retry", "post_users", "--limit", "2"])

    assert result.exit_code == 0
    # Oldest two replayed (and succeeded → dropped); newest two stay queued.
    remaining = store.read("post_users")
    assert [e.record["id"] for e in remaining] == [3, 4]
    assert [rec["id"] for call in dest.calls for rec in call] == [1, 2]


def test_retry_negative_limit_errors(project: Path) -> None:
    # A negative --limit used to silently clamp to 0 (a no-op); now it errors.
    result = runner.invoke(app, ["retry", "post_users", "--limit", "-1"])
    assert result.exit_code == 1
    assert "--limit must be >= 0" in result.output
