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


class _FakeStagedDestination:
    """Stages every chunk, then attributes configured failures globally."""

    def __init__(self, fail_ids: set[int], finalize_error: Exception | None = None) -> None:
        self.fail_ids = fail_ids
        self.finalize_error = finalize_error
        self.stage_calls: list[list[dict]] = []
        self.finalize_calls = 0
        self._records: list[dict] = []

    def stage(self, records, config, sync_options):  # type: ignore[no-untyped-def]
        self.stage_calls.append(records)
        self._records.extend(records)

    def finalize(self, config, sync_options):  # type: ignore[no-untyped-def]
        self.finalize_calls += 1
        if self.finalize_error is not None:
            raise self.finalize_error

        result = SyncResult()
        for i, rec in enumerate(self._records):
            if rec.get("id") in self.fail_ids:
                result.failed += 1
                result.row_errors.append(
                    RowError(
                        batch_index=i,
                        record_preview=str(rec)[:200],
                        http_status=503,
                        error_message="staged record still failing",
                    )
                )
            else:
                result.success += 1
        self._records.clear()
        return result


class _FakeSalesforceBulkDestination:
    """Mirrors SalesforceBulkDestination's real quirk: every RowError from a
    failed-results CSV row reports ``batch_index=0``, regardless of which
    accumulated record actually failed (drt/destinations/salesforce_bulk.py).
    """

    def __init__(self, fail_ids: set[int]) -> None:
        self.fail_ids = fail_ids
        self._records: list[dict] = []
        self.finalize_calls = 0

    def stage(self, records, config, sync_options):  # type: ignore[no-untyped-def]
        self._records.extend(records)

    def finalize(self, config, sync_options):  # type: ignore[no-untyped-def]
        self.finalize_calls += 1
        result = SyncResult()
        for rec in self._records:
            if rec.get("id") in self.fail_ids:
                result.failed += 1
                result.row_errors.append(
                    RowError(
                        batch_index=0,
                        record_preview=str(rec)[:200],
                        http_status=None,
                        error_message="sf__Error from failedResults",
                    )
                )
            else:
                result.success += 1
        self._records.clear()
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


@pytest.fixture
def salesforce_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
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
                "destination": {
                    "type": "salesforce_bulk",
                    "object_name": "Contact",
                    "instance_url_env": "SF_INSTANCE_URL",
                    "client_id_env": "SF_CLIENT_ID",
                    "client_secret_env": "SF_CLIENT_SECRET",
                    "username_env": "SF_USERNAME",
                    "password_env": "SF_PASSWORD",
                },
                "sync": {"batch_size": 2, "dlq": {"enabled": True}},
            }
        )
    )
    return tmp_path


def _patch_dest(
    monkeypatch: pytest.MonkeyPatch, dest: _FakeDestination | _FakeStagedDestination
) -> None:
    monkeypatch.setattr(helpers, "get_destination", lambda sync: dest)


def test_retry_works_without_drt_project_yml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A directory with syncs/ + .drt/dlq/ but no drt_project.yml must still
    work — retry only needs the destination (records replay verbatim), never
    the project's source/profile. Regression guard: an earlier factory
    refactor made the CLI command eagerly call load_project() unconditionally,
    which raises FileNotFoundError before replay_dead_letters()'s own
    already-correct "no project file -> local default" fallback ever runs."""
    monkeypatch.chdir(tmp_path)
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
    _seed(tmp_path, [1, 2])
    _patch_dest(monkeypatch, _FakeDestination(fail_ids=set()))

    result = runner.invoke(app, ["retry", "post_users"])

    assert result.exit_code == 0, result.output
    assert "2 succeeded, 0 still failing" in result.output


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


def test_retry_staged_success_drains_queue(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _seed(project, [1, 2, 3])
    dest = _FakeStagedDestination(fail_ids=set())
    _patch_dest(monkeypatch, dest)

    result = runner.invoke(app, ["retry", "post_users"])

    assert result.exit_code == 0, result.output
    assert "3 succeeded, 0 still failing" in result.output
    assert store.depth("post_users") == 0
    assert [len(c) for c in dest.stage_calls] == [2, 1]
    assert dest.finalize_calls == 1


def test_retry_staged_partial_failure_uses_global_batch_indices(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _seed(project, [1, 2, 3])
    dest = _FakeStagedDestination(fail_ids={3})
    _patch_dest(monkeypatch, dest)

    result = runner.invoke(app, ["retry", "post_users"])

    assert result.exit_code == 0, result.output
    assert "2 succeeded, 1 still failing" in result.output
    remaining = store.read("post_users")
    assert [e.record["id"] for e in remaining] == [3]
    assert remaining[0].attempts == 2
    assert remaining[0].error_message == "staged record still failing"
    # id=3 was staged in the second chunk, but finalize() reports index 2 in
    # the one accumulated record set rather than index 0 in that chunk.
    assert [len(c) for c in dest.stage_calls] == [2, 1]
    assert dest.finalize_calls == 1


def test_retry_staged_finalize_exception_is_reported_and_keeps_queue(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _seed(project, [1, 2, 3])
    dest = _FakeStagedDestination(
        fail_ids=set(), finalize_error=RuntimeError("bulk job rejected")
    )
    _patch_dest(monkeypatch, dest)

    result = runner.invoke(app, ["retry", "post_users"])

    assert result.exit_code == 1
    assert "Retry failed for 'post_users'" in result.output
    assert "job rejected" in result.output
    remaining = store.read("post_users")
    assert [e.record["id"] for e in remaining] == [1, 2, 3]
    assert [e.attempts for e in remaining] == [2, 2, 2]
    assert dest.finalize_calls == 1


def test_retry_salesforce_bulk_never_trusts_batch_index_attribution(
    salesforce_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Salesforce's failedResults CSV carries no original-position info, so
    its destination always reports RowError.batch_index=0 (verified against
    drt/destinations/salesforce_bulk.py). Naive attribution (failed count ==
    len(distinct indices) -> trust it) would wrongly conclude only the
    record at index 0 failed and silently drop the other two entries from
    the DLQ, even though which record actually failed is unknown.
    """
    store = _seed(salesforce_project, [1, 2, 3])
    dest = _FakeSalesforceBulkDestination(fail_ids={2})
    _patch_dest(monkeypatch, dest)

    result = runner.invoke(app, ["retry", "post_users"])

    assert result.exit_code == 0, result.output
    assert "0 succeeded, 3 still failing" in result.output
    remaining = store.read("post_users")
    assert {e.record["id"] for e in remaining} == {1, 2, 3}
    assert dest.finalize_calls == 1


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


def test_retry_survives_concurrent_append(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The literal #955 scenario: a concurrent ``drt run`` appends a new dead
    letter to the same sync's DLQ while ``drt retry`` is mid-flight — after
    ``replay_dead_letters()`` has already read the queue but before it
    writes back. Before the ``reconcile()`` fix, that append was silently
    lost: ``replace()`` overwrote the whole queue with content computed from
    ``drt retry``'s stale read, which never saw the new entry.
    """
    store = _seed(project, [1, 2])

    class _ConcurrentAppendDestination:
        def load(self, records, config, sync_options):  # type: ignore[no-untyped-def]
            # Simulates another process's `drt run` appending a fresh dead
            # letter mid-retry — after replay_dead_letters()'s own read at
            # the top of the function, before its write-back at the end.
            store.append(
                "post_users", [DeadLetter(record={"id": 99}, error_message="new failure")]
            )
            result = SyncResult()
            result.success = len(records)
            return result

    _patch_dest(monkeypatch, _ConcurrentAppendDestination())  # type: ignore[arg-type]

    result = runner.invoke(app, ["retry", "post_users"])

    assert result.exit_code == 0
    # The two replayed records succeeded and were removed by id; the
    # concurrently-appended one was never named in the reconcile call, so it
    # survives untouched.
    remaining = store.read("post_users")
    assert [e.record["id"] for e in remaining] == [99]
