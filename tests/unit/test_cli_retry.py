"""Tests for ``drt retry`` — Dead Letter Queue replay command (#278)."""

from __future__ import annotations

from collections.abc import Collection
from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

import drt.cli._helpers as helpers
from drt.cli.main import app
from drt.config.models import DestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError
from drt.state.audit_trail import AuditEntry
from drt.state.dlq import DeadLetter, DlqStore
from drt.state.factory import StateBundle

runner = CliRunner()


class _FakeLedger:
    """Minimal IdempotencyLedger fake — records what was checked/marked.

    Stateful like a real ledger: mark_delivered() actually updates the set
    already_delivered() checks against, so a second replay_dead_letters()
    call against the same fake sees an earlier call's marks (needed to
    prove #1127/#1128's fix is load-bearing, not just that the call happened)."""

    def __init__(self, already_delivered_keys: set[str] | None = None) -> None:
        self.already_delivered_keys = set(already_delivered_keys or set())
        self.checked: list[set[str]] = []
        self.marked: list[list[str]] = []

    def already_delivered(self, sync_name: str, keys: Collection[str]) -> set[str]:
        self.checked.append(set(keys))
        return set(keys) & self.already_delivered_keys

    def mark_delivered(self, sync_name: str, keys: Collection[str], delivered_at: str) -> None:
        self.marked.append(list(keys))
        self.already_delivered_keys.update(keys)

    def prune(self, sync_name: str, retention_days: int) -> int:
        return 0


class _FakeAuditTrail:
    """Minimal ComplianceAuditTrail fake — records every log_delivered() call."""

    def __init__(self) -> None:
        self.logged: list[tuple[str, str | None, str | None, str, list[AuditEntry]]] = []

    def log_delivered(
        self,
        sync_name: str,
        run_id: str | None,
        sync_run_id: str | None,
        destination_type: str,
        entries: list[AuditEntry],
        delivered_at: str,
    ) -> None:
        self.logged.append((sync_name, run_id, sync_run_id, destination_type, entries))

    def prune(self, sync_name: str, retain_days: int) -> int:
        return 0


class _FakeDestination:
    """Replays records; fails any whose ``id`` is in ``fail_ids`` (per batch)."""

    def __init__(self, fail_ids: set[int]) -> None:
        self.fail_ids = fail_ids
        self.calls: list[list[dict[str, Any]]] = []

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
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


class _FakeRaisingDestination:
    """Succeeds for the first ``succeed_calls`` chunks, then raises — the
    documented, uncaught ``Destination.load()`` contract (an unrecoverable
    batch-level failure). Used to prove #1127's fix: an earlier chunk's
    confirmed delivery must already be durable before a later chunk's
    exception propagates."""

    def __init__(self, succeed_calls: int) -> None:
        self.succeed_calls = succeed_calls
        self.calls = 0

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        self.calls += 1
        if self.calls > self.succeed_calls:
            raise RuntimeError("boom: connection lost")
        result = SyncResult()
        result.success = len(records)
        return result


class _FakeSkippingDestination:
    """Reports a ``match_policy``-style skip for ``skip_ids`` — no RowError,
    no batch_index, just a bare ``skipped``/``skipped_no_match`` counter
    bump (#757's own shape). Used to prove retry.py's ledger/audit marking
    fails closed on this exact case, matching ``successful_indices()``."""

    def __init__(self, skip_ids: set[int]) -> None:
        self.skip_ids = skip_ids

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        result = SyncResult()
        for rec in records:
            if rec.get("id") in self.skip_ids:
                result.skipped += 1
                result.skipped_no_match += 1
            else:
                result.success += 1
        return result


class _FakeFailAndSkipDestination:
    """One chunk, three outcomes: a RowError failure, a bare match_policy
    skip, and a genuine success — exercises the `confirmed_idx` guard inside
    retry.py's *partial*-failure branch (the full-success shortcut at
    ``result.failed == 0`` never runs when this chunk also has a failure)."""

    def __init__(self, fail_ids: set[int], skip_ids: set[int]) -> None:
        self.fail_ids = fail_ids
        self.skip_ids = skip_ids

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
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
            elif rec.get("id") in self.skip_ids:
                result.skipped += 1
                result.skipped_no_match += 1
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


class _FakeChunkLocalStagedDestination:
    """A protocol-conforming StagedDestination whose finalize() reports
    RowError.batch_index relative to the individual stage() chunk a failure
    came from, not the full accumulated retry set. StagedDestination's own
    Protocol (drt/destinations/base.py) never says which convention a
    finalize() result uses -- this is the plausible interpretation an
    unrelated third-party implementation could pick (it also matches
    Destination.load()'s own always-chunk-local batch_index), used to prove
    retry.py doesn't assume global indexing across multiple stage() calls.
    """

    def __init__(self, fail_ids: set[int]) -> None:
        self.fail_ids = fail_ids
        self.stage_calls: list[list[dict]] = []
        self._chunks: list[list[dict]] = []

    def stage(self, records, config, sync_options):  # type: ignore[no-untyped-def]
        self.stage_calls.append(records)
        self._chunks.append(records)

    def finalize(self, config, sync_options):  # type: ignore[no-untyped-def]
        result = SyncResult()
        for chunk in self._chunks:
            for i, rec in enumerate(chunk):
                if rec.get("id") in self.fail_ids:
                    result.failed += 1
                    result.row_errors.append(
                        RowError(
                            batch_index=i,  # local to this chunk, not global
                            record_preview=str(rec)[:200],
                            http_status=503,
                            error_message="chunk-local failure",
                        )
                    )
                else:
                    result.success += 1
        self._chunks = []
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


@pytest.fixture
def ledger_audit_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A project with idempotency + audit_trail enabled (#1118) — the
    ledger/audit_trail instances themselves are injected per-test via
    ``_patch_state_bundle``, not resolved from ``connection_profile`` (no
    real warehouse connection needed for these unit tests)."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "drt_project.yml").write_text(
        yaml.dump(
            {
                "name": "t",
                "version": "0.1",
                "profile": "default",
                "state": {
                    "backend": "warehouse",
                    "connection_profile": "pg_state",
                    "idempotency": True,
                    "audit_trail": {"enabled": True, "retain_days": 30, "fields": ["id"]},
                },
            }
        )
    )
    (tmp_path / "syncs").mkdir()
    (tmp_path / "syncs" / "post_users.yml").write_text(
        yaml.dump(
            {
                "name": "post_users",
                "model": "ref('users')",
                "destination": {"type": "rest_api", "url": "https://example.com"},
                "sync": {
                    "batch_size": 2,
                    "dlq": {"enabled": True},
                    "idempotency_key": "{{ row['id'] }}",
                },
            }
        )
    )
    return tmp_path


def _patch_state_bundle(
    monkeypatch: pytest.MonkeyPatch,
    dlq_store: DlqStore,
    ledger: _FakeLedger | None,
    audit_trail: _FakeAuditTrail | None,
) -> None:
    # state/history are never touched by replay_dead_letters() (retry only
    # needs .dlq/.ledger/.audit_trail) — a bare sentinel is enough.
    bundle = StateBundle(
        state=object(),
        history=object(),
        dlq=dlq_store,
        ledger=ledger,
        audit_trail=audit_trail,
    )
    monkeypatch.setattr("drt.state.factory.build_state_bundle", lambda project, project_dir: bundle)


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
    monkeypatch: pytest.MonkeyPatch,
    dest: (
        _FakeDestination
        | _FakeRaisingDestination
        | _FakeSkippingDestination
        | _FakeFailAndSkipDestination
        | _FakeStagedDestination
        | _FakeChunkLocalStagedDestination
        | _FakeSalesforceBulkDestination
    ),
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


def test_retry_staged_success_drains_queue(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _seed(project, [1, 2, 3])
    dest = _FakeStagedDestination(fail_ids=set())
    _patch_dest(monkeypatch, dest)

    result = runner.invoke(app, ["retry", "post_users"])

    assert result.exit_code == 0, result.output
    assert "3 succeeded, 0 still failing" in result.output
    assert store.depth("post_users") == 0
    assert [len(c) for c in dest.stage_calls] == [2, 1]
    assert dest.finalize_calls == 1


def test_retry_staged_limit_zero_stages_and_finalizes_nothing(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """--limit 0 against a staged destination leaves the DLQ untouched and
    never reaches stage()/finalize() — a queue can be non-empty overall
    while the requested retry slice is empty.
    """
    store = _seed(project, [1, 2, 3])
    dest = _FakeStagedDestination(fail_ids=set())
    _patch_dest(monkeypatch, dest)

    result = runner.invoke(app, ["retry", "post_users", "--limit", "0"])

    assert result.exit_code == 0, result.output
    assert "0 succeeded, 0 still failing" in result.output
    assert store.depth("post_users") == 3
    assert dest.stage_calls == []
    assert dest.finalize_calls == 0


def test_retry_staged_partial_failure_single_chunk_trusts_attribution(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A retry set small enough for one stage() call has no chunk-local vs.
    global ambiguity, so a fully-attributable finalize() result is trusted.
    """
    store = _seed(project, [1, 2])
    dest = _FakeStagedDestination(fail_ids={2})
    _patch_dest(monkeypatch, dest)

    result = runner.invoke(app, ["retry", "post_users"])

    assert result.exit_code == 0, result.output
    assert "1 succeeded, 1 still failing" in result.output
    remaining = store.read("post_users")
    assert [e.record["id"] for e in remaining] == [2]
    assert [len(c) for c in dest.stage_calls] == [2]
    assert dest.finalize_calls == 1


def test_retry_staged_partial_failure_multi_chunk_never_trusts_batch_index(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """StagedDestination's Protocol doesn't define whether RowError.batch_index
    from finalize() is local to the stage() chunk it came from or global
    across the whole accumulated set (caught in Codex review on #1037's PR).
    id=3 fails alone in the second, single-record chunk and this fake
    reports batch_index=0 relative to *that* chunk (chunk-local) rather than
    2 (the global position) -- without the multi-chunk conservative guard,
    the old "len(distinct indices) == failed" check would wrongly conclude
    only the record at global index 0 (id=1, which actually succeeded)
    failed, dropping the real failure (id=3) from the DLQ while re-queuing
    an already-succeeded record.
    """
    store = _seed(project, [1, 2, 3])
    dest = _FakeChunkLocalStagedDestination(fail_ids={3})
    _patch_dest(monkeypatch, dest)

    result = runner.invoke(app, ["retry", "post_users"])

    assert result.exit_code == 0, result.output
    assert "0 succeeded, 3 still failing" in result.output
    remaining = store.read("post_users")
    assert {e.record["id"] for e in remaining} == {1, 2, 3}
    assert [len(c) for c in dest.stage_calls] == [2, 1]


def test_retry_staged_finalize_exception_is_reported_and_keeps_queue(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _seed(project, [1, 2, 3])
    dest = _FakeStagedDestination(fail_ids=set(), finalize_error=RuntimeError("bulk job rejected"))
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
    record at index 0 failed and silently drop the other entry from the DLQ,
    even though which record actually failed is unknown.

    Deliberately only 2 entries against this fixture's batch_size=2 (a
    single stage() call) — the multi-chunk guard would otherwise force
    ``pinpointed = False`` on its own and this test wouldn't actually
    exercise the salesforce_bulk-specific check.
    """
    store = _seed(salesforce_project, [1, 2])
    dest = _FakeSalesforceBulkDestination(fail_ids={2})
    _patch_dest(monkeypatch, dest)

    result = runner.invoke(app, ["retry", "post_users"])

    assert result.exit_code == 0, result.output
    assert "0 succeeded, 2 still failing" in result.output
    remaining = store.read("post_users")
    assert {e.record["id"] for e in remaining} == {1, 2}
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
            store.append("post_users", [DeadLetter(record={"id": 99}, error_message="new failure")])
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


def _seed_ledger_audit(tmp_path: Path, ids: list[int]) -> DlqStore:
    store = DlqStore(tmp_path)
    store.append(
        "post_users",
        [DeadLetter(record={"id": i}, error_message="boom") for i in ids],
    )
    return store


def test_retry_marks_ledger_and_logs_audit_on_success(
    ledger_audit_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#1118: a successful retry marks the idempotency ledger and logs the
    compliance audit trail — the two gaps #1099/#1100 left open (retry
    bypasses run_sync() and its batch loop entirely)."""
    store = _seed_ledger_audit(ledger_audit_project, [1, 2])
    dest = _FakeDestination(fail_ids=set())
    _patch_dest(monkeypatch, dest)
    ledger = _FakeLedger()
    audit_trail = _FakeAuditTrail()
    _patch_state_bundle(monkeypatch, store, ledger, audit_trail)

    result = runner.invoke(app, ["retry", "post_users"])

    assert result.exit_code == 0, result.output
    assert "2 succeeded, 0 still failing" in result.output
    assert sorted(sum(ledger.marked, [])) == ["1", "2"]
    logged_keys = {e.record_key for _, _, _, _, entries in audit_trail.logged for e in entries}
    assert logged_keys == {"1", "2"}
    # Retry has no run identity of its own — passing the failing run's would
    # misattribute the delivery to a run that never actually sent it.
    for sync_name, run_id, sync_run_id, dest_type, _ in audit_trail.logged:
        assert (sync_name, run_id, sync_run_id, dest_type) == ("post_users", None, None, "rest_api")


def test_retry_persists_ledger_and_audit_for_earlier_chunk_before_a_later_chunk_raises(
    ledger_audit_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#1127/#1128: a later chunk's dest.load() raising (the documented,
    uncaught Destination.load() contract) must not lose an earlier chunk's
    already-confirmed delivery from the idempotency ledger / audit trail.
    DLQ removal itself is deliberately NOT persisted per chunk (a per-chunk
    reconcile() would make retry cost quadratic — caught in Codex review on
    #1128 — since DlqBackend has no cheaper mutation-only primitive), so
    both entries stay queued after the raise. The ledger mark is still the
    load-bearing half: it's what stops the next retry from resending id=1.
    batch_size: 1 forces id=1 and id=2 into separate chunks; the fake
    destination succeeds on the first load() call and raises on the
    second."""
    from drt.cli.commands.retry import replay_dead_letters
    from drt.config.parser import load_syncs

    (ledger_audit_project / "syncs" / "post_users.yml").write_text(
        yaml.dump(
            {
                "name": "post_users",
                "model": "ref('users')",
                "destination": {"type": "rest_api", "url": "https://example.com"},
                "sync": {
                    "batch_size": 1,
                    "dlq": {"enabled": True},
                    "idempotency_key": "{{ row['id'] }}",
                },
            }
        )
    )
    store = _seed_ledger_audit(ledger_audit_project, [1, 2])
    _patch_dest(monkeypatch, _FakeRaisingDestination(succeed_calls=1))
    ledger = _FakeLedger()
    audit_trail = _FakeAuditTrail()
    _patch_state_bundle(monkeypatch, store, ledger, audit_trail)
    sync = next(s for s in load_syncs(ledger_audit_project) if s.name == "post_users")

    with pytest.raises(RuntimeError, match="boom"):
        replay_dead_letters(sync, project_dir=ledger_audit_project)

    # Chunk 1 (id=1) was ledger-marked and audit-logged before chunk 2's
    # dest.load() raised ...
    assert sorted(sum(ledger.marked, [])) == ["1"]
    logged_keys = {e.record_key for _, _, _, _, entries in audit_trail.logged for e in entries}
    assert logged_keys == {"1"}
    # ... but DLQ removal is deferred to one reconcile() call at the very
    # end, which this raise never reached — both entries are still queued.
    assert [e.record["id"] for e in store.read("post_users")] == [1, 2]

    # The fix is load-bearing: prove it by retrying again against a fresh
    # destination that would happily resend id=1. The ledger mark from the
    # first (crashed) attempt must make this retry skip id=1 as a duplicate
    # and only actually send id=2.
    second_dest = _FakeDestination(fail_ids=set())
    _patch_dest(monkeypatch, second_dest)
    summary = replay_dead_letters(sync, project_dir=ledger_audit_project)

    assert summary["skipped_duplicate"] == 1
    assert summary["succeeded"] == 1
    assert [rec["id"] for call in second_dest.calls for rec in call] == [2]
    # id=1 is a duplicate hit, not a failure — it stays queued for an
    # operator to inspect (see the ledger-hit contract above), while id=2
    # is gone.
    assert [e.record["id"] for e in store.read("post_users")] == [1]


def test_retry_excludes_unattributed_skip_from_ledger_and_audit(
    ledger_audit_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Codex review finding on #1126: a match_policy-style skip (no RowError,
    no batch_index — #757's skipped_no_match) must never be marked delivered
    or audit-logged. Both ids land in the same retry_group (batch_size: 2),
    so successful_indices() (drt.state.idempotency) fails closed for the
    *whole* group — id=1's genuine delivery loses ledger/audit protection
    too, the same conservative trade-off run_sync() already makes. Both
    records still leave the DLQ (that's the pre-existing, unrelated "don't
    requeue a skip forever" behavior) — only the ledger/audit write is
    withheld. Mirrors the run_sync()-side regression already covered in
    test_engine_idempotency.py."""
    store = _seed_ledger_audit(ledger_audit_project, [1, 2])
    dest = _FakeSkippingDestination(skip_ids={2})
    _patch_dest(monkeypatch, dest)
    ledger = _FakeLedger()
    audit_trail = _FakeAuditTrail()
    _patch_state_bundle(monkeypatch, store, ledger, audit_trail)

    result = runner.invoke(app, ["retry", "post_users"])

    assert result.exit_code == 0, result.output
    # Both records leave the DLQ (neither is a re-queueable failure) ...
    assert "2 succeeded, 0 still failing" in result.output
    # ... but neither reaches the ledger or the audit log.
    assert ledger.marked == []
    assert audit_trail.logged == []


def test_retry_excludes_unattributed_skip_alongside_a_pinpointed_failure(
    ledger_audit_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same fail-closed contract as the all-succeed case above, but through
    the *partial*-failure branch: one chunk holds a genuine RowError failure
    (id=1), an unattributed skip (id=2), and a genuine success (id=3).
    Pinpointing still correctly identifies id=1 as the only failure — id=2
    and id=3 both leave the DLQ as before — but the skip means
    successful_indices() fails closed for the whole chunk, so id=3's
    otherwise-confirmable delivery is withheld from the ledger/audit log
    too, same trade-off as the all-succeed case."""
    (ledger_audit_project / "syncs" / "post_users.yml").write_text(
        yaml.dump(
            {
                "name": "post_users",
                "model": "ref('users')",
                "destination": {"type": "rest_api", "url": "https://example.com"},
                "sync": {
                    "batch_size": 3,
                    "dlq": {"enabled": True},
                    "idempotency_key": "{{ row['id'] }}",
                },
            }
        )
    )
    store = _seed_ledger_audit(ledger_audit_project, [1, 2, 3])
    dest = _FakeFailAndSkipDestination(fail_ids={1}, skip_ids={2})
    _patch_dest(monkeypatch, dest)
    ledger = _FakeLedger()
    audit_trail = _FakeAuditTrail()
    _patch_state_bundle(monkeypatch, store, ledger, audit_trail)

    result = runner.invoke(app, ["retry", "post_users"])

    assert result.exit_code == 0, result.output
    assert "2 succeeded, 1 still failing" in result.output
    # id=1 (the real failure) stays queued; id=2 and id=3 both leave the DLQ.
    remaining = store.read("post_users")
    assert [e.record["id"] for e in remaining] == [1]
    # But neither id=2 (a skip) nor id=3 (a genuine delivery) reaches the
    # ledger or audit log — the whole chunk fails closed on id=2's skip.
    assert ledger.marked == []
    assert audit_trail.logged == []


def test_retry_skips_already_delivered_and_leaves_it_queued(
    ledger_audit_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ledger hit means *some version* of this record was already
    delivered, not that this exact queued payload was — so it must not be
    silently deleted (the #955 blast radius). It's skipped this round and
    left queued instead."""
    store = _seed_ledger_audit(ledger_audit_project, [1, 2])
    dest = _FakeDestination(fail_ids=set())
    _patch_dest(monkeypatch, dest)
    ledger = _FakeLedger(already_delivered_keys={"1"})
    audit_trail = _FakeAuditTrail()
    _patch_state_bundle(monkeypatch, store, ledger, audit_trail)

    result = runner.invoke(app, ["retry", "post_users"])

    assert result.exit_code == 0, result.output
    assert "1 succeeded, 0 still failing" in result.output
    assert "1 record(s) skipped (already delivered) and left queued" in result.output
    # id=1 was never sent at all.
    assert [rec["id"] for call in dest.calls for rec in call] == [2]
    remaining = store.read("post_users")
    assert [e.record["id"] for e in remaining] == [1]
    # Only the genuinely-delivered id=2 gets marked/logged, never the skip.
    assert sorted(sum(ledger.marked, [])) == ["2"]
    logged_keys = {e.record_key for _, _, _, _, entries in audit_trail.logged for e in entries}
    assert logged_keys == {"2"}


def test_retry_staged_destination_excludes_ledger_and_audit(
    ledger_audit_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mirrors run_sync()'s own is_staged exclusion: a staged destination's
    success isn't known until finalize(), so neither feature applies —
    applying them where the engine itself refuses would be an asymmetry."""
    store = _seed_ledger_audit(ledger_audit_project, [1, 2])
    dest = _FakeStagedDestination(fail_ids=set())
    _patch_dest(monkeypatch, dest)
    ledger = _FakeLedger()
    audit_trail = _FakeAuditTrail()
    _patch_state_bundle(monkeypatch, store, ledger, audit_trail)

    result = runner.invoke(app, ["retry", "post_users"])

    assert result.exit_code == 0, result.output
    assert "2 succeeded, 0 still failing" in result.output
    assert store.depth("post_users") == 0
    assert ledger.checked == []
    assert ledger.marked == []
    assert audit_trail.logged == []


def test_retry_swap_mode_replace_excludes_ledger_and_audit(
    ledger_audit_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mirrors run_sync()'s own _swap_mode exclusion: mode: replace with
    replace_strategy: swap reports batch success against a shadow table
    before the real cutover, so neither feature applies here either — even
    though this destination isn't a StagedDestination."""
    (ledger_audit_project / "syncs" / "post_users.yml").write_text(
        yaml.dump(
            {
                "name": "post_users",
                "model": "ref('users')",
                "destination": {"type": "rest_api", "url": "https://example.com"},
                "sync": {
                    "batch_size": 2,
                    "dlq": {"enabled": True},
                    "idempotency_key": "{{ row['id'] }}",
                    "mode": "replace",
                    "replace_strategy": "swap",
                },
            }
        )
    )
    store = _seed_ledger_audit(ledger_audit_project, [1, 2])
    dest = _FakeDestination(fail_ids=set())
    _patch_dest(monkeypatch, dest)
    ledger = _FakeLedger()
    audit_trail = _FakeAuditTrail()
    _patch_state_bundle(monkeypatch, store, ledger, audit_trail)

    result = runner.invoke(app, ["retry", "post_users"])

    assert result.exit_code == 0, result.output
    assert "2 succeeded, 0 still failing" in result.output
    assert ledger.checked == []
    assert ledger.marked == []
    assert audit_trail.logged == []


def test_retry_without_ledger_or_audit_is_unaffected(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The default project fixture (state.backend: local, no idempotency/
    audit_trail) must behave exactly as before #1118 — no skipped_duplicate
    line, no crash from the new code paths being no-ops."""
    store = _seed(project, [1, 2])
    dest = _FakeDestination(fail_ids=set())
    _patch_dest(monkeypatch, dest)

    result = runner.invoke(app, ["retry", "post_users"])

    assert result.exit_code == 0, result.output
    assert "2 succeeded, 0 still failing" in result.output
    assert "skipped" not in result.output.lower()
    assert store.depth("post_users") == 0
