"""Tests for the warehouse-backed idempotency ledger's engine wiring (#1099).

Focus: the check-then-send-then-mark contract is load-bearing, not
incidental (see drt.state.idempotency's module docstring) — a regression
back to claim-before-send would silently make every retry after a
transient destination failure drop that record forever. These tests prove
the ordering directly (mark_delivered is never called before a load, and
never for a row load() reported as failed), plus the key-template
resolution and its own bug class (a default that folds in run_id would
silently defeat cross-run dedup).
"""

from __future__ import annotations

from collections.abc import Collection
from pathlib import Path

from drt.config.credentials import BigQueryProfile
from drt.config.models import DestinationConfig, SyncConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError
from drt.engine.sync import run_sync

from .test_engine import FakeDestination, FakeSource, FakeStagedDestination


class FakeLedger:
    """In-memory ``IdempotencyLedger``, recording calls in order for assertions."""

    def __init__(self, pre_delivered: set[str] | None = None) -> None:
        self.delivered: set[str] = set(pre_delivered or set())
        self.calls: list[tuple[str, ...]] = []

    def already_delivered(self, sync_name: str, keys: Collection[str]) -> set[str]:
        self.calls.append(("already_delivered", sync_name, tuple(keys)))
        return {k for k in keys if k in self.delivered}

    def mark_delivered(self, sync_name: str, keys: Collection[str], delivered_at: str) -> None:
        self.calls.append(("mark_delivered", sync_name, tuple(keys)))
        self.delivered.update(keys)

    def prune(self, sync_name: str, retention_days: int) -> int:
        self.calls.append(("prune", sync_name, retention_days))
        return 0


class PartialFailureDestination:
    """Fails the record at `fail_index` within each batch with a pinpointed
    RowError; every other record succeeds. Mirrors test_engine.py's
    `_RowErrorDestination` — FakeDestination never populates row_errors."""

    def __init__(self, fail_index: int | None = None) -> None:
        self.fail_index = fail_index
        self.calls: list[list[dict]] = []

    def load(
        self,
        records: list[dict],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        self.calls.append(records)
        result = SyncResult()
        if self.fail_index is not None and 0 <= self.fail_index < len(records):
            result.success = len(records) - 1
            result.failed = 1
            result.row_errors = [
                RowError(
                    batch_index=self.fail_index,
                    record_preview="{}",
                    http_status=None,
                    error_message="boom",
                )
            ]
        else:
            result.success = len(records)
        return result


class SkippedNoMatchDestination:
    """Reports one row as `skipped_no_match` (#757's match_policy skip) with
    NO corresponding RowError — the exact shape a bare counter with no
    per-row index produces. Every other row succeeds."""

    def __init__(self, skip_index: int) -> None:
        self.skip_index = skip_index
        self.calls: list[list[dict]] = []

    def load(
        self, records: list[dict], config: DestinationConfig, sync_options: SyncOptions
    ) -> SyncResult:
        self.calls.append(records)
        result = SyncResult()
        result.success = len(records) - 1
        result.skipped = 1
        result.skipped_no_match = 1
        return result


class ReplaceCapableDestination:
    """Declares ModeCapable(replace) so `mode: replace, replace_strategy:
    swap` clears the engine's fail-fast mode check — every batch reports
    success, mirroring the real dialects' shadow-table write."""

    def supported_modes(self) -> frozenset[str]:
        return frozenset({"replace"})

    def load(
        self, records: list[dict], config: DestinationConfig, sync_options: SyncOptions
    ) -> SyncResult:
        return SyncResult(success=len(records))


class RaisingDestination:
    """Raises instead of returning a SyncResult — the whole-batch failure
    case where nothing was ever confirmed successful."""

    def load(
        self,
        records: list[dict],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        raise RuntimeError("destination unreachable")


def _make_profile() -> BigQueryProfile:
    return BigQueryProfile(type="bigquery", project="p", dataset="d")


def _make_sync(
    *, idempotency_key: str | None = None, upsert_key: list[str] | None = None
) -> SyncConfig:
    # rest_api has no upsert_key field at all (matches test_engine.py's own
    # _make_sync) — used for the "no upsert_key available" cases. ClickHouse
    # is the one dialect whose upsert_key is optional (informational only,
    # see ClickHouseDestinationConfig), so it doubles as "destination that
    # has an upsert_key" without also requiring `sync.mode: upsert`, which
    # Postgres's/MySQL's *required* upsert_key would otherwise force.
    if upsert_key:
        destination: dict = {
            "type": "clickhouse",
            "host": "h",
            "database": "d",
            "table": "t",
            "upsert_key": upsert_key,
        }
    else:
        destination = {"type": "rest_api", "url": "https://example.com"}
    sync_opts: dict = {"batch_size": 10, "on_error": "skip"}
    if idempotency_key:
        sync_opts["idempotency_key"] = idempotency_key
    return SyncConfig.model_validate(
        {
            "name": "test_sync",
            "model": "ref('table')",
            "destination": destination,
            "sync": sync_opts,
        }
    )


def test_no_ledger_behaves_exactly_as_before(tmp_path: Path) -> None:
    dest = FakeDestination()
    result = run_sync(
        _make_sync(idempotency_key="{{ row.id }}"),
        FakeSource([{"id": 1}, {"id": 2}]),
        dest,
        _make_profile(),
        tmp_path,
    )
    assert result.success == 2
    assert result.skipped_duplicate == 0
    assert len(dest.calls[0]) == 2


def test_already_delivered_records_are_filtered_before_load(tmp_path: Path) -> None:
    ledger = FakeLedger(pre_delivered={"1"})
    dest = FakeDestination()

    result = run_sync(
        _make_sync(idempotency_key="{{ row.id }}"),
        FakeSource([{"id": 1}, {"id": 2}]),
        dest,
        _make_profile(),
        tmp_path,
        idempotency_ledger=ledger,
    )

    assert result.skipped_duplicate == 1
    assert result.skipped == 1
    assert result.success == 1
    # Only the non-duplicate record ever reached the destination.
    assert dest.calls == [[{"id": 2}]]


def test_whole_batch_of_duplicates_never_reaches_the_destination(tmp_path: Path) -> None:
    """Every record in the batch already delivered -- the filtered
    record_batch is empty, so the batch loop must `continue` without
    calling destination.load() at all (not call it with an empty list)."""
    ledger = FakeLedger(pre_delivered={"1", "2"})
    dest = FakeDestination()

    result = run_sync(
        _make_sync(idempotency_key="{{ row.id }}"),
        FakeSource([{"id": 1}, {"id": 2}]),
        dest,
        _make_profile(),
        tmp_path,
        idempotency_ledger=ledger,
    )

    assert result.skipped_duplicate == 2
    assert result.success == 0
    assert dest.calls == []


def test_a_broken_key_template_disables_dedup_for_that_row_without_failing_it(
    tmp_path: Path,
) -> None:
    """`compute_idempotency_key` (drt.state.idempotency) is best-effort by
    design (see its docstring): a template referencing a column the row
    doesn't have must
    disable ledger protection for that one row, not drop it or fail the
    sync. render_value raises ValueError on a missing attribute (Jinja's
    StrictUndefined, per computed_fields' own docstring) -- proving the
    except-and-return-None path actually degrades gracefully rather than
    just existing in the source."""
    ledger = FakeLedger()
    dest = FakeDestination()

    result = run_sync(
        _make_sync(idempotency_key="{{ row.does_not_exist }}"),
        FakeSource([{"id": 1}]),
        dest,
        _make_profile(),
        tmp_path,
        idempotency_ledger=ledger,
    )

    assert result.success == 1
    assert result.skipped_duplicate == 0
    assert dest.calls == [[{"id": 1}]]
    # No key was ever computable, so the ledger never sees this record.
    assert not any(call[0] in ("already_delivered", "mark_delivered") for call in ledger.calls)


def test_mark_delivered_never_called_before_load(tmp_path: Path) -> None:
    """Regression for the claim-before-send design bug caught before
    implementation (see #1099's issue comment): already_delivered (a read)
    must be the only ledger call before destination.load(); mark_delivered
    must never appear ahead of it in the call order."""
    ledger = FakeLedger()
    dest = FakeDestination()

    run_sync(
        _make_sync(idempotency_key="{{ row.id }}"),
        FakeSource([{"id": 1}]),
        dest,
        _make_profile(),
        tmp_path,
        idempotency_ledger=ledger,
    )

    kinds = [call[0] for call in ledger.calls]
    assert kinds.index("already_delivered") < kinds.index("mark_delivered")


def test_mark_delivered_excludes_rows_load_reported_as_failed(tmp_path: Path) -> None:
    ledger = FakeLedger()
    dest = PartialFailureDestination(fail_index=0)

    run_sync(
        _make_sync(idempotency_key="{{ row.id }}"),
        FakeSource([{"id": 1}, {"id": 2}]),
        dest,
        _make_profile(),
        tmp_path,
        idempotency_ledger=ledger,
    )

    mark_calls = [call for call in ledger.calls if call[0] == "mark_delivered"]
    assert len(mark_calls) == 1
    assert mark_calls[0][2] == ("2",)  # id=1 failed and must not be marked


def test_mark_delivered_never_fires_for_a_batch_containing_an_unattributed_skip(
    tmp_path: Path,
) -> None:
    """Regression for a Codex-review finding on #1100 (shared via
    successful_indices() in drt.state.idempotency, so it applies here too): match_policy's
    skipped_no_match (#757) is a bare counter with no per-row batch_index,
    unlike a RowError. Treating "not in row_errors" as "therefore
    delivered" would wrongly mark the skipped row as delivered forever.
    The fix fails closed for the WHOLE batch when any unattributed skip is
    present, rather than guessing which index it was."""
    ledger = FakeLedger()
    dest = SkippedNoMatchDestination(skip_index=0)

    run_sync(
        _make_sync(idempotency_key="{{ row.id }}"),
        FakeSource([{"id": 1}, {"id": 2}]),
        dest,
        _make_profile(),
        tmp_path,
        idempotency_ledger=ledger,
    )

    assert not any(call[0] == "mark_delivered" for call in ledger.calls)


def test_swap_mode_replace_never_uses_the_ledger(tmp_path: Path) -> None:
    """`mode: replace, replace_strategy: swap` writes each batch to a
    shadow table (BaseSqlDestination._load_replace_swap); the real cutover
    only happens in a later, separate finalize_sync() rename. Marking
    delivery per-batch here would claim success for rows a failed rename
    never actually put in the real target table (Codex review on #1100,
    which shares this gate)."""
    ledger = FakeLedger()
    dest = ReplaceCapableDestination()
    sync = SyncConfig.model_validate(
        {
            "name": "test_sync",
            "model": "ref('table')",
            "destination": {
                "type": "postgres",
                "host": "h",
                "dbname": "d",
                "table": "t",
                "upsert_key": ["id"],
            },
            "sync": {"batch_size": 10, "mode": "replace", "replace_strategy": "swap"},
        }
    )

    run_sync(
        sync,
        FakeSource([{"id": 1}]),
        dest,
        _make_profile(),
        tmp_path,
        idempotency_ledger=ledger,
    )

    assert not any(call[0] in ("already_delivered", "mark_delivered") for call in ledger.calls)


def test_mark_delivered_never_called_when_load_raises(tmp_path: Path) -> None:
    """The exact scenario the check-then-mark fix exists for: a destination
    call that fails outright (not a per-row error) must leave the ledger
    untouched, so a retry can still send the record — claim-before-send
    would have already marked it "delivered" here, silently dropping every
    future retry."""
    ledger = FakeLedger()

    try:
        run_sync(
            _make_sync(idempotency_key="{{ row.id }}"),
            FakeSource([{"id": 1}]),
            RaisingDestination(),
            _make_profile(),
            tmp_path,
            idempotency_ledger=ledger,
        )
    except RuntimeError:
        pass

    assert not any(call[0] == "mark_delivered" for call in ledger.calls)


def test_explicit_idempotency_key_template_is_used(tmp_path: Path) -> None:
    ledger = FakeLedger()
    dest = FakeDestination()

    run_sync(
        _make_sync(idempotency_key="{{ row.email }}"),
        FakeSource([{"id": 1, "email": "a@example.com"}]),
        dest,
        _make_profile(),
        tmp_path,
        idempotency_ledger=ledger,
    )

    mark_calls = [call for call in ledger.calls if call[0] == "mark_delivered"]
    assert mark_calls[0][2] == ("a@example.com",)


def test_default_key_falls_back_to_destination_upsert_key(tmp_path: Path) -> None:
    ledger = FakeLedger()
    dest = FakeDestination()

    run_sync(
        _make_sync(upsert_key=["id"]),
        FakeSource([{"id": 42}]),
        dest,
        _make_profile(),
        tmp_path,
        idempotency_ledger=ledger,
    )

    mark_calls = [call for call in ledger.calls if call[0] == "mark_delivered"]
    assert mark_calls[0][2] == ("42",)


def test_default_key_ignores_run_id_across_two_runs(tmp_path: Path) -> None:
    """Regression for the second design bug caught before implementation:
    the same logical row sent by two separate `drt run` invocations (each
    with its own run_id) must resolve to the SAME ledger key, or a
    warehouse-persisted ledger could never recognize the exact cross-run
    duplicate it exists to catch."""
    ledger = FakeLedger()
    dest = FakeDestination()
    sync = _make_sync(upsert_key=["id"])

    run_sync(
        sync,
        FakeSource([{"id": 42}]),
        dest,
        _make_profile(),
        tmp_path,
        idempotency_ledger=ledger,
        run_id="run-1",
    )
    # Second, separate invocation of the same sync — same row, different run_id.
    result = run_sync(
        sync,
        FakeSource([{"id": 42}]),
        dest,
        _make_profile(),
        tmp_path,
        idempotency_ledger=ledger,
        run_id="run-2",
    )

    assert result.skipped_duplicate == 1


def test_no_resolvable_key_disables_ledger_without_error(tmp_path: Path) -> None:
    ledger = FakeLedger()
    dest = FakeDestination()

    result = run_sync(
        _make_sync(),  # no idempotency_key, no upsert_key
        FakeSource([{"id": 1}]),
        dest,
        _make_profile(),
        tmp_path,
        idempotency_ledger=ledger,
    )

    assert result.success == 1
    assert result.skipped_duplicate == 0
    # prune() still runs as sync-level maintenance regardless of whether
    # this particular run resolved a key — only the per-record read/write
    # calls are gated on a resolvable template.
    assert not any(call[0] in ("already_delivered", "mark_delivered") for call in ledger.calls)


def test_ledger_has_no_effect_during_dry_run(tmp_path: Path) -> None:
    ledger = FakeLedger(pre_delivered={"1"})
    dest = FakeDestination()

    result = run_sync(
        _make_sync(idempotency_key="{{ row.id }}"),
        FakeSource([{"id": 1}]),
        dest,
        _make_profile(),
        tmp_path,
        dry_run=True,
        idempotency_ledger=ledger,
    )

    assert result.skipped_duplicate == 0
    assert ledger.calls == []


def test_staged_destination_ignores_the_ledger(tmp_path: Path) -> None:
    ledger = FakeLedger()
    dest = FakeStagedDestination(fail=False)

    run_sync(
        _make_sync(idempotency_key="{{ row.id }}"),
        FakeSource([{"id": 1}]),
        dest,
        _make_profile(),
        tmp_path,
        idempotency_ledger=ledger,
    )

    assert not any(call[0] in ("already_delivered", "mark_delivered") for call in ledger.calls)


def test_prune_called_once_after_a_real_run(tmp_path: Path) -> None:
    ledger = FakeLedger()
    dest = FakeDestination()

    run_sync(
        _make_sync(idempotency_key="{{ row.id }}"),
        FakeSource([{"id": 1}]),
        dest,
        _make_profile(),
        tmp_path,
        idempotency_ledger=ledger,
    )

    prune_calls = [call for call in ledger.calls if call[0] == "prune"]
    assert len(prune_calls) == 1


def test_prune_not_called_on_dry_run(tmp_path: Path) -> None:
    ledger = FakeLedger()
    dest = FakeDestination()

    run_sync(
        _make_sync(idempotency_key="{{ row.id }}"),
        FakeSource([{"id": 1}]),
        dest,
        _make_profile(),
        tmp_path,
        dry_run=True,
        idempotency_ledger=ledger,
    )

    assert not any(call[0] == "prune" for call in ledger.calls)
