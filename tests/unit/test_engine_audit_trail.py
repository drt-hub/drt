"""Tests for the compliance audit trail's engine wiring (#1100).

Focus: field capture happens after `sync.mask`, not before (see
drt.state.audit_trail's module docstring) — logging the pre-mask value
would recreate the exact PII liability `sync.mask` exists to prevent, in a
second table, while also misrepresenting "what went where" (the
destination never received the pre-mask value). Also covers the one real
interaction with #1099: a record filtered out as `skipped_duplicate` by
the idempotency ledger must not get a new audit entry for a run that never
actually delivered it.
"""

from __future__ import annotations

from pathlib import Path

from drt.config.credentials import BigQueryProfile
from drt.config.models import DestinationConfig, SyncConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError
from drt.engine.sync import run_sync
from drt.state.audit_trail import AuditEntry

from .test_engine import FakeDestination, FakeSource, FakeStagedDestination
from .test_engine_idempotency import FakeLedger


class FakeAuditTrail:
    """In-memory ``ComplianceAuditTrail``, recording calls for assertions."""

    def __init__(self) -> None:
        self.logged: list[AuditEntry] = []
        self.calls: list[tuple[str, ...]] = []

    def log_delivered(
        self,
        sync_name: str,
        run_id: str | None,
        sync_run_id: str | None,
        destination_type: str,
        entries: list[AuditEntry],
        delivered_at: str,
    ) -> None:
        self.calls.append(("log_delivered", sync_name, run_id, sync_run_id, destination_type))
        self.logged.extend(entries)

    def prune(self, sync_name: str, retain_days: int) -> int:
        self.calls.append(("prune", sync_name, str(retain_days)))
        return 0


class PartialFailureDestination:
    """Fails the record at `fail_index` with a pinpointed RowError; every
    other record succeeds. Mirrors test_engine_idempotency.py's own fake."""

    def __init__(self, fail_index: int | None = None) -> None:
        self.fail_index = fail_index

    def load(
        self, records: list[dict], config: DestinationConfig, sync_options: SyncOptions
    ) -> SyncResult:
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


class RaisingDestination:
    def load(
        self, records: list[dict], config: DestinationConfig, sync_options: SyncOptions
    ) -> SyncResult:
        raise RuntimeError("destination unreachable")


def _make_profile() -> BigQueryProfile:
    return BigQueryProfile(type="bigquery", project="p", dataset="d")


def _make_sync(*, mask: dict[str, str] | None = None) -> SyncConfig:
    sync_opts: dict = {"batch_size": 10, "on_error": "skip"}
    if mask:
        sync_opts["mask"] = mask
    return SyncConfig.model_validate(
        {
            "name": "test_sync",
            "model": "ref('table')",
            "destination": {"type": "rest_api", "url": "https://example.com"},
            "sync": sync_opts,
        }
    )


def test_no_audit_trail_behaves_exactly_as_before(tmp_path: Path) -> None:
    dest = FakeDestination()
    result = run_sync(_make_sync(), FakeSource([{"id": 1}]), dest, _make_profile(), tmp_path)
    assert result.success == 1


def test_delivered_records_are_logged_with_configured_fields(tmp_path: Path) -> None:
    audit = FakeAuditTrail()
    dest = FakeDestination()

    run_sync(
        _make_sync(),
        FakeSource([{"id": 1, "email": "a@example.com"}]),
        dest,
        _make_profile(),
        tmp_path,
        audit_trail=audit,
        audit_fields=["email"],
    )

    assert len(audit.logged) == 1
    assert audit.logged[0] == AuditEntry(
        record_key="a@example.com", fields={"email": "a@example.com"}
    )


def test_fields_are_captured_after_mask_not_before(tmp_path: Path) -> None:
    """The core design correction: logging the pre-mask raw email would
    recreate the exact PII liability sync.mask exists to prevent, and
    misrepresent what was actually delivered (the destination only ever
    received the redacted value)."""
    audit = FakeAuditTrail()
    dest = FakeDestination()

    run_sync(
        _make_sync(mask={"email": "redact"}),
        FakeSource([{"id": 1, "email": "alice@example.com"}]),
        dest,
        _make_profile(),
        tmp_path,
        audit_trail=audit,
        audit_fields=["email"],
    )

    assert len(audit.logged) == 1
    assert audit.logged[0].fields["email"] == "[REDACTED]"
    assert "alice@example.com" not in audit.logged[0].record_key


def test_a_field_missing_from_this_record_is_simply_omitted(tmp_path: Path) -> None:
    """Different syncs have different schemas -- a configured field this
    sync's records never produce must not error or null-pad, just be
    absent from the logged JSON."""
    audit = FakeAuditTrail()
    dest = FakeDestination()

    run_sync(
        _make_sync(),
        FakeSource([{"id": 1}]),  # no "email" field at all
        dest,
        _make_profile(),
        tmp_path,
        audit_trail=audit,
        audit_fields=["email"],
    )

    assert len(audit.logged) == 1
    assert audit.logged[0] == AuditEntry(record_key="", fields={})


def test_record_key_joins_present_fields_in_configured_order(tmp_path: Path) -> None:
    audit = FakeAuditTrail()
    dest = FakeDestination()

    run_sync(
        _make_sync(),
        FakeSource([{"id": 1, "email": "a@example.com", "user_id": "u1"}]),
        dest,
        _make_profile(),
        tmp_path,
        audit_trail=audit,
        audit_fields=["email", "user_id"],
    )

    assert audit.logged[0].record_key == "a@example.com:u1"


def test_only_successfully_delivered_records_are_logged(tmp_path: Path) -> None:
    audit = FakeAuditTrail()
    dest = PartialFailureDestination(fail_index=0)

    run_sync(
        _make_sync(),
        FakeSource([{"id": 1, "email": "a@example.com"}, {"id": 2, "email": "b@example.com"}]),
        dest,
        _make_profile(),
        tmp_path,
        audit_trail=audit,
        audit_fields=["email"],
    )

    assert [e.record_key for e in audit.logged] == ["b@example.com"]


def test_log_delivered_never_called_when_load_raises(tmp_path: Path) -> None:
    audit = FakeAuditTrail()

    try:
        run_sync(
            _make_sync(),
            FakeSource([{"id": 1, "email": "a@example.com"}]),
            RaisingDestination(),
            _make_profile(),
            tmp_path,
            audit_trail=audit,
            audit_fields=["email"],
        )
    except RuntimeError:
        pass

    assert audit.logged == []


def test_duplicate_records_filtered_by_the_idempotency_ledger_are_not_audited(
    tmp_path: Path,
) -> None:
    """The one real interaction with #1099: a record #1099's ledger
    already recognizes as delivered on an earlier run never reaches
    destination.load() this run, so it must not get a new audit row either
    -- the audit filter reads the post-idempotency-filter batch, not a
    pre-filter copy."""
    ledger = FakeLedger(pre_delivered={"1"})
    audit = FakeAuditTrail()
    dest = FakeDestination()

    sync = _make_sync()
    sync.sync.idempotency_key = "{{ row.id }}"

    run_sync(
        sync,
        FakeSource([{"id": 1, "email": "a@example.com"}, {"id": 2, "email": "b@example.com"}]),
        dest,
        _make_profile(),
        tmp_path,
        idempotency_ledger=ledger,
        audit_trail=audit,
        audit_fields=["email"],
    )

    assert [e.record_key for e in audit.logged] == ["b@example.com"]


def test_staged_destination_ignores_the_audit_trail(tmp_path: Path) -> None:
    """Staged destinations are out of scope (their "success" is determined
    at finalize(), not per stage() call) -- same shape as #1099's own
    staged-destination no-op."""
    audit = FakeAuditTrail()

    run_sync(
        _make_sync(),
        FakeSource([{"id": 1, "email": "a@example.com"}]),
        FakeStagedDestination(fail=False),
        _make_profile(),
        tmp_path,
        audit_trail=audit,
        audit_fields=["email"],
    )

    assert audit.logged == []
    assert not any(call[0] == "log_delivered" for call in audit.calls)


def test_no_effect_during_dry_run(tmp_path: Path) -> None:
    audit = FakeAuditTrail()
    dest = FakeDestination()

    run_sync(
        _make_sync(),
        FakeSource([{"id": 1, "email": "a@example.com"}]),
        dest,
        _make_profile(),
        tmp_path,
        dry_run=True,
        audit_trail=audit,
        audit_fields=["email"],
    )

    assert audit.calls == []


def test_prune_called_once_after_a_real_run_with_configured_retention(tmp_path: Path) -> None:
    audit = FakeAuditTrail()
    dest = FakeDestination()

    run_sync(
        _make_sync(),
        FakeSource([{"id": 1, "email": "a@example.com"}]),
        dest,
        _make_profile(),
        tmp_path,
        audit_trail=audit,
        audit_fields=["email"],
        audit_retain_days=45,
    )

    prune_calls = [call for call in audit.calls if call[0] == "prune"]
    assert prune_calls == [("prune", "test_sync", "45")]


def test_prune_not_called_on_dry_run(tmp_path: Path) -> None:
    audit = FakeAuditTrail()
    dest = FakeDestination()

    run_sync(
        _make_sync(),
        FakeSource([{"id": 1, "email": "a@example.com"}]),
        dest,
        _make_profile(),
        tmp_path,
        dry_run=True,
        audit_trail=audit,
        audit_fields=["email"],
    )

    assert not any(call[0] == "prune" for call in audit.calls)


def test_run_id_and_sync_run_id_are_passed_through(tmp_path: Path) -> None:
    audit = FakeAuditTrail()
    dest = FakeDestination()

    result = run_sync(
        _make_sync(),
        FakeSource([{"id": 1, "email": "a@example.com"}]),
        dest,
        _make_profile(),
        tmp_path,
        audit_trail=audit,
        audit_fields=["email"],
        run_id="invocation-1",
    )

    log_call = next(c for c in audit.calls if c[0] == "log_delivered")
    assert log_call == (
        "log_delivered",
        "test_sync",
        "invocation-1",
        result.sync_run_id,
        "rest_api",
    )


def test_run_id_is_none_for_a_library_caller_that_never_set_one(tmp_path: Path) -> None:
    audit = FakeAuditTrail()
    dest = FakeDestination()

    run_sync(
        _make_sync(),
        FakeSource([{"id": 1, "email": "a@example.com"}]),
        dest,
        _make_profile(),
        tmp_path,
        audit_trail=audit,
        audit_fields=["email"],
    )

    log_call = next(c for c in audit.calls if c[0] == "log_delivered")
    assert log_call[2] is None  # run_id
