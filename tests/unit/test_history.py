"""Unit tests for sync execution history (#276)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from drt.state.history import HistoryEntry, HistoryManager


def _entry(
    sync_name: str = "demo",
    started_at: str | None = None,
    status: str = "success",
    records_synced: int = 10,
    records_failed: int = 0,
    errors: list[str] | None = None,
) -> HistoryEntry:
    if started_at is None:
        started_at = datetime.now(timezone.utc).isoformat()
    return HistoryEntry(
        sync_name=sync_name,
        started_at=started_at,
        completed_at=started_at,  # close enough for unit tests
        duration_seconds=1.5,
        status=status,
        records_synced=records_synced,
        records_failed=records_failed,
        errors=errors or [],
    )


# ---------------------------------------------------------------------------
# append + read
# ---------------------------------------------------------------------------


class TestHistoryAppendAndRead:
    def test_append_creates_file_with_one_jsonl_line(self, tmp_path: Path) -> None:
        mgr = HistoryManager(tmp_path)
        mgr.append(_entry(sync_name="alpha"))

        out = (tmp_path / ".drt" / "history" / "alpha.jsonl").read_text().splitlines()
        assert len(out) == 1
        data = json.loads(out[0])
        assert data["sync_name"] == "alpha"
        assert data["status"] == "success"

    def test_read_returns_newest_first(self, tmp_path: Path) -> None:
        mgr = HistoryManager(tmp_path)
        # Insert in chronological order; read should reverse them.
        mgr.append(_entry(sync_name="s", started_at="2026-05-01T10:00:00+00:00"))
        mgr.append(_entry(sync_name="s", started_at="2026-05-02T10:00:00+00:00"))
        mgr.append(_entry(sync_name="s", started_at="2026-05-03T10:00:00+00:00"))

        entries = mgr.read(sync_name="s")
        assert [e.started_at[:10] for e in entries] == [
            "2026-05-03",
            "2026-05-02",
            "2026-05-01",
        ]

    def test_read_limit_caps_results(self, tmp_path: Path) -> None:
        mgr = HistoryManager(tmp_path)
        for i in range(10):
            mgr.append(
                _entry(
                    sync_name="s",
                    started_at=f"2026-05-{i+1:02d}T00:00:00+00:00",
                )
            )
        assert len(mgr.read(sync_name="s", limit=3)) == 3

    def test_read_all_syncs_merges_and_sorts(self, tmp_path: Path) -> None:
        mgr = HistoryManager(tmp_path)
        mgr.append(_entry(sync_name="alpha", started_at="2026-05-01T00:00:00+00:00"))
        mgr.append(_entry(sync_name="beta", started_at="2026-05-02T00:00:00+00:00"))
        mgr.append(_entry(sync_name="alpha", started_at="2026-05-03T00:00:00+00:00"))

        entries = mgr.read(sync_name=None)
        # All three returned, newest first regardless of which sync.
        assert len(entries) == 3
        assert entries[0].sync_name == "alpha"
        assert entries[0].started_at.startswith("2026-05-03")
        assert entries[1].sync_name == "beta"
        assert entries[2].started_at.startswith("2026-05-01")

    def test_read_returns_empty_when_no_history_dir(self, tmp_path: Path) -> None:
        assert HistoryManager(tmp_path).read() == []

    def test_read_skips_malformed_lines(self, tmp_path: Path) -> None:
        mgr = HistoryManager(tmp_path)
        mgr.append(_entry(sync_name="s"))
        # Inject a corrupted line directly.
        path = tmp_path / ".drt" / "history" / "s.jsonl"
        path.write_text(path.read_text() + "this is not json\n")
        # Append another good entry on top.
        mgr.append(_entry(sync_name="s"))

        entries = mgr.read(sync_name="s")
        # Two valid entries, malformed line skipped (warning logged).
        assert len(entries) == 2

    def test_errors_are_truncated_to_max(self, tmp_path: Path) -> None:
        mgr = HistoryManager(tmp_path)
        many_errors = [f"err {i}" for i in range(20)]
        mgr.append(_entry(sync_name="s", errors=many_errors))
        entries = mgr.read(sync_name="s")
        assert len(entries[0].errors) == HistoryManager._MAX_ERRORS_PER_ENTRY


# ---------------------------------------------------------------------------
# prune
# ---------------------------------------------------------------------------


class TestHistoryPrune:
    def test_prune_removes_old_entries(self, tmp_path: Path) -> None:
        mgr = HistoryManager(tmp_path)
        now = datetime.now(timezone.utc)
        old = (now - timedelta(days=45)).isoformat()
        recent = (now - timedelta(days=2)).isoformat()
        mgr.append(_entry(sync_name="s", started_at=old))
        mgr.append(_entry(sync_name="s", started_at=recent))

        removed = mgr.prune("s", retention_days=30)
        assert removed == 1
        remaining = mgr.read(sync_name="s")
        assert len(remaining) == 1
        assert remaining[0].started_at == recent

    def test_prune_noop_when_all_recent(self, tmp_path: Path) -> None:
        mgr = HistoryManager(tmp_path)
        for i in range(3):
            mgr.append(
                _entry(
                    sync_name="s",
                    started_at=(datetime.now(timezone.utc) - timedelta(hours=i)).isoformat(),
                )
            )
        assert mgr.prune("s", retention_days=30) == 0

    def test_prune_noop_when_file_missing(self, tmp_path: Path) -> None:
        assert HistoryManager(tmp_path).prune("nonexistent", retention_days=30) == 0

    def test_prune_keeps_malformed_timestamp(self, tmp_path: Path) -> None:
        """Better to keep a row a human can inspect than silently drop it."""
        mgr = HistoryManager(tmp_path)
        path = tmp_path / ".drt" / "history" / "s.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        # Hand-write an entry with an unparseable started_at.
        bad = {
            "sync_name": "s",
            "started_at": "not-a-date",
            "completed_at": "not-a-date",
            "duration_seconds": 0.0,
            "status": "success",
            "records_synced": 1,
            "records_failed": 0,
            "errors": [],
            "cursor_value_used": None,
            "dry_run": False,
        }
        path.write_text(json.dumps(bad) + "\n")
        # Prune should not crash and should keep the entry.
        removed = mgr.prune("s", retention_days=1)
        assert removed == 0
        assert len(mgr.read(sync_name="s")) == 1


# ---------------------------------------------------------------------------
# Engine integration
# ---------------------------------------------------------------------------


class TestEngineHistoryIntegration:
    """run_sync writes a HistoryEntry on completion (and skips on dry-run)."""

    def _setup_sync(self):
        from collections.abc import Iterator

        from drt.config.credentials import BigQueryProfile
        from drt.config.models import DestinationConfig, SyncConfig, SyncOptions
        from drt.destinations.base import SyncResult

        class FakeSource:
            def extract(self, query: str, config: object) -> Iterator[dict]:
                yield from [{"id": 1}, {"id": 2}, {"id": 3}]

            def test_connection(self, config: object) -> bool:
                return True

        class FakeDestination:
            def load(
                self,
                records: list[dict],
                config: DestinationConfig,
                sync_options: SyncOptions,
            ) -> SyncResult:
                r = SyncResult()
                r.success = len(records)
                return r

        sync = SyncConfig.model_validate(
            {
                "name": "history_demo",
                "model": "ref('users')",
                "destination": {"type": "rest_api", "url": "https://example.com"},
                "sync": {"batch_size": 10, "on_error": "skip"},
            }
        )
        return FakeSource(), FakeDestination(), sync, BigQueryProfile(
            type="bigquery", project="p", dataset="d"
        )

    def test_history_entry_appended_after_successful_run(self, tmp_path: Path) -> None:
        from drt.engine.sync import run_sync

        source, dest, sync, profile = self._setup_sync()
        mgr = HistoryManager(tmp_path)
        run_sync(sync, source, dest, profile, tmp_path, history_manager=mgr)

        entries = mgr.read(sync_name="history_demo")
        assert len(entries) == 1
        e = entries[0]
        assert e.status == "success"
        assert e.records_synced == 3
        assert e.records_failed == 0
        assert e.duration_seconds >= 0

    def test_history_skipped_on_dry_run(self, tmp_path: Path) -> None:
        from drt.engine.sync import run_sync

        source, dest, sync, profile = self._setup_sync()
        mgr = HistoryManager(tmp_path)
        run_sync(sync, source, dest, profile, tmp_path, dry_run=True, history_manager=mgr)

        assert mgr.read(sync_name="history_demo") == []

    def test_history_records_failure_status_when_destination_raises(
        self, tmp_path: Path
    ) -> None:
        import pytest

        from drt.config.credentials import BigQueryProfile
        from drt.config.models import DestinationConfig, SyncConfig, SyncOptions
        from drt.destinations.base import SyncResult
        from drt.engine.sync import run_sync

        class FakeSource:
            def extract(self, query: str, config: object):
                yield {"id": 1}

            def test_connection(self, config: object) -> bool:
                return True

        class ExplodingDestination:
            def load(
                self,
                records: list[dict],
                config: DestinationConfig,
                sync_options: SyncOptions,
            ) -> SyncResult:
                raise RuntimeError("upstream down")

        sync = SyncConfig.model_validate(
            {
                "name": "explode_sync",
                "model": "ref('x')",
                "destination": {"type": "rest_api", "url": "https://example.com"},
                "sync": {"batch_size": 10, "on_error": "fail"},
            }
        )
        profile = BigQueryProfile(type="bigquery", project="p", dataset="d")
        mgr = HistoryManager(tmp_path)

        with pytest.raises(RuntimeError, match="upstream down"):
            run_sync(sync, FakeSource(), ExplodingDestination(), profile, tmp_path,
                     history_manager=mgr)

        entries = mgr.read(sync_name="explode_sync")
        assert len(entries) == 1
        e = entries[0]
        assert e.status == "failed"
        assert any("upstream down" in err for err in e.errors)
