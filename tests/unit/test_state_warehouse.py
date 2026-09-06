"""Unit tests for the Postgres-backed warehouse state/history/DLQ stores (#920).

Mock-based, like tests/unit/test_postgres_source.py's ManagedTableCapable
tests: proves dispatch, parameter shape, and the UndefinedTable fallback
path. Real round-trip behavior (upsert, prune, FIFO cap, reconcile) is
proven live against a real Postgres in
tests/integration/local_sql/test_warehouse_state_backend_smoke.py — a mock
cursor can't validate the SQL itself (#908's lesson).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("psycopg2")

import psycopg2

from drt.config.credentials import PostgresProfile
from drt.state.dlq import DeadLetter
from drt.state.history import HistoryEntry
from drt.state.manager import SyncState
from drt.state.warehouse import (
    PostgresWarehouseDlqBackend,
    PostgresWarehouseHistoryStore,
    PostgresWarehouseStateStore,
)


def _profile(**overrides: object) -> PostgresProfile:
    defaults: dict = {"type": "postgres", "host": "h", "dbname": "d", "user": "u"}
    defaults.update(overrides)
    return PostgresProfile(**defaults)


def _mock_conn(*, fetchone=None, fetchall=None, rowcount: int = 0) -> MagicMock:
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = fetchone
    cur.fetchall.return_value = fetchall or []
    cur.rowcount = rowcount
    conn.cursor.return_value = cur
    return conn


def _undefined_table_conn() -> MagicMock:
    conn = MagicMock()
    cur = MagicMock()
    cur.execute.side_effect = psycopg2.errors.UndefinedTable("relation does not exist")
    conn.cursor.return_value = cur
    return conn


class TestPostgresWarehouseStateStore:
    def test_get_last_sync_returns_none_before_any_table_exists(self) -> None:
        conn = _undefined_table_conn()
        with patch("drt.state.warehouse._connect", return_value=conn):
            assert PostgresWarehouseStateStore(_profile()).get_last_sync("s") is None

    def test_get_all_returns_empty_before_any_table_exists(self) -> None:
        conn = _undefined_table_conn()
        with patch("drt.state.warehouse._connect", return_value=conn):
            assert PostgresWarehouseStateStore(_profile()).get_all() == {}

    def test_reset_returns_false_before_any_table_exists(self) -> None:
        conn = _undefined_table_conn()
        with patch("drt.state.warehouse._connect", return_value=conn):
            assert PostgresWarehouseStateStore(_profile()).reset("s") is False

    def test_get_last_sync_reconstructs_sync_state(self) -> None:
        conn = _mock_conn(fetchone=("2026-09-06T00:00:00+00:00", 5, "success", None, "cursor-1"))
        with patch("drt.state.warehouse._connect", return_value=conn):
            state = PostgresWarehouseStateStore(_profile()).get_last_sync("my_sync")

        assert state == SyncState(
            sync_name="my_sync",
            last_run_at="2026-09-06T00:00:00+00:00",
            records_synced=5,
            status="success",
            error=None,
            last_cursor_value="cursor-1",
        )

    def test_save_sync_ensures_schema_and_upserts(self) -> None:
        conn = _mock_conn()
        with (
            patch("drt.state.warehouse._connect", return_value=conn),
            patch("drt.sources.postgres.PostgresSource.ensure_managed_schema") as ensure_schema,
            patch(
                "drt.sources.postgres.PostgresSource.managed_table_exists", return_value=False
            ) as table_exists,
        ):
            PostgresWarehouseStateStore(_profile()).save_sync(
                SyncState(
                    sync_name="s",
                    last_run_at="t",
                    records_synced=1,
                    status="success",
                )
            )

        ensure_schema.assert_called_once()
        table_exists.assert_called_once()
        conn.commit.assert_called()
        executed = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
        assert any("ON CONFLICT" in sql for sql in executed)

    def test_save_sync_skips_create_table_when_preprovisioned(self) -> None:
        """The escape hatch (#960/#695 discipline): a pre-provisioned table
        must never see the CREATE statement, even the IF NOT EXISTS form —
        caught missing in Codex review on this PR."""
        conn = _mock_conn()
        with (
            patch("drt.state.warehouse._connect", return_value=conn),
            patch("drt.sources.postgres.PostgresSource.ensure_managed_schema"),
            patch("drt.sources.postgres.PostgresSource.managed_table_exists", return_value=True),
        ):
            PostgresWarehouseStateStore(_profile()).save_sync(
                SyncState(sync_name="s", last_run_at="t", records_synced=1, status="success")
            )

        executed = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
        assert not any("CREATE TABLE" in sql for sql in executed)


class TestPostgresWarehouseHistoryStore:
    def test_read_returns_empty_before_any_table_exists(self) -> None:
        conn = _undefined_table_conn()
        with patch("drt.state.warehouse._connect", return_value=conn):
            assert PostgresWarehouseHistoryStore(_profile()).read() == []

    def test_prune_returns_zero_before_any_table_exists(self) -> None:
        conn = _undefined_table_conn()
        with patch("drt.state.warehouse._connect", return_value=conn):
            assert PostgresWarehouseHistoryStore(_profile()).prune("s", 30) == 0

    def test_append_is_best_effort_and_never_raises(self) -> None:
        with patch("drt.state.warehouse._connect", side_effect=RuntimeError("connection failed")):
            # Must not raise -- see the HistoryStore Protocol's append() contract.
            PostgresWarehouseHistoryStore(_profile()).append(
                HistoryEntry(
                    sync_name="s",
                    started_at="t0",
                    completed_at="t1",
                    duration_seconds=1.0,
                    status="success",
                    records_synced=1,
                    records_failed=0,
                )
            )

    def test_read_reconstructs_history_entry(self) -> None:
        row = ("s", "t0", "t1", 1.5, "success", 3, 0, [], None, False, "run-1", "sync-run-1")
        conn = _mock_conn(fetchall=[row])
        with patch("drt.state.warehouse._connect", return_value=conn):
            entries = PostgresWarehouseHistoryStore(_profile()).read("s")

        assert entries == [
            HistoryEntry(
                sync_name="s",
                started_at="t0",
                completed_at="t1",
                duration_seconds=1.5,
                status="success",
                records_synced=3,
                records_failed=0,
                errors=[],
                cursor_value_used=None,
                dry_run=False,
                run_id="run-1",
                sync_run_id="sync-run-1",
            )
        ]


class TestPostgresWarehouseDlqBackend:
    def test_read_returns_empty_before_any_table_exists(self) -> None:
        conn = _undefined_table_conn()
        with patch("drt.state.warehouse._connect", return_value=conn):
            assert PostgresWarehouseDlqBackend(_profile()).read("s") == []

    def test_depth_returns_zero_before_any_table_exists(self) -> None:
        conn = _undefined_table_conn()
        with patch("drt.state.warehouse._connect", return_value=conn):
            assert PostgresWarehouseDlqBackend(_profile()).depth("s") == 0

    def test_all_depths_returns_empty_before_any_table_exists(self) -> None:
        conn = _undefined_table_conn()
        with patch("drt.state.warehouse._connect", return_value=conn):
            assert PostgresWarehouseDlqBackend(_profile()).all_depths() == {}

    def test_all_depths_skips_zero_depth_entries(self) -> None:
        conn = _mock_conn(fetchall=[("a", 3), ("b", 0)])
        with patch("drt.state.warehouse._connect", return_value=conn):
            assert PostgresWarehouseDlqBackend(_profile()).all_depths() == {"a": 3}

    def test_append_with_empty_list_is_a_pure_depth_read(self) -> None:
        conn = _mock_conn(fetchone=(0,))
        with patch("drt.state.warehouse._connect", return_value=conn):
            assert PostgresWarehouseDlqBackend(_profile()).append("s", []) == 0
        conn.commit.assert_not_called()

    def test_read_reconstructs_dead_letter(self) -> None:
        row = ("id-1", {"a": 1}, "boom", 500, "t0", 2, "run-1")
        conn = _mock_conn(fetchall=[row])
        with patch("drt.state.warehouse._connect", return_value=conn):
            entries = PostgresWarehouseDlqBackend(_profile()).read("s")

        assert entries == [
            DeadLetter(
                record={"a": 1},
                error_message="boom",
                http_status=500,
                timestamp="t0",
                attempts=2,
                sync_run_id="run-1",
                id="id-1",
            )
        ]
