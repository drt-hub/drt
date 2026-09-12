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
from drt.state.audit_trail import AuditEntry
from drt.state.dlq import DeadLetter
from drt.state.history import HistoryEntry
from drt.state.manager import SyncState
from drt.state.warehouse import (
    PostgresComplianceAuditTrail,
    PostgresWarehouseDlqBackend,
    PostgresWarehouseHistoryStore,
    PostgresWarehouseIdempotencyLedger,
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

    def test_append_batches_entries_into_one_multi_row_insert(self) -> None:
        """#1121: a small batch stays one round trip, not N."""
        conn = _mock_conn(fetchone=(3,))
        entries = [
            DeadLetter(record={"n": i}, error_message="boom", id=f"id-{i}") for i in range(3)
        ]
        with (
            patch("drt.state.warehouse._connect", return_value=conn),
            patch("drt.sources.postgres.PostgresSource.ensure_managed_schema"),
            patch("drt.sources.postgres.PostgresSource.managed_table_exists", return_value=True),
        ):
            PostgresWarehouseDlqBackend(_profile()).append("s", entries)

        insert_calls = [
            c
            for c in conn.cursor.return_value.execute.call_args_list
            if "INSERT INTO" in str(c.args[0])
        ]
        assert len(insert_calls) == 1
        sql, params = insert_calls[0].args
        assert str(sql).count("(%s, %s, %s, %s, %s, %s, %s, %s)") == 3
        assert "ON CONFLICT (id) DO UPDATE SET" in str(sql)
        assert len(params) == 3 * 8

    def test_append_chunks_many_entries_across_multiple_insert_statements(self) -> None:
        """250 rows fit per INSERT at 8 columns under the 2000-param budget
        (_rows_per_chunk) -- 600 entries need three chunks."""
        conn = _mock_conn(fetchone=(600,))
        entries = [
            DeadLetter(record={"n": i}, error_message="boom", id=f"id-{i}") for i in range(600)
        ]
        with (
            patch("drt.state.warehouse._connect", return_value=conn),
            patch("drt.sources.postgres.PostgresSource.ensure_managed_schema"),
            patch("drt.sources.postgres.PostgresSource.managed_table_exists", return_value=True),
        ):
            PostgresWarehouseDlqBackend(_profile()).append("s", entries)

        insert_calls = [
            c
            for c in conn.cursor.return_value.execute.call_args_list
            if "INSERT INTO" in str(c.args[0])
        ]
        assert len(insert_calls) == 3
        assert sum(len(c.args[1]) // 8 for c in insert_calls) == 600

    def test_append_dedupes_entries_sharing_the_same_id(self) -> None:
        """Two entries with the same id in one call would make Postgres
        raise "ON CONFLICT DO UPDATE command cannot affect row a second
        time" inside one multi-row statement -- dedup keeps the last one,
        matching the old per-entry loop's last-wins outcome."""
        conn = _mock_conn(fetchone=(1,))
        entries = [
            DeadLetter(record={"n": 1}, error_message="first", id="id-1"),
            DeadLetter(record={"n": 2}, error_message="second", id="id-1"),
        ]
        with (
            patch("drt.state.warehouse._connect", return_value=conn),
            patch("drt.sources.postgres.PostgresSource.ensure_managed_schema"),
            patch("drt.sources.postgres.PostgresSource.managed_table_exists", return_value=True),
        ):
            PostgresWarehouseDlqBackend(_profile()).append("s", entries)

        insert_calls = [
            c
            for c in conn.cursor.return_value.execute.call_args_list
            if "INSERT INTO" in str(c.args[0])
        ]
        assert len(insert_calls) == 1
        _, params = insert_calls[0].args
        assert len(params) == 8  # one row, not two
        assert params[3] == "second"  # error_message column, last entry wins

    def test_replace_chunks_many_entries_across_multiple_insert_statements(self) -> None:
        conn = _mock_conn(fetchall=[])
        entries = [
            DeadLetter(record={"n": i}, error_message="boom", id=f"id-{i}") for i in range(600)
        ]
        with (
            patch("drt.state.warehouse._connect", return_value=conn),
            patch("drt.sources.postgres.PostgresSource.ensure_managed_schema"),
            patch("drt.sources.postgres.PostgresSource.managed_table_exists", return_value=True),
        ):
            PostgresWarehouseDlqBackend(_profile()).replace("s", entries)

        insert_calls = [
            c
            for c in conn.cursor.return_value.execute.call_args_list
            if "INSERT INTO" in str(c.args[0])
        ]
        assert len(insert_calls) == 3
        assert sum(len(c.args[1]) // 8 for c in insert_calls) == 600
        assert not any(
            "ON CONFLICT" in str(c.args[0]) for c in conn.cursor.return_value.execute.call_args_list
        )

    def test_reconcile_chunks_many_updates_across_multiple_statements(self) -> None:
        """285 rows fit per UPDATE...FROM(VALUES...) at 7 columns under the
        2000-param budget (plus one shared sync_name param per statement) --
        600 updates need three chunks."""
        conn = _mock_conn(fetchall=[])
        updates = {
            f"id-{i}": DeadLetter(record={"n": i}, error_message="still failing", id=f"id-{i}")
            for i in range(600)
        }
        with (
            patch("drt.state.warehouse._connect", return_value=conn),
            patch("drt.sources.postgres.PostgresSource.ensure_managed_schema"),
            patch("drt.sources.postgres.PostgresSource.managed_table_exists", return_value=True),
        ):
            PostgresWarehouseDlqBackend(_profile()).reconcile("s", updates=updates)

        update_calls = [
            c for c in conn.cursor.return_value.execute.call_args_list if "UPDATE" in str(c.args[0])
        ]
        assert len(update_calls) == 3
        # Each chunk's params are 7 columns per row plus one trailing
        # sync_name — subtract that shared param before dividing by 7.
        assert sum((len(c.args[1]) - 1) // 7 for c in update_calls) == 600

    def test_reconcile_updates_a_single_entry_via_values(self) -> None:
        conn = _mock_conn(fetchall=[])
        updated = DeadLetter(record={"a": 2}, error_message="still failing", attempts=3, id="id-1")
        with (
            patch("drt.state.warehouse._connect", return_value=conn),
            patch("drt.sources.postgres.PostgresSource.ensure_managed_schema"),
            patch("drt.sources.postgres.PostgresSource.managed_table_exists", return_value=True),
        ):
            PostgresWarehouseDlqBackend(_profile()).reconcile("s", updates={"id-1": updated})

        update_call = next(
            c for c in conn.cursor.return_value.execute.call_args_list if "UPDATE" in str(c.args[0])
        )
        sql, params = update_call.args
        assert "FROM (VALUES" in str(sql)
        assert params[0] == "id-1"
        assert params[-1] == "s"  # sync_name, bound last

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


class TestPostgresWarehouseIdempotencyLedger:
    def test_already_delivered_returns_empty_before_any_table_exists(self) -> None:
        conn = _undefined_table_conn()
        with patch("drt.state.warehouse._connect", return_value=conn):
            result = PostgresWarehouseIdempotencyLedger(_profile()).already_delivered(
                "s", ["k1", "k2"]
            )
        assert result == set()

    def test_already_delivered_skips_query_for_empty_keys(self) -> None:
        conn = _mock_conn()
        with patch("drt.state.warehouse._connect", return_value=conn) as connect:
            result = PostgresWarehouseIdempotencyLedger(_profile()).already_delivered("s", [])
        assert result == set()
        connect.assert_not_called()

    def test_already_delivered_returns_matched_keys(self) -> None:
        conn = _mock_conn(fetchall=[("k1",), ("k2",)])
        with patch("drt.state.warehouse._connect", return_value=conn):
            result = PostgresWarehouseIdempotencyLedger(_profile()).already_delivered(
                "s", ["k1", "k2", "k3"]
            )
        assert result == {"k1", "k2"}
        executed = conn.cursor.return_value.execute.call_args
        assert executed.args[1] == ("s", ["k1", "k2", "k3"])

    def test_mark_delivered_ensures_schema_and_inserts_each_key(self) -> None:
        conn = _mock_conn()
        with (
            patch("drt.state.warehouse._connect", return_value=conn),
            patch("drt.sources.postgres.PostgresSource.ensure_managed_schema") as ensure_schema,
            patch(
                "drt.sources.postgres.PostgresSource.managed_table_exists", return_value=False
            ) as table_exists,
        ):
            PostgresWarehouseIdempotencyLedger(_profile()).mark_delivered(
                "s", ["k1", "k2"], "2026-09-06T00:00:00+00:00"
            )

        ensure_schema.assert_called_once()
        table_exists.assert_called_once()
        conn.commit.assert_called()
        executemany = conn.cursor.return_value.executemany.call_args
        assert executemany.args[1] == [
            ("s", "k1", "2026-09-06T00:00:00+00:00"),
            ("s", "k2", "2026-09-06T00:00:00+00:00"),
        ]
        assert "ON CONFLICT" in str(executemany.args[0])

    def test_mark_delivered_is_a_noop_for_empty_keys(self) -> None:
        conn = _mock_conn()
        with patch("drt.state.warehouse._connect", return_value=conn) as connect:
            PostgresWarehouseIdempotencyLedger(_profile()).mark_delivered("s", [], "t")
        connect.assert_not_called()

    def test_mark_delivered_swallows_connection_failure(self) -> None:
        """Best-effort, like HistoryStore.append — a ledger write failure
        must never propagate and fail an otherwise-successful sync."""
        with patch("drt.state.warehouse._connect", side_effect=RuntimeError("boom")):
            PostgresWarehouseIdempotencyLedger(_profile()).mark_delivered("s", ["k1"], "t")

    def test_prune_returns_zero_before_any_table_exists(self) -> None:
        conn = _undefined_table_conn()
        with patch("drt.state.warehouse._connect", return_value=conn):
            removed = PostgresWarehouseIdempotencyLedger(_profile()).prune("s", 7)
        assert removed == 0

    def test_prune_deletes_and_returns_rowcount(self) -> None:
        conn = _mock_conn(rowcount=3)
        with patch("drt.state.warehouse._connect", return_value=conn):
            removed = PostgresWarehouseIdempotencyLedger(_profile()).prune("s", 7)
        assert removed == 3
        conn.commit.assert_called()


class TestPostgresComplianceAuditTrail:
    def test_log_delivered_is_a_noop_for_empty_entries(self) -> None:
        conn = _mock_conn()
        with patch("drt.state.warehouse._connect", return_value=conn) as connect:
            PostgresComplianceAuditTrail(_profile()).log_delivered(
                "s", "r1", "sr1", "slack", [], "t"
            )
        connect.assert_not_called()

    def test_log_delivered_ensures_schema_and_inserts_each_entry(self) -> None:
        conn = _mock_conn()
        with (
            patch("drt.state.warehouse._connect", return_value=conn),
            patch("drt.sources.postgres.PostgresSource.ensure_managed_schema") as ensure_schema,
            patch(
                "drt.sources.postgres.PostgresSource.managed_table_exists", return_value=False
            ) as table_exists,
        ):
            PostgresComplianceAuditTrail(_profile()).log_delivered(
                "orders",
                "run-1",
                "sync-run-1",
                "slack",
                [
                    AuditEntry(record_key="a@example.com", fields={"email": "a@example.com"}),
                    AuditEntry(record_key="b@example.com", fields={"email": "b@example.com"}),
                ],
                "2026-09-07T00:00:00+00:00",
            )

        ensure_schema.assert_called_once()
        table_exists.assert_called_once()
        conn.commit.assert_called()
        executemany = conn.cursor.return_value.executemany.call_args
        assert executemany.args[1] == [
            (
                "orders",
                "run-1",
                "sync-run-1",
                "a@example.com",
                '{"email": "a@example.com"}',
                "slack",
                "2026-09-07T00:00:00+00:00",
            ),
            (
                "orders",
                "run-1",
                "sync-run-1",
                "b@example.com",
                '{"email": "b@example.com"}',
                "slack",
                "2026-09-07T00:00:00+00:00",
            ),
        ]

    def test_log_delivered_accepts_a_none_run_id_and_sync_run_id(self) -> None:
        """run_id is None for library callers; sync_run_id is typed
        optional to match SyncResult's own honest nullability (not
        asserted non-null) -- neither should raise or be coerced."""
        conn = _mock_conn()
        with (
            patch("drt.state.warehouse._connect", return_value=conn),
            patch("drt.sources.postgres.PostgresSource.ensure_managed_schema"),
            patch("drt.sources.postgres.PostgresSource.managed_table_exists", return_value=True),
        ):
            PostgresComplianceAuditTrail(_profile()).log_delivered(
                "orders",
                None,
                None,
                "slack",
                [AuditEntry(record_key="k", fields={})],
                "t",
            )

        executemany = conn.cursor.return_value.executemany.call_args
        assert executemany.args[1] == [("orders", None, None, "k", "{}", "slack", "t")]

    def test_log_delivered_swallows_connection_failure(self) -> None:
        """Best-effort, like HistoryStore.append — an audit write failure
        must never propagate and fail an otherwise-successful sync."""
        with patch("drt.state.warehouse._connect", side_effect=RuntimeError("boom")):
            PostgresComplianceAuditTrail(_profile()).log_delivered(
                "s", "r", "sr", "slack", [AuditEntry(record_key="k", fields={})], "t"
            )

    def test_prune_returns_zero_before_any_table_exists(self) -> None:
        conn = _undefined_table_conn()
        with patch("drt.state.warehouse._connect", return_value=conn):
            removed = PostgresComplianceAuditTrail(_profile()).prune("s", 30)
        assert removed == 0

    def test_prune_deletes_and_returns_rowcount(self) -> None:
        conn = _mock_conn(rowcount=5)
        with patch("drt.state.warehouse._connect", return_value=conn):
            removed = PostgresComplianceAuditTrail(_profile()).prune("s", 30)
        assert removed == 5
        conn.commit.assert_called()
