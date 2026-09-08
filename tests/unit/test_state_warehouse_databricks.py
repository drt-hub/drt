"""Unit tests for the Databricks-backed warehouse state/history/DLQ stores (#1108).

Mock-based, like tests/unit/test_state_warehouse_snowflake.py's Snowflake
counterpart: proves dispatch, parameter shape, the probe-then-UPDATE-or-INSERT
translation for single-row upserts, and `replace()`'s scratch-table-free
stale-id-delete + chunked VALUES-MERGE design (see warehouse_databricks.py's
module docstring for the full history — an earlier scratch-table draft was
caught by Codex review breaking the documented escape hatch and getting
Databricks' MERGE clause order wrong before this shape was settled on). Real
round-trip behavior is proven live in
tests/integration/dwh/test_databricks_warehouse_state_smoke.py — a mock
cursor can't validate the SQL itself (#908's lesson).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from drt.config.credentials import DatabricksProfile
from drt.state.dlq import DeadLetter
from drt.state.history import HistoryEntry
from drt.state.manager import SyncState
from drt.state.warehouse_databricks import (
    DatabricksWarehouseDlqBackend,
    DatabricksWarehouseHistoryStore,
    DatabricksWarehouseStateStore,
)


def _profile(**overrides: object) -> DatabricksProfile:
    defaults: dict = {
        "type": "databricks",
        "server_hostname": "dbc-xxx.cloud.databricks.com",
        "http_path": "/sql/1.0/warehouses/abc",
        "catalog": "main",
    }
    defaults.update(overrides)
    return DatabricksProfile(**defaults)


def _mock_conn(*, fetchone=None, fetchall=None) -> MagicMock:
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = fetchone
    cur.fetchall.return_value = fetchall or []
    conn.cursor.return_value = cur
    return conn


def _sequenced_conn(*, fetchone_sequence=None, fetchall=None) -> MagicMock:
    """A mock connection whose cursor's fetchone() returns a different value
    on each call -- needed for tests where one method call issues more than
    one probe (e.g. append()'s per-row existence check followed by depth()'s
    final COUNT)."""
    conn = MagicMock()
    cur = MagicMock()
    if fetchone_sequence is not None:
        cur.fetchone.side_effect = fetchone_sequence
    cur.fetchall.return_value = fetchall or []
    conn.cursor.return_value = cur
    return conn


class TestEnsureTableExists:
    """The concurrent-first-use race guard shared by all three stores'
    _ensure_table() -- same shape as DatabricksSource.ensure_managed_schema()
    (#1108), tested directly rather than through one store's _ensure_table
    wrapper since the logic is common to all three."""

    def test_swallows_the_concurrent_create_race(self) -> None:
        from drt.state.warehouse_databricks import _ensure_table_exists

        conn = MagicMock()
        conn.cursor.return_value.execute.side_effect = Exception("concurrent create race")
        with (
            patch("drt.sources.databricks.DatabricksSource.ensure_managed_schema"),
            patch(
                "drt.sources.databricks.DatabricksSource.managed_table_exists",
                side_effect=[False, True],
            ),
        ):
            _ensure_table_exists(conn, _profile(), "_drt_runs", "id STRING")  # must not raise

    def test_reraises_when_still_absent_after_create_fails(self) -> None:
        from drt.state.warehouse_databricks import _ensure_table_exists

        conn = MagicMock()
        conn.cursor.return_value.execute.side_effect = Exception("insufficient privileges")
        with (
            patch("drt.sources.databricks.DatabricksSource.ensure_managed_schema"),
            patch(
                "drt.sources.databricks.DatabricksSource.managed_table_exists",
                side_effect=[False, False],
            ),
        ):
            try:
                _ensure_table_exists(conn, _profile(), "_drt_runs", "id STRING")
                raise AssertionError("expected the exception to propagate")
            except Exception as e:
                assert "insufficient privileges" in str(e)


class TestDatabricksWarehouseStateStore:
    def test_get_last_sync_returns_none_before_any_table_exists(self) -> None:
        with patch("drt.state.warehouse_databricks._table_exists", return_value=False):
            assert DatabricksWarehouseStateStore(_profile()).get_last_sync("s") is None

    def test_get_all_returns_empty_before_any_table_exists(self) -> None:
        with patch("drt.state.warehouse_databricks._table_exists", return_value=False):
            assert DatabricksWarehouseStateStore(_profile()).get_all() == {}

    def test_get_all_reconstructs_every_sync(self) -> None:
        rows = [
            ("a", "t0", 1, "success", None, None),
            ("b", "t1", 2, "failed", "boom", "cur-2"),
        ]
        conn = _mock_conn(fetchall=rows)
        with (
            patch("drt.state.warehouse_databricks._table_exists", return_value=True),
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
        ):
            result = DatabricksWarehouseStateStore(_profile()).get_all()

        assert set(result) == {"a", "b"}
        assert result["b"].status == "failed"
        assert result["b"].error == "boom"

    def test_reset_returns_false_before_any_table_exists(self) -> None:
        with patch("drt.state.warehouse_databricks._table_exists", return_value=False):
            assert DatabricksWarehouseStateStore(_profile()).reset("s") is False

    def test_reset_deletes_and_returns_true_when_a_row_existed(self) -> None:
        """Existence is probed before DELETE rather than trusting
        cursor.rowcount afterward -- see module docstring for why."""
        conn = _mock_conn(fetchone=(1,))
        with (
            patch("drt.state.warehouse_databricks._table_exists", return_value=True),
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
        ):
            assert DatabricksWarehouseStateStore(_profile()).reset("s") is True
        executed = [str(c.args[0]) for c in conn.cursor.return_value.execute.call_args_list]
        assert any(sql.startswith("SELECT 1 FROM") for sql in executed)
        assert any(sql.startswith("DELETE FROM") for sql in executed)

    def test_reset_returns_false_when_row_absent_even_though_table_exists(self) -> None:
        conn = _mock_conn(fetchone=None)
        with (
            patch("drt.state.warehouse_databricks._table_exists", return_value=True),
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
        ):
            assert DatabricksWarehouseStateStore(_profile()).reset("s") is False
        # DELETE still runs unconditionally -- idempotent no-op when absent.
        executed = [str(c.args[0]) for c in conn.cursor.return_value.execute.call_args_list]
        assert any(sql.startswith("DELETE FROM") for sql in executed)

    def test_now_returns_an_iso_timestamp(self) -> None:
        from datetime import datetime

        datetime.fromisoformat(DatabricksWarehouseStateStore(_profile()).now())

    def test_get_last_sync_reconstructs_sync_state(self) -> None:
        conn = _mock_conn(fetchone=("2026-09-06T00:00:00+00:00", 5, "success", None, "cursor-1"))
        with (
            patch("drt.state.warehouse_databricks._table_exists", return_value=True),
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
        ):
            state = DatabricksWarehouseStateStore(_profile()).get_last_sync("my_sync")

        assert state == SyncState(
            sync_name="my_sync",
            last_run_at="2026-09-06T00:00:00+00:00",
            records_synced=5,
            status="success",
            error=None,
            last_cursor_value="cursor-1",
        )

    def test_get_last_sync_returns_none_when_table_exists_but_sync_never_ran(self) -> None:
        conn = _mock_conn(fetchone=None)
        with (
            patch("drt.state.warehouse_databricks._table_exists", return_value=True),
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
        ):
            assert DatabricksWarehouseStateStore(_profile()).get_last_sync("never_run") is None

    def test_save_sync_ensures_schema_and_inserts_when_absent(self) -> None:
        conn = _mock_conn(fetchone=None)  # row probe: absent
        with (
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
            patch("drt.sources.databricks.DatabricksSource.ensure_managed_schema") as ensure_schema,
            patch(
                "drt.sources.databricks.DatabricksSource.managed_table_exists", return_value=False
            ) as table_exists,
        ):
            DatabricksWarehouseStateStore(_profile()).save_sync(
                SyncState(sync_name="s", last_run_at="t", records_synced=1, status="success")
            )

        ensure_schema.assert_called_once()
        table_exists.assert_called_once()
        executed = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
        assert any(sql.startswith("SELECT 1 FROM") for sql in executed)
        assert any(
            sql.startswith("INSERT INTO") and "VALUES (?, ?, ?, ?, ?, ?)" in sql for sql in executed
        )
        assert not any(sql.startswith("UPDATE") for sql in executed)
        assert not any("MERGE" in sql or "CREATE OR REPLACE" in sql for sql in executed)

    def test_save_sync_updates_when_present(self) -> None:
        conn = _mock_conn(fetchone=(1,))  # row probe: exists
        with (
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
            patch("drt.sources.databricks.DatabricksSource.ensure_managed_schema"),
            patch(
                "drt.sources.databricks.DatabricksSource.managed_table_exists", return_value=True
            ),
        ):
            DatabricksWarehouseStateStore(_profile()).save_sync(
                SyncState(sync_name="s", last_run_at="t", records_synced=1, status="success")
            )

        executed = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
        assert any(sql.startswith("UPDATE") for sql in executed)
        assert not any(sql.startswith("INSERT INTO") for sql in executed)

    def test_save_sync_skips_create_table_when_preprovisioned(self) -> None:
        """The escape hatch: a pre-provisioned table must never see the
        CREATE statement, even the IF NOT EXISTS form -- and (unlike an
        earlier draft) no scratch table CREATE either, since save_sync now
        issues only SELECT/UPDATE/INSERT against the real table."""
        conn = _mock_conn()
        with (
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
            patch("drt.sources.databricks.DatabricksSource.ensure_managed_schema"),
            patch(
                "drt.sources.databricks.DatabricksSource.managed_table_exists", return_value=True
            ),
        ):
            DatabricksWarehouseStateStore(_profile()).save_sync(
                SyncState(sync_name="s", last_run_at="t", records_synced=1, status="success")
            )

        executed = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
        assert not any("CREATE" in sql for sql in executed)


class TestDatabricksWarehouseHistoryStore:
    def test_read_returns_empty_before_any_table_exists(self) -> None:
        with patch("drt.state.warehouse_databricks._table_exists", return_value=False):
            assert DatabricksWarehouseHistoryStore(_profile()).read() == []

    def test_read_with_no_sync_name_reads_across_every_sync(self) -> None:
        row = ("s", "t0", "t1", 1.5, "success", 3, 0, [], None, False, "run-1", "sync-run-1")
        conn = _mock_conn(fetchall=[row])
        with (
            patch("drt.state.warehouse_databricks._table_exists", return_value=True),
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
        ):
            entries = DatabricksWarehouseHistoryStore(_profile()).read(sync_name=None)

        assert len(entries) == 1
        sql = str(conn.cursor.return_value.execute.call_args.args[0])
        assert "WHERE sync_name" not in sql

    def test_prune_returns_zero_before_any_table_exists(self) -> None:
        with patch("drt.state.warehouse_databricks._table_exists", return_value=False):
            assert DatabricksWarehouseHistoryStore(_profile()).prune("s", 30) == 0

    def test_prune_counts_then_deletes_and_returns_the_count(self) -> None:
        """Counted before DELETE rather than trusting cursor.rowcount
        afterward -- see module docstring for why."""
        conn = _mock_conn(fetchone=(3,))
        with (
            patch("drt.state.warehouse_databricks._table_exists", return_value=True),
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
        ):
            assert DatabricksWarehouseHistoryStore(_profile()).prune("s", 30) == 3
        executed = [str(c.args[0]) for c in conn.cursor.return_value.execute.call_args_list]
        assert any(sql.startswith("SELECT COUNT(*)") for sql in executed)
        assert any(sql.startswith("DELETE FROM") for sql in executed)

    def test_prune_skips_delete_when_nothing_matches(self) -> None:
        conn = _mock_conn(fetchone=(0,))
        with (
            patch("drt.state.warehouse_databricks._table_exists", return_value=True),
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
        ):
            assert DatabricksWarehouseHistoryStore(_profile()).prune("s", 30) == 0
        executed = [str(c.args[0]) for c in conn.cursor.return_value.execute.call_args_list]
        assert not any(sql.startswith("DELETE FROM") for sql in executed)

    def test_append_is_best_effort_and_never_raises(self) -> None:
        with patch(
            "drt.state.warehouse_databricks._connect",
            side_effect=RuntimeError("connection failed"),
        ):
            # Must not raise -- see the HistoryStore Protocol's append() contract.
            DatabricksWarehouseHistoryStore(_profile()).append(
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

    def test_append_uses_select_form_for_the_parse_json_column(self) -> None:
        """Databricks disallows a function call (parse_json) inside a VALUES
        literal list, so the errors column write must use INSERT ... SELECT,
        not INSERT ... VALUES."""
        conn = _mock_conn()
        with (
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
            patch("drt.sources.databricks.DatabricksSource.ensure_managed_schema"),
            patch(
                "drt.sources.databricks.DatabricksSource.managed_table_exists", return_value=True
            ),
        ):
            DatabricksWarehouseHistoryStore(_profile()).append(
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

        sql = str(conn.cursor.return_value.execute.call_args.args[0])
        assert "INSERT INTO" in sql
        assert "SELECT" in sql
        assert "parse_json(?)" in sql
        assert "VALUES" not in sql

    def test_read_reconstructs_history_entry_from_a_json_string_variant(self) -> None:
        row = ("s", "t0", "t1", 1.5, "success", 3, 0, "[]", None, False, "run-1", "sync-run-1")
        conn = _mock_conn(fetchall=[row])
        with (
            patch("drt.state.warehouse_databricks._table_exists", return_value=True),
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
        ):
            entries = DatabricksWarehouseHistoryStore(_profile()).read("s")

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

    def test_read_reconstructs_history_entry_from_an_already_parsed_variant(self) -> None:
        row = ("s", "t0", "t1", 1.5, "success", 3, 0, [], None, False, "run-1", "sync-run-1")
        conn = _mock_conn(fetchall=[row])
        with (
            patch("drt.state.warehouse_databricks._table_exists", return_value=True),
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
        ):
            entries = DatabricksWarehouseHistoryStore(_profile()).read("s")

        assert entries[0].errors == []


def _dead_letter(**overrides: object) -> DeadLetter:
    defaults: dict = {
        "record": {"a": 1},
        "error_message": "boom",
        "http_status": 500,
        "timestamp": "t0",
        "attempts": 1,
        "sync_run_id": "run-1",
        "id": "id-1",
    }
    defaults.update(overrides)
    return DeadLetter(**defaults)


class TestDatabricksWarehouseDlqBackend:
    def test_read_returns_empty_before_any_table_exists(self) -> None:
        with patch("drt.state.warehouse_databricks._table_exists", return_value=False):
            assert DatabricksWarehouseDlqBackend(_profile()).read("s") == []

    def test_depth_returns_zero_before_any_table_exists(self) -> None:
        with patch("drt.state.warehouse_databricks._table_exists", return_value=False):
            assert DatabricksWarehouseDlqBackend(_profile()).depth("s") == 0

    def test_all_depths_returns_empty_before_any_table_exists(self) -> None:
        with patch("drt.state.warehouse_databricks._table_exists", return_value=False):
            assert DatabricksWarehouseDlqBackend(_profile()).all_depths() == {}

    def test_all_depths_skips_zero_depth_entries(self) -> None:
        conn = _mock_conn(fetchall=[("a", 3), ("b", 0)])
        with (
            patch("drt.state.warehouse_databricks._table_exists", return_value=True),
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
        ):
            assert DatabricksWarehouseDlqBackend(_profile()).all_depths() == {"a": 3}

    def test_append_with_empty_list_is_a_pure_depth_read(self) -> None:
        conn = _mock_conn(fetchone=(0,))
        with (
            patch("drt.state.warehouse_databricks._table_exists", return_value=True),
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
        ):
            assert DatabricksWarehouseDlqBackend(_profile()).append("s", []) == 0

    def test_append_inserts_new_entry_via_probe_then_insert(self) -> None:
        """No scratch table, no MERGE -- see module docstring for why an
        earlier scratch-table + MERGE draft broke the documented escape
        hatch and was replaced with this per-entry probe."""
        conn = _sequenced_conn(fetchone_sequence=[None, (1,)])  # probe absent, then depth()
        with (
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
            patch("drt.sources.databricks.DatabricksSource.ensure_managed_schema"),
            patch(
                "drt.sources.databricks.DatabricksSource.managed_table_exists", return_value=True
            ),
        ):
            depth = DatabricksWarehouseDlqBackend(_profile()).append("s", [_dead_letter()])

        assert depth == 1
        executed = [str(c.args[0]) for c in conn.cursor.return_value.execute.call_args_list]
        assert any(sql.startswith("SELECT 1 FROM") for sql in executed)
        assert any(sql.startswith("INSERT INTO") and "parse_json(?)" in sql for sql in executed)
        assert not any(sql.startswith("UPDATE") for sql in executed)
        assert not any("MERGE" in sql or "CREATE OR REPLACE" in sql for sql in executed)

    def test_append_updates_existing_entry_via_probe_then_update(self) -> None:
        """The UPDATE branch must not touch sync_name -- matching every
        other dialect's DLQ upsert precedent (see append()'s docstring):
        reassigning sync_name on an id match would let one sync's append()
        silently move another sync's row into its own queue."""
        conn = _sequenced_conn(fetchone_sequence=[(1,), (1,)])  # probe exists, then depth()
        with (
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
            patch("drt.sources.databricks.DatabricksSource.ensure_managed_schema"),
            patch(
                "drt.sources.databricks.DatabricksSource.managed_table_exists", return_value=True
            ),
        ):
            DatabricksWarehouseDlqBackend(_profile()).append("s", [_dead_letter()])

        executed = [str(c.args[0]) for c in conn.cursor.return_value.execute.call_args_list]
        update_sql = next(sql for sql in executed if sql.startswith("UPDATE"))
        assert "parse_json(?)" in update_sql
        assert "sync_name" not in update_sql
        assert not any(sql.startswith("INSERT INTO") for sql in executed)

    def test_replace_upserts_via_values_merge_in_databricks_clause_order(self) -> None:
        """The #955 failure class this guards against, closed without a
        scratch table or an explicit transaction (Delta has neither
        available under the documented escape hatch / at all, respectively)
        -- see module docstring for the full design history."""
        conn = _mock_conn(fetchall=[])  # no existing ids -> nothing stale
        with (
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
            patch("drt.sources.databricks.DatabricksSource.ensure_managed_schema"),
            patch(
                "drt.sources.databricks.DatabricksSource.managed_table_exists", return_value=True
            ),
        ):
            DatabricksWarehouseDlqBackend(_profile()).replace("s", [_dead_letter()])

        executed = [str(c.args[0]) for c in conn.cursor.return_value.execute.call_args_list]
        assert any(sql.startswith("SELECT id FROM") for sql in executed)
        assert not any(sql.startswith("DELETE FROM") for sql in executed)
        assert not any("CREATE" in sql for sql in executed)
        merge_sql = next(sql for sql in executed if sql.startswith("MERGE INTO"))
        assert "USING (VALUES" in merge_sql
        assert "parse_json(s.record)" in merge_sql
        # Databricks MERGE grammar: WHEN MATCHED before WHEN NOT MATCHED,
        # and no WHEN NOT MATCHED BY SOURCE at all in this design.
        assert merge_sql.index("WHEN MATCHED") < merge_sql.index("WHEN NOT MATCHED THEN")
        assert "WHEN NOT MATCHED BY SOURCE" not in merge_sql

    def test_replace_deletes_stale_ids_absent_from_new_entries(self) -> None:
        conn = _mock_conn(fetchall=[("keep-1",), ("stale-1",)])
        with (
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
            patch("drt.sources.databricks.DatabricksSource.ensure_managed_schema"),
            patch(
                "drt.sources.databricks.DatabricksSource.managed_table_exists", return_value=True
            ),
        ):
            DatabricksWarehouseDlqBackend(_profile()).replace("s", [_dead_letter(id="keep-1")])

        delete_call = next(
            c
            for c in conn.cursor.return_value.execute.call_args_list
            if str(c.args[0]).startswith("DELETE FROM")
        )
        sql, params = delete_call.args
        assert "IN (?)" in sql
        assert params == ["s", "stale-1"]

    def test_replace_upserts_before_deleting_stale_ids(self) -> None:
        """Ordering matters for crash-safety, not just final correctness
        (see module docstring): deleting stale ids before the upsert would
        mean a crash or a failed merge partway through leaves the queue with
        the old rows already gone and the new ones not yet landed."""
        conn = _mock_conn(fetchall=[("stale-1",)])
        with (
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
            patch("drt.sources.databricks.DatabricksSource.ensure_managed_schema"),
            patch(
                "drt.sources.databricks.DatabricksSource.managed_table_exists", return_value=True
            ),
        ):
            DatabricksWarehouseDlqBackend(_profile()).replace("s", [_dead_letter(id="new-1")])

        calls = conn.cursor.return_value.execute.call_args_list
        merge_index = next(i for i, c in enumerate(calls) if str(c.args[0]).startswith("MERGE"))
        delete_index = next(
            i for i, c in enumerate(calls) if str(c.args[0]).startswith("DELETE FROM")
        )
        assert merge_index < delete_index

    def test_replace_chunks_many_entries_across_multiple_merge_statements(self) -> None:
        """31 rows fit per MERGE at 8 columns under the native 255-marker
        limit (#734's _rows_per_chunk, reused rather than re-derived) -- 40
        entries need two chunks, and the whole operation is only atomic
        within a chunk, not across them (see module docstring)."""
        conn = _mock_conn(fetchall=[])
        entries = [_dead_letter(id=f"id-{i}", record={"n": i}) for i in range(40)]
        with (
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
            patch("drt.sources.databricks.DatabricksSource.ensure_managed_schema"),
            patch(
                "drt.sources.databricks.DatabricksSource.managed_table_exists", return_value=True
            ),
        ):
            DatabricksWarehouseDlqBackend(_profile()).replace("s", entries)

        merge_calls = [
            c
            for c in conn.cursor.return_value.execute.call_args_list
            if str(c.args[0]).startswith("MERGE INTO")
        ]
        assert len(merge_calls) == 2
        assert sum(len(c.args[1]) // 8 for c in merge_calls) == 40

    def test_replace_with_empty_list_deletes_directly_not_via_merge(self) -> None:
        """clear() must go through a plain DELETE rather than trusting a
        zero-row MERGE source's semantics on this platform."""
        conn = _mock_conn()
        with (
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
            patch("drt.sources.databricks.DatabricksSource.ensure_managed_schema"),
            patch(
                "drt.sources.databricks.DatabricksSource.managed_table_exists", return_value=True
            ),
        ):
            DatabricksWarehouseDlqBackend(_profile()).replace("s", [])

        executed = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
        assert any(sql.startswith("DELETE FROM") for sql in executed)
        assert not any("MERGE INTO" in sql for sql in executed)
        assert not any("CREATE" in sql for sql in executed)

    def test_clear_deletes_with_an_empty_list(self) -> None:
        conn = _mock_conn()
        with (
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
            patch("drt.sources.databricks.DatabricksSource.ensure_managed_schema"),
            patch(
                "drt.sources.databricks.DatabricksSource.managed_table_exists", return_value=True
            ),
        ):
            DatabricksWarehouseDlqBackend(_profile()).clear("s")

        executed = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
        assert any(sql.startswith("DELETE FROM") for sql in executed)
        assert not any("INSERT INTO" in sql for sql in executed)

    def test_reconcile_removes_with_explicit_placeholders(self) -> None:
        conn = _mock_conn(fetchall=[])
        with (
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
            patch("drt.sources.databricks.DatabricksSource.ensure_managed_schema"),
            patch(
                "drt.sources.databricks.DatabricksSource.managed_table_exists", return_value=True
            ),
        ):
            DatabricksWarehouseDlqBackend(_profile()).reconcile("s", remove_ids=["id-1", "id-2"])

        delete_call = next(
            c
            for c in conn.cursor.return_value.execute.call_args_list
            if str(c.args[0]).startswith("DELETE FROM")
        )
        sql, params = delete_call.args
        assert "IN (?, ?)" in sql
        assert params == ["s", "id-1", "id-2"]

    def test_reconcile_chunks_large_remove_id_lists(self) -> None:
        """Stays under the native 255-marker limit (#734) -- one slot
        reserved for sync_name in each chunk's statement."""
        conn = _mock_conn(fetchall=[])
        ids = [f"id-{i}" for i in range(300)]
        with (
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
            patch("drt.sources.databricks.DatabricksSource.ensure_managed_schema"),
            patch(
                "drt.sources.databricks.DatabricksSource.managed_table_exists", return_value=True
            ),
        ):
            DatabricksWarehouseDlqBackend(_profile()).reconcile("s", remove_ids=ids)

        delete_calls = [
            c
            for c in conn.cursor.return_value.execute.call_args_list
            if str(c.args[0]).startswith("DELETE FROM")
        ]
        assert len(delete_calls) == 2
        assert sum(len(c.args[1]) - 1 for c in delete_calls) == 300

    def test_reconcile_updates_via_parse_json(self) -> None:
        conn = _mock_conn(fetchall=[])
        with (
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
            patch("drt.sources.databricks.DatabricksSource.ensure_managed_schema"),
            patch(
                "drt.sources.databricks.DatabricksSource.managed_table_exists", return_value=True
            ),
        ):
            DatabricksWarehouseDlqBackend(_profile()).reconcile(
                "s",
                updates={
                    "id-1": _dead_letter(
                        record={"a": 2},
                        error_message="still failing",
                        timestamp="t1",
                        attempts=3,
                        sync_run_id="run-2",
                    )
                },
            )

        update_call = next(
            c
            for c in conn.cursor.return_value.execute.call_args_list
            if str(c.args[0]).startswith("UPDATE")
        )
        sql, params = update_call.args
        assert "parse_json(?)" in sql
        assert params[0] == '{"a": 2}'
        assert params[-2:] == ["s", "id-1"]

    def test_read_reconstructs_dead_letter_from_a_json_string_variant(self) -> None:
        row = ("id-1", '{"a": 1}', "boom", 500, "t0", 2, "run-1")
        conn = _mock_conn(fetchall=[row])
        with (
            patch("drt.state.warehouse_databricks._table_exists", return_value=True),
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
        ):
            entries = DatabricksWarehouseDlqBackend(_profile()).read("s")

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

    def test_read_reconstructs_dead_letter_from_an_already_parsed_variant(self) -> None:
        row = ("id-1", {"a": 1}, "boom", 500, "t0", 2, "run-1")
        conn = _mock_conn(fetchall=[row])
        with (
            patch("drt.state.warehouse_databricks._table_exists", return_value=True),
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
        ):
            entries = DatabricksWarehouseDlqBackend(_profile()).read("s")

        assert entries[0].record == {"a": 1}
