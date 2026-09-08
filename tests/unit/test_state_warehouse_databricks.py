"""Unit tests for the Databricks-backed warehouse state/history/DLQ stores (#1108).

Mock-based, like tests/unit/test_state_warehouse_snowflake.py's Snowflake
counterpart: proves dispatch, parameter shape, the inline-derived-table MERGE
translation, and `replace()`'s staged-MERGE atomicity strategy (Delta has no
multi-statement transactions at all, so this dialect cannot wrap
DELETE-then-INSERT the way Snowflake does). Real round-trip behavior is
proven live in tests/integration/dwh/test_databricks_warehouse_state_smoke.py
— a mock cursor can't validate the SQL itself (#908's lesson).
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


def _mock_conn(*, fetchone=None, fetchall=None, rowcount: int = 0) -> MagicMock:
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = fetchone
    cur.fetchall.return_value = fetchall or []
    cur.rowcount = rowcount
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
        conn = _mock_conn(rowcount=1)
        with (
            patch("drt.state.warehouse_databricks._table_exists", return_value=True),
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
        ):
            assert DatabricksWarehouseStateStore(_profile()).reset("s") is True
        sql = str(conn.cursor.return_value.execute.call_args.args[0])
        assert sql.startswith("DELETE FROM")

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

    def test_save_sync_ensures_schema_and_merges_via_inline_source(self) -> None:
        conn = _mock_conn()
        with (
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
            patch(
                "drt.sources.databricks.DatabricksSource.ensure_managed_schema"
            ) as ensure_schema,
            patch(
                "drt.sources.databricks.DatabricksSource.managed_table_exists", return_value=False
            ) as table_exists,
        ):
            DatabricksWarehouseStateStore(_profile()).save_sync(
                SyncState(
                    sync_name="s",
                    last_run_at="t",
                    records_synced=1,
                    status="success",
                )
            )

        ensure_schema.assert_called_once()
        table_exists.assert_called_once()
        executed = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
        assert any(
            "MERGE INTO" in sql and "USING (SELECT" in sql and "WHEN MATCHED" in sql
            for sql in executed
        )

    def test_save_sync_skips_create_table_when_preprovisioned(self) -> None:
        """The escape hatch: a pre-provisioned table must never see the
        CREATE statement, even the IF NOT EXISTS form."""
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
        assert not any("CREATE TABLE" in sql for sql in executed)


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

    def test_prune_deletes_and_returns_the_removed_count(self) -> None:
        conn = _mock_conn(rowcount=3)
        with (
            patch("drt.state.warehouse_databricks._table_exists", return_value=True),
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
        ):
            assert DatabricksWarehouseHistoryStore(_profile()).prune("s", 30) == 3
        sql = str(conn.cursor.return_value.execute.call_args.args[0])
        assert sql.startswith("DELETE FROM")

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

    def test_append_merges_each_entry_via_inline_source(self) -> None:
        conn = _mock_conn(fetchone=(1,))
        with (
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
            patch("drt.sources.databricks.DatabricksSource.ensure_managed_schema"),
            patch(
                "drt.sources.databricks.DatabricksSource.managed_table_exists", return_value=True
            ),
        ):
            DatabricksWarehouseDlqBackend(_profile()).append(
                "s",
                [
                    DeadLetter(
                        record={"a": 1},
                        error_message="boom",
                        http_status=500,
                        timestamp="t0",
                        attempts=1,
                        sync_run_id="run-1",
                        id="id-1",
                    )
                ],
            )

        executed = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
        assert any("MERGE INTO" in sql and "USING (SELECT" in sql for sql in executed)
        assert any("parse_json(?)" in sql for sql in executed)

    def test_replace_with_entries_stages_and_merges_atomically(self) -> None:
        """The #955 failure class this guards against, closed via a single
        atomic MERGE rather than an explicit transaction (Delta has none):
        the queue is staged into a scratch table, then one MERGE deletes
        stale rows / updates matches / inserts new rows in one Delta commit."""
        conn = _mock_conn()
        with (
            patch("drt.state.warehouse_databricks._connect", return_value=conn),
            patch("drt.sources.databricks.DatabricksSource.ensure_managed_schema"),
            patch(
                "drt.sources.databricks.DatabricksSource.managed_table_exists", return_value=True
            ),
        ):
            DatabricksWarehouseDlqBackend(_profile()).replace(
                "s",
                [
                    DeadLetter(
                        record={"a": 1},
                        error_message="boom",
                        http_status=500,
                        timestamp="t0",
                        attempts=1,
                        sync_run_id="run-1",
                        id="id-1",
                    )
                ],
            )

        executed = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
        assert any(sql.startswith("CREATE OR REPLACE TABLE") for sql in executed)
        assert any(
            "INSERT INTO" in sql and "parse_json(?)" in sql and "__drt_dlq_replace_" in sql
            for sql in executed
        )
        merge_sql = next(sql for sql in executed if sql.startswith("MERGE INTO"))
        assert "WHEN NOT MATCHED BY SOURCE AND t.sync_name = ?" in merge_sql
        assert "WHEN MATCHED THEN UPDATE" in merge_sql
        assert "WHEN NOT MATCHED THEN INSERT" in merge_sql
        assert any(sql.startswith("DROP TABLE IF EXISTS") for sql in executed)

    def test_replace_with_empty_list_deletes_directly_not_via_merge(self) -> None:
        """Empty-source MERGE semantics on Delta are unverified by any prior
        art in this codebase — clear() must go through a plain DELETE
        instead of trusting a zero-row MERGE source."""
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
        assert not any("CREATE OR REPLACE TABLE" in sql for sql in executed)

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
                    "id-1": DeadLetter(
                        record={"a": 2},
                        error_message="still failing",
                        http_status=500,
                        timestamp="t1",
                        attempts=3,
                        sync_run_id="run-2",
                        id="id-1",
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
