"""Unit tests for the Snowflake-backed warehouse state/history/DLQ stores (#1106).

Mock-based, like tests/unit/test_state_warehouse.py's Postgres counterpart:
proves dispatch, parameter shape, the MERGE-vs-ON-CONFLICT translation, and
the explicit-transaction wrapping DlqBackend.append/.replace()/.reconcile()
need (Snowflake autocommits per-statement by default, unlike psycopg2). Real
round-trip behavior is proven live in
tests/integration/dwh/test_snowflake_warehouse_state_smoke.py — a mock
cursor can't validate the SQL itself (#908's lesson).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from drt.config.credentials import SnowflakeProfile
from drt.state.dlq import DeadLetter
from drt.state.history import HistoryEntry
from drt.state.manager import SyncState
from drt.state.warehouse_snowflake import (
    SnowflakeWarehouseDlqBackend,
    SnowflakeWarehouseHistoryStore,
    SnowflakeWarehouseStateStore,
)


def _profile(**overrides: object) -> SnowflakeProfile:
    defaults: dict = {
        "type": "snowflake",
        "account": "xy12345",
        "user": "u",
        "database": "ANALYTICS",
    }
    defaults.update(overrides)
    return SnowflakeProfile(**defaults)


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
    _ensure_table() -- same shape as SnowflakeSource.ensure_managed_schema()
    (#1106), tested directly rather than through one store's _ensure_table
    wrapper since the logic is common to all three."""

    def test_swallows_the_concurrent_create_race(self) -> None:
        from drt.state.warehouse_snowflake import _ensure_table_exists

        conn = MagicMock()
        conn.cursor.return_value.execute.side_effect = Exception("concurrent create race")
        with (
            patch("drt.sources.snowflake.SnowflakeSource.ensure_managed_schema"),
            patch(
                "drt.sources.snowflake.SnowflakeSource.managed_table_exists",
                side_effect=[False, True],
            ),
        ):
            _ensure_table_exists(conn, _profile(), "_drt_runs", "id TEXT")  # must not raise

    def test_reraises_when_still_absent_after_create_fails(self) -> None:
        from drt.state.warehouse_snowflake import _ensure_table_exists

        conn = MagicMock()
        conn.cursor.return_value.execute.side_effect = Exception("insufficient privileges")
        with (
            patch("drt.sources.snowflake.SnowflakeSource.ensure_managed_schema"),
            patch(
                "drt.sources.snowflake.SnowflakeSource.managed_table_exists",
                side_effect=[False, False],
            ),
        ):
            try:
                _ensure_table_exists(conn, _profile(), "_drt_runs", "id TEXT")
                raise AssertionError("expected the exception to propagate")
            except Exception as e:
                assert "insufficient privileges" in str(e)


class TestSnowflakeWarehouseStateStore:
    def test_get_last_sync_returns_none_before_any_table_exists(self) -> None:
        with patch("drt.state.warehouse_snowflake._table_exists", return_value=False):
            assert SnowflakeWarehouseStateStore(_profile()).get_last_sync("s") is None

    def test_get_all_returns_empty_before_any_table_exists(self) -> None:
        with patch("drt.state.warehouse_snowflake._table_exists", return_value=False):
            assert SnowflakeWarehouseStateStore(_profile()).get_all() == {}

    def test_get_all_reconstructs_every_sync(self) -> None:
        rows = [
            ("a", "t0", 1, "success", None, None),
            ("b", "t1", 2, "failed", "boom", "cur-2"),
        ]
        conn = _mock_conn(fetchall=rows)
        with (
            patch("drt.state.warehouse_snowflake._table_exists", return_value=True),
            patch("drt.state.warehouse_snowflake._connect", return_value=conn),
        ):
            result = SnowflakeWarehouseStateStore(_profile()).get_all()

        assert set(result) == {"a", "b"}
        assert result["b"].status == "failed"
        assert result["b"].error == "boom"

    def test_reset_returns_false_before_any_table_exists(self) -> None:
        with patch("drt.state.warehouse_snowflake._table_exists", return_value=False):
            assert SnowflakeWarehouseStateStore(_profile()).reset("s") is False

    def test_reset_deletes_and_returns_true_when_a_row_existed(self) -> None:
        conn = _mock_conn(rowcount=1)
        with (
            patch("drt.state.warehouse_snowflake._table_exists", return_value=True),
            patch("drt.state.warehouse_snowflake._connect", return_value=conn),
        ):
            assert SnowflakeWarehouseStateStore(_profile()).reset("s") is True
        sql = str(conn.cursor.return_value.execute.call_args.args[0])
        assert sql.startswith("DELETE FROM")

    def test_now_returns_an_iso_timestamp(self) -> None:
        from datetime import datetime

        # Just needs to round-trip through fromisoformat -- no live clock
        # dependency to mock, this is a pure formatting contract.
        datetime.fromisoformat(SnowflakeWarehouseStateStore(_profile()).now())

    def test_get_last_sync_reconstructs_sync_state(self) -> None:
        conn = _mock_conn(fetchone=("2026-09-06T00:00:00+00:00", 5, "success", None, "cursor-1"))
        with (
            patch("drt.state.warehouse_snowflake._table_exists", return_value=True),
            patch("drt.state.warehouse_snowflake._connect", return_value=conn),
        ):
            state = SnowflakeWarehouseStateStore(_profile()).get_last_sync("my_sync")

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
            patch("drt.state.warehouse_snowflake._table_exists", return_value=True),
            patch("drt.state.warehouse_snowflake._connect", return_value=conn),
        ):
            assert SnowflakeWarehouseStateStore(_profile()).get_last_sync("never_run") is None

    def test_save_sync_ensures_schema_and_merges(self) -> None:
        conn = _mock_conn()
        with (
            patch("drt.state.warehouse_snowflake._connect", return_value=conn),
            patch("drt.sources.snowflake.SnowflakeSource.ensure_managed_schema") as ensure_schema,
            patch(
                "drt.sources.snowflake.SnowflakeSource.managed_table_exists", return_value=False
            ) as table_exists,
        ):
            SnowflakeWarehouseStateStore(_profile()).save_sync(
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
        assert any("MERGE INTO" in sql and "WHEN MATCHED" in sql for sql in executed)
        # Single-statement write relies on the connection's default
        # per-statement autocommit -- no explicit commit() call.
        conn.commit.assert_not_called()

    def test_save_sync_skips_create_table_when_preprovisioned(self) -> None:
        """The escape hatch: a pre-provisioned table must never see the
        CREATE statement, even the IF NOT EXISTS form."""
        conn = _mock_conn()
        with (
            patch("drt.state.warehouse_snowflake._connect", return_value=conn),
            patch("drt.sources.snowflake.SnowflakeSource.ensure_managed_schema"),
            patch("drt.sources.snowflake.SnowflakeSource.managed_table_exists", return_value=True),
        ):
            SnowflakeWarehouseStateStore(_profile()).save_sync(
                SyncState(sync_name="s", last_run_at="t", records_synced=1, status="success")
            )

        executed = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
        assert not any("CREATE TABLE" in sql for sql in executed)


class TestSnowflakeWarehouseHistoryStore:
    def test_read_returns_empty_before_any_table_exists(self) -> None:
        with patch("drt.state.warehouse_snowflake._table_exists", return_value=False):
            assert SnowflakeWarehouseHistoryStore(_profile()).read() == []

    def test_read_with_no_sync_name_reads_across_every_sync(self) -> None:
        row = ("s", "t0", "t1", 1.5, "success", 3, 0, [], None, False, "run-1", "sync-run-1")
        conn = _mock_conn(fetchall=[row])
        with (
            patch("drt.state.warehouse_snowflake._table_exists", return_value=True),
            patch("drt.state.warehouse_snowflake._connect", return_value=conn),
        ):
            entries = SnowflakeWarehouseHistoryStore(_profile()).read(sync_name=None)

        assert len(entries) == 1
        sql = str(conn.cursor.return_value.execute.call_args.args[0])
        assert "WHERE sync_name" not in sql

    def test_prune_returns_zero_before_any_table_exists(self) -> None:
        with patch("drt.state.warehouse_snowflake._table_exists", return_value=False):
            assert SnowflakeWarehouseHistoryStore(_profile()).prune("s", 30) == 0

    def test_prune_deletes_and_returns_the_removed_count(self) -> None:
        conn = _mock_conn(rowcount=3)
        with (
            patch("drt.state.warehouse_snowflake._table_exists", return_value=True),
            patch("drt.state.warehouse_snowflake._connect", return_value=conn),
        ):
            assert SnowflakeWarehouseHistoryStore(_profile()).prune("s", 30) == 3
        sql = str(conn.cursor.return_value.execute.call_args.args[0])
        assert sql.startswith("DELETE FROM")

    def test_append_is_best_effort_and_never_raises(self) -> None:
        with patch(
            "drt.state.warehouse_snowflake._connect", side_effect=RuntimeError("connection failed")
        ):
            # Must not raise -- see the HistoryStore Protocol's append() contract.
            SnowflakeWarehouseHistoryStore(_profile()).append(
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
        """Snowflake disallows a function call (PARSE_JSON) inside a VALUES
        literal list, so the errors column write must use INSERT ... SELECT,
        not INSERT ... VALUES."""
        conn = _mock_conn()
        with (
            patch("drt.state.warehouse_snowflake._connect", return_value=conn),
            patch("drt.sources.snowflake.SnowflakeSource.ensure_managed_schema"),
            patch("drt.sources.snowflake.SnowflakeSource.managed_table_exists", return_value=True),
        ):
            SnowflakeWarehouseHistoryStore(_profile()).append(
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
        assert "PARSE_JSON(%s)" in sql
        assert "VALUES" not in sql

    def test_read_reconstructs_history_entry_from_a_json_string_variant(self) -> None:
        """Covers the branch where the connector hands VARIANT back as a
        JSON-encoded string rather than an already-parsed object."""
        row = ("s", "t0", "t1", 1.5, "success", 3, 0, "[]", None, False, "run-1", "sync-run-1")
        conn = _mock_conn(fetchall=[row])
        with (
            patch("drt.state.warehouse_snowflake._table_exists", return_value=True),
            patch("drt.state.warehouse_snowflake._connect", return_value=conn),
        ):
            entries = SnowflakeWarehouseHistoryStore(_profile()).read("s")

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
        """Covers the other branch: the connector hands VARIANT back
        already deserialized into a Python list."""
        row = ("s", "t0", "t1", 1.5, "success", 3, 0, [], None, False, "run-1", "sync-run-1")
        conn = _mock_conn(fetchall=[row])
        with (
            patch("drt.state.warehouse_snowflake._table_exists", return_value=True),
            patch("drt.state.warehouse_snowflake._connect", return_value=conn),
        ):
            entries = SnowflakeWarehouseHistoryStore(_profile()).read("s")

        assert entries[0].errors == []


class TestSnowflakeWarehouseDlqBackend:
    def test_read_returns_empty_before_any_table_exists(self) -> None:
        with patch("drt.state.warehouse_snowflake._table_exists", return_value=False):
            assert SnowflakeWarehouseDlqBackend(_profile()).read("s") == []

    def test_depth_returns_zero_before_any_table_exists(self) -> None:
        with patch("drt.state.warehouse_snowflake._table_exists", return_value=False):
            assert SnowflakeWarehouseDlqBackend(_profile()).depth("s") == 0

    def test_all_depths_returns_empty_before_any_table_exists(self) -> None:
        with patch("drt.state.warehouse_snowflake._table_exists", return_value=False):
            assert SnowflakeWarehouseDlqBackend(_profile()).all_depths() == {}

    def test_all_depths_skips_zero_depth_entries(self) -> None:
        conn = _mock_conn(fetchall=[("a", 3), ("b", 0)])
        with (
            patch("drt.state.warehouse_snowflake._table_exists", return_value=True),
            patch("drt.state.warehouse_snowflake._connect", return_value=conn),
        ):
            assert SnowflakeWarehouseDlqBackend(_profile()).all_depths() == {"a": 3}

    def test_append_with_empty_list_is_a_pure_depth_read(self) -> None:
        conn = _mock_conn(fetchone=(0,))
        with (
            patch("drt.state.warehouse_snowflake._table_exists", return_value=True),
            patch("drt.state.warehouse_snowflake._connect", return_value=conn),
        ):
            assert SnowflakeWarehouseDlqBackend(_profile()).append("s", []) == 0
        conn.autocommit.assert_not_called()

    def test_append_merges_within_an_explicit_transaction(self) -> None:
        conn = _mock_conn(fetchone=(1,))
        with (
            patch("drt.state.warehouse_snowflake._connect", return_value=conn),
            patch("drt.sources.snowflake.SnowflakeSource.ensure_managed_schema"),
            patch("drt.sources.snowflake.SnowflakeSource.managed_table_exists", return_value=True),
        ):
            SnowflakeWarehouseDlqBackend(_profile()).append(
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

        conn.autocommit.assert_any_call(False)
        conn.commit.assert_called_once()
        executed = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
        assert any("MERGE INTO" in sql for sql in executed)
        assert any("PARSE_JSON(%s)" in sql for sql in executed)

    def test_replace_wraps_delete_and_inserts_in_one_transaction(self) -> None:
        """The #955 failure class this guards against: an unwrapped
        DELETE-then-INSERT under Snowflake's default per-statement
        autocommit would erase the queue permanently on a crash between the
        two. Confirms the explicit transaction actually wraps both."""
        conn = _mock_conn()
        with (
            patch("drt.state.warehouse_snowflake._connect", return_value=conn),
            patch("drt.sources.snowflake.SnowflakeSource.ensure_managed_schema"),
            patch("drt.sources.snowflake.SnowflakeSource.managed_table_exists", return_value=True),
        ):
            SnowflakeWarehouseDlqBackend(_profile()).replace(
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

        conn.autocommit.assert_any_call(False)
        conn.commit.assert_called_once()
        executed = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
        assert any(sql.startswith("DELETE FROM") for sql in executed)
        assert any("INSERT INTO" in sql and "PARSE_JSON(%s)" in sql for sql in executed)

    def test_replace_rolls_back_and_reraises_on_failure(self) -> None:
        conn = _mock_conn()
        conn.cursor.return_value.execute.side_effect = [None, RuntimeError("boom")]
        with (
            patch("drt.state.warehouse_snowflake._connect", return_value=conn),
            patch("drt.sources.snowflake.SnowflakeSource.ensure_managed_schema"),
            patch("drt.sources.snowflake.SnowflakeSource.managed_table_exists", return_value=True),
        ):
            try:
                SnowflakeWarehouseDlqBackend(_profile()).replace(
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
                raise AssertionError("expected RuntimeError to propagate")
            except RuntimeError:
                pass

        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()

    def test_reconcile_removes_with_explicit_placeholders(self) -> None:
        """Snowflake's connector doesn't auto-expand a Python sequence into
        a SQL list the way psycopg2's ANY(%s) does -- must be %s-per-value."""
        conn = _mock_conn(fetchall=[])
        with (
            patch("drt.state.warehouse_snowflake._connect", return_value=conn),
            patch("drt.sources.snowflake.SnowflakeSource.ensure_managed_schema"),
            patch("drt.sources.snowflake.SnowflakeSource.managed_table_exists", return_value=True),
        ):
            SnowflakeWarehouseDlqBackend(_profile()).reconcile("s", remove_ids=["id-1", "id-2"])

        delete_call = next(
            c
            for c in conn.cursor.return_value.execute.call_args_list
            if str(c.args[0]).startswith("DELETE FROM")
        )
        sql, params = delete_call.args
        assert "IN (%s, %s)" in sql
        assert params == ("s", "id-1", "id-2")

    def test_reconcile_updates_via_parse_json(self) -> None:
        conn = _mock_conn(fetchall=[])
        with (
            patch("drt.state.warehouse_snowflake._connect", return_value=conn),
            patch("drt.sources.snowflake.SnowflakeSource.ensure_managed_schema"),
            patch("drt.sources.snowflake.SnowflakeSource.managed_table_exists", return_value=True),
        ):
            SnowflakeWarehouseDlqBackend(_profile()).reconcile(
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
        assert "PARSE_JSON(%s)" in sql
        assert params[0] == '{"a": 2}'
        assert params[-2:] == ("s", "id-1")

    def test_clear_replaces_with_an_empty_list(self) -> None:
        conn = _mock_conn()
        with (
            patch("drt.state.warehouse_snowflake._connect", return_value=conn),
            patch("drt.sources.snowflake.SnowflakeSource.ensure_managed_schema"),
            patch("drt.sources.snowflake.SnowflakeSource.managed_table_exists", return_value=True),
        ):
            SnowflakeWarehouseDlqBackend(_profile()).clear("s")

        executed = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
        assert any(sql.startswith("DELETE FROM") for sql in executed)
        assert not any("INSERT INTO" in sql for sql in executed)

    def test_read_reconstructs_dead_letter_from_a_json_string_variant(self) -> None:
        row = ("id-1", '{"a": 1}', "boom", 500, "t0", 2, "run-1")
        conn = _mock_conn(fetchall=[row])
        with (
            patch("drt.state.warehouse_snowflake._table_exists", return_value=True),
            patch("drt.state.warehouse_snowflake._connect", return_value=conn),
        ):
            entries = SnowflakeWarehouseDlqBackend(_profile()).read("s")

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
            patch("drt.state.warehouse_snowflake._table_exists", return_value=True),
            patch("drt.state.warehouse_snowflake._connect", return_value=conn),
        ):
            entries = SnowflakeWarehouseDlqBackend(_profile()).read("s")

        assert entries[0].record == {"a": 1}
