"""Backfill-specific paths of the PostgreSQL warehouse state stores.

Covers the exclusive-boundary marker (legacy-table fallback, ALTER + save) and the
strict, non-evicting DLQ append that guards backfill windows from silently losing
dead letters (ADR 0021). Mock-based; the real round-trip is proven live in
tests/integration/local_sql.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("psycopg2")

import psycopg2

from drt.config.credentials import PostgresProfile
from drt.state.dlq import DeadLetter, DlqCapacityExceeded
from drt.state.manager import SyncState
from drt.state.warehouse import PostgresWarehouseDlqBackend, PostgresWarehouseStateStore


def _profile() -> PostgresProfile:
    return PostgresProfile(type="postgres", host="h", dbname="d", user="u")


def _conn(*, fetchone: Any = None, fetchall: Any = None) -> MagicMock:
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = fetchone
    cur.fetchall.return_value = fetchall or []
    conn.cursor.return_value = cur
    return conn


def _executed(conn: MagicMock) -> list[str]:
    return [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]


def _letter(letter_id: str) -> DeadLetter:
    return DeadLetter(
        id=letter_id,
        record={"id": letter_id},
        error_message="boom",
        http_status=500,
        timestamp="2026-11-24T00:00:00+00:00",
        attempts=1,
        sync_run_id="run-1",
    )


_MISSING_BOUNDARY = psycopg2.errors.UndefinedColumn('column "cursor_boundary" does not exist')


class TestLegacyRunsTable:
    def test_get_last_sync_falls_back_to_the_legacy_select(self) -> None:
        conn = _conn(fetchone=("2026-11-24T00:00:00+00:00", 3, "success", None, "7"))
        conn.cursor.return_value.execute.side_effect = [_MISSING_BOUNDARY, None]

        with patch("drt.state.warehouse._connect", return_value=conn):
            state = PostgresWarehouseStateStore(_profile()).get_last_sync("s")

        conn.rollback.assert_called_once()
        executed = _executed(conn)
        assert "cursor_boundary" in executed[0]
        assert "cursor_boundary" not in executed[1]
        assert state is not None
        assert state.last_cursor_value == "7"
        assert state.cursor_boundary is None

    def test_get_all_falls_back_to_the_legacy_select(self) -> None:
        conn = _conn(fetchall=[("s", "2026-11-24T00:00:00+00:00", 3, "success", None, "7")])
        conn.cursor.return_value.execute.side_effect = [_MISSING_BOUNDARY, None]

        with patch("drt.state.warehouse._connect", return_value=conn):
            states = PostgresWarehouseStateStore(_profile()).get_all()

        conn.rollback.assert_called_once()
        assert "cursor_boundary" not in _executed(conn)[1]
        assert states["s"].last_cursor_value == "7"
        assert states["s"].cursor_boundary is None


class TestSaveSyncBoundary:
    def test_boundary_adds_the_column_then_writes_it(self) -> None:
        conn = _conn()
        state = SyncState(
            sync_name="s",
            last_run_at="t",
            records_synced=1,
            status="success",
            last_cursor_value="10",
            cursor_boundary="exclusive",
        )
        with (
            patch("drt.state.warehouse._connect", return_value=conn),
            patch("drt.sources.postgres.PostgresSource.ensure_managed_schema"),
            patch("drt.sources.postgres.PostgresSource.managed_table_exists", return_value=True),
        ):
            PostgresWarehouseStateStore(_profile()).save_sync(state)

        executed = _executed(conn)
        alter = next(i for i, sql in enumerate(executed) if "ADD COLUMN IF NOT EXISTS" in sql)
        upsert = next(i for i, sql in enumerate(executed) if "ON CONFLICT" in sql)
        assert alter < upsert
        assert "cursor_boundary = EXCLUDED.cursor_boundary" in executed[upsert]
        params = conn.cursor.return_value.execute.call_args_list[upsert].args[1]
        assert params[-2:] == ("10", "exclusive")
        conn.commit.assert_called()

    def test_no_boundary_never_touches_the_column(self) -> None:
        conn = _conn()
        with (
            patch("drt.state.warehouse._connect", return_value=conn),
            patch("drt.sources.postgres.PostgresSource.ensure_managed_schema"),
            patch("drt.sources.postgres.PostgresSource.managed_table_exists", return_value=True),
        ):
            PostgresWarehouseStateStore(_profile()).save_sync(
                SyncState("s", "t", 1, "success", last_cursor_value="10")
            )

        assert not any("cursor_boundary" in sql for sql in _executed(conn))


@pytest.fixture(autouse=True)
def _no_real_schema_provisioning() -> Any:
    """The table-provisioning helpers open their own connection; keep them offline."""
    with (
        patch("drt.sources.postgres.PostgresSource.ensure_managed_schema"),
        patch("drt.sources.postgres.PostgresSource.managed_table_exists", return_value=True),
    ):
        yield


class TestStrictDlqAppend:
    def _backend(self) -> PostgresWarehouseDlqBackend:
        return PostgresWarehouseDlqBackend(_profile())

    def test_inserts_new_ids_under_capacity_without_deleting_anything(self) -> None:
        conn = _conn(fetchall=[("old",)])
        with patch("drt.state.warehouse._connect", return_value=conn):
            depth = self._backend()._append_strict("s", [_letter("a"), _letter("b")], max_records=3)

        executed = _executed(conn)
        assert "FOR UPDATE" in executed[0]
        assert any("INSERT INTO" in sql and "ON CONFLICT (id)" in sql for sql in executed)
        assert not any("DELETE" in sql for sql in executed)
        assert depth == 3
        conn.commit.assert_called_once()
        conn.close.assert_called_once()

    def test_over_capacity_fails_before_any_write_and_rolls_back(self) -> None:
        conn = _conn(fetchall=[("old",)])
        with (
            patch("drt.state.warehouse._connect", return_value=conn),
            pytest.raises(DlqCapacityExceeded, match="dlq.max_records"),
        ):
            self._backend()._append_strict("s", [_letter("new")], max_records=1)

        assert not any("INSERT INTO" in sql for sql in _executed(conn))
        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()
        conn.close.assert_called_once()

    def test_replacing_an_existing_id_at_capacity_consumes_no_capacity(self) -> None:
        conn = _conn(fetchall=[("same",)])
        with patch("drt.state.warehouse._connect", return_value=conn):
            depth = self._backend()._append_strict("s", [_letter("same")], max_records=1)

        assert depth == 1
        assert any("DO UPDATE SET" in sql for sql in _executed(conn))
        conn.commit.assert_called_once()

    def test_duplicate_ids_in_one_call_write_one_row(self) -> None:
        conn = _conn(fetchall=[])
        with patch("drt.state.warehouse._connect", return_value=conn):
            self._backend()._append_strict("s", [_letter("a"), _letter("a")], max_records=5)

        inserts = [
            call
            for call in conn.cursor.return_value.execute.call_args_list
            if "INSERT INTO" in str(call.args[0])
        ]
        assert len(inserts) == 1
        params = inserts[0].args[1]
        assert params.count("a") == 1
        assert json.loads(params[2]) == {"id": "a"}

    def test_incoming_ids_override_the_capacity_candidates(self) -> None:
        conn = _conn(fetchall=[("x",), ("y",)])
        with (
            patch("drt.state.warehouse._connect", return_value=conn),
            pytest.raises(DlqCapacityExceeded),
        ):
            self._backend()._append_strict(
                "s", [_letter("x")], max_records=2, incoming_ids={"x", "z"}
            )
        conn.rollback.assert_called_once()
