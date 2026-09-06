"""Warehouse-backed state, history, and DLQ stores — Postgres-first (#920, ADR 0005 step 4).

Reuses an existing connection profile (``state.connection_profile`` in
``profiles.yml``) instead of a bucket, so drt's own run-state, execution
history, and dead-letter queue become SQL-queryable alongside the data the
project already syncs. Built directly on the managed-table primitive from
#960 (``ManagedTableCapable``): schema creation goes through
``PostgresSource.ensure_managed_schema()``, following the same
probe-before-DDL / pre-provisioned-schema escape hatch discipline.

**Table DDL lives here, not on the primitive** — #960's `ManagedTableCapable`
deliberately owns only schema/table *existence* checking, not column
definitions (see its docstring), so each consumer hardcodes its own tables.
Three tables, one per Protocol:

- ``_drt_runs`` — one row per sync, upserted (:class:`PostgresWarehouseStateStore`).
- ``_drt_history`` — one row per run, append-only (:class:`PostgresWarehouseHistoryStore`).
- ``_drt_dlq`` — one row per dead-letter entry, keyed by id (:class:`PostgresWarehouseDlqBackend`).

**No retry/precondition machinery, unlike the object-store backends**
(``drt/state/_objectstore.py``). A warehouse ``INSERT ... ON CONFLICT DO
UPDATE`` is atomic per row — Postgres's own row lock arbitrates concurrent
writers, so there is no read-modify-write race to retry and
``StateContentionError`` is never raised by this backend. That is a genuine
simplification, not a gap: the failure class #919 exists to prevent (two
writers silently clobbering each other via a client-side read-modify-write)
structurally cannot happen when the database does the read-modify-write
atomically server-side.

Each method opens and closes its own connection, matching
``ManagedTableCapable``'s own style — state operations are not a hot path
(at most once per sync, per batch, or per CLI invocation).

**Two known, cross-backend limitations raised in Codex review, checked
against existing precedent rather than fixed here:**

- **No enforced per-project namespace.** Rows are keyed by ``sync_name``
  alone, so two drt projects sharing one ``connection_profile`` +
  ``managed_schema`` and a common sync name will collide. This is not novel
  to this backend: ``gcs``/``s3`` have the identical property today —
  ``state.prefix`` is an optional, undocumented-as-required operator
  convention for exactly this isolation (``docs/guides/remote-state.md``'s
  own examples use a project-specific prefix like
  ``production/customer-activation``), not an enforced tenant key. The
  warehouse backend's equivalent knob is ``PostgresProfile.managed_schema``
  itself, already settable per profile since #960 — operators sharing one
  Postgres database across projects should give each project's profile a
  distinct ``managed_schema``, the same convention as a distinct ``prefix``.
- **Concurrent runs of the *same* sync can move a cursor backward.** A
  slower run's ``INSERT ... ON CONFLICT DO UPDATE`` can commit after a
  faster, newer run's — the ``ON CONFLICT`` clause is atomic per statement
  but still last-writer-wins across statements, with no ordering guarantee
  between them. This is also not novel: ``drt/engine/observer.py``'s own
  ``on_sync_completed`` comment already documents this exact race as an
  accepted, cross-backend gap ("true cross-process atomicity needs a
  CAS/generation-token primitive the StateStore Protocol doesn't have yet
  ... a run that reads here, loses a race to a concurrent writer, and then
  writes anyway can still regress the cursor") — local/gcs/s3 share the
  identical exposure today. Closing it needs a StateStore Protocol change
  (a compare-and-set primitive all four backends would implement), not a
  per-backend fix here.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Collection, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from drt.config.credentials import PostgresProfile
from drt.sources.postgres import PostgresSource
from drt.state.dlq import DeadLetter
from drt.state.history import HistoryEntry
from drt.state.manager import SyncState

logger = logging.getLogger(__name__)

_RUNS_TABLE = "_drt_runs"
_HISTORY_TABLE = "_drt_history"
_DLQ_TABLE = "_drt_dlq"


def _connect(profile: PostgresProfile) -> Any:
    # Delegates to PostgresSource's own connection builder rather than
    # duplicating psycopg2.connect(...) + resolve_env(...) here — one place
    # decides how a PostgresProfile becomes a live connection.
    return PostgresSource()._connect(profile)


def _qualified(profile: PostgresProfile, table: str) -> Any:
    from psycopg2 import sql as _pgsql

    return _pgsql.SQL("{}.{}").format(
        _pgsql.Identifier(profile.managed_schema), _pgsql.Identifier(table)
    )


def _is_undefined_table(exc: Exception) -> bool:
    import psycopg2

    return isinstance(exc, psycopg2.errors.UndefinedTable)


class PostgresWarehouseStateStore:
    """``StateStore`` backed by a ``_drt_runs`` row per sync (#920)."""

    def __init__(self, profile: PostgresProfile) -> None:
        self._profile = profile

    def _ensure_table(self, conn: Any) -> None:
        from psycopg2 import sql as _pgsql

        source = PostgresSource()
        source.ensure_managed_schema(self._profile)
        # Probe first (#695/#960 discipline): a pre-provisioned table and a
        # role with no CREATE privilege on tables in this schema must never
        # see the CREATE statement at all, exactly like ensure_managed_schema()
        # itself never issues CREATE SCHEMA once the schema already exists.
        if source.managed_table_exists(self._profile, _RUNS_TABLE):
            return
        cur = conn.cursor()
        cur.execute(
            _pgsql.SQL(
                "CREATE TABLE IF NOT EXISTS {} ("
                "sync_name TEXT PRIMARY KEY, "
                "last_run_at TEXT NOT NULL, "
                "records_synced BIGINT NOT NULL, "
                "status TEXT NOT NULL, "
                "error TEXT, "
                "last_cursor_value TEXT"
                ")"
            ).format(_qualified(self._profile, _RUNS_TABLE))
        )
        conn.commit()

    def get_last_sync(self, sync_name: str) -> SyncState | None:
        conn = _connect(self._profile)
        try:
            cur = conn.cursor()
            try:
                cur.execute(
                    _pgsql_select_runs().format(_qualified(self._profile, _RUNS_TABLE)),
                    (sync_name,),
                )
            except Exception as exc:
                if _is_undefined_table(exc):
                    return None
                raise
            row = cur.fetchone()
            if row is None:
                return None
            return _row_to_sync_state(sync_name, row)
        finally:
            conn.close()

    def get_all(self) -> dict[str, SyncState]:
        conn = _connect(self._profile)
        try:
            cur = conn.cursor()
            try:
                cur.execute(_pgsql_select_all_runs().format(_qualified(self._profile, _RUNS_TABLE)))
            except Exception as exc:
                if _is_undefined_table(exc):
                    return {}
                raise
            return {row[0]: _row_to_sync_state(row[0], row[1:]) for row in cur.fetchall()}
        finally:
            conn.close()

    def save_sync(self, state: SyncState) -> None:
        from psycopg2 import sql as _pgsql

        conn = _connect(self._profile)
        try:
            self._ensure_table(conn)
            cur = conn.cursor()
            cur.execute(
                _pgsql.SQL(
                    "INSERT INTO {} (sync_name, last_run_at, records_synced, status, "
                    "error, last_cursor_value) VALUES (%s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (sync_name) DO UPDATE SET "
                    "last_run_at = EXCLUDED.last_run_at, "
                    "records_synced = EXCLUDED.records_synced, "
                    "status = EXCLUDED.status, "
                    "error = EXCLUDED.error, "
                    "last_cursor_value = EXCLUDED.last_cursor_value"
                ).format(_qualified(self._profile, _RUNS_TABLE)),
                (
                    state.sync_name,
                    state.last_run_at,
                    state.records_synced,
                    state.status,
                    state.error,
                    state.last_cursor_value,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def reset(self, sync_name: str) -> bool:
        from psycopg2 import sql as _pgsql

        conn = _connect(self._profile)
        try:
            cur = conn.cursor()
            try:
                cur.execute(
                    _pgsql.SQL("DELETE FROM {} WHERE sync_name = %s").format(
                        _qualified(self._profile, _RUNS_TABLE)
                    ),
                    (sync_name,),
                )
            except Exception as exc:
                if _is_undefined_table(exc):
                    return False
                raise
            deleted = bool(cur.rowcount > 0)
            conn.commit()
            return deleted
        finally:
            conn.close()

    def now(self) -> str:
        return datetime.now(timezone.utc).isoformat()


def _pgsql_select_runs() -> Any:
    from psycopg2 import sql as _pgsql

    return _pgsql.SQL(
        "SELECT last_run_at, records_synced, status, error, last_cursor_value "
        "FROM {} WHERE sync_name = %s"
    )


def _pgsql_select_all_runs() -> Any:
    from psycopg2 import sql as _pgsql

    return _pgsql.SQL(
        "SELECT sync_name, last_run_at, records_synced, status, error, last_cursor_value FROM {}"
    )


def _row_to_sync_state(sync_name: str, row: tuple[Any, ...]) -> SyncState:
    last_run_at, records_synced, status, error, last_cursor_value = row
    return SyncState(
        sync_name=sync_name,
        last_run_at=last_run_at,
        records_synced=records_synced,
        status=status,
        error=error,
        last_cursor_value=last_cursor_value,
    )


class PostgresWarehouseHistoryStore:
    """``HistoryStore`` backed by an append-only ``_drt_history`` table (#920)."""

    def __init__(self, profile: PostgresProfile) -> None:
        self._profile = profile

    def _ensure_table(self, conn: Any) -> None:
        from psycopg2 import sql as _pgsql

        source = PostgresSource()
        source.ensure_managed_schema(self._profile)
        if source.managed_table_exists(self._profile, _HISTORY_TABLE):
            return
        cur = conn.cursor()
        cur.execute(
            _pgsql.SQL(
                # No serial/identity primary key: HistoryEntry has no id
                # concept of its own (unlike DeadLetter), and a BIGSERIAL
                # column's implicit sequence needs its own GRANT USAGE,
                # which the escape hatch's plain table-privilege grants
                # (SELECT/INSERT/UPDATE/DELETE) don't cover — caught live
                # by test_writes_succeed_with_preprovisioned_tables_and_no_
                # create_privilege failing with "permission denied for
                # sequence" before this fix.
                "CREATE TABLE IF NOT EXISTS {} ("
                "sync_name TEXT NOT NULL, "
                "started_at TEXT NOT NULL, "
                "completed_at TEXT NOT NULL, "
                "duration_seconds DOUBLE PRECISION NOT NULL, "
                "status TEXT NOT NULL, "
                "records_synced BIGINT NOT NULL, "
                "records_failed BIGINT NOT NULL, "
                "errors JSONB NOT NULL DEFAULT '[]', "
                "cursor_value_used TEXT, "
                "dry_run BOOLEAN NOT NULL DEFAULT FALSE, "
                "run_id TEXT, "
                "sync_run_id TEXT"
                ")"
            ).format(_qualified(self._profile, _HISTORY_TABLE))
        )
        conn.commit()

    def append(self, entry: HistoryEntry) -> None:
        """Best-effort, like every other ``HistoryStore`` — see the Protocol."""
        from psycopg2 import sql as _pgsql

        try:
            conn = _connect(self._profile)
            try:
                self._ensure_table(conn)
                cur = conn.cursor()
                cur.execute(
                    _pgsql.SQL(
                        "INSERT INTO {} (sync_name, started_at, completed_at, "
                        "duration_seconds, status, records_synced, records_failed, "
                        "errors, cursor_value_used, dry_run, run_id, sync_run_id) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    ).format(_qualified(self._profile, _HISTORY_TABLE)),
                    (
                        entry.sync_name,
                        entry.started_at,
                        entry.completed_at,
                        entry.duration_seconds,
                        entry.status,
                        entry.records_synced,
                        entry.records_failed,
                        json.dumps(entry.errors[:5]),
                        entry.cursor_value_used,
                        entry.dry_run,
                        entry.run_id,
                        entry.sync_run_id,
                    ),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:  # noqa: BLE001 - best-effort, see Protocol docstring
            logger.warning("warehouse history append failed for sync=%s: %s", entry.sync_name, exc)

    def read(self, sync_name: str | None = None, limit: int = 20) -> list[HistoryEntry]:
        from psycopg2 import sql as _pgsql

        conn = _connect(self._profile)
        try:
            cur = conn.cursor()
            columns = (
                "sync_name, started_at, completed_at, duration_seconds, status, "
                "records_synced, records_failed, errors, cursor_value_used, dry_run, "
                "run_id, sync_run_id"
            )
            try:
                if sync_name is not None:
                    cur.execute(
                        _pgsql.SQL(
                            f"SELECT {columns} FROM {{}} WHERE sync_name = %s "
                            "ORDER BY started_at DESC LIMIT %s"
                        ).format(_qualified(self._profile, _HISTORY_TABLE)),
                        (sync_name, limit),
                    )
                else:
                    cur.execute(
                        _pgsql.SQL(
                            f"SELECT {columns} FROM {{}} ORDER BY started_at DESC LIMIT %s"
                        ).format(_qualified(self._profile, _HISTORY_TABLE)),
                        (limit,),
                    )
            except Exception as exc:
                if _is_undefined_table(exc):
                    return []
                raise
            return [_row_to_history_entry(row) for row in cur.fetchall()]
        finally:
            conn.close()

    def prune(self, sync_name: str, retention_days: int) -> int:
        from psycopg2 import sql as _pgsql

        cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).isoformat()
        conn = _connect(self._profile)
        try:
            cur = conn.cursor()
            try:
                cur.execute(
                    _pgsql.SQL("DELETE FROM {} WHERE sync_name = %s AND started_at < %s").format(
                        _qualified(self._profile, _HISTORY_TABLE)
                    ),
                    (sync_name, cutoff),
                )
            except Exception as exc:
                if _is_undefined_table(exc):
                    return 0
                raise
            removed = int(cur.rowcount)
            conn.commit()
            return removed
        finally:
            conn.close()


def _row_to_history_entry(row: tuple[Any, ...]) -> HistoryEntry:
    (
        sync_name,
        started_at,
        completed_at,
        duration_seconds,
        status,
        records_synced,
        records_failed,
        errors,
        cursor_value_used,
        dry_run,
        run_id,
        sync_run_id,
    ) = row
    return HistoryEntry(
        sync_name=sync_name,
        started_at=started_at,
        completed_at=completed_at,
        duration_seconds=duration_seconds,
        status=status,
        records_synced=records_synced,
        records_failed=records_failed,
        errors=errors if isinstance(errors, list) else json.loads(errors),
        cursor_value_used=cursor_value_used,
        dry_run=dry_run,
        run_id=run_id,
        sync_run_id=sync_run_id,
    )


class PostgresWarehouseDlqBackend:
    """``DlqBackend`` backed by a ``_drt_dlq`` row per dead-letter entry (#920)."""

    def __init__(self, profile: PostgresProfile) -> None:
        self._profile = profile

    def _ensure_table(self, conn: Any) -> None:
        from psycopg2 import sql as _pgsql

        source = PostgresSource()
        source.ensure_managed_schema(self._profile)
        if source.managed_table_exists(self._profile, _DLQ_TABLE):
            return
        cur = conn.cursor()
        cur.execute(
            # No BIGSERIAL "seq" column: its implicit sequence needs its own
            # GRANT USAGE the escape hatch's plain table-privilege grants
            # don't cover (same issue _drt_history's id column had — see
            # that table's comment). FIFO ordering instead uses (ts, id),
            # both already-present columns with no sequence dependency.
            _pgsql.SQL(
                "CREATE TABLE IF NOT EXISTS {} ("
                "id TEXT PRIMARY KEY, "
                "sync_name TEXT NOT NULL, "
                "record JSONB NOT NULL, "
                "error_message TEXT NOT NULL, "
                "http_status INTEGER, "
                "ts TEXT NOT NULL, "
                "attempts INTEGER NOT NULL, "
                "sync_run_id TEXT"
                ")"
            ).format(_qualified(self._profile, _DLQ_TABLE))
        )
        conn.commit()

    def append(
        self, sync_name: str, entries: list[DeadLetter], *, max_records: int = 10_000
    ) -> int:
        from psycopg2 import sql as _pgsql

        if not entries:
            return self.depth(sync_name)
        conn = _connect(self._profile)
        try:
            self._ensure_table(conn)
            cur = conn.cursor()
            for entry in entries:
                cur.execute(
                    _pgsql.SQL(
                        "INSERT INTO {} (id, sync_name, record, error_message, "
                        "http_status, ts, attempts, sync_run_id) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                        "ON CONFLICT (id) DO UPDATE SET "
                        "record = EXCLUDED.record, error_message = EXCLUDED.error_message, "
                        "http_status = EXCLUDED.http_status, ts = EXCLUDED.ts, "
                        "attempts = EXCLUDED.attempts, sync_run_id = EXCLUDED.sync_run_id"
                    ).format(_qualified(self._profile, _DLQ_TABLE)),
                    (
                        entry.id,
                        sync_name,
                        json.dumps(entry.record),
                        entry.error_message,
                        entry.http_status,
                        entry.timestamp,
                        entry.attempts,
                        entry.sync_run_id,
                    ),
                )
            if max_records > 0:
                cur.execute(
                    _pgsql.SQL(
                        "DELETE FROM {t} WHERE sync_name = %s AND id NOT IN ("
                        "SELECT id FROM {t} WHERE sync_name = %s "
                        "ORDER BY ts DESC, id DESC LIMIT %s)"
                    ).format(t=_qualified(self._profile, _DLQ_TABLE)),
                    (sync_name, sync_name, max_records),
                )
            conn.commit()
            return self.depth(sync_name)
        finally:
            conn.close()

    def replace(self, sync_name: str, entries: list[DeadLetter]) -> None:
        from psycopg2 import sql as _pgsql

        conn = _connect(self._profile)
        try:
            self._ensure_table(conn)
            cur = conn.cursor()
            cur.execute(
                _pgsql.SQL("DELETE FROM {} WHERE sync_name = %s").format(
                    _qualified(self._profile, _DLQ_TABLE)
                ),
                (sync_name,),
            )
            conn.commit()
        finally:
            conn.close()
        if entries:
            self.append(sync_name, entries, max_records=0)

    def clear(self, sync_name: str) -> None:
        self.replace(sync_name, [])

    def read(self, sync_name: str) -> list[DeadLetter]:
        from psycopg2 import sql as _pgsql

        conn = _connect(self._profile)
        try:
            cur = conn.cursor()
            try:
                cur.execute(
                    _pgsql.SQL(
                        "SELECT id, record, error_message, http_status, ts, "
                        "attempts, sync_run_id FROM {} WHERE sync_name = %s "
                        "ORDER BY ts ASC, id ASC"
                    ).format(_qualified(self._profile, _DLQ_TABLE)),
                    (sync_name,),
                )
            except Exception as exc:
                if _is_undefined_table(exc):
                    return []
                raise
            return [_row_to_dead_letter(row) for row in cur.fetchall()]
        finally:
            conn.close()

    def depth(self, sync_name: str) -> int:
        from psycopg2 import sql as _pgsql

        conn = _connect(self._profile)
        try:
            cur = conn.cursor()
            try:
                cur.execute(
                    _pgsql.SQL("SELECT COUNT(*) FROM {} WHERE sync_name = %s").format(
                        _qualified(self._profile, _DLQ_TABLE)
                    ),
                    (sync_name,),
                )
            except Exception as exc:
                if _is_undefined_table(exc):
                    return 0
                raise
            row = cur.fetchone()
            return int(row[0]) if row else 0
        finally:
            conn.close()

    def all_depths(self) -> dict[str, int]:
        from psycopg2 import sql as _pgsql

        conn = _connect(self._profile)
        try:
            cur = conn.cursor()
            try:
                cur.execute(
                    _pgsql.SQL("SELECT sync_name, COUNT(*) FROM {} GROUP BY sync_name").format(
                        _qualified(self._profile, _DLQ_TABLE)
                    )
                )
            except Exception as exc:
                if _is_undefined_table(exc):
                    return {}
                raise
            return {row[0]: int(row[1]) for row in cur.fetchall() if row[1]}
        finally:
            conn.close()

    def reconcile(
        self,
        sync_name: str,
        *,
        remove_ids: Collection[str] = (),
        updates: Mapping[str, DeadLetter] | None = None,
    ) -> list[DeadLetter]:
        from psycopg2 import sql as _pgsql

        updates = updates or {}
        conn = _connect(self._profile)
        try:
            self._ensure_table(conn)
            cur = conn.cursor()
            if remove_ids:
                cur.execute(
                    _pgsql.SQL("DELETE FROM {} WHERE sync_name = %s AND id = ANY(%s)").format(
                        _qualified(self._profile, _DLQ_TABLE)
                    ),
                    (sync_name, list(remove_ids)),
                )
            for entry_id, entry in updates.items():
                cur.execute(
                    _pgsql.SQL(
                        "UPDATE {} SET record = %s, error_message = %s, "
                        "http_status = %s, ts = %s, attempts = %s, sync_run_id = %s "
                        "WHERE sync_name = %s AND id = %s"
                    ).format(_qualified(self._profile, _DLQ_TABLE)),
                    (
                        json.dumps(entry.record),
                        entry.error_message,
                        entry.http_status,
                        entry.timestamp,
                        entry.attempts,
                        entry.sync_run_id,
                        sync_name,
                        entry_id,
                    ),
                )
            conn.commit()
        finally:
            conn.close()
        return self.read(sync_name)


def _row_to_dead_letter(row: tuple[Any, ...]) -> DeadLetter:
    entry_id, record, error_message, http_status, ts, attempts, sync_run_id = row
    return DeadLetter(
        record=record if isinstance(record, dict) else json.loads(record),
        error_message=error_message,
        http_status=http_status,
        timestamp=ts,
        attempts=attempts,
        sync_run_id=sync_run_id,
        id=entry_id,
    )
