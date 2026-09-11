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
Five tables, one per Protocol:

- ``_drt_runs`` — one row per sync, upserted (:class:`PostgresWarehouseStateStore`).
- ``_drt_history`` — one row per run, append-only (:class:`PostgresWarehouseHistoryStore`).
- ``_drt_dlq`` — one row per dead-letter entry, keyed by id (:class:`PostgresWarehouseDlqBackend`).
- ``_drt_idempotency`` — one row per delivered record, keyed by
  ``(sync_name, idempotency_key)`` (:class:`PostgresWarehouseIdempotencyLedger`,
  #1099). Opt-in via ``state.idempotency: true`` on top of this backend.
- ``_drt_audit_log`` — one row per delivered record, append-only
  (:class:`PostgresComplianceAuditTrail`, #1100). Opt-in via
  ``state.audit_trail.enabled: true``.

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
from drt.state.audit_trail import AuditEntry
from drt.state.dlq import DeadLetter
from drt.state.history import HistoryEntry
from drt.state.manager import SyncState

logger = logging.getLogger(__name__)

_RUNS_TABLE = "_drt_runs"
_HISTORY_TABLE = "_drt_history"
_DLQ_TABLE = "_drt_dlq"
_IDEMPOTENCY_TABLE = "_drt_idempotency"
_AUDIT_LOG_TABLE = "_drt_audit_log"


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


# Batches DlqBackend.append()/.replace()/.reconcile()'s per-entry writes into
# bounded multi-row statements (#1121, caught in Codex review on #1120 as a
# cross-dialect issue, not Snowflake-specific — see warehouse_snowflake.py's
# own module docstring for the parallel note). 2000 mirrors
# destinations/snowflake.py's _MERGE_PARAM_BUDGET — the same conservative,
# empirically-verified-elsewhere budget, well under Postgres's own
# protocol-level 65535-parameter ceiling. Kept file-local rather than a
# shared import: this repo already keeps destinations/databricks.py's
# _NATIVE_PARAM_LIMIT and destinations/snowflake.py's _MERGE_PARAM_BUDGET as
# two separate constants, not one shared one, and reaching from drt/state/
# into drt/destinations/ for either would be the wrong import direction
# (state stores are consumers of destination-shaped data, not the reverse).
_DLQ_PARAM_BUDGET = 2000


def _rows_per_chunk(n_cols: int) -> int:
    return max(1, _DLQ_PARAM_BUDGET // max(1, n_cols))


def _chunked(items: list[Any], size: int) -> list[list[Any]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _ensure_table_exists(
    conn: Any, profile: PostgresProfile, table_name: str, column_defs: str
) -> None:
    """Create ``table_name`` under ``profile.managed_schema`` if it is absent.

    Two race conditions this specifically guards, both confirmed live
    (self-review after Codex hit its usage limit mid-review, #920):

    1. **The escape hatch** (#695/#960 discipline): probe via
       ``managed_table_exists()`` before issuing any ``CREATE`` — a
       pre-provisioned table and a role with no ``CREATE`` privilege on
       tables in this schema must never see the statement at all, exactly
       like ``ensure_managed_schema()`` itself never issues ``CREATE
       SCHEMA`` once the schema already exists.
    2. **Concurrent first use.** ``CREATE TABLE IF NOT EXISTS`` is NOT
       atomic across sessions in Postgres: two sessions can both pass the
       probe above (table doesn't exist yet) and both attempt the CREATE:
       the loser gets a catalog ``UniqueViolation``
       (``pg_type_typname_nsp_index`` or similar), not a graceful no-op.
       Reproduced with 8 concurrent first writes to the same
       never-before-existing table — every run raised on at least one
       thread before this fix. If the table exists after the error, the
       other session won the race; anything else re-raises.
    """
    from psycopg2 import sql as _pgsql

    source = PostgresSource()
    source.ensure_managed_schema(profile)
    if source.managed_table_exists(profile, table_name):
        return
    cur = conn.cursor()
    try:
        cur.execute(
            _pgsql.SQL(f"CREATE TABLE IF NOT EXISTS {{}} ({column_defs})").format(
                _qualified(profile, table_name)
            )
        )
        conn.commit()
    except Exception:
        conn.rollback()
        if not source.managed_table_exists(profile, table_name):
            raise


class PostgresWarehouseStateStore:
    """``StateStore`` backed by a ``_drt_runs`` row per sync (#920)."""

    def __init__(self, profile: PostgresProfile) -> None:
        self._profile = profile

    def _ensure_table(self, conn: Any) -> None:
        _ensure_table_exists(
            conn,
            self._profile,
            _RUNS_TABLE,
            "sync_name TEXT PRIMARY KEY, "
            "last_run_at TEXT NOT NULL, "
            "records_synced BIGINT NOT NULL, "
            "status TEXT NOT NULL, "
            "error TEXT, "
            "last_cursor_value TEXT",
        )

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
        _ensure_table_exists(
            conn,
            self._profile,
            _HISTORY_TABLE,
            # No serial/identity primary key: HistoryEntry has no id
            # concept of its own (unlike DeadLetter), and a BIGSERIAL
            # column's implicit sequence needs its own GRANT USAGE, which
            # the escape hatch's plain table-privilege grants
            # (SELECT/INSERT/UPDATE/DELETE) don't cover — caught live by
            # test_writes_succeed_with_preprovisioned_tables_and_no_create_
            # privilege failing with "permission denied for sequence"
            # before this fix.
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
            "sync_run_id TEXT",
        )

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
        _ensure_table_exists(
            conn,
            self._profile,
            _DLQ_TABLE,
            # No BIGSERIAL "seq" column: its implicit sequence needs its own
            # GRANT USAGE the escape hatch's plain table-privilege grants
            # don't cover (same issue _drt_history's id column had — see
            # that table's comment). FIFO ordering instead uses (ts, id),
            # both already-present columns with no sequence dependency.
            "id TEXT PRIMARY KEY, "
            "sync_name TEXT NOT NULL, "
            "record JSONB NOT NULL, "
            "error_message TEXT NOT NULL, "
            "http_status INTEGER, "
            "ts TEXT NOT NULL, "
            "attempts INTEGER NOT NULL, "
            "sync_run_id TEXT",
        )

    def append(
        self, sync_name: str, entries: list[DeadLetter], *, max_records: int = 10_000
    ) -> int:
        from psycopg2 import sql as _pgsql

        if not entries:
            return self.depth(sync_name)
        # Two entries sharing an id within one call would make Postgres
        # raise "ON CONFLICT DO UPDATE command cannot affect row a second
        # time" inside one multi-row statement — possible for legacy,
        # pre-#955 dead letters whose id is a content hash rather than a
        # random uuid (the same collision class Codex review caught on
        # #1128's Round 1 attempt at drt/cli/commands/retry.py). The old
        # per-entry loop tolerated this silently (each duplicate re-executed
        # its own upsert, last one winning); deduping here by id, keeping
        # the last occurrence, preserves that exact outcome.
        deduped: dict[str, DeadLetter] = {}
        for entry in entries:
            deduped[entry.id] = entry
        conn = _connect(self._profile)
        try:
            self._ensure_table(conn)
            cur = conn.cursor()
            for chunk in _chunked(list(deduped.values()), _rows_per_chunk(8)):
                values_sql = ", ".join(["(%s, %s, %s, %s, %s, %s, %s, %s)"] * len(chunk))
                params: list[Any] = []
                for entry in chunk:
                    params.extend(
                        (
                            entry.id,
                            sync_name,
                            json.dumps(entry.record),
                            entry.error_message,
                            entry.http_status,
                            entry.timestamp,
                            entry.attempts,
                            entry.sync_run_id,
                        )
                    )
                cur.execute(
                    _pgsql.SQL(
                        "INSERT INTO {} (id, sync_name, record, error_message, "
                        "http_status, ts, attempts, sync_run_id) "
                        f"VALUES {values_sql} "
                        "ON CONFLICT (id) DO UPDATE SET "
                        "record = EXCLUDED.record, error_message = EXCLUDED.error_message, "
                        "http_status = EXCLUDED.http_status, ts = EXCLUDED.ts, "
                        "attempts = EXCLUDED.attempts, sync_run_id = EXCLUDED.sync_run_id"
                    ).format(_qualified(self._profile, _DLQ_TABLE)),
                    params,
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
        """Wholesale-replace the queue in one transaction.

        Deliberately does NOT delete-then-call ``append()`` on a second
        connection: that would commit the delete before the insert even
        starts, so a connection drop, permission error, or process crash
        between the two calls would permanently erase the queue without
        ever writing the replacement — caught in Codex review on this PR.
        One connection, one commit: either the whole replacement lands or
        none of it does.
        """
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
            for chunk in _chunked(entries, _rows_per_chunk(8)):
                values_sql = ", ".join(["(%s, %s, %s, %s, %s, %s, %s, %s)"] * len(chunk))
                params: list[Any] = []
                for entry in chunk:
                    params.extend(
                        (
                            entry.id,
                            sync_name,
                            json.dumps(entry.record),
                            entry.error_message,
                            entry.http_status,
                            entry.timestamp,
                            entry.attempts,
                            entry.sync_run_id,
                        )
                    )
                cur.execute(
                    _pgsql.SQL(
                        "INSERT INTO {} (id, sync_name, record, error_message, "
                        "http_status, ts, attempts, sync_run_id) "
                        f"VALUES {values_sql}"
                    ).format(_qualified(self._profile, _DLQ_TABLE)),
                    params,
                )
            conn.commit()
        finally:
            conn.close()

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
            for chunk in _chunked(list(updates.items()), _rows_per_chunk(7)):
                values_sql = ", ".join(
                    [
                        "(%s::text, %s::jsonb, %s::text, %s::integer, "
                        "%s::text, %s::integer, %s::text)"
                    ]
                    * len(chunk)
                )
                params: list[Any] = []
                for entry_id, entry in chunk:
                    params.extend(
                        (
                            entry_id,
                            json.dumps(entry.record),
                            entry.error_message,
                            entry.http_status,
                            entry.timestamp,
                            entry.attempts,
                            entry.sync_run_id,
                        )
                    )
                params.append(sync_name)
                cur.execute(
                    _pgsql.SQL(
                        "UPDATE {table} AS t SET "
                        "record = v.record, error_message = v.error_message, "
                        "http_status = v.http_status, ts = v.ts, "
                        "attempts = v.attempts, sync_run_id = v.sync_run_id "
                        f"FROM (VALUES {values_sql}) AS v(id, record, error_message, "
                        "http_status, ts, attempts, sync_run_id) "
                        "WHERE t.sync_name = %s AND t.id = v.id"
                    ).format(table=_qualified(self._profile, _DLQ_TABLE)),
                    params,
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


class PostgresWarehouseIdempotencyLedger:
    """``IdempotencyLedger`` backed by a ``_drt_idempotency`` row per delivered
    record, keyed by ``(sync_name, idempotency_key)`` (#1099).

    See ``drt/state/idempotency.py``'s module docstring for the write-path
    contract this implements (check-then-send-then-mark) and why marking is
    a separate, later call from checking rather than one atomic claim.
    """

    def __init__(self, profile: PostgresProfile) -> None:
        self._profile = profile

    def _ensure_table(self, conn: Any) -> None:
        _ensure_table_exists(
            conn,
            self._profile,
            _IDEMPOTENCY_TABLE,
            "sync_name TEXT NOT NULL, "
            "idempotency_key TEXT NOT NULL, "
            "delivered_at TEXT NOT NULL, "
            "PRIMARY KEY (sync_name, idempotency_key)",
        )

    def already_delivered(self, sync_name: str, keys: Collection[str]) -> set[str]:
        from psycopg2 import sql as _pgsql

        keys = list(keys)
        if not keys:
            return set()
        conn = _connect(self._profile)
        try:
            cur = conn.cursor()
            try:
                cur.execute(
                    _pgsql.SQL(
                        "SELECT idempotency_key FROM {} "
                        "WHERE sync_name = %s AND idempotency_key = ANY(%s)"
                    ).format(_qualified(self._profile, _IDEMPOTENCY_TABLE)),
                    (sync_name, keys),
                )
            except Exception as exc:
                if _is_undefined_table(exc):
                    return set()
                raise
            return {row[0] for row in cur.fetchall()}
        finally:
            conn.close()

    def mark_delivered(self, sync_name: str, keys: Collection[str], delivered_at: str) -> None:
        """Best-effort, like ``HistoryStore.append`` — see the Protocol."""
        from psycopg2 import sql as _pgsql

        keys = list(keys)
        if not keys:
            return
        try:
            conn = _connect(self._profile)
            try:
                self._ensure_table(conn)
                cur = conn.cursor()
                cur.executemany(
                    _pgsql.SQL(
                        "INSERT INTO {} (sync_name, idempotency_key, delivered_at) "
                        "VALUES (%s, %s, %s) ON CONFLICT (sync_name, idempotency_key) DO NOTHING"
                    ).format(_qualified(self._profile, _IDEMPOTENCY_TABLE)),
                    [(sync_name, key, delivered_at) for key in keys],
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:  # noqa: BLE001 - best-effort, see Protocol docstring
            logger.warning("idempotency ledger mark failed for sync=%s: %s", sync_name, exc)

    def prune(self, sync_name: str, retention_days: int) -> int:
        from psycopg2 import sql as _pgsql

        cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).isoformat()
        conn = _connect(self._profile)
        try:
            cur = conn.cursor()
            try:
                cur.execute(
                    _pgsql.SQL("DELETE FROM {} WHERE sync_name = %s AND delivered_at < %s").format(
                        _qualified(self._profile, _IDEMPOTENCY_TABLE)
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


class PostgresComplianceAuditTrail:
    """``ComplianceAuditTrail`` backed by an append-only ``_drt_audit_log``
    table, one row per delivered record (#1100).

    No primary key, matching ``_drt_history``'s reasoning: an identity
    column's implicit sequence needs its own ``GRANT USAGE`` that the
    escape hatch's plain table-privilege grants don't cover, and this table
    has no per-row identity concept of its own to key on anyway (unlike the
    DLQ's ``id``, needed for retry-by-identity).
    """

    def __init__(self, profile: PostgresProfile) -> None:
        self._profile = profile

    def _ensure_table(self, conn: Any) -> None:
        _ensure_table_exists(
            conn,
            self._profile,
            _AUDIT_LOG_TABLE,
            "sync_name TEXT NOT NULL, "
            "run_id TEXT, "
            # Nullable like run_id: always generated by run_sync() in
            # practice, but SyncResult.sync_run_id's own type is honestly
            # optional (matches DeadLetter.sync_run_id) rather than a
            # guarantee this table should enforce.
            "sync_run_id TEXT, "
            "record_key TEXT NOT NULL, "
            "logged_fields_json JSONB NOT NULL, "
            "destination_type TEXT NOT NULL, "
            "delivered_at TEXT NOT NULL",
        )

    def log_delivered(
        self,
        sync_name: str,
        run_id: str | None,
        sync_run_id: str | None,
        destination_type: str,
        entries: list[AuditEntry],
        delivered_at: str,
    ) -> None:
        """Best-effort, like ``HistoryStore.append`` — see the Protocol."""
        from psycopg2 import sql as _pgsql

        if not entries:
            return
        try:
            conn = _connect(self._profile)
            try:
                self._ensure_table(conn)
                cur = conn.cursor()
                cur.executemany(
                    _pgsql.SQL(
                        "INSERT INTO {} (sync_name, run_id, sync_run_id, record_key, "
                        "logged_fields_json, destination_type, delivered_at) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    ).format(_qualified(self._profile, _AUDIT_LOG_TABLE)),
                    [
                        (
                            sync_name,
                            run_id,
                            sync_run_id,
                            entry.record_key,
                            json.dumps(entry.fields),
                            destination_type,
                            delivered_at,
                        )
                        for entry in entries
                    ],
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:  # noqa: BLE001 - best-effort, see Protocol docstring
            logger.warning("compliance audit log write failed for sync=%s: %s", sync_name, exc)

    def prune(self, sync_name: str, retain_days: int) -> int:
        from psycopg2 import sql as _pgsql

        cutoff = (datetime.now(timezone.utc) - timedelta(days=retain_days)).isoformat()
        conn = _connect(self._profile)
        try:
            cur = conn.cursor()
            try:
                cur.execute(
                    _pgsql.SQL("DELETE FROM {} WHERE sync_name = %s AND delivered_at < %s").format(
                        _qualified(self._profile, _AUDIT_LOG_TABLE)
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
