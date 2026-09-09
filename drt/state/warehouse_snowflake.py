"""Warehouse-backed state, history, and DLQ stores — Snowflake leg (#1106).

Snowflake counterpart to ``drt/state/warehouse.py``'s Postgres-first
implementation (#920, ADR 0005 step 4) — same three tables
(``_drt_runs``/``_drt_history``/``_drt_dlq``), same ``StateStore``/
``HistoryStore``/``DlqBackend`` Protocols, built on the same
``ManagedTableCapable`` primitive (#960/#1106's ``SnowflakeSource`` leg).
Kept in its own module rather than added to ``warehouse.py``, mirroring how
``drt/sources/postgres.py``/``drt/sources/snowflake.py`` are already split
one file per dialect.

**Design decisions specific to this dialect** (posted to #1106 before
implementation):

- **`MERGE` replaces `INSERT ... ON CONFLICT DO UPDATE`** — Snowflake has no
  `ON CONFLICT`. `_drt_runs` merges on `sync_name`, `_drt_dlq` merges on
  `id`; both single-row `MERGE ... WHEN MATCHED / WHEN NOT MATCHED`,
  matching the shape `drt/destinations/snowflake.py`'s own mirror MERGE
  already establishes for this connector.
- **Explicit transactions only where more than one DML statement must land
  together.** Snowflake's connector autocommits every statement
  individually by default (confirmed: no `.commit()` call anywhere in
  `drt/destinations/snowflake.py`), unlike psycopg2's default of one
  transaction per connection until an explicit `commit()`. A single-
  statement method (`save_sync`, `reset`, `HistoryStore.append`,
  `HistoryStore.prune`) needs nothing extra — one autocommitted statement is
  already atomic. `DlqBackend.append` (N upserts + a prune `DELETE`),
  `.replace()` (`DELETE` + N inserts), and `.reconcile()` (N removes/
  updates) each wrap their connection with `conn.autocommit(False)` /
  explicit `commit()`-or-`rollback()` (see `_snowflake_transaction` below)
  so a crash partway through can't leave a half-applied write — the
  `.replace()` case specifically is the #955 failure class (a crash between
  an unwrapped delete-then-insert permanently erasing the queue).
- **`errors`/`record` are `VARIANT`, not `JSONB`.** Snowflake disallows
  function calls (`PARSE_JSON`) in an `INSERT ... VALUES` literal list, so
  any write touching one of these columns uses the `INSERT ... SELECT`
  form instead (same `PARSE_JSON` wrap site already established in
  `drt/destinations/snowflake.py` for JSON-category columns). Reading back,
  whether the connector hands a VARIANT column back as an already-parsed
  Python object or a JSON-encoded string is unverified by any prior art in
  this codebase (the existing complex-type smoke test sidesteps the
  question entirely via SQL-side `::STRING` accessors) — `_json_or_parsed`
  below handles both, confirmed live by this leg's own smoke test.
- **No `_is_undefined_table`-style exception classification.** Postgres's
  implementation catches `psycopg2.errors.UndefinedTable` to make a read
  against a never-written store return empty rather than raise. Snowflake's
  equivalent driver exception shape has no precedent in this codebase to
  build on with confidence, so every read here probes
  `managed_table_exists()` first instead — one extra round trip on every
  call (state reads are not a hot path, per `warehouse.py`'s own docstring)
  in exchange for not guessing at unverified driver internals.
- **No `conn.rollback()` inside `_ensure_table_exists`'s race guard** — same
  reasoning as `SnowflakeSource.ensure_managed_schema()` (#1106): DDL
  autocommits with no savepoints, so a failed `CREATE TABLE` leaves nothing
  open to roll back.

Cross-dialect limitations documented on `warehouse.py` (no enforced
per-project namespace; concurrent runs of the same sync can move a cursor
backward under last-writer-wins) apply identically here — see that module's
docstring rather than repeating it.

**One more, raised in Codex review and checked against precedent rather than
fixed here:** `DlqBackend.append`/`.replace()`/`.reconcile()` issue one
`MERGE`/`INSERT`/`UPDATE` per dead-letter entry rather than a bounded
multi-row batch — a sync producing thousands of dead letters means
thousands of round trips. This is not a Snowflake-specific regression:
`warehouse.py`'s Postgres implementation has the identical one-statement-
per-entry loop today. Batching either dialect is a genuine improvement
worth making, but doing it only for Snowflake here would leave the two
implementations with different write-volume characteristics for no
principled reason — tracked as a follow-up applying to both (#1121).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Collection, Mapping
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

from drt.config.credentials import SnowflakeProfile
from drt.sources.snowflake import SnowflakeSource
from drt.state.dlq import DeadLetter
from drt.state.history import HistoryEntry
from drt.state.manager import SyncState

logger = logging.getLogger(__name__)

_RUNS_TABLE = "_drt_runs"
_HISTORY_TABLE = "_drt_history"
_DLQ_TABLE = "_drt_dlq"


def _connect(profile: SnowflakeProfile) -> Any:
    return SnowflakeSource()._connect(profile)


def _qualified(profile: SnowflakeProfile, table: str) -> str:
    # Unquoted, matching #1106's ManagedTableCapable convention (Snowflake
    # folds to uppercase; this connector has no quoting helper of its own).
    return f"{profile.database}.{profile.managed_schema}.{table}"


def _table_exists(profile: SnowflakeProfile, table_name: str) -> bool:
    return SnowflakeSource().managed_table_exists(profile, table_name)


@contextmanager
def _snowflake_transaction(conn: Any) -> Any:
    """Explicit transaction for statements that must land together.

    See module docstring — needed only where a method issues more than one
    DML statement that must be atomic; single-statement methods rely on the
    connection's default per-statement autocommit instead.
    """
    conn.autocommit(False)
    try:
        yield
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def _json_or_parsed(value: Any) -> Any:
    """A VARIANT column read back as either a JSON string or an
    already-parsed object, depending on connector behavior not otherwise
    verified in this codebase (see module docstring)."""
    return json.loads(value) if isinstance(value, str) else value


def _ensure_table_exists(
    conn: Any, profile: SnowflakeProfile, table_name: str, column_defs: str
) -> None:
    """Create ``table_name`` under ``profile.managed_schema`` if absent.

    Same escape-hatch + concurrent-first-use discipline as
    ``SnowflakeSource.ensure_managed_schema()`` (#1106): probe before any
    ``CREATE``, and on a ``CREATE`` failure, re-probe rather than assume —
    swallow if another session won the race, re-raise otherwise. No
    ``conn.rollback()``: Snowflake DDL autocommits and has no savepoints.
    """
    source = SnowflakeSource()
    source.ensure_managed_schema(profile)
    if source.managed_table_exists(profile, table_name):
        return
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE TABLE IF NOT EXISTS {_qualified(profile, table_name)} ({column_defs})")
    except Exception:
        if not source.managed_table_exists(profile, table_name):
            raise


class SnowflakeWarehouseStateStore:
    """``StateStore`` backed by a ``_drt_runs`` row per sync (#1106)."""

    def __init__(self, profile: SnowflakeProfile) -> None:
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
        if not _table_exists(self._profile, _RUNS_TABLE):
            return None
        conn = _connect(self._profile)
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT last_run_at, records_synced, status, error, last_cursor_value "
                f"FROM {_qualified(self._profile, _RUNS_TABLE)} WHERE sync_name = %s",
                (sync_name,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            return _row_to_sync_state(sync_name, row)
        finally:
            conn.close()

    def get_all(self) -> dict[str, SyncState]:
        if not _table_exists(self._profile, _RUNS_TABLE):
            return {}
        conn = _connect(self._profile)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT sync_name, last_run_at, records_synced, status, error, "
                f"last_cursor_value FROM {_qualified(self._profile, _RUNS_TABLE)}"
            )
            return {row[0]: _row_to_sync_state(row[0], row[1:]) for row in cur.fetchall()}
        finally:
            conn.close()

    def save_sync(self, state: SyncState) -> None:
        conn = _connect(self._profile)
        try:
            self._ensure_table(conn)
            cur = conn.cursor()
            t = _qualified(self._profile, _RUNS_TABLE)
            cur.execute(
                f"MERGE INTO {t} AS t USING (SELECT %s AS sync_name, %s AS last_run_at, "
                "%s AS records_synced, %s AS status, %s AS error, %s AS last_cursor_value) AS s "
                "ON t.sync_name = s.sync_name "
                "WHEN MATCHED THEN UPDATE SET last_run_at = s.last_run_at, "
                "records_synced = s.records_synced, status = s.status, error = s.error, "
                "last_cursor_value = s.last_cursor_value "
                "WHEN NOT MATCHED THEN INSERT (sync_name, last_run_at, records_synced, "
                "status, error, last_cursor_value) VALUES (s.sync_name, s.last_run_at, "
                "s.records_synced, s.status, s.error, s.last_cursor_value)",
                (
                    state.sync_name,
                    state.last_run_at,
                    state.records_synced,
                    state.status,
                    state.error,
                    state.last_cursor_value,
                ),
            )
        finally:
            conn.close()

    def reset(self, sync_name: str) -> bool:
        if not _table_exists(self._profile, _RUNS_TABLE):
            return False
        conn = _connect(self._profile)
        try:
            cur = conn.cursor()
            cur.execute(
                f"DELETE FROM {_qualified(self._profile, _RUNS_TABLE)} WHERE sync_name = %s",
                (sync_name,),
            )
            return bool(cur.rowcount and cur.rowcount > 0)
        finally:
            conn.close()

    def now(self) -> str:
        return datetime.now(timezone.utc).isoformat()


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


class SnowflakeWarehouseHistoryStore:
    """``HistoryStore`` backed by an append-only ``_drt_history`` table (#1106)."""

    def __init__(self, profile: SnowflakeProfile) -> None:
        self._profile = profile

    def _ensure_table(self, conn: Any) -> None:
        _ensure_table_exists(
            conn,
            self._profile,
            _HISTORY_TABLE,
            "sync_name TEXT NOT NULL, "
            "started_at TEXT NOT NULL, "
            "completed_at TEXT NOT NULL, "
            "duration_seconds DOUBLE NOT NULL, "
            "status TEXT NOT NULL, "
            "records_synced BIGINT NOT NULL, "
            "records_failed BIGINT NOT NULL, "
            "errors VARIANT NOT NULL DEFAULT PARSE_JSON('[]'), "
            "cursor_value_used TEXT, "
            "dry_run BOOLEAN NOT NULL DEFAULT FALSE, "
            "run_id TEXT, "
            "sync_run_id TEXT",
        )

    def append(self, entry: HistoryEntry) -> None:
        """Best-effort, like every other ``HistoryStore`` — see the Protocol."""
        try:
            conn = _connect(self._profile)
            try:
                self._ensure_table(conn)
                cur = conn.cursor()
                t = _qualified(self._profile, _HISTORY_TABLE)
                # SELECT form, not VALUES: Snowflake disallows a function
                # call (PARSE_JSON) inside a VALUES literal list.
                cur.execute(
                    f"INSERT INTO {t} (sync_name, started_at, completed_at, "
                    "duration_seconds, status, records_synced, records_failed, "
                    "errors, cursor_value_used, dry_run, run_id, sync_run_id) "
                    "SELECT %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), %s, %s, %s, %s",
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
            finally:
                conn.close()
        except Exception as exc:  # noqa: BLE001 - best-effort, see Protocol docstring
            logger.warning("warehouse history append failed for sync=%s: %s", entry.sync_name, exc)

    def read(self, sync_name: str | None = None, limit: int = 20) -> list[HistoryEntry]:
        if not _table_exists(self._profile, _HISTORY_TABLE):
            return []
        conn = _connect(self._profile)
        try:
            cur = conn.cursor()
            t = _qualified(self._profile, _HISTORY_TABLE)
            columns = (
                "sync_name, started_at, completed_at, duration_seconds, status, "
                "records_synced, records_failed, errors, cursor_value_used, dry_run, "
                "run_id, sync_run_id"
            )
            if sync_name is not None:
                cur.execute(
                    f"SELECT {columns} FROM {t} WHERE sync_name = %s "
                    "ORDER BY started_at DESC LIMIT %s",
                    (sync_name, limit),
                )
            else:
                cur.execute(
                    f"SELECT {columns} FROM {t} ORDER BY started_at DESC LIMIT %s", (limit,)
                )
            return [_row_to_history_entry(row) for row in cur.fetchall()]
        finally:
            conn.close()

    def prune(self, sync_name: str, retention_days: int) -> int:
        if not _table_exists(self._profile, _HISTORY_TABLE):
            return 0
        cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).isoformat()
        conn = _connect(self._profile)
        try:
            cur = conn.cursor()
            cur.execute(
                f"DELETE FROM {_qualified(self._profile, _HISTORY_TABLE)} "
                "WHERE sync_name = %s AND started_at < %s",
                (sync_name, cutoff),
            )
            return int(cur.rowcount or 0)
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
        errors=_json_or_parsed(errors),
        cursor_value_used=cursor_value_used,
        dry_run=dry_run,
        run_id=run_id,
        sync_run_id=sync_run_id,
    )


class SnowflakeWarehouseDlqBackend:
    """``DlqBackend`` backed by a ``_drt_dlq`` row per dead-letter entry (#1106)."""

    def __init__(self, profile: SnowflakeProfile) -> None:
        self._profile = profile

    def _ensure_table(self, conn: Any) -> None:
        _ensure_table_exists(
            conn,
            self._profile,
            _DLQ_TABLE,
            "id TEXT PRIMARY KEY, "
            "sync_name TEXT NOT NULL, "
            "record VARIANT NOT NULL, "
            "error_message TEXT NOT NULL, "
            "http_status INTEGER, "
            "ts TEXT NOT NULL, "
            "attempts INTEGER NOT NULL, "
            "sync_run_id TEXT",
        )

    def append(
        self, sync_name: str, entries: list[DeadLetter], *, max_records: int = 10_000
    ) -> int:
        if not entries:
            return self.depth(sync_name)
        conn = _connect(self._profile)
        try:
            self._ensure_table(conn)
            t = _qualified(self._profile, _DLQ_TABLE)
            with _snowflake_transaction(conn):
                cur = conn.cursor()
                for entry in entries:
                    cur.execute(
                        f"MERGE INTO {t} AS t USING (SELECT %s AS id, %s AS sync_name, "
                        "PARSE_JSON(%s) AS record, %s AS error_message, %s AS http_status, "
                        "%s AS ts, %s AS attempts, %s AS sync_run_id) AS s "
                        "ON t.id = s.id "
                        "WHEN MATCHED THEN UPDATE SET record = s.record, "
                        "error_message = s.error_message, http_status = s.http_status, "
                        "ts = s.ts, attempts = s.attempts, sync_run_id = s.sync_run_id "
                        "WHEN NOT MATCHED THEN INSERT (id, sync_name, record, error_message, "
                        "http_status, ts, attempts, sync_run_id) VALUES (s.id, s.sync_name, "
                        "s.record, s.error_message, s.http_status, s.ts, s.attempts, "
                        "s.sync_run_id)",
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
                        f"DELETE FROM {t} WHERE sync_name = %s AND id NOT IN ("
                        f"SELECT id FROM {t} WHERE sync_name = %s "
                        "ORDER BY ts DESC, id DESC LIMIT %s)",
                        (sync_name, sync_name, max_records),
                    )
            return self.depth(sync_name)
        finally:
            conn.close()

    def replace(self, sync_name: str, entries: list[DeadLetter]) -> None:
        """Wholesale-replace the queue in one explicit transaction.

        Snowflake autocommits each statement individually by default, unlike
        Postgres's one-transaction-per-connection default — an unwrapped
        DELETE-then-INSERT here would commit the delete immediately, so a
        crash before the inserts land would permanently erase the queue
        (the #955 failure class). ``_snowflake_transaction`` makes the whole
        replacement one commit or none.
        """
        conn = _connect(self._profile)
        try:
            self._ensure_table(conn)
            t = _qualified(self._profile, _DLQ_TABLE)
            with _snowflake_transaction(conn):
                cur = conn.cursor()
                cur.execute(f"DELETE FROM {t} WHERE sync_name = %s", (sync_name,))
                for entry in entries:
                    cur.execute(
                        f"INSERT INTO {t} (id, sync_name, record, error_message, "
                        "http_status, ts, attempts, sync_run_id) "
                        "SELECT %s, %s, PARSE_JSON(%s), %s, %s, %s, %s, %s",
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
        finally:
            conn.close()

    def clear(self, sync_name: str) -> None:
        self.replace(sync_name, [])

    def read(self, sync_name: str) -> list[DeadLetter]:
        if not _table_exists(self._profile, _DLQ_TABLE):
            return []
        conn = _connect(self._profile)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT id, record, error_message, http_status, ts, attempts, sync_run_id "
                f"FROM {_qualified(self._profile, _DLQ_TABLE)} WHERE sync_name = %s "
                "ORDER BY ts ASC, id ASC",
                (sync_name,),
            )
            return [_row_to_dead_letter(row) for row in cur.fetchall()]
        finally:
            conn.close()

    def depth(self, sync_name: str) -> int:
        if not _table_exists(self._profile, _DLQ_TABLE):
            return 0
        conn = _connect(self._profile)
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT COUNT(*) FROM {_qualified(self._profile, _DLQ_TABLE)} "
                "WHERE sync_name = %s",
                (sync_name,),
            )
            row = cur.fetchone()
            return int(row[0]) if row else 0
        finally:
            conn.close()

    def all_depths(self) -> dict[str, int]:
        if not _table_exists(self._profile, _DLQ_TABLE):
            return {}
        conn = _connect(self._profile)
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT sync_name, COUNT(*) FROM {_qualified(self._profile, _DLQ_TABLE)} "
                "GROUP BY sync_name"
            )
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
        updates = updates or {}
        conn = _connect(self._profile)
        try:
            self._ensure_table(conn)
            t = _qualified(self._profile, _DLQ_TABLE)
            with _snowflake_transaction(conn):
                cur = conn.cursor()
                if remove_ids:
                    # Explicit %s-per-value placeholders, not a single
                    # array-bound parameter — matches the established
                    # pattern in drt/destinations/snowflake.py's
                    # _build_mirror_delete (the connector doesn't
                    # auto-expand a Python sequence into a SQL list the way
                    # psycopg2's ANY(%s) does).
                    ids = list(remove_ids)
                    placeholders = ", ".join(["%s"] * len(ids))
                    cur.execute(
                        f"DELETE FROM {t} WHERE sync_name = %s AND id IN ({placeholders})",
                        (sync_name, *ids),
                    )
                for entry_id, entry in updates.items():
                    cur.execute(
                        f"UPDATE {t} SET record = PARSE_JSON(%s), error_message = %s, "
                        "http_status = %s, ts = %s, attempts = %s, sync_run_id = %s "
                        "WHERE sync_name = %s AND id = %s",
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
        finally:
            conn.close()
        return self.read(sync_name)


def _row_to_dead_letter(row: tuple[Any, ...]) -> DeadLetter:
    entry_id, record, error_message, http_status, ts, attempts, sync_run_id = row
    return DeadLetter(
        record=_json_or_parsed(record),
        error_message=error_message,
        http_status=http_status,
        timestamp=ts,
        attempts=attempts,
        sync_run_id=sync_run_id,
        id=entry_id,
    )
