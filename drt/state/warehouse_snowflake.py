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

**`DlqBackend.append`/`.replace()`/`.reconcile()` batch into bounded
multi-row `MERGE` statements (#1121)** rather than one `MERGE`/`INSERT`/
`UPDATE` per dead-letter entry — raised in Codex review on #1120, closed
here after the identical Postgres and Databricks legs (#1129/#1130).
`append()` and `replace()` share one upsert-by-id `MERGE` helper
(`_upsert_dlq_entries`); `reconcile()`'s `updates` gets a sibling,
update-only `MERGE` helper (`_update_dlq_entries`, no `WHEN NOT MATCHED`
branch); `reconcile()`'s `remove_ids` `DELETE` is also now chunked, closing
a related gap this leg alone had (an unbounded single statement whose
parameter count grew 1:1 with `remove_ids`, unlike Postgres's single
`ANY(%s)` array param or Databricks' already-chunked equivalent).
`PARSE_JSON` still cannot appear inside a `VALUES` literal list on this
connector, so the batched writes use the same generic-alias-then-outer-
`SELECT` technique `destinations/snowflake.py`'s own mirror `MERGE`
established, applied via a file-local helper rather than an import across
the `state`/`destinations` boundary — see `_MERGE_PARAM_BUDGET`'s own
comment.
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


# Batches DlqBackend.append()/.replace()/.reconcile()'s per-entry writes into
# bounded multi-row statements (#1121, this module's own docstring flagged it
# as a follow-up). 2000 mirrors destinations/snowflake.py's own
# _MERGE_PARAM_BUDGET — the same conservative, empirically-verified budget
# (see that module's comment for the live-account basis), well under the
# ~32000-row point that budget was tested up to. Kept file-local rather than
# imported: this repo already keeps destinations/databricks.py's
# _NATIVE_PARAM_LIMIT and destinations/snowflake.py's _MERGE_PARAM_BUDGET as
# two separate constants, not one shared one, and reaching from drt/state/
# into drt/destinations/ would be the wrong import direction (state stores
# are consumers of destination-shaped data, not the reverse) — same
# reasoning as warehouse.py's own _DLQ_PARAM_BUDGET.
_MERGE_PARAM_BUDGET = 2000


def _rows_per_chunk(n_cols: int) -> int:
    return max(1, _MERGE_PARAM_BUDGET // max(1, n_cols))


def _dlq_merge_values_params(sync_name: str, entry: DeadLetter) -> list[Any]:
    return [
        entry.id,
        sync_name,
        json.dumps(entry.record),
        entry.error_message,
        entry.http_status,
        entry.timestamp,
        entry.attempts,
        entry.sync_run_id,
    ]


def _upsert_dlq_entries(cur: Any, t: str, sync_name: str, entries: list[DeadLetter]) -> None:
    """Chunked ``MERGE`` upsert shared by ``DlqBackend.append()``/``.replace()``
    (#1121) — reused rather than duplicated, since both need the identical
    upsert-by-id write.

    ``PARSE_JSON`` cannot appear inside a ``VALUES`` literal list on this
    connector (module docstring) — the ``VALUES``-derived source uses
    generic column aliases (``v0``..``v7``), and ``PARSE_JSON`` is applied
    in the outer ``SELECT``'s projection instead, the same technique
    ``destinations/snowflake.py``'s own mirror ``MERGE`` already established
    for this connector (kept file-local here, not imported — see
    ``_MERGE_PARAM_BUDGET``'s own comment for why).

    Never touches ``sync_name`` on a match, matching every other dialect's
    DLQ upsert precedent: matching globally on ``id`` and reassigning
    ``sync_name`` on a match would let one sync's write silently move
    another sync's row into its own queue on an id collision.
    """
    row_placeholder = "(" + ", ".join(["%s"] * 8) + ")"
    for chunk in _chunked(entries, _rows_per_chunk(8)):
        values_sql = ", ".join([row_placeholder] * len(chunk))
        params: list[Any] = []
        for entry in chunk:
            params.extend(_dlq_merge_values_params(sync_name, entry))
        cur.execute(
            f"MERGE INTO {t} AS t USING ("
            "SELECT v0 AS id, v1 AS sync_name, PARSE_JSON(v2) AS record, "
            "v3 AS error_message, v4 AS http_status, v5 AS ts, v6 AS attempts, "
            "v7 AS sync_run_id "
            f"FROM (VALUES {values_sql}) AS raw(v0, v1, v2, v3, v4, v5, v6, v7)"
            ") AS s "
            "ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET record = s.record, "
            "error_message = s.error_message, http_status = s.http_status, "
            "ts = s.ts, attempts = s.attempts, sync_run_id = s.sync_run_id "
            "WHEN NOT MATCHED THEN INSERT (id, sync_name, record, error_message, "
            "http_status, ts, attempts, sync_run_id) VALUES (s.id, s.sync_name, "
            "s.record, s.error_message, s.http_status, s.ts, s.attempts, "
            "s.sync_run_id)",
            params,
        )


def _update_dlq_entries(
    cur: Any, t: str, sync_name: str, update_items: list[tuple[str, DeadLetter]]
) -> None:
    """Chunked ``MERGE`` update-only for ``DlqBackend.reconcile()``'s
    ``updates`` (#1121). No ``WHEN NOT MATCHED`` branch — these touch
    existing rows only and must never insert, unlike ``_upsert_dlq_entries``.
    ``sync_name`` is a single shared bound parameter (every update in one
    ``reconcile()`` call is for the same sync), not a per-row ``VALUES``
    column.
    """
    row_placeholder = "(" + ", ".join(["%s"] * 7) + ")"
    for chunk in _chunked(update_items, _rows_per_chunk(7)):
        values_sql = ", ".join([row_placeholder] * len(chunk))
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
            f"MERGE INTO {t} AS t USING ("
            "SELECT v0 AS id, PARSE_JSON(v1) AS record, v2 AS error_message, "
            "v3 AS http_status, v4 AS ts, v5 AS attempts, v6 AS sync_run_id "
            f"FROM (VALUES {values_sql}) AS raw(v0, v1, v2, v3, v4, v5, v6)"
            ") AS s "
            "ON t.id = s.id AND t.sync_name = %s "
            "WHEN MATCHED THEN UPDATE SET record = s.record, "
            "error_message = s.error_message, http_status = s.http_status, "
            "ts = s.ts, attempts = s.attempts, sync_run_id = s.sync_run_id",
            params,
        )


def _chunked(items: list[Any], size: int) -> list[list[Any]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


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
        """Chunked ``MERGE`` upsert via ``_upsert_dlq_entries`` (#1121) —
        was a per-entry ``MERGE`` loop; see that function's docstring for
        the ``sync_name``-on-match precedent it preserves.

        Two entries sharing an id within one call would make Snowflake's
        ``MERGE`` raise (it does not support updating/deleting the same
        target row more than once in one statement) — possible for legacy,
        pre-#955 dead letters whose id is a content hash (the same
        collision class guarded against on the Postgres/Databricks legs,
        #1129/#1130). Deduping here by id, keeping the last occurrence,
        preserves the old per-entry loop's last-wins outcome.
        """
        if not entries:
            return self.depth(sync_name)
        deduped: dict[str, DeadLetter] = {}
        for entry in entries:
            deduped[entry.id] = entry
        conn = _connect(self._profile)
        try:
            self._ensure_table(conn)
            t = _qualified(self._profile, _DLQ_TABLE)
            with _snowflake_transaction(conn):
                cur = conn.cursor()
                _upsert_dlq_entries(cur, t, sync_name, list(deduped.values()))
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

        Upserts via ``_upsert_dlq_entries`` (#1121, shared with ``append()``)
        rather than a plain ``INSERT`` — every id is guaranteed new
        immediately after the ``DELETE`` above, so ``MERGE``'s
        ``WHEN NOT MATCHED`` branch applies to every row; reusing the same
        helper here avoids a second, divergent multi-row write shape.
        """
        conn = _connect(self._profile)
        try:
            self._ensure_table(conn)
            t = _qualified(self._profile, _DLQ_TABLE)
            with _snowflake_transaction(conn):
                cur = conn.cursor()
                cur.execute(f"DELETE FROM {t} WHERE sync_name = %s", (sync_name,))
                _upsert_dlq_entries(cur, t, sync_name, entries)
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
                    # psycopg2's ANY(%s) does). Chunked (#1121) — an
                    # unbounded single statement's parameter count would
                    # otherwise grow 1:1 with remove_ids, unlike Postgres's
                    # single ANY(%s) array param or Databricks' own
                    # already-chunked equivalent.
                    ids = list(remove_ids)
                    for chunk in _chunked(ids, _rows_per_chunk(1)):
                        placeholders = ", ".join(["%s"] * len(chunk))
                        cur.execute(
                            f"DELETE FROM {t} WHERE sync_name = %s AND id IN ({placeholders})",
                            (sync_name, *chunk),
                        )
                if updates:
                    _update_dlq_entries(cur, t, sync_name, list(updates.items()))
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
