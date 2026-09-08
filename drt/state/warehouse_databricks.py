"""Warehouse-backed state, history, and DLQ stores — Databricks leg (#1108).

Databricks counterpart to ``drt/state/warehouse.py``'s Postgres-first
implementation and ``drt/state/warehouse_snowflake.py``'s Snowflake leg
(#920/#1106, ADR 0005 step 4), built on the same ``ManagedTableCapable``
primitive (#960/#1108's ``DatabricksSource`` leg). Kept in its own module,
mirroring how ``drt/sources/postgres.py``/``snowflake.py``/``databricks.py``
are already split one file per dialect.

**Design decisions specific to this dialect** (posted to #1108 before
implementation):

- **Delta Lake has no multi-statement transactions at all** — a stronger
  constraint than Snowflake's merely-autocommit-by-default (which at least
  offers ``conn.autocommit(False)`` as an explicit-transaction escape hatch).
  This is already documented as a platform limitation in this codebase's own
  ``_finalize_mirror_tracked`` docstring (``drt/destinations/databricks.py``),
  not something a method here could work around with connection settings.
- **`DlqBackend.replace()`'s atomicity (the #955 failure class) is closed via
  a single atomic `MERGE`, not a transaction.** A crash between an unwrapped
  DELETE-then-INSERT would permanently erase the queue — the same failure
  Snowflake closed by wrapping both statements in one explicit transaction.
  Databricks cannot do that, so this reuses the anti-join `MERGE` shape
  `_delete_via_staged_keys` already established for mirror deletes (#692/#908):
  stage the new entries into a scratch table, then one
  `MERGE ... WHEN NOT MATCHED BY SOURCE AND t.sync_name = ? THEN DELETE /
  WHEN MATCHED THEN UPDATE / WHEN NOT MATCHED THEN INSERT` — one Delta commit,
  all-or-nothing. `replace(sync_name, [])` (i.e. `clear()`) is special-cased
  as a plain `DELETE` instead: whether an empty-source `MERGE` actually
  deletes every unmatched target row, or short-circuits to a no-op on this
  platform, is unverified by any prior art in this codebase — a mock cursor
  cannot prove it either way, so this is pinned down by this leg's own live
  smoke test rather than assumed.
- **Every upsert (`save_sync`, `DlqBackend.append`) stages into a real
  scratch table and `MERGE`s from it — the same call shape this connector's
  existing bulk-load `MERGE` sites already use (`_load_upsert`, mirror
  finalize)**, not an inline derived-table `MERGE ... USING (SELECT ? AS
  col, ...) AS s` the way `warehouse_snowflake.py` does with `%s` markers.
  An inline subquery source would work as *documented* Delta `MERGE INTO`
  syntax, but whether native `?` parameter binding resolves correctly inside
  one on this exact call shape has no precedent in this codebase to build on
  with confidence — and getting it wrong would break `save_sync` on every
  single warehouse-backed Databricks run, not some edge path (the #908
  lesson: a mock cursor accepts any SQL text, live behavior is the only
  proof). `save_sync` stages its one row and `append()` stages all of
  `entries` (one `INSERT ... SELECT ... parse_json(?)` per row, since VARIANT
  forces that regardless) before a single closing `MERGE` applies them all —
  a few extra statements on a path that is not a hot path (state reads/writes
  are not, per `warehouse.py`'s own docstring), in exchange for reusing an
  already-live-verified pattern instead of staking correctness on an
  unverified one.
- **`DlqBackend.append()`'s one final `MERGE` covers the whole batch**,
  falling out of the staged-upsert design above rather than being a
  deliberately built guarantee — batching *across dialects* is still tracked
  separately as follow-up work (#1121), since Postgres/Snowflake still write
  one statement per entry. The individual staging inserts above are not
  atomic with each other, but a partial staging table is disposable; the
  real table only ever sees the one all-or-nothing final `MERGE`.
- **`errors`/`record` are `VARIANT`, not `JSONB`.** Already an established
  write category on this connector (Layer 3, #317) and exercised nightly by
  the existing smoke suite, so this is reuse, not new platform risk. Databricks
  disallows function calls (`parse_json`) in a `VALUES` literal list exactly
  like Snowflake disallows `PARSE_JSON` there, so every write touching one of
  these columns uses the `INSERT ... SELECT` form instead. Reading back,
  whether the connector hands a VARIANT column back as an already-parsed
  Python object or a JSON-encoded string is unverified by any prior art in
  this codebase (same open question Snowflake's leg noted) — `_json_or_parsed`
  below handles both, pinned down by this leg's own live smoke test.
- **No case-folding normalization.** Unlike the Snowflake leg's
  `UPPER()`-normalized probes, Unity Catalog is case-*preserving*, not
  case-folding, for unquoted identifiers — a plain, un-normalized probe
  already agrees between what this module creates and what
  `ManagedTableCapable` later finds.
- **No `_is_undefined_table`-style exception classification.** Every read
  here probes `managed_table_exists()` first instead of catching a
  driver-specific "table doesn't exist" exception — same reasoning as the
  Snowflake leg: one extra round trip per read (state reads are not a hot
  path) in exchange for not guessing at unverified driver internals.
- **No `conn.rollback()` inside `_ensure_table_exists`'s race guard** — same
  reasoning as `DatabricksSource.ensure_managed_schema()` (#1108): Delta has
  no multi-statement transactions or savepoints at all, so a failed
  `CREATE TABLE` leaves nothing open to roll back.

Cross-dialect limitations documented on `warehouse.py` (no enforced
per-project namespace; concurrent runs of the same sync can move a cursor
backward under last-writer-wins) apply identically here — see that module's
docstring rather than repeating it.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Collection, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from drt.config.credentials import DatabricksProfile
from drt.destinations.databricks import _NATIVE_PARAM_LIMIT
from drt.sources.databricks import DatabricksSource
from drt.state.dlq import DeadLetter
from drt.state.history import HistoryEntry
from drt.state.manager import SyncState

logger = logging.getLogger(__name__)

_RUNS_TABLE = "_drt_runs"
_HISTORY_TABLE = "_drt_history"
_DLQ_TABLE = "_drt_dlq"


def _connect(profile: DatabricksProfile) -> Any:
    return DatabricksSource()._connect(profile)


def _qualified(profile: DatabricksProfile, table: str) -> str:
    # Unquoted, matching #1108's ManagedTableCapable convention — Unity
    # Catalog is case-preserving, not case-folding, so no normalization is
    # needed the way Snowflake's UPPER() probes need it.
    return f"{profile.catalog}.{profile.managed_schema}.{table}"


def _table_exists(profile: DatabricksProfile, table_name: str) -> bool:
    return DatabricksSource().managed_table_exists(profile, table_name)


def _staging_table(profile: DatabricksProfile, kind: str, discriminator: str) -> str:
    """A per-call scratch table name for a staged upsert MERGE.

    ``discriminator`` (typically ``sync_name``) is hashed rather than used as
    a sanitized substring: sync names are developer-controlled slugs, not
    guaranteed identifier-safe (spaces, dots), and hashing sidesteps that
    without needing a sanitizer. Suffixing by it at all (not one shared
    scratch name per ``kind``) avoids the exact concurrent-writer race #692's
    review caught for the mirror finalizer's own scratch table — two
    `--threads N>1` syncs writing different sync_names' rows around the same
    time must not share one scratch table.
    """
    digest = hashlib.sha256(discriminator.encode()).hexdigest()[:16]
    return f"{profile.catalog}.{profile.managed_schema}.__drt_{kind}_{digest}"


def _ensure_table_exists(
    conn: Any, profile: DatabricksProfile, table_name: str, column_defs: str
) -> None:
    """Create ``table_name`` under ``profile.managed_schema`` if absent.

    Same escape-hatch + concurrent-first-use discipline as
    ``DatabricksSource.ensure_managed_schema()`` (#1108): probe before any
    ``CREATE``, and on a ``CREATE`` failure, re-probe rather than assume —
    swallow if another session won the race, re-raise otherwise. No
    ``conn.rollback()``: Delta has no multi-statement transactions or
    savepoints at all.
    """
    source = DatabricksSource()
    source.ensure_managed_schema(profile)
    if source.managed_table_exists(profile, table_name):
        return
    cur = conn.cursor()
    try:
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {_qualified(profile, table_name)} "
            f"({column_defs}) USING DELTA"
        )
    except Exception:
        if not source.managed_table_exists(profile, table_name):
            raise


def _json_or_parsed(value: Any) -> Any:
    """A VARIANT column read back as either a JSON string or an
    already-parsed object, depending on connector behavior not otherwise
    verified in this codebase (see module docstring)."""
    return json.loads(value) if isinstance(value, str) else value


class DatabricksWarehouseStateStore:
    """``StateStore`` backed by a ``_drt_runs`` row per sync (#1108)."""

    def __init__(self, profile: DatabricksProfile) -> None:
        self._profile = profile

    def _ensure_table(self, conn: Any) -> None:
        _ensure_table_exists(
            conn,
            self._profile,
            _RUNS_TABLE,
            "sync_name STRING, "
            "last_run_at STRING NOT NULL, "
            "records_synced BIGINT NOT NULL, "
            "status STRING NOT NULL, "
            "error STRING, "
            "last_cursor_value STRING",
        )

    def get_last_sync(self, sync_name: str) -> SyncState | None:
        if not _table_exists(self._profile, _RUNS_TABLE):
            return None
        conn = _connect(self._profile)
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT last_run_at, records_synced, status, error, last_cursor_value "
                f"FROM {_qualified(self._profile, _RUNS_TABLE)} WHERE sync_name = ?",
                [sync_name],
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
        """Upsert via a staged ``MERGE`` — see module docstring: single-row
        upserts here stage into a real scratch table and MERGE from it,
        matching this connector's existing proven ``_load_upsert`` shape,
        rather than an inline derived-table ``MERGE ... USING (SELECT ...)``
        source whose native ``?`` parameter binding on this exact call shape
        has no precedent in this codebase to build on with confidence."""
        conn = _connect(self._profile)
        try:
            self._ensure_table(conn)
            t = _qualified(self._profile, _RUNS_TABLE)
            cur = conn.cursor()
            staging_table = _staging_table(self._profile, "runs_save", state.sync_name)
            cur.execute(f"CREATE OR REPLACE TABLE {staging_table} AS SELECT * FROM {t} WHERE 1=0")
            try:
                cur.execute(
                    f"INSERT INTO {staging_table} (sync_name, last_run_at, records_synced, "
                    "status, error, last_cursor_value) VALUES (?, ?, ?, ?, ?, ?)",
                    [
                        state.sync_name,
                        state.last_run_at,
                        state.records_synced,
                        state.status,
                        state.error,
                        state.last_cursor_value,
                    ],
                )
                cur.execute(
                    f"MERGE INTO {t} AS t USING {staging_table} AS s "
                    "ON t.sync_name = s.sync_name "
                    "WHEN MATCHED THEN UPDATE SET last_run_at = s.last_run_at, "
                    "records_synced = s.records_synced, status = s.status, error = s.error, "
                    "last_cursor_value = s.last_cursor_value "
                    "WHEN NOT MATCHED THEN INSERT (sync_name, last_run_at, records_synced, "
                    "status, error, last_cursor_value) VALUES (s.sync_name, s.last_run_at, "
                    "s.records_synced, s.status, s.error, s.last_cursor_value)"
                )
            finally:
                cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
        finally:
            conn.close()

    def reset(self, sync_name: str) -> bool:
        if not _table_exists(self._profile, _RUNS_TABLE):
            return False
        conn = _connect(self._profile)
        try:
            cur = conn.cursor()
            cur.execute(
                f"DELETE FROM {_qualified(self._profile, _RUNS_TABLE)} WHERE sync_name = ?",
                [sync_name],
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


class DatabricksWarehouseHistoryStore:
    """``HistoryStore`` backed by an append-only ``_drt_history`` table (#1108)."""

    def __init__(self, profile: DatabricksProfile) -> None:
        self._profile = profile

    def _ensure_table(self, conn: Any) -> None:
        _ensure_table_exists(
            conn,
            self._profile,
            _HISTORY_TABLE,
            "sync_name STRING NOT NULL, "
            "started_at STRING NOT NULL, "
            "completed_at STRING NOT NULL, "
            "duration_seconds DOUBLE NOT NULL, "
            "status STRING NOT NULL, "
            "records_synced BIGINT NOT NULL, "
            "records_failed BIGINT NOT NULL, "
            "errors VARIANT NOT NULL, "
            "cursor_value_used STRING, "
            "dry_run BOOLEAN NOT NULL, "
            "run_id STRING, "
            "sync_run_id STRING",
        )

    def append(self, entry: HistoryEntry) -> None:
        """Best-effort, like every other ``HistoryStore`` — see the Protocol."""
        try:
            conn = _connect(self._profile)
            try:
                self._ensure_table(conn)
                cur = conn.cursor()
                t = _qualified(self._profile, _HISTORY_TABLE)
                # SELECT form, not VALUES: Databricks disallows a function
                # call (parse_json) inside a VALUES literal list.
                cur.execute(
                    f"INSERT INTO {t} (sync_name, started_at, completed_at, "
                    "duration_seconds, status, records_synced, records_failed, "
                    "errors, cursor_value_used, dry_run, run_id, sync_run_id) "
                    "SELECT ?, ?, ?, ?, ?, ?, ?, parse_json(?), ?, ?, ?, ?",
                    [
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
                    ],
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
                    f"SELECT {columns} FROM {t} WHERE sync_name = ? "
                    "ORDER BY started_at DESC LIMIT ?",
                    [sync_name, limit],
                )
            else:
                cur.execute(f"SELECT {columns} FROM {t} ORDER BY started_at DESC LIMIT ?", [limit])
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
                "WHERE sync_name = ? AND started_at < ?",
                [sync_name, cutoff],
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


class DatabricksWarehouseDlqBackend:
    """``DlqBackend`` backed by a ``_drt_dlq`` row per dead-letter entry (#1108)."""

    def __init__(self, profile: DatabricksProfile) -> None:
        self._profile = profile

    def _ensure_table(self, conn: Any) -> None:
        _ensure_table_exists(
            conn,
            self._profile,
            _DLQ_TABLE,
            "id STRING, "
            "sync_name STRING NOT NULL, "
            "record VARIANT NOT NULL, "
            "error_message STRING NOT NULL, "
            "http_status INT, "
            "ts STRING NOT NULL, "
            "attempts INT NOT NULL, "
            "sync_run_id STRING",
        )

    def append(
        self, sync_name: str, entries: list[DeadLetter], *, max_records: int = 10_000
    ) -> int:
        """Stage ``entries`` into a scratch table, then upsert all of them in
        one ``MERGE`` (no delete clause) — the same staged-upsert shape
        ``save_sync`` uses, not the batched-atomicity apparatus the module
        docstring explicitly declines to build. The individual staging
        inserts are not atomic with each other, but that only risks a
        partial *scratch* table (dropped either way); the real table only
        ever sees the one all-or-nothing final ``MERGE``."""
        if not entries:
            return self.depth(sync_name)
        conn = _connect(self._profile)
        try:
            self._ensure_table(conn)
            t = _qualified(self._profile, _DLQ_TABLE)
            cur = conn.cursor()
            staging_table = _staging_table(self._profile, "dlq_append", sync_name)
            cur.execute(f"CREATE OR REPLACE TABLE {staging_table} AS SELECT * FROM {t} WHERE 1=0")
            try:
                for entry in entries:
                    cur.execute(
                        f"INSERT INTO {staging_table} (id, sync_name, record, error_message, "
                        "http_status, ts, attempts, sync_run_id) "
                        "SELECT ?, ?, parse_json(?), ?, ?, ?, ?, ?",
                        [
                            entry.id,
                            sync_name,
                            json.dumps(entry.record),
                            entry.error_message,
                            entry.http_status,
                            entry.timestamp,
                            entry.attempts,
                            entry.sync_run_id,
                        ],
                    )
                cur.execute(
                    f"MERGE INTO {t} AS t USING {staging_table} AS s "
                    "ON t.id = s.id "
                    "WHEN MATCHED THEN UPDATE SET record = s.record, "
                    "error_message = s.error_message, http_status = s.http_status, "
                    "ts = s.ts, attempts = s.attempts, sync_run_id = s.sync_run_id "
                    "WHEN NOT MATCHED THEN INSERT (id, sync_name, record, error_message, "
                    "http_status, ts, attempts, sync_run_id) VALUES (s.id, s.sync_name, "
                    "s.record, s.error_message, s.http_status, s.ts, s.attempts, "
                    "s.sync_run_id)"
                )
            finally:
                cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
            if max_records > 0:
                cur.execute(
                    f"DELETE FROM {t} WHERE sync_name = ? AND id NOT IN ("
                    f"SELECT id FROM {t} WHERE sync_name = ? "
                    "ORDER BY ts DESC, id DESC LIMIT ?)",
                    [sync_name, sync_name, max_records],
                )
            return self.depth(sync_name)
        finally:
            conn.close()

    def replace(self, sync_name: str, entries: list[DeadLetter]) -> None:
        """Wholesale-replace the queue via one atomic ``MERGE`` (see module
        docstring for why a Snowflake-style explicit transaction is not an
        option on this dialect, and why the empty-``entries`` case is
        special-cased rather than routed through the same ``MERGE``)."""
        conn = _connect(self._profile)
        try:
            self._ensure_table(conn)
            t = _qualified(self._profile, _DLQ_TABLE)
            cur = conn.cursor()
            if not entries:
                cur.execute(f"DELETE FROM {t} WHERE sync_name = ?", [sync_name])
                return
            staging_table = _staging_table(self._profile, "dlq_replace", sync_name)
            cur.execute(f"CREATE OR REPLACE TABLE {staging_table} AS SELECT * FROM {t} WHERE 1=0")
            try:
                for entry in entries:
                    cur.execute(
                        f"INSERT INTO {staging_table} (id, sync_name, record, error_message, "
                        "http_status, ts, attempts, sync_run_id) "
                        "SELECT ?, ?, parse_json(?), ?, ?, ?, ?, ?",
                        [
                            entry.id,
                            sync_name,
                            json.dumps(entry.record),
                            entry.error_message,
                            entry.http_status,
                            entry.timestamp,
                            entry.attempts,
                            entry.sync_run_id,
                        ],
                    )
                cur.execute(
                    f"MERGE INTO {t} AS t USING {staging_table} AS s "
                    "ON t.id = s.id "
                    "WHEN NOT MATCHED BY SOURCE AND t.sync_name = ? THEN DELETE "
                    "WHEN MATCHED THEN UPDATE SET record = s.record, "
                    "error_message = s.error_message, http_status = s.http_status, "
                    "ts = s.ts, attempts = s.attempts, sync_run_id = s.sync_run_id "
                    "WHEN NOT MATCHED THEN INSERT (id, sync_name, record, error_message, "
                    "http_status, ts, attempts, sync_run_id) VALUES (s.id, s.sync_name, "
                    "s.record, s.error_message, s.http_status, s.ts, s.attempts, "
                    "s.sync_run_id)",
                    [sync_name],
                )
            finally:
                cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
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
                f"FROM {_qualified(self._profile, _DLQ_TABLE)} WHERE sync_name = ? "
                "ORDER BY ts ASC, id ASC",
                [sync_name],
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
                f"SELECT COUNT(*) FROM {_qualified(self._profile, _DLQ_TABLE)} WHERE sync_name = ?",
                [sync_name],
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
            cur = conn.cursor()
            if remove_ids:
                # Chunked to stay under the native 255-marker limit (#734) —
                # one slot reserved for sync_name in each chunk's statement.
                ids = list(remove_ids)
                chunk_size = _NATIVE_PARAM_LIMIT - 1
                for start in range(0, len(ids), chunk_size):
                    chunk = ids[start : start + chunk_size]
                    placeholders = ", ".join(["?"] * len(chunk))
                    cur.execute(
                        f"DELETE FROM {t} WHERE sync_name = ? AND id IN ({placeholders})",
                        [sync_name, *chunk],
                    )
            for entry_id, entry in updates.items():
                cur.execute(
                    f"UPDATE {t} SET record = parse_json(?), error_message = ?, "
                    "http_status = ?, ts = ?, attempts = ?, sync_run_id = ? "
                    "WHERE sync_name = ? AND id = ?",
                    [
                        json.dumps(entry.record),
                        entry.error_message,
                        entry.http_status,
                        entry.timestamp,
                        entry.attempts,
                        entry.sync_run_id,
                        sync_name,
                        entry_id,
                    ],
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
