"""Warehouse-backed state, history, and DLQ stores — Databricks leg (#1108).

Databricks counterpart to ``drt/state/warehouse.py``'s Postgres-first
implementation and ``drt/state/warehouse_snowflake.py``'s Snowflake leg
(#920/#1106, ADR 0005 step 4), built on the same ``ManagedTableCapable``
primitive (#960/#1108's ``DatabricksSource`` leg). Kept in its own module,
mirroring how ``drt/sources/postgres.py``/``snowflake.py``/``databricks.py``
are already split one file per dialect.

**Design decisions specific to this dialect** (posted to #1108 before
implementation, revised after Codex review caught a real design defect
before merge — see below):

- **Delta Lake has no multi-statement transactions at all** — a stronger
  constraint than Snowflake's merely-autocommit-by-default (which at least
  offers ``conn.autocommit(False)`` as an explicit-transaction escape hatch).
  This is already documented as a platform limitation in this codebase's own
  ``_finalize_mirror_tracked`` docstring (``drt/destinations/databricks.py``),
  not something a method here could work around with connection settings.
- **No runtime DDL outside `_ensure_table_exists`, anywhere in this module.**
  An earlier draft staged every upsert (`save_sync`, `DlqBackend.append`)
  into a scratch table via `CREATE OR REPLACE TABLE` before `MERGE`-ing from
  it. Codex review (twice, independently) caught that this breaks the
  documented escape hatch: an operator who pre-provisions the three managed
  tables and grants only DML (no `CREATE TABLE`) — the same escape hatch
  `docs/guides/warehouse-state.md` documents and Postgres/Snowflake's own
  legs preserve — would have every write fail, since the scratch table's
  `CREATE` still needs a privilege the pre-provisioned-tables path
  deliberately has none of. Every method here now does DML only
  (`SELECT`/`UPDATE`/`INSERT`/`DELETE`/`MERGE` against the three real
  tables), never a scratch table.
- **`save_sync` and `DlqBackend.append` are probe-then-`UPDATE`-or-`INSERT`**,
  not `MERGE`: `SELECT 1 FROM t WHERE <key> = ? LIMIT`-style existence check,
  then the matching statement — the same probe-before-act discipline
  `ensure_managed_schema`/`_ensure_table_exists` already use, one level
  deeper (per-row instead of per-table). This has a check-then-act window
  for two concurrent writers touching the *same* `sync_name`/`id`
  simultaneously — where a single-statement `MERGE` would guarantee no
  duplicate row, this can produce one. That is a real, accepted trade-off,
  not an oversight: it is strictly milder than the scratch-table design's
  actual failure mode (one writer's `CREATE OR REPLACE` could destroy or
  corrupt a concurrent writer's staged data outright), and concurrent runs
  of the *same* sync are already out of scope per `warehouse.py`'s own
  documented cross-dialect limitations. `DlqBackend.append()` is therefore a
  per-entry loop like every other dialect (matching `warehouse_snowflake.py`
  and the already-tracked #1121 batching follow-up), not a batched
  operation of any kind.
- **`DlqBackend.replace()`'s atomicity (the #955 failure class) is closed
  without a scratch table and without `WHEN NOT MATCHED BY SOURCE`:**
  existing ids for `sync_name` are read first (`SELECT id FROM t WHERE
  sync_name = ?`) to compute which are absent from the new `entries`
  (stale), the new entries are upserted **first** via a chunked
  `MERGE INTO t USING (VALUES (?, ?, ...), ...) AS s(id, sync_name, record,
  ...) ON t.id = s.id WHEN MATCHED THEN UPDATE / WHEN NOT MATCHED THEN
  INSERT` (`_rows_per_chunk` from `drt/destinations/databricks.py`, reused
  rather than a second copy of the same 255-marker budget), and only
  **after every upsert chunk has succeeded** are the stale ids deleted by
  explicit id (`DELETE ... WHERE id IN (...)`, chunked like `reconcile()`'s
  `remove_ids` — safe to split across statements, since each chunk deletes a
  disjoint subset of the same stale-id set). This upsert-then-delete order
  is deliberate, not incidental: deleting stale ids *before* the upsert
  (an earlier draft's order) means a crash or a failed merge partway through
  leaves the queue with the old rows already gone and the new ones not yet
  landed — an empty queue, exactly the failure this method exists to
  prevent (caught in review). A single chunk's `MERGE` is one atomic Delta
  commit; a multi-chunk `replace()` is **not** fully atomic across chunks —
  say so plainly rather than overclaiming. What *is* still guaranteed: the
  queue is never left empty or destroyed by a crash mid-`replace()` — a
  crash before every upsert chunk lands leaves the old queue untouched
  (stale-delete never ran) plus whichever new entries already landed, never
  nothing. An earlier draft used a single `MERGE` with `WHEN NOT
  MATCHED BY SOURCE AND t.sync_name = ? THEN DELETE` (mirroring the anti-join
  shape `_delete_via_staged_keys` already uses for mirror deletes, #692/#908)
  — Codex review found two problems with it: it required the scratch table
  this rewrite removes, and Databricks' `MERGE` grammar requires `WHEN
  MATCHED` clauses before `WHEN NOT MATCHED`, before `WHEN NOT MATCHED BY
  SOURCE` (doc-derived, not precedent-derived — no call site in this repo
  combines more than one clause type in a single `MERGE`, so there was no
  existing example to check the ordering against before this review caught
  it). The `VALUES`-derived-table `MERGE` source is the same native `?`
  binding inside a `VALUES` clause already proven nightly by #734's
  multi-row `INSERT` batching — a much shorter extrapolation than either the
  removed scratch table or a `FROM`-less `SELECT ? AS x` derived table.
  `replace(sync_name, [])` (`clear()`) is special-cased as a plain `DELETE`
  — no entries means every existing id is "stale", so the id-diffing path
  above degenerates to it anyway, and it avoids depending on whatever this
  platform does with a zero-row `VALUES` source.
- **`reset()` and `HistoryStore.prune()` do not trust `cursor.rowcount`.**
  Codex review, reading the installed `databricks-sql-connector` source
  directly, found that `rowcount` is left at the PEP 249 sentinel `-1` on
  connector/server combinations that do not report affected-row counts for
  `DELETE` — within the range this package already declares support for.
  Trusting it would make `reset()` report "nothing existed" after
  successfully deleting a row, and `prune()` return `-1` instead of a count.
  Both instead determine the answer before deleting: `reset()` probes
  existence (`SELECT 1 ... LIMIT`-style) and returns that; `prune()` counts
  matching rows (`SELECT COUNT(*) ...`) and returns that count, only
  executing the `DELETE` if the count is nonzero.
- **`errors`/`record` are `VARIANT`, not `JSONB`.** Already an established
  write category on this connector (Layer 3, #317) and exercised nightly by
  the existing smoke suite, so this is reuse, not new platform risk. Databricks
  disallows function calls (`parse_json`) in a `VALUES` literal list exactly
  like Snowflake disallows `PARSE_JSON` there — this applies to `HistoryStore
  .append()`'s single-row `INSERT ... SELECT` (the `VALUES` clause itself
  never carries `parse_json(?)` there) and to `replace()`'s `VALUES`-sourced
  `MERGE` (the `VALUES` constructor carries `record` as a plain bound
  string; `parse_json(s.record)` is only called in the `MERGE`'s `UPDATE
  SET`/`INSERT VALUES` expressions, a different grammar position the
  restriction does not reach — the same reasoning `_value_clause` documents
  for why a bulk `INSERT` switches to the `SELECT` form). Reading back,
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

import json
import logging
from collections.abc import Collection, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from drt.config.credentials import DatabricksProfile
from drt.destinations.databricks import _NATIVE_PARAM_LIMIT, _rows_per_chunk
from drt.sources.databricks import DatabricksSource
from drt.state.dlq import DeadLetter
from drt.state.history import HistoryEntry
from drt.state.manager import SyncState

logger = logging.getLogger(__name__)

_RUNS_TABLE = "_drt_runs"
_HISTORY_TABLE = "_drt_history"
_DLQ_TABLE = "_drt_dlq"
_DLQ_COLUMNS = (
    "id",
    "sync_name",
    "record",
    "error_message",
    "http_status",
    "ts",
    "attempts",
    "sync_run_id",
)


def _connect(profile: DatabricksProfile) -> Any:
    return DatabricksSource()._connect(profile)


def _qualified(profile: DatabricksProfile, table: str) -> str:
    # Unquoted, matching #1108's ManagedTableCapable convention — Unity
    # Catalog is case-preserving, not case-folding, so no normalization is
    # needed the way Snowflake's UPPER() probes need it.
    return f"{profile.catalog}.{profile.managed_schema}.{table}"


def _table_exists(profile: DatabricksProfile, table_name: str) -> bool:
    return DatabricksSource().managed_table_exists(profile, table_name)


def _ensure_table_exists(
    conn: Any, profile: DatabricksProfile, table_name: str, column_defs: str
) -> None:
    """Create ``table_name`` under ``profile.managed_schema`` if absent.

    Same escape-hatch + concurrent-first-use discipline as
    ``DatabricksSource.ensure_managed_schema()`` (#1108): probe before any
    ``CREATE``, and on a ``CREATE`` failure, re-probe rather than assume —
    swallow if another session won the race, re-raise otherwise. No
    ``conn.rollback()``: Delta has no multi-statement transactions or
    savepoints at all. The only ``CREATE`` in this whole module — every
    other method is DML-only (see module docstring).
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
        """Probe-then-``UPDATE``-or-``INSERT`` — see module docstring for
        why this isn't a ``MERGE`` (no scratch table, so no source relation
        without a `?`-in-`VALUES`/derived-table shape unverified for a
        single row) and isn't decided from ``cursor.rowcount`` (unreliable
        on this connector, see module docstring)."""
        conn = _connect(self._profile)
        try:
            self._ensure_table(conn)
            t = _qualified(self._profile, _RUNS_TABLE)
            cur = conn.cursor()
            cur.execute(f"SELECT 1 FROM {t} WHERE sync_name = ?", [state.sync_name])
            exists = cur.fetchone() is not None
            if exists:
                cur.execute(
                    f"UPDATE {t} SET last_run_at = ?, records_synced = ?, status = ?, "
                    "error = ?, last_cursor_value = ? WHERE sync_name = ?",
                    [
                        state.last_run_at,
                        state.records_synced,
                        state.status,
                        state.error,
                        state.last_cursor_value,
                        state.sync_name,
                    ],
                )
            else:
                cur.execute(
                    f"INSERT INTO {t} (sync_name, last_run_at, records_synced, status, "
                    "error, last_cursor_value) VALUES (?, ?, ?, ?, ?, ?)",
                    [
                        state.sync_name,
                        state.last_run_at,
                        state.records_synced,
                        state.status,
                        state.error,
                        state.last_cursor_value,
                    ],
                )
        finally:
            conn.close()

    def reset(self, sync_name: str) -> bool:
        if not _table_exists(self._profile, _RUNS_TABLE):
            return False
        conn = _connect(self._profile)
        try:
            t = _qualified(self._profile, _RUNS_TABLE)
            cur = conn.cursor()
            # Probed before DELETE rather than trusting cursor.rowcount
            # afterward — see module docstring.
            cur.execute(f"SELECT 1 FROM {t} WHERE sync_name = ?", [sync_name])
            existed = cur.fetchone() is not None
            cur.execute(f"DELETE FROM {t} WHERE sync_name = ?", [sync_name])
            return existed
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
        """Best-effort, like every other ``HistoryStore`` — see the Protocol.
        Pure append (no upsert semantics needed), so this is a single
        ``INSERT`` with no scratch table involved either way."""
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
            t = _qualified(self._profile, _HISTORY_TABLE)
            cur = conn.cursor()
            # Counted before DELETE rather than trusting cursor.rowcount
            # afterward — see module docstring.
            cur.execute(
                f"SELECT COUNT(*) FROM {t} WHERE sync_name = ? AND started_at < ?",
                [sync_name, cutoff],
            )
            row = cur.fetchone()
            count = int(row[0]) if row else 0
            if count:
                cur.execute(
                    f"DELETE FROM {t} WHERE sync_name = ? AND started_at < ?",
                    [sync_name, cutoff],
                )
            return count
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
        """Per-entry probe-then-``UPDATE``-or-``INSERT`` — see module
        docstring for why (no scratch table, matches every other dialect's
        one-statement-per-entry shape, #1121 tracks batching separately).

        The ``UPDATE`` branch does not touch ``sync_name`` on a match,
        matching Postgres's/Snowflake's own DLQ upsert precedent (this is a
        shared, pre-existing property across every dialect, not new here):
        matching globally on ``id`` and reassigning ``sync_name`` on match
        would let one sync's ``append()`` silently move another sync's row
        into its own queue on an id collision."""
        if not entries:
            return self.depth(sync_name)
        conn = _connect(self._profile)
        try:
            self._ensure_table(conn)
            t = _qualified(self._profile, _DLQ_TABLE)
            cur = conn.cursor()
            for entry in entries:
                cur.execute(f"SELECT 1 FROM {t} WHERE id = ?", [entry.id])
                exists = cur.fetchone() is not None
                if exists:
                    cur.execute(
                        f"UPDATE {t} SET record = parse_json(?), "
                        "error_message = ?, http_status = ?, ts = ?, attempts = ?, "
                        "sync_run_id = ? WHERE id = ?",
                        [
                            json.dumps(entry.record),
                            entry.error_message,
                            entry.http_status,
                            entry.timestamp,
                            entry.attempts,
                            entry.sync_run_id,
                            entry.id,
                        ],
                    )
                else:
                    cur.execute(
                        f"INSERT INTO {t} (id, sync_name, record, error_message, "
                        "http_status, ts, attempts, sync_run_id) "
                        "SELECT ?, ?, parse_json(?), ?, ?, ?, ?, ?",
                        _dlq_merge_values_params(sync_name, entry),
                    )
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
        """Wholesale-replace the queue without a scratch table or an
        explicit transaction — see module docstring for the full design
        history (an earlier scratch-table + single-``MERGE`` draft broke the
        documented escape hatch and got Databricks' ``MERGE`` clause order
        wrong; Codex review caught both before merge).

        New/changed entries are upserted **first**, via a chunked
        ``MERGE ... USING (VALUES ...) AS s(...)``; stale ids (existing but
        absent from ``entries``) are only deleted, by explicit id and
        chunked, **after every upsert chunk has succeeded**. This order
        matters for crash-safety, not just final correctness: deleting stale
        ids before the upsert would mean a crash or a failed merge (e.g. a
        constraint violation partway through) leaves the queue with the old
        rows already gone and the new ones not yet landed — an empty queue,
        exactly the #955 failure class this method exists to prevent (caught
        in review; an earlier draft had this order reversed). With upserts
        first, a crash before every chunk lands leaves the old queue
        untouched (stale-delete never ran) plus whichever new entries did
        land — never nothing. Not atomic across chunks — the queue is never
        left empty or destroyed by a crash mid-call, but is not guaranteed to
        reach the new state in one commit when `entries` needs more than one
        chunk.
        """
        conn = _connect(self._profile)
        try:
            self._ensure_table(conn)
            t = _qualified(self._profile, _DLQ_TABLE)
            cur = conn.cursor()
            if not entries:
                cur.execute(f"DELETE FROM {t} WHERE sync_name = ?", [sync_name])
                return

            cur.execute(f"SELECT id FROM {t} WHERE sync_name = ?", [sync_name])
            existing_ids = {row[0] for row in cur.fetchall()}
            new_ids = {entry.id for entry in entries}
            stale_ids = list(existing_ids - new_ids)

            rows_per_chunk = _rows_per_chunk(len(_DLQ_COLUMNS))
            for start in range(0, len(entries), rows_per_chunk):
                chunk_entries = entries[start : start + rows_per_chunk]
                values_sql = ", ".join(["(?, ?, ?, ?, ?, ?, ?, ?)"] * len(chunk_entries))
                params: list[Any] = []
                for entry in chunk_entries:
                    params.extend(_dlq_merge_values_params(sync_name, entry))
                cur.execute(
                    f"MERGE INTO {t} AS t USING (VALUES {values_sql}) AS "
                    f"s({', '.join(_DLQ_COLUMNS)}) "
                    "ON t.id = s.id "
                    "WHEN MATCHED THEN UPDATE SET record = parse_json(s.record), "
                    "error_message = s.error_message, http_status = s.http_status, "
                    "ts = s.ts, attempts = s.attempts, sync_run_id = s.sync_run_id "
                    "WHEN NOT MATCHED THEN INSERT (id, sync_name, record, error_message, "
                    "http_status, ts, attempts, sync_run_id) VALUES (s.id, s.sync_name, "
                    "parse_json(s.record), s.error_message, s.http_status, s.ts, "
                    "s.attempts, s.sync_run_id)",
                    params,
                )

            if stale_ids:
                chunk_size = _NATIVE_PARAM_LIMIT - 1  # one slot for sync_name
                for start in range(0, len(stale_ids), chunk_size):
                    chunk = stale_ids[start : start + chunk_size]
                    placeholders = ", ".join(["?"] * len(chunk))
                    cur.execute(
                        f"DELETE FROM {t} WHERE sync_name = ? AND id IN ({placeholders})",
                        [sync_name, *chunk],
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
