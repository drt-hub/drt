"""Warehouse-backed state, history, and DLQ stores — BigQuery leg (#1107).

BigQuery counterpart to the Postgres, Snowflake, and Databricks warehouse
backends. It implements the existing frozen ``StateStore``, ``HistoryStore``,
and ``DlqBackend`` Protocols without changing their public surface.

The dataset and three tables are bootstrapped through BigQuery's client API,
using :class:`drt.sources.bigquery.BigQuerySource`'s managed-table probes. All
row writes are query-job DML with named query parameters; user-controlled
values are never interpolated into SQL. BigQuery has no multi-statement
transaction spanning these jobs, so the write ordering mirrors the
Databricks leg: state and append upserts probe then act, while DLQ replacement
upserts every chunk before deleting stale IDs. A multi-chunk replacement is
not fully atomic, but a failed upsert cannot first erase the old queue (the
#955 failure class).

``_drt_history.errors`` and ``_drt_dlq.record`` deliberately use ``STRING``.
They are serialized with ``json.dumps`` and parsed with ``json.loads`` rather
than relying on BigQuery's JSON type.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Collection, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any

from drt.config.credentials import BigQueryProfile
from drt.sources.bigquery import BigQuerySource
from drt.state.dlq import DeadLetter
from drt.state.history import HistoryEntry
from drt.state.manager import SyncState

logger = logging.getLogger(__name__)

_RUNS_TABLE = "_drt_runs"
_HISTORY_TABLE = "_drt_history"
_DLQ_TABLE = "_drt_dlq"
_DLQ_MERGE_CHUNK_SIZE = 500
_IDENTIFIER_PATTERNS = {
    "project": re.compile(r"^[A-Za-z0-9_.:-]+$"),
    "dataset": re.compile(r"^[A-Za-z0-9_]+$"),
    "table": re.compile(r"^[A-Za-z0-9_]+$"),
}

_RUNS_SCHEMA = (
    ("sync_name", "STRING", "REQUIRED"),
    ("last_run_at", "STRING", "REQUIRED"),
    ("records_synced", "INT64", "REQUIRED"),
    ("status", "STRING", "REQUIRED"),
    ("error", "STRING", "NULLABLE"),
    ("last_cursor_value", "STRING", "NULLABLE"),
)
_HISTORY_SCHEMA = (
    ("sync_name", "STRING", "REQUIRED"),
    ("started_at", "STRING", "REQUIRED"),
    ("completed_at", "STRING", "REQUIRED"),
    ("duration_seconds", "FLOAT64", "REQUIRED"),
    ("status", "STRING", "REQUIRED"),
    ("records_synced", "INT64", "REQUIRED"),
    ("records_failed", "INT64", "REQUIRED"),
    ("errors", "STRING", "REQUIRED"),
    ("cursor_value_used", "STRING", "NULLABLE"),
    ("dry_run", "BOOL", "REQUIRED"),
    ("run_id", "STRING", "NULLABLE"),
    ("sync_run_id", "STRING", "NULLABLE"),
)
_DLQ_SCHEMA = (
    ("id", "STRING", "REQUIRED"),
    ("sync_name", "STRING", "REQUIRED"),
    ("record", "STRING", "REQUIRED"),
    ("error_message", "STRING", "REQUIRED"),
    ("http_status", "INT64", "NULLABLE"),
    ("ts", "STRING", "REQUIRED"),
    ("attempts", "INT64", "REQUIRED"),
    ("sync_run_id", "STRING", "NULLABLE"),
)


def _connect(profile: BigQueryProfile) -> Any:
    return BigQuerySource()._build_client(profile)


def _validate_identifier(kind: str, value: str) -> str:
    pattern = _IDENTIFIER_PATTERNS[kind]
    if not value or pattern.fullmatch(value) is None:
        raise ValueError(f"Invalid BigQuery {kind} identifier: {value!r}")
    return value


def _table_id(profile: BigQueryProfile, table: str) -> str:
    project = _validate_identifier("project", profile.project)
    dataset = _validate_identifier("dataset", profile.managed_schema)
    table = _validate_identifier("table", table)
    return f"{project}.{dataset}.{table}"


def _qualified(profile: BigQueryProfile, table: str) -> str:
    return f"`{_table_id(profile, table)}`"


def _table_exists(profile: BigQueryProfile, table_name: str) -> bool:
    return BigQuerySource().managed_table_exists(profile, table_name)


def _scalar(name: str, type_: str, value: Any) -> Any:
    from google.cloud import bigquery

    return bigquery.ScalarQueryParameter(name, type_, value)


def _array(name: str, type_: str, values: Sequence[Any]) -> Any:
    from google.cloud import bigquery

    return bigquery.ArrayQueryParameter(name, type_, list(values))


def _query(client: Any, sql: str, parameters: Sequence[Any] = ()) -> Any:
    from google.cloud import bigquery

    job_config = bigquery.QueryJobConfig(query_parameters=list(parameters))
    return client.query(sql, job_config=job_config).result()


def _ensure_table_exists(
    client: Any,
    profile: BigQueryProfile,
    table_name: str,
    schema: Sequence[tuple[str, str, str]],
) -> None:
    """Create one managed table if absent, preserving the DML-only escape hatch.

    The pre-create probe means an operator can provision the table and grant
    only query-job DML. If concurrent first users race on ``create_table``, a
    second probe distinguishes the harmless loser from a real failure.
    """
    from google.cloud import bigquery

    source = BigQuerySource()
    source.ensure_managed_schema(profile)
    if source.managed_table_exists(profile, table_name):
        return
    fields = [bigquery.SchemaField(name, type_, mode=mode) for name, type_, mode in schema]
    table = bigquery.Table(_table_id(profile, table_name), schema=fields)
    try:
        client.create_table(table)
    except Exception:
        if not source.managed_table_exists(profile, table_name):
            raise


class BigQueryWarehouseStateStore:
    """``StateStore`` backed by a ``_drt_runs`` row per sync (#1107)."""

    def __init__(self, profile: BigQueryProfile) -> None:
        self._profile = profile

    def _ensure_table(self, client: Any) -> None:
        _ensure_table_exists(client, self._profile, _RUNS_TABLE, _RUNS_SCHEMA)

    def get_last_sync(self, sync_name: str) -> SyncState | None:
        if not _table_exists(self._profile, _RUNS_TABLE):
            return None
        client = _connect(self._profile)
        rows = list(
            _query(
                client,
                "SELECT last_run_at, records_synced, status, error, last_cursor_value "
                f"FROM {_qualified(self._profile, _RUNS_TABLE)} "
                "WHERE sync_name = @sync_name LIMIT 1",
                [_scalar("sync_name", "STRING", sync_name)],
            )
        )
        if not rows:
            return None
        return _row_to_sync_state(sync_name, rows[0])

    def get_all(self) -> dict[str, SyncState]:
        if not _table_exists(self._profile, _RUNS_TABLE):
            return {}
        client = _connect(self._profile)
        rows = _query(
            client,
            "SELECT sync_name, last_run_at, records_synced, status, error, "
            f"last_cursor_value FROM {_qualified(self._profile, _RUNS_TABLE)}",
        )
        return {row[0]: _row_to_sync_state(row[0], row[1:]) for row in rows}

    def save_sync(self, state: SyncState) -> None:
        client = _connect(self._profile)
        self._ensure_table(client)
        table = _qualified(self._profile, _RUNS_TABLE)
        probe = list(
            _query(
                client,
                f"SELECT 1 FROM {table} WHERE sync_name = @sync_name LIMIT 1",
                [_scalar("sync_name", "STRING", state.sync_name)],
            )
        )
        parameters = [
            _scalar("sync_name", "STRING", state.sync_name),
            _scalar("last_run_at", "STRING", state.last_run_at),
            _scalar("records_synced", "INT64", state.records_synced),
            _scalar("status", "STRING", state.status),
            _scalar("error", "STRING", state.error),
            _scalar("last_cursor_value", "STRING", state.last_cursor_value),
        ]
        if probe:
            _query(
                client,
                f"UPDATE {table} SET last_run_at = @last_run_at, "
                "records_synced = @records_synced, status = @status, error = @error, "
                "last_cursor_value = @last_cursor_value WHERE sync_name = @sync_name",
                parameters,
            )
        else:
            _query(
                client,
                f"INSERT INTO {table} (sync_name, last_run_at, records_synced, status, "
                "error, last_cursor_value) VALUES (@sync_name, @last_run_at, "
                "@records_synced, @status, @error, @last_cursor_value)",
                parameters,
            )

    def reset(self, sync_name: str) -> bool:
        if not _table_exists(self._profile, _RUNS_TABLE):
            return False
        client = _connect(self._profile)
        table = _qualified(self._profile, _RUNS_TABLE)
        parameter = _scalar("sync_name", "STRING", sync_name)
        existed = bool(
            list(
                _query(
                    client,
                    f"SELECT 1 FROM {table} WHERE sync_name = @sync_name LIMIT 1",
                    [parameter],
                )
            )
        )
        _query(client, f"DELETE FROM {table} WHERE sync_name = @sync_name", [parameter])
        return existed

    def now(self) -> str:
        return datetime.now(timezone.utc).isoformat()


def _row_to_sync_state(sync_name: str, row: Sequence[Any]) -> SyncState:
    last_run_at, records_synced, status, error, last_cursor_value = row
    return SyncState(
        sync_name=sync_name,
        last_run_at=last_run_at,
        records_synced=records_synced,
        status=status,
        error=error,
        last_cursor_value=last_cursor_value,
    )


class BigQueryWarehouseHistoryStore:
    """``HistoryStore`` backed by append-only ``_drt_history`` rows (#1107)."""

    def __init__(self, profile: BigQueryProfile) -> None:
        self._profile = profile

    def _ensure_table(self, client: Any) -> None:
        _ensure_table_exists(client, self._profile, _HISTORY_TABLE, _HISTORY_SCHEMA)

    def append(self, entry: HistoryEntry) -> None:
        """Best-effort, matching the existing ``HistoryStore`` contract."""
        try:
            client = _connect(self._profile)
            self._ensure_table(client)
            _query(
                client,
                f"INSERT INTO {_qualified(self._profile, _HISTORY_TABLE)} "
                "(sync_name, started_at, completed_at, duration_seconds, status, "
                "records_synced, records_failed, errors, cursor_value_used, dry_run, "
                "run_id, sync_run_id) VALUES (@sync_name, @started_at, @completed_at, "
                "@duration_seconds, @status, @records_synced, @records_failed, @errors, "
                "@cursor_value_used, @dry_run, @run_id, @sync_run_id)",
                [
                    _scalar("sync_name", "STRING", entry.sync_name),
                    _scalar("started_at", "STRING", entry.started_at),
                    _scalar("completed_at", "STRING", entry.completed_at),
                    _scalar("duration_seconds", "FLOAT64", entry.duration_seconds),
                    _scalar("status", "STRING", entry.status),
                    _scalar("records_synced", "INT64", entry.records_synced),
                    _scalar("records_failed", "INT64", entry.records_failed),
                    _scalar("errors", "STRING", json.dumps(entry.errors[:5])),
                    _scalar("cursor_value_used", "STRING", entry.cursor_value_used),
                    _scalar("dry_run", "BOOL", entry.dry_run),
                    _scalar("run_id", "STRING", entry.run_id),
                    _scalar("sync_run_id", "STRING", entry.sync_run_id),
                ],
            )
        except Exception as exc:  # noqa: BLE001 - best-effort, see Protocol docstring
            logger.warning("warehouse history append failed for sync=%s: %s", entry.sync_name, exc)

    def read(self, sync_name: str | None = None, limit: int = 20) -> list[HistoryEntry]:
        if not _table_exists(self._profile, _HISTORY_TABLE):
            return []
        client = _connect(self._profile)
        table = _qualified(self._profile, _HISTORY_TABLE)
        columns = (
            "sync_name, started_at, completed_at, duration_seconds, status, "
            "records_synced, records_failed, errors, cursor_value_used, dry_run, "
            "run_id, sync_run_id"
        )
        parameters = [_scalar("limit", "INT64", limit)]
        if sync_name is None:
            sql = f"SELECT {columns} FROM {table} ORDER BY started_at DESC LIMIT @limit"
        else:
            sql = (
                f"SELECT {columns} FROM {table} WHERE sync_name = @sync_name "
                "ORDER BY started_at DESC LIMIT @limit"
            )
            parameters.insert(0, _scalar("sync_name", "STRING", sync_name))
        return [_row_to_history_entry(row) for row in _query(client, sql, parameters)]

    def prune(self, sync_name: str, retention_days: int) -> int:
        if not _table_exists(self._profile, _HISTORY_TABLE):
            return 0
        cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).isoformat()
        client = _connect(self._profile)
        table = _qualified(self._profile, _HISTORY_TABLE)
        parameters = [
            _scalar("sync_name", "STRING", sync_name),
            _scalar("cutoff", "STRING", cutoff),
        ]
        rows = list(
            _query(
                client,
                f"SELECT COUNT(*) FROM {table} "
                "WHERE sync_name = @sync_name AND started_at < @cutoff",
                parameters,
            )
        )
        count = int(rows[0][0]) if rows else 0
        if count:
            _query(
                client,
                f"DELETE FROM {table} WHERE sync_name = @sync_name AND started_at < @cutoff",
                parameters,
            )
        return count


def _row_to_history_entry(row: Sequence[Any]) -> HistoryEntry:
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
        errors=json.loads(errors),
        cursor_value_used=cursor_value_used,
        dry_run=dry_run,
        run_id=run_id,
        sync_run_id=sync_run_id,
    )


def _dead_letter_values(sync_name: str, entry: DeadLetter) -> tuple[Any, ...]:
    return (
        entry.id,
        sync_name,
        json.dumps(entry.record),
        entry.error_message,
        entry.http_status,
        entry.timestamp,
        entry.attempts,
        entry.sync_run_id,
    )


def _struct_parameter(values: Sequence[Any], names_and_types: Sequence[tuple[str, str]]) -> Any:
    from google.cloud import bigquery

    return bigquery.StructQueryParameter(
        None,
        *[
            bigquery.ScalarQueryParameter(name, type_, value)
            for (name, type_), value in zip(names_and_types, values, strict=True)
        ],
    )


_DLQ_STRUCT_FIELDS = (
    ("id", "STRING"),
    ("sync_name", "STRING"),
    ("record", "STRING"),
    ("error_message", "STRING"),
    ("http_status", "INT64"),
    ("ts", "STRING"),
    ("attempts", "INT64"),
    ("sync_run_id", "STRING"),
)
_DLQ_UPDATE_STRUCT_FIELDS = (
    ("id", "STRING"),
    ("record", "STRING"),
    ("error_message", "STRING"),
    ("http_status", "INT64"),
    ("ts", "STRING"),
    ("attempts", "INT64"),
    ("sync_run_id", "STRING"),
)


def _struct_array(name: str, rows: Sequence[Any], fields: Sequence[tuple[str, str]]) -> Any:
    from google.cloud import bigquery

    field_types = [
        bigquery.ScalarQueryParameterType(type_, name=field_name) for field_name, type_ in fields
    ]
    struct_type = bigquery.StructQueryParameterType(*field_types)
    return bigquery.ArrayQueryParameter(name, struct_type, list(rows))


def _chunks(items: Sequence[Any], size: int = _DLQ_MERGE_CHUNK_SIZE) -> list[Sequence[Any]]:
    return [items[start : start + size] for start in range(0, len(items), size)]


def _upsert_dlq_entries(
    client: Any, table: str, sync_name: str, entries: Sequence[DeadLetter]
) -> None:
    """Chunked BigQuery ``MERGE`` from a real ``ARRAY<STRUCT>`` parameter."""
    for chunk in _chunks(entries):
        rows = [
            _struct_parameter(_dead_letter_values(sync_name, entry), _DLQ_STRUCT_FIELDS)
            for entry in chunk
        ]
        _query(
            client,
            f"MERGE {table} AS t USING (SELECT * FROM UNNEST(@rows)) AS s "
            "ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET record = s.record, "
            "error_message = s.error_message, http_status = s.http_status, ts = s.ts, "
            "attempts = s.attempts, sync_run_id = s.sync_run_id "
            "WHEN NOT MATCHED THEN INSERT (id, sync_name, record, error_message, "
            "http_status, ts, attempts, sync_run_id) VALUES (s.id, s.sync_name, "
            "s.record, s.error_message, s.http_status, s.ts, s.attempts, s.sync_run_id)",
            [_struct_array("rows", rows, _DLQ_STRUCT_FIELDS)],
        )


def _update_dlq_entries(
    client: Any,
    table: str,
    sync_name: str,
    update_items: Sequence[tuple[str, DeadLetter]],
) -> None:
    for chunk in _chunks(update_items):
        rows = [
            _struct_parameter(
                (
                    entry_id,
                    json.dumps(entry.record),
                    entry.error_message,
                    entry.http_status,
                    entry.timestamp,
                    entry.attempts,
                    entry.sync_run_id,
                ),
                _DLQ_UPDATE_STRUCT_FIELDS,
            )
            for entry_id, entry in chunk
        ]
        _query(
            client,
            f"MERGE {table} AS t USING (SELECT * FROM UNNEST(@rows)) AS s "
            "ON t.id = s.id AND t.sync_name = @sync_name "
            "WHEN MATCHED THEN UPDATE SET record = s.record, "
            "error_message = s.error_message, http_status = s.http_status, ts = s.ts, "
            "attempts = s.attempts, sync_run_id = s.sync_run_id",
            [
                _struct_array("rows", rows, _DLQ_UPDATE_STRUCT_FIELDS),
                _scalar("sync_name", "STRING", sync_name),
            ],
        )


class BigQueryWarehouseDlqBackend:
    """``DlqBackend`` backed by a ``_drt_dlq`` row per entry (#1107)."""

    def __init__(self, profile: BigQueryProfile) -> None:
        self._profile = profile

    def _ensure_table(self, client: Any) -> None:
        _ensure_table_exists(client, self._profile, _DLQ_TABLE, _DLQ_SCHEMA)

    def append(
        self, sync_name: str, entries: list[DeadLetter], *, max_records: int = 10_000
    ) -> int:
        if not entries:
            return self.depth(sync_name)
        deduped: dict[str, DeadLetter] = {}
        for entry in entries:
            deduped[entry.id] = entry

        client = _connect(self._profile)
        self._ensure_table(client)
        table = _qualified(self._profile, _DLQ_TABLE)
        for entry in deduped.values():
            id_parameter = _scalar("id", "STRING", entry.id)
            exists = bool(
                list(
                    _query(
                        client,
                        f"SELECT 1 FROM {table} WHERE id = @id LIMIT 1",
                        [id_parameter],
                    )
                )
            )
            values = _dead_letter_values(sync_name, entry)
            parameters = [
                _scalar(name, type_, value)
                for (name, type_), value in zip(_DLQ_STRUCT_FIELDS, values, strict=True)
            ]
            if exists:
                _query(
                    client,
                    f"UPDATE {table} SET record = @record, error_message = @error_message, "
                    "http_status = @http_status, ts = @ts, attempts = @attempts, "
                    "sync_run_id = @sync_run_id WHERE id = @id",
                    [parameter for parameter in parameters if parameter.name != "sync_name"],
                )
            else:
                _query(
                    client,
                    f"INSERT INTO {table} (id, sync_name, record, error_message, "
                    "http_status, ts, attempts, sync_run_id) VALUES (@id, @sync_name, "
                    "@record, @error_message, @http_status, @ts, @attempts, @sync_run_id)",
                    parameters,
                )
        if max_records > 0:
            _query(
                client,
                f"DELETE FROM {table} WHERE sync_name = @sync_name AND id NOT IN ("
                f"SELECT id FROM {table} WHERE sync_name = @sync_name "
                "ORDER BY ts DESC, id DESC LIMIT @max_records)",
                [
                    _scalar("sync_name", "STRING", sync_name),
                    _scalar("max_records", "INT64", max_records),
                ],
            )
        return self.depth(sync_name)

    def replace(self, sync_name: str, entries: list[DeadLetter]) -> None:
        client = _connect(self._profile)
        self._ensure_table(client)
        table = _qualified(self._profile, _DLQ_TABLE)
        sync_parameter = _scalar("sync_name", "STRING", sync_name)
        if not entries:
            _query(client, f"DELETE FROM {table} WHERE sync_name = @sync_name", [sync_parameter])
            return

        rows = _query(
            client,
            f"SELECT id FROM {table} WHERE sync_name = @sync_name",
            [sync_parameter],
        )
        existing_ids = {row[0] for row in rows}
        deduped: dict[str, DeadLetter] = {}
        for entry in entries:
            deduped[entry.id] = entry
        new_entries = list(deduped.values())
        stale_ids = list(existing_ids - set(deduped))

        # Deliberate crash-safe ordering: never delete the old queue until
        # every replacement upsert has succeeded.
        _upsert_dlq_entries(client, table, sync_name, new_entries)
        for chunk in _chunks(stale_ids):
            _query(
                client,
                f"DELETE FROM {table} WHERE sync_name = @sync_name AND id IN UNNEST(@ids)",
                [sync_parameter, _array("ids", "STRING", chunk)],
            )

    def clear(self, sync_name: str) -> None:
        self.replace(sync_name, [])

    def read(self, sync_name: str) -> list[DeadLetter]:
        if not _table_exists(self._profile, _DLQ_TABLE):
            return []
        client = _connect(self._profile)
        rows = _query(
            client,
            "SELECT id, record, error_message, http_status, ts, attempts, sync_run_id "
            f"FROM {_qualified(self._profile, _DLQ_TABLE)} WHERE sync_name = @sync_name "
            "ORDER BY ts ASC, id ASC",
            [_scalar("sync_name", "STRING", sync_name)],
        )
        return [_row_to_dead_letter(row) for row in rows]

    def depth(self, sync_name: str) -> int:
        if not _table_exists(self._profile, _DLQ_TABLE):
            return 0
        client = _connect(self._profile)
        rows = list(
            _query(
                client,
                f"SELECT COUNT(*) FROM {_qualified(self._profile, _DLQ_TABLE)} "
                "WHERE sync_name = @sync_name",
                [_scalar("sync_name", "STRING", sync_name)],
            )
        )
        return int(rows[0][0]) if rows else 0

    def all_depths(self) -> dict[str, int]:
        if not _table_exists(self._profile, _DLQ_TABLE):
            return {}
        client = _connect(self._profile)
        rows = _query(
            client,
            f"SELECT sync_name, COUNT(*) FROM {_qualified(self._profile, _DLQ_TABLE)} "
            "GROUP BY sync_name",
        )
        return {row[0]: int(row[1]) for row in rows if row[1]}

    def reconcile(
        self,
        sync_name: str,
        *,
        remove_ids: Collection[str] = (),
        updates: Mapping[str, DeadLetter] | None = None,
    ) -> list[DeadLetter]:
        updates = updates or {}
        client = _connect(self._profile)
        self._ensure_table(client)
        table = _qualified(self._profile, _DLQ_TABLE)
        sync_parameter = _scalar("sync_name", "STRING", sync_name)
        for chunk in _chunks(list(remove_ids)):
            _query(
                client,
                f"DELETE FROM {table} WHERE sync_name = @sync_name AND id IN UNNEST(@ids)",
                [sync_parameter, _array("ids", "STRING", chunk)],
            )
        if updates:
            _update_dlq_entries(client, table, sync_name, list(updates.items()))
        return self.read(sync_name)


def _row_to_dead_letter(row: Sequence[Any]) -> DeadLetter:
    entry_id, record, error_message, http_status, ts, attempts, sync_run_id = row
    return DeadLetter(
        record=json.loads(record),
        error_message=error_message,
        http_status=http_status,
        timestamp=ts,
        attempts=attempts,
        sync_run_id=sync_run_id,
        id=entry_id,
    )
