"""BigQuery destination — write records back to BigQuery tables.

Supports:

- INSERT (append, ``config.mode: insert``) via the streaming insert API
  (``insert_rows_json``), which reports per-row errors.
- MERGE (upsert, ``config.mode: merge``) — load the batch into a temp table
  (``<table>_drt_tmp``) via ``load_table_from_json``, run a single
  ``MERGE INTO target USING tmp ON <upsert_key>`` (UPDATE matched / INSERT
  not-matched), then drop the temp table. BigQuery load + MERGE are
  job-level, so merge error handling is batch-level (coarser than the
  per-row staging used by the Snowflake / Databricks destinations).
- ``sync.mode: replace`` (#1055) — load-job replacement, with two strategies:
  - ``replace_strategy: truncate`` (default) — the first batch uses
    ``WRITE_TRUNCATE`` and later batches use ``WRITE_APPEND``. This avoids
    BigQuery's streaming-buffer restriction on ``TRUNCATE TABLE``.
  - ``replace_strategy: swap`` — load the same way into a
    ``<table>__drt_swap`` shadow, then atomically overwrite the target with a
    copy job using ``WRITE_TRUNCATE`` and drop the shadow.
- ``sync.mode: mirror`` (#1055) — MERGE each batch, stage its observed keys in
  ``<table>__drt_mirror_keys``, then delete target rows missing from that key
  table in :meth:`finalize_sync`. ``mirror.strategy: destination`` (the
  default) and ``mirror.scope`` are supported; ``tracked`` and ``diff`` are
  rejected explicitly.

Auth mirrors the BigQuery source: Application Default Credentials by default,
or a service-account ``keyfile``.

Install: ``pip install drt-core[bigquery]`` (``google-cloud-bigquery``).

The MERGE-via-temp-table approach and the ADC / keyfile auth are based on the
original contribution by @PFCAaron12 (Gloria Aaron) in
https://github.com/drt-hub/drt/pull/584; reshaped here to drt's ``Destination``
protocol (``load`` returning a ``SyncResult``, per-row error capture, and
``config.mode`` dispatch).
"""

from __future__ import annotations

import os
import re
from typing import Any

from drt.config.models import BigQueryDestinationConfig, DestinationConfig, SyncOptions
from drt.config.query_tags import normalize_bigquery_label
from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError, record_row_error
from drt.destinations.sql_base import _union_columns
from drt.destinations.sql_utils import (
    check_mirror_supported,
    unsupported_tracked_scope_msg,
)

_SWAP_SUFFIX = "__drt_swap"
_MIRROR_KEYS_SUFFIX = "__drt_mirror_keys"
_TMP_SUFFIX = "_drt_tmp"
_IDENTIFIER_PATTERNS = {
    "project": re.compile(r"^[A-Za-z0-9_.:-]+$"),
    "dataset": re.compile(r"^[A-Za-z0-9_]+$"),
    "table": re.compile(r"^[A-Za-z0-9_]+$"),
    "column": re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$"),
}


class BigQueryDestination:
    """Write records into BigQuery tables."""

    def __init__(self) -> None:
        self._replace_started = False
        self._swap_shadow_created = False
        self._swap_table_id: str | None = None
        self._mirror_keys_table_id: str | None = None
        self._mirror_aborted = False

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, BigQueryDestinationConfig)
        if not records:
            return SyncResult()

        self._validate_records(records)
        table_id = self._table_id(config)

        if sync_options.mode == "mirror":
            self._validate_sql_columns(records)
            self._validate_mirror(records, config, sync_options)
        elif sync_options.mode != "replace" and config.mode == "merge":
            self._validate_sql_columns(records)
            self._validate_upsert_keys_present(records, config.upsert_key)

        client = self._build_client(config)
        if sync_options.mode == "replace":
            return self._replace(client, table_id, records, sync_options)
        if sync_options.mode == "mirror":
            return self._merge(
                client,
                table_id,
                records,
                config,
                sync_options,
                mirror=True,
            )

        result = SyncResult()
        if config.mode == "insert":
            self._insert(client, table_id, records, sync_options, result)
        elif config.mode == "merge":
            self._merge(client, table_id, records, config, sync_options, result)
        else:
            raise ValueError(f"Unsupported mode: {config.mode}")

        return result

    def _insert(
        self,
        client: Any,
        table_id: str,
        records: list[dict[str, Any]],
        sync_options: SyncOptions,
        result: SyncResult,
    ) -> None:
        """Append via the streaming insert API, mapping per-row errors."""
        # insert_rows_json returns a list of {"index": int, "errors": [...]}
        # dicts — empty means every row landed.
        errors = client.insert_rows_json(table_id, records)
        failed_indices = {e.get("index") for e in errors if "index" in e}
        if errors and not failed_indices:
            # Errors without row indices — treat the whole batch as failed.
            failed_indices = set(range(len(records)))

        for i, row in enumerate(records):
            if i in failed_indices:
                msg = next(
                    (str(e.get("errors", e)) for e in errors if e.get("index") == i),
                    "insert failed",
                )
                record_row_error(
                    result,
                    i,
                    str(row)[:200],
                    RuntimeError(msg),
                    error_message=msg,
                )
            else:
                result.success += 1

        if failed_indices and sync_options.on_error == "fail":
            raise RuntimeError(f"BigQuery insert failed for {len(failed_indices)} row(s)")

    def _merge(
        self,
        client: Any,
        table_id: str,
        records: list[dict[str, Any]],
        config: BigQueryDestinationConfig,
        sync_options: SyncOptions,
        result: SyncResult | None = None,
        *,
        mirror: bool = False,
    ) -> SyncResult:
        """Upsert via a temp table + a single MERGE statement.

        Both calls are BigQuery *jobs* (unlike ``_insert``'s streaming-insert
        REST call, which has no job to label), so both get ``labels`` from
        ``sync_options._query_tags`` (#768) — the load and the query use
        different config classes (``LoadJobConfig`` / ``QueryJobConfig``),
        so this builds one of each rather than sharing a single object.
        """
        keys = config.upsert_key
        assert keys  # guarded in load() / _validate_mirror()
        if result is None:
            result = SyncResult()
        tmp_table_id = f"{table_id}{_TMP_SUFFIX}"
        columns = list(records[0].keys())
        labels = self._labels(sync_options._query_tags)
        mirror_keys_staged = False

        try:
            client.load_table_from_json(
                records,
                tmp_table_id,
                job_config=self._load_job_config(labels, write_disposition="truncate"),
            ).result()

            if mirror:
                self._stage_mirror_keys(
                    client,
                    table_id,
                    tmp_table_id,
                    keys,
                    sync_options,
                    labels,
                )
                mirror_keys_staged = True

            on_clause = " AND ".join(
                [f"T.{self._quote_column(k)} = S.{self._quote_column(k)}" for k in keys]
            )
            update_cols = [c for c in columns if c not in keys]
            update_set = ", ".join(
                [f"{self._quote_column(c)} = S.{self._quote_column(c)}" for c in update_cols]
            )
            insert_cols = ", ".join(self._quote_column(c) for c in columns)
            insert_vals = ", ".join([f"S.{self._quote_column(c)}" for c in columns])
            matched = f"WHEN MATCHED THEN UPDATE SET {update_set} " if update_cols else ""

            merge_sql = (
                f"MERGE {self._quote_table(table_id)} T "
                f"USING {self._quote_table(tmp_table_id)} S "
                f"ON {on_clause} "
                f"{matched}"
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
            )
            client.query(merge_sql, job_config=self._query_job_config(labels)).result()
            result.success += len(records)
        except Exception as e:
            if mirror and not mirror_keys_staged:
                # Without a complete source-key staging set, the final
                # anti-join could delete rows this run actually observed.
                self._mirror_aborted = True
            result.failed += len(records)
            result.row_errors.append(
                RowError(
                    batch_index=0,
                    record_preview=str(records[0])[:200],
                    http_status=None,
                    error_message=str(e),
                )
            )
            if sync_options.on_error == "fail":
                if mirror:
                    self._cleanup_mirror_staging(client)
                raise
        finally:
            client.delete_table(tmp_table_id, not_found_ok=True)

        return result

    def _replace(
        self,
        client: Any,
        table_id: str,
        records: list[dict[str, Any]],
        sync_options: SyncOptions,
    ) -> SyncResult:
        """Load one replace batch via atomic BigQuery load-job dispositions."""
        swap = sync_options.replace_strategy == "swap"
        destination = f"{table_id}{_SWAP_SUFFIX}" if swap else table_id
        labels = self._labels(sync_options._query_tags)
        result = SyncResult()

        try:
            if swap and not self._swap_shadow_created:
                # Seed the shadow from the target so its schema, partitioning,
                # and clustering match. The copied rows are immediately
                # discarded: copy jobs do not copy the target's streaming
                # buffer, so this shadow is safe to TRUNCATE even when the
                # live target is not (#1055).
                client.copy_table(
                    table_id,
                    destination,
                    job_config=self._copy_job_config(labels),
                ).result()
                client.query(
                    f"TRUNCATE TABLE {self._quote_table(destination)}",
                    job_config=self._query_job_config(labels),
                ).result()
                self._swap_shadow_created = True
                self._swap_table_id = table_id

            disposition = "append" if swap or self._replace_started else "truncate"
            client.load_table_from_json(
                records,
                destination,
                job_config=self._load_job_config(labels, write_disposition=disposition),
            ).result()
            result.success = len(records)
            if not swap:
                self._replace_started = True
        except Exception as e:
            result.failed = len(records)
            result.row_errors.append(
                RowError(
                    batch_index=0,
                    record_preview=str(records[0])[:200],
                    http_status=None,
                    error_message=str(e),
                )
            )
            if sync_options.on_error == "fail":
                if swap:
                    try:
                        client.delete_table(destination, not_found_ok=True)
                    finally:
                        self._reset_swap_state()
                else:
                    self._replace_started = False
                raise
        return result

    def _stage_mirror_keys(
        self,
        client: Any,
        table_id: str,
        tmp_table_id: str,
        upsert_key: list[str],
        sync_options: SyncOptions,
        labels: dict[str, str] | None,
    ) -> None:
        """Stage observed keys from the load-job temp table, never an IN list."""
        scope = sync_options.mirror.scope if sync_options.mirror is not None else None
        columns = list(dict.fromkeys([*upsert_key, *(scope or [])]))
        column_sql = ", ".join(self._quote_column(c) for c in columns)
        keys_table_id = f"{table_id}{_MIRROR_KEYS_SUFFIX}"
        keys_table = self._quote_table(keys_table_id)
        tmp_table = self._quote_table(tmp_table_id)
        if self._mirror_keys_table_id is None:
            sql = (
                f"CREATE OR REPLACE TABLE {keys_table} AS "
                f"SELECT DISTINCT {column_sql} FROM {tmp_table}"
            )
        else:
            sql = (
                f"INSERT INTO {keys_table} ({column_sql}) "
                f"SELECT DISTINCT {column_sql} FROM {tmp_table}"
            )
        client.query(sql, job_config=self._query_job_config(labels)).result()
        self._mirror_keys_table_id = keys_table_id

    def finalize_sync(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult | None:
        """Finish a swap replace or the mirror delete pass."""
        assert isinstance(config, BigQueryDestinationConfig)
        if sync_options.mode == "mirror":
            return self._finalize_mirror(config, sync_options)
        if sync_options.mode == "replace" and sync_options.replace_strategy != "swap":
            self._replace_started = False
            return None
        if (
            sync_options.mode != "replace"
            or not self._swap_shadow_created
            or self._swap_table_id is None
        ):
            return None

        client = self._build_client(config)
        table_id = self._swap_table_id
        shadow_table_id = f"{table_id}{_SWAP_SUFFIX}"
        labels = self._labels(sync_options._query_tags)
        try:
            client.copy_table(
                shadow_table_id,
                table_id,
                job_config=self._copy_job_config(labels),
            ).result()
            return SyncResult()
        finally:
            try:
                client.delete_table(shadow_table_id, not_found_ok=True)
            finally:
                self._reset_swap_state()

    def _finalize_mirror(
        self,
        config: BigQueryDestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult | None:
        keys_table_id = self._mirror_keys_table_id
        if keys_table_id is None:
            self._mirror_aborted = False
            return None

        client = self._build_client(config)
        labels = self._labels(sync_options._query_tags)
        try:
            if self._mirror_aborted:
                return None
            upsert_key = config.upsert_key
            assert upsert_key  # guarded by _validate_mirror()
            key_match = " AND ".join(
                [f"T.{self._quote_column(c)} = K.{self._quote_column(c)}" for c in upsert_key]
            )
            scope = sync_options.mirror.scope if sync_options.mirror is not None else None
            scope_prefix = ""
            if scope:
                scope_match = " AND ".join(
                    [f"T.{self._quote_column(c)} = K.{self._quote_column(c)}" for c in scope]
                )
                scope_prefix = (
                    f"EXISTS (SELECT 1 FROM {self._quote_table(keys_table_id)} K "
                    f"WHERE {scope_match}) AND "
                )
            sql = (
                f"DELETE FROM {self._quote_table(self._table_id(config))} AS T "
                f"WHERE {scope_prefix}NOT EXISTS "
                f"(SELECT 1 FROM {self._quote_table(keys_table_id)} K WHERE {key_match})"
            )
            client.query(sql, job_config=self._query_job_config(labels)).result()
            return SyncResult()
        finally:
            try:
                client.delete_table(keys_table_id, not_found_ok=True)
            finally:
                self._mirror_keys_table_id = None
                self._mirror_aborted = False

    def _cleanup_mirror_staging(self, client: Any) -> None:
        if self._mirror_keys_table_id is not None:
            client.delete_table(self._mirror_keys_table_id, not_found_ok=True)
        self._mirror_keys_table_id = None
        self._mirror_aborted = False

    def _reset_swap_state(self) -> None:
        self._swap_shadow_created = False
        self._swap_table_id = None

    def _validate_mirror(
        self,
        records: list[dict[str, Any]],
        config: BigQueryDestinationConfig,
        sync_options: SyncOptions,
    ) -> None:
        mirror = sync_options.mirror
        if mirror is not None and mirror.strategy == "diff":
            raise ValueError(
                "mirror.strategy: diff is not supported on bigquery until the "
                "BigQuery diff-source leg ships (#1113)."
            )
        if mirror is not None and mirror.strategy == "tracked":
            raise ValueError(unsupported_tracked_scope_msg("bigquery"))
        check_mirror_supported(
            config,
            sync_options,
            "bigquery",
            # BigQuery supports scope with destination strategy. The tracked
            # strategy was rejected explicitly just above.
            supports_tracked_scope=True,
        )
        assert config.upsert_key
        self._validate_upsert_keys_present(records, config.upsert_key)
        if mirror is not None and mirror.scope:
            missing = [c for c in mirror.scope if not all(c in record for record in records)]
            if missing:
                raise ValueError(
                    "mirror.scope columns missing from the model output: "
                    f"{missing} (available: {sorted(_union_columns(records))})"
                )

    def _validate_records(self, records: list[dict[str, Any]]) -> None:
        empty = [i for i, record in enumerate(records) if not record]
        if empty:
            raise ValueError(f"records at index {empty} have no fields at all -- nothing to write")

    def _validate_sql_columns(self, records: list[dict[str, Any]]) -> None:
        for column in _union_columns(records):
            self._validate_identifier("column", column)

    def _validate_upsert_keys_present(
        self, records: list[dict[str, Any]], upsert_key: list[str] | None
    ) -> None:
        if not upsert_key:
            raise ValueError("upsert_key is required for merge mode")
        missing = [c for c in upsert_key if not all(c in record for record in records)]
        if missing:
            raise ValueError(
                f"upsert_key columns missing from the model output: {missing} "
                "(every record must include every upsert_key column)"
            )

    @staticmethod
    def _validate_identifier(kind: str, value: str) -> str:
        if not value or _IDENTIFIER_PATTERNS[kind].fullmatch(value) is None:
            raise ValueError(f"Invalid BigQuery {kind} identifier: {value!r}")
        return value

    def _table_id(self, config: BigQueryDestinationConfig) -> str:
        project = self._validate_identifier("project", config.project)
        dataset = self._validate_identifier("dataset", config.dataset)
        table = self._validate_identifier("table", config.table)
        return f"{project}.{dataset}.{table}"

    def _quote_column(self, column: str) -> str:
        return f"`{self._validate_identifier('column', column)}`"

    @staticmethod
    def _quote_table(table_id: str) -> str:
        return f"`{table_id}`"

    def supported_modes(self) -> frozenset[str]:
        """Declare the advanced sync modes implemented by BigQuery (#1055)."""
        return frozenset({"replace", "mirror"})

    def _labels(self, query_tags: dict[str, str] | None) -> dict[str, str] | None:
        """BigQuery-safe label dict from the raw tag payload (#768), or
        ``None`` when tagging is off — see :func:`normalize_bigquery_label`
        for the constraint (lowercase ``[a-z0-9_-]``, <=63 chars)."""
        if not query_tags:
            return None
        return {
            normalize_bigquery_label(k): normalize_bigquery_label(v) for k, v in query_tags.items()
        }

    def _load_job_config(
        self,
        labels: dict[str, str] | None,
        *,
        write_disposition: str | None = None,
    ) -> Any:
        if labels is None and write_disposition is None:
            return None
        from google.cloud import bigquery

        kwargs: dict[str, Any] = {}
        if labels is not None:
            kwargs["labels"] = labels
        if write_disposition is not None:
            dispositions = {
                "truncate": bigquery.WriteDisposition.WRITE_TRUNCATE,
                "append": bigquery.WriteDisposition.WRITE_APPEND,
            }
            kwargs["write_disposition"] = dispositions[write_disposition]
        return bigquery.LoadJobConfig(**kwargs)

    def _query_job_config(self, labels: dict[str, str] | None) -> Any:
        if labels is None:
            return None
        from google.cloud import bigquery

        return bigquery.QueryJobConfig(labels=labels)

    def _copy_job_config(self, labels: dict[str, str] | None) -> Any:
        from google.cloud import bigquery

        kwargs: dict[str, Any] = {
            "write_disposition": bigquery.WriteDisposition.WRITE_TRUNCATE,
        }
        if labels is not None:
            kwargs["labels"] = labels
        return bigquery.CopyJobConfig(**kwargs)

    def test_connection(self, config: DestinationConfig) -> None:
        """Test access to the configured target table without creating a job.

        ``get_table`` requires ``bigquery.tables.get``, which is included in
        the same ``roles/bigquery.dataEditor`` role used by the documented
        minimal append-only credential. This avoids requiring the broader
        project-level ``bigquery.jobs.create`` permission just to validate.
        """
        assert isinstance(config, BigQueryDestinationConfig)
        client = self._build_client(config)
        table_id = self._table_id(config)
        client.get_table(table_id)

    def _build_client(self, config: BigQueryDestinationConfig) -> Any:
        """Build a BigQuery client (ADC by default, or a service-account keyfile)."""
        try:
            from google.cloud import bigquery
        except ImportError as e:
            raise ImportError(
                "BigQuery destination requires: pip install drt-core[bigquery]"
            ) from e

        if config.method == "keyfile":
            if not config.keyfile:
                raise ValueError("keyfile is required when method is 'keyfile'.")
            from google.oauth2 import service_account

            creds = service_account.Credentials.from_service_account_file(  # type: ignore[no-untyped-call]
                os.path.expanduser(config.keyfile)
            )
            return bigquery.Client(
                project=config.project,
                credentials=creds,
                location=config.location,
            )

        return bigquery.Client(project=config.project, location=config.location)
