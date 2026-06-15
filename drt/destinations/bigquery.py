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
from typing import Any

from drt.config.models import BigQueryDestinationConfig, DestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError


class BigQueryDestination:
    """Write records into BigQuery tables."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, BigQueryDestinationConfig)
        if not records:
            return SyncResult()

        client = self._build_client(config)
        result = SyncResult()
        table_id = f"{config.project}.{config.dataset}.{config.table}"

        if config.mode == "insert":
            self._insert(client, table_id, records, sync_options, result)
        elif config.mode == "merge":
            if not config.upsert_key:
                raise ValueError("upsert_key is required for merge mode")
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
                result.failed += 1
                msg = next(
                    (str(e.get("errors", e)) for e in errors if e.get("index") == i),
                    "insert failed",
                )
                result.row_errors.append(
                    RowError(
                        batch_index=i,
                        record_preview=str(row)[:200],
                        http_status=None,
                        error_message=msg,
                    )
                )
            else:
                result.success += 1

        if failed_indices and sync_options.on_error == "fail":
            raise RuntimeError(
                f"BigQuery insert failed for {len(failed_indices)} row(s)"
            )

    def _merge(
        self,
        client: Any,
        table_id: str,
        records: list[dict[str, Any]],
        config: BigQueryDestinationConfig,
        sync_options: SyncOptions,
        result: SyncResult,
    ) -> None:
        """Upsert via a temp table + a single MERGE statement."""
        keys = config.upsert_key
        assert keys  # guarded in load()
        tmp_table_id = f"{table_id}_drt_tmp"
        columns = list(records[0].keys())

        try:
            client.load_table_from_json(records, tmp_table_id).result()

            on_clause = " AND ".join([f"T.{k} = S.{k}" for k in keys])
            update_cols = [c for c in columns if c not in keys]
            update_set = ", ".join([f"{c} = S.{c}" for c in update_cols])
            insert_cols = ", ".join(columns)
            insert_vals = ", ".join([f"S.{c}" for c in columns])
            matched = (
                f"WHEN MATCHED THEN UPDATE SET {update_set} " if update_cols else ""
            )

            merge_sql = (
                f"MERGE `{table_id}` T USING `{tmp_table_id}` S "
                f"ON {on_clause} "
                f"{matched}"
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
            )
            client.query(merge_sql).result()
            result.success += len(records)
        except Exception as e:
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
                raise
        finally:
            client.delete_table(tmp_table_id, not_found_ok=True)

    def test_connection(self, config: DestinationConfig) -> None:
        """Test connectivity by running ``SELECT 1``."""
        assert isinstance(config, BigQueryDestinationConfig)
        client = self._build_client(config)
        client.query("SELECT 1").result()

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
