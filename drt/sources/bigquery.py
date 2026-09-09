"""BigQuery source implementation.

Requires: pip install drt-core[bigquery]

Authentication methods:
  application_default — uses gcloud ADC (recommended for local dev)
  keyfile             — explicit service account JSON file
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any

from drt.config.credentials import BigQueryProfile, ProfileConfigLike
from drt.config.query_tags import normalize_bigquery_label


class BigQuerySource:
    """Extract records from Google BigQuery."""

    def extract(
        self,
        query: str,
        config: ProfileConfigLike,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Run a SQL query and yield rows as dicts.

        ``query_tags`` (#768) becomes BigQuery job labels — the native
        mechanism ``INFORMATION_SCHEMA.JOBS`` cost-attribution queries key
        on, and the reason this needs its own path rather than relying on
        the engine's SQL-comment fallback alone: labels are queryable
        structured metadata, a comment is not.
        """
        assert isinstance(config, BigQueryProfile)
        client = self._build_client(config)
        job_config = self._job_config(query_tags)
        rows = client.query(query, job_config=job_config).result()
        for row in rows:
            yield dict(row)

    def _job_config(self, query_tags: dict[str, str] | None) -> Any:
        """``QueryJobConfig(labels=...)``, or ``None`` when tagging is off.

        BigQuery labels are queryable key-value pairs, not free text — both
        key and value are lowercase ``[a-z0-9_-]``, <=63 chars
        (:func:`normalize_bigquery_label`). ``client.query(query,
        job_config=None)`` behaves identically to omitting ``job_config``,
        so ``None`` here is not a special case downstream.
        """
        if not query_tags:
            return None
        from google.cloud import bigquery

        labels = {
            normalize_bigquery_label(k): normalize_bigquery_label(v) for k, v in query_tags.items()
        }
        return bigquery.QueryJobConfig(labels=labels)

    def test_connection(self, config: ProfileConfigLike) -> bool:
        """Return True if BigQuery is reachable with the given profile."""
        assert isinstance(config, BigQueryProfile)
        try:
            client = self._build_client(config)
            client.query("SELECT 1").result()
            return True
        except Exception:
            return False

    def _build_client(self, config: BigQueryProfile) -> Any:
        try:
            from google.cloud import bigquery
        except ImportError as e:
            raise ImportError("BigQuery support requires: pip install drt-core[bigquery]") from e

        if config.method == "keyfile" and config.keyfile:
            from google.oauth2 import service_account

            creds = service_account.Credentials.from_service_account_file(  # type: ignore[no-untyped-call]
                os.path.expanduser(config.keyfile)
            )
            return bigquery.Client(
                project=config.project,
                credentials=creds,
                location=config.location,
            )

        # Application Default Credentials (gcloud auth application-default login)
        return bigquery.Client(project=config.project, location=config.location)

    # --- ManagedTableCapable (#960/#1107, ADR 0005 step 3) ------------------
    #
    # Client-API based (get_dataset/create_dataset/get_table/delete_table),
    # not SQL DDL — mirrors destinations/bigquery.py's test_connection(),
    # which deliberately uses get_table() instead of a query job specifically
    # to avoid requiring the project-level bigquery.jobs.create permission
    # just to check something exists. Every probe/create/drop here stays on
    # that same client-API surface rather than mixing in DDL execution
    # (CREATE SCHEMA/TABLE, DROP TABLE), which would need bigquery.jobs.create
    # for the create/drop half even if the probes didn't.
    #
    # Unlike Postgres/Snowflake/Databricks, a single client.get_table() call
    # here already distinguishes "table absent" from "dataset absent" with
    # the same exception (NotFound) — there is no Databricks-style
    # SCHEMA_NOT_FOUND special case to guard against, so managed_table_exists
    # needs no separate dataset probe first.

    def ensure_managed_schema(self, config: ProfileConfigLike) -> None:
        """Create ``config.managed_schema`` as a dataset in ``config.project``
        if it does not already exist.

        Same two-part discipline as the other dialects:

        1. **The escape hatch**: probe first (``get_dataset``) — a
           locked-down principal with no ``bigquery.datasets.create``
           permission, but an admin-pre-provisioned dataset, must never have
           ``create_dataset`` called at all.
        2. **Concurrent first use**: on any exception from ``create_dataset``,
           re-probe rather than assuming failure — if the dataset exists now
           (another session won a first-use race, surfaced by BigQuery as
           ``Conflict``), that's success; anything else re-raises.
        """
        assert isinstance(config, BigQueryProfile)
        from google.api_core.exceptions import NotFound
        from google.cloud import bigquery

        client = self._build_client(config)
        dataset_ref = f"{config.project}.{config.managed_schema}"
        try:
            client.get_dataset(dataset_ref)
            return
        except NotFound:
            pass
        try:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = config.location
            client.create_dataset(dataset)
        except Exception:
            # Re-probe rather than assuming failure: another session may
            # have won a first-use race (surfaced as Conflict, but caught
            # broadly since the exact type isn't load-bearing here). The
            # inner except's `pass` exits that frame before the bare
            # `raise` below, so `raise` re-raises the original create
            # failure being handled by this outer `except`, not NotFound.
            try:
                client.get_dataset(dataset_ref)
                return
            except NotFound:
                pass
            raise

    def managed_table_exists(self, config: ProfileConfigLike, table_name: str) -> bool:
        """Only a plain base ``TABLE`` counts — a view/materialized view/
        snapshot/external table sharing this name is a different resource
        this capability doesn't own (caught in Codex review: treating any of
        those as "the managed table exists" would make a consumer skip its
        own ``CREATE TABLE`` and then fail on writes against, say, a view)."""
        assert isinstance(config, BigQueryProfile)
        from google.api_core.exceptions import NotFound

        client = self._build_client(config)
        table_id = f"{config.project}.{config.managed_schema}.{table_name}"
        try:
            table = client.get_table(table_id)
        except NotFound:
            return False
        return bool(table.table_type == "TABLE")

    def drop_managed_table(self, config: ProfileConfigLike, table_name: str) -> None:
        """No-op if absent *or* if ``table_name`` resolves to a non-table
        resource (view/materialized view/snapshot/external table) — this
        capability only ever creates plain base tables, so it must never
        delete something it doesn't own just because the name matches
        (caught in the same Codex review as the probe fix above)."""
        assert isinstance(config, BigQueryProfile)
        from google.api_core.exceptions import NotFound

        client = self._build_client(config)
        table_id = f"{config.project}.{config.managed_schema}.{table_name}"
        try:
            table = client.get_table(table_id)
        except NotFound:
            return
        if table.table_type != "TABLE":
            return
        client.delete_table(table_id)
