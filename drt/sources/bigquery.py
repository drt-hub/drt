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

from drt.config.credentials import BigQueryProfile, ProfileConfig
from drt.config.query_tags import normalize_bigquery_label


class BigQuerySource:
    """Extract records from Google BigQuery."""

    def extract(
        self,
        query: str,
        config: ProfileConfig,
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

    def test_connection(self, config: ProfileConfig) -> bool:
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
