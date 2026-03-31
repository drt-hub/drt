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


class BigQuerySource:
    """Extract records from Google BigQuery."""

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict[str, Any]]:
        """Run a SQL query and yield rows as dicts."""
        assert isinstance(config, BigQueryProfile)
        client = self._build_client(config)
        rows = client.query(query).result()
        for row in rows:
            yield dict(row)

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

            creds = service_account.Credentials.from_service_account_file(
                os.path.expanduser(config.keyfile)
            )
            return bigquery.Client(
                project=config.project,
                credentials=creds,
                location=config.location,
            )

        # Application Default Credentials (gcloud auth application-default login)
        return bigquery.Client(project=config.project, location=config.location)
