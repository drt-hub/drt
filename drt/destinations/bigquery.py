"""
BigQuery destination connector for drt.

Supports:
- INSERT (append)
- MERGE (upsert)

Authentication:
- Application Default Credentials
- Service Account Keyfile

Enables DWH-to-DWH workflows — e.g., syncing processed data from one dataset
to another, or writing ML predictions back to BigQuery for BI consumption.

Example config:
```yaml
destination:
  type: bigquery
  project: my-gcp-project
  dataset: target_dataset
  table: user_scores
  upsert_key: [user_id]
  method: application_default
"""

import logging
from dataclasses import dataclass
from typing import Any

from google.cloud import bigquery
from google.oauth2 import service_account

logger = logging.getLogger(__name__)


@dataclass
class BigQueryDestinationConfig:
    type: str
    project: str
    dataset: str
    table: str
    upsert_key: list[str] | None = None
    method: str = "application_default"  # or "service_account"
    keyfile: str | None = None


class BigQueryDestination:
    def __init__(self, config: BigQueryDestinationConfig):
        self.config = config
        self.client = self._create_client()

    def _create_client(self) -> bigquery.Client:
        """Initialize BigQuery client based on auth method."""
        if self.config.method == "service_account":
            if not self.config.keyfile:
                raise ValueError("keyfile must be provided for service_account auth")

            credentials = service_account.Credentials.from_service_account_file(self.config.keyfile)
            return bigquery.Client(
                project=self.config.project,
                credentials=credentials,
            )

        # Default: Application Default Credentials
        return bigquery.Client(project=self.config.project)

    @property
    def table_id(self) -> str:
        return f"{self.config.project}.{self.config.dataset}.{self.config.table}"

    def write(self, rows: list[dict[str, Any]], mode: str = "insert"):
        """
        Write data to BigQuery.

        mode:
            - insert: append rows
            - merge: upsert rows using upsert_key
        """
        if not rows:
            logger.info("No rows to write.")
            return

        if mode == "insert":
            self._insert(rows)
        elif mode == "merge":
            if not self.config.upsert_key:
                raise ValueError("upsert_key must be provided for merge mode")
            self._merge(rows)
        else:
            raise ValueError(f"Unsupported mode: {mode}")

    def _insert(self, rows: list[dict[str, Any]]):
        """Append rows to BigQuery table."""
        errors = self.client.insert_rows_json(self.table_id, rows)

        if errors:
            logger.error(f"Insert errors: {errors}")
            raise RuntimeError(f"BigQuery insert failed: {errors}")

        logger.info(f"Inserted {len(rows)} rows into {self.table_id}")

    def _merge(self, rows: list[dict[str, Any]]):
        """
        Perform MERGE (upsert) using a temporary table.
        """
        temp_table_id = f"{self.table_id}_temp"

        # Step 1: Load rows into temp table
        job = self.client.load_table_from_json(rows, temp_table_id)
        job.result()

        logger.info(f"Loaded {len(rows)} rows into temp table {temp_table_id}")

        # Step 2: Build MERGE query
        keys = self.config.upsert_key
        on_clause = " AND ".join([f"T.{k} = S.{k}" for k in keys])

        update_columns = rows[0].keys()
        update_set = ", ".join([f"{col} = S.{col}" for col in update_columns if col not in keys])

        insert_columns = ", ".join(update_columns)
        insert_values = ", ".join([f"S.{col}" for col in update_columns])

        merge_query = f"""
        MERGE `{self.table_id}` T
        USING `{temp_table_id}` S
        ON {on_clause}
        WHEN MATCHED THEN
          UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
          INSERT ({insert_columns})
          VALUES ({insert_values})
        """

        logger.debug(f"MERGE query:\n{merge_query}")

        # Step 3: Execute MERGE
        query_job = self.client.query(merge_query)
        query_job.result()

        logger.info(f"Merge completed into {self.table_id}")

        # Step 4: Cleanup temp table
        self.client.delete_table(temp_table_id, not_found_ok=True)
        logger.info(f"Deleted temp table {temp_table_id}")
