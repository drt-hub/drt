"""Watermark storage backends for incremental sync.

Provides pluggable storage for cursor/watermark values:
- LocalWatermarkStorage: file-based (.drt/watermarks.json)
- GCSWatermarkStorage: Google Cloud Storage blob
- BigQueryWatermarkStorage: BigQuery _drt_watermarks table
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Protocol


class WatermarkStorage(Protocol):
    """Read and write watermark values for incremental syncs."""

    def get(self, sync_name: str) -> str | None: ...
    def save(self, sync_name: str, value: str) -> None: ...


class LocalWatermarkStorage:
    """File-based watermark storage using .drt/watermarks.json."""

    def __init__(self, project_dir: Path) -> None:
        self._state_dir = project_dir / ".drt"
        self._file = self._state_dir / "watermarks.json"

    def _load(self) -> dict[str, str]:
        if not self._file.exists():
            return {}
        try:
            with self._file.open() as f:
                data: dict[str, str] = json.load(f) or {}
                return data
        except (json.JSONDecodeError, ValueError):
            return {}

    def _save_all(self, data: dict[str, str]) -> None:
        self._state_dir.mkdir(exist_ok=True)
        with self._file.open("w") as f:
            json.dump(data, f, indent=2)

    def get(self, sync_name: str) -> str | None:
        return self._load().get(sync_name)

    def save(self, sync_name: str, value: str) -> None:
        data = self._load()
        data[sync_name] = value
        self._save_all(data)


def _gcs_client() -> Any:
    """Lazy GCS client — import only when needed."""
    try:
        from google.cloud import storage  # type: ignore[import-untyped]
    except ImportError as e:
        raise ImportError(
            "GCS watermark storage requires: pip install drt-core[gcs]"
        ) from e
    return storage.Client()


class GCSWatermarkStorage:
    """Google Cloud Storage watermark backend.

    Stores watermarks as a JSON object in a single GCS blob.
    """

    def __init__(self, bucket: str, key: str) -> None:
        self._bucket_name = bucket
        self._key = key

    def _blob(self) -> Any:
        client = _gcs_client()
        return client.bucket(self._bucket_name).blob(self._key)

    def _load(self) -> dict[str, str]:
        blob = self._blob()
        if not blob.exists():
            return {}
        try:
            data: dict[str, str] = json.loads(blob.download_as_text())
            return data
        except (json.JSONDecodeError, ValueError):
            return {}

    def get(self, sync_name: str) -> str | None:
        return self._load().get(sync_name)

    def save(self, sync_name: str, value: str) -> None:
        data = self._load()
        data[sync_name] = value
        self._blob().upload_from_string(
            json.dumps(data, indent=2), content_type="application/json",
        )


def _bq_client(project: str | None = None) -> Any:
    """Lazy BigQuery client — import only when needed."""
    try:
        from google.cloud import bigquery  # type: ignore[import-untyped]
    except ImportError as e:
        raise ImportError(
            "BigQuery watermark storage requires: pip install drt-core[bigquery]"
        ) from e
    return bigquery.Client(project=project)


class BigQueryWatermarkStorage:
    """BigQuery watermark backend.

    Stores watermarks in a ``_drt_watermarks`` table within the specified dataset.
    Table is auto-created on first write.
    """

    def __init__(self, project: str, dataset: str) -> None:
        self._project = project
        self._dataset = dataset
        self._table = f"`{project}`.`{dataset}`._drt_watermarks"
        self._table_ensured = False

    def _client(self) -> Any:
        return _bq_client(self._project)

    def _ensure_table(self) -> None:
        if self._table_ensured:
            return
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {self._table} ("
            "  sync_name STRING NOT NULL,"
            "  watermark_value STRING NOT NULL,"
            "  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()"
            ")"
        )
        self._client().query(ddl).result()
        self._table_ensured = True

    def _query_config(self, params: list[tuple[str, str, str]]) -> Any:
        """Build a QueryJobConfig with parameterized query parameters."""
        from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter  # type: ignore[import-untyped]

        return QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter(name, type_, val)
                for name, type_, val in params
            ]
        )

    def get(self, sync_name: str) -> str | None:
        client = self._client()
        query = (
            f"SELECT watermark_value FROM {self._table} "
            "WHERE sync_name = @sync_name"
        )
        job_config = self._query_config([("sync_name", "STRING", sync_name)])
        rows = list(client.query(query, job_config=job_config).result())
        if not rows:
            return None
        return str(rows[0].watermark_value)

    def save(self, sync_name: str, value: str) -> None:
        self._ensure_table()
        client = self._client()
        merge = (
            f"MERGE {self._table} AS t "
            "USING (SELECT @sync_name AS sync_name, "
            "@value AS watermark_value) AS s "
            "ON t.sync_name = s.sync_name "
            "WHEN MATCHED THEN UPDATE SET "
            "  watermark_value = s.watermark_value, "
            "  updated_at = CURRENT_TIMESTAMP() "
            "WHEN NOT MATCHED THEN INSERT (sync_name, watermark_value) "
            "  VALUES (s.sync_name, s.watermark_value)"
        )
        job_config = self._query_config([
            ("sync_name", "STRING", sync_name),
            ("value", "STRING", value),
        ])
        client.query(merge, job_config=job_config).result()
