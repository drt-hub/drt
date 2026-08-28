"""Watermark storage backends for incremental sync.

Provides pluggable storage for cursor/watermark values:
- LocalWatermarkStorage: file-based (.drt/watermarks.json)
- GCSWatermarkStorage: Google Cloud Storage blob
- BigQueryWatermarkStorage: BigQuery _drt_watermarks table
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

# How many times a conditional watermark write may lose its race before giving
# up. Contention here is two syncs finishing at the same moment, not sustained
# load, so a handful of retries covers it; anything beyond that is a signal
# worth surfacing rather than absorbing.
_MAX_WRITE_ATTEMPTS = 5


class WatermarkContentionError(RuntimeError):
    """A conditional watermark write kept losing its race and was abandoned.

    Raised rather than swallowed: a watermark that silently fails to persist
    sends the next run back to a stale cursor, which is the failure this
    conditional write exists to prevent (#919).
    """


@runtime_checkable
class WatermarkStorage(Protocol):
    """Read and write watermark values for incremental syncs.

    Stability: Stable (frozen at v1.0, see ADR 0007 for the breaking-change policy).
    """

    def get(self, sync_name: str) -> str | None:
        """Return the stored watermark for ``sync_name``, or ``None`` if unset.

        Never raises for an unknown sync — an absent watermark is a normal
        first-run state, not an error.
        """
        ...

    def save(self, sync_name: str, value: str) -> None:
        """Persist ``value`` as the new watermark for ``sync_name``.

        Raises:
            WatermarkContentionError: a conditional write kept losing its
                race against concurrent writers past ``_MAX_WRITE_ATTEMPTS``
                (#919). Never silently drops the write.
        """
        ...

    def delete(self, sync_name: str) -> None:
        """Clear the stored watermark for ``sync_name`` (#776).

        A no-op when nothing is stored: reset is a recovery path, and someone
        clearing a poisoned cursor should not have to know whether a watermark
        was ever written. Never raises for an unknown sync.
        """
        ...


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

    def delete(self, sync_name: str) -> None:
        data = self._load()
        if data.pop(sync_name, None) is None:
            return  # nothing stored — don't create the file on a fresh project
        self._save_all(data)


def _gcs_client() -> Any:
    """Lazy GCS client — import only when needed."""
    try:
        # Import the submodule directly rather than the `storage` attribute
        # of the `google.cloud` namespace: google-cloud-bigquery ships
        # `py.typed`, which makes mypy treat `google.cloud` as a typed
        # namespace and reject `from google.cloud import storage` with
        # `attr-defined` if only `[bigquery]` (not `[gcs]`) is installed
        # (#561). Importing the submodule sidesteps the attribute lookup.
        from google.cloud.storage import Client  # type: ignore[import-untyped]
    except ImportError as e:
        raise ImportError("GCS watermark storage requires: pip install drt-core[gcs]") from e
    return Client()


def _gcs_precondition_errors() -> tuple[type[Exception], type[Exception]]:
    """Lazy ``(NotFound, PreconditionFailed)``, imported only when needed.

    Same reasoning as ``_gcs_client``: these live in ``google-api-core``, which
    only arrives with an extra, so importing them at module scope would make
    ``[gcs]`` a hard dependency of every install. Kept as a helper rather than
    inlined so the conditional-write path stays testable without one.
    """
    try:
        from google.api_core.exceptions import (  # type: ignore[import-untyped]
            NotFound,
            PreconditionFailed,
        )
    except ImportError as e:
        raise ImportError("GCS watermark storage requires: pip install drt-core[gcs]") from e
    return NotFound, PreconditionFailed


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

    def _read_for_update(self, blob: Any) -> tuple[dict[str, str], int]:
        """Load the watermarks along with the generation they were read at.

        Separate from ``_load`` because only the write path needs the
        generation: ``get`` is a plain read and has nothing to be conditional
        about. A generation of ``0`` means the object does not exist yet, which
        is also GCS's precondition value for "only if still absent".

        Raises the GCS precondition error if a competing writer lands between
        the ``reload()`` and the pinned download: the object still exists, just
        at a newer generation, so that is a 412 and not a 404. Callers must
        treat it as contention and retry, which is what ``_write`` does.
        """
        not_found, _ = _gcs_precondition_errors()

        try:
            blob.reload()
        except not_found:
            return {}, 0
        generation = int(blob.generation)
        try:
            # Pinned to the generation just read, so the bytes and the
            # precondition token cannot come from different versions.
            raw = blob.download_as_text(if_generation_match=generation)
        except not_found:
            return {}, 0
        try:
            data: dict[str, str] = json.loads(raw)
            return data, generation
        except (json.JSONDecodeError, ValueError):
            return {}, generation

    def _write(self, mutate: Callable[[dict[str, str]], bool]) -> None:
        """Apply ``mutate`` to the stored watermarks under a generation precondition.

        The whole blob is one object, so a read-modify-write cycle without a
        precondition loses any update that landed in between: two syncs
        finishing together would each write their own key over a base that no
        longer reflected the other, and the later upload would silently discard
        the earlier one (#919). ``if_generation_match`` turns that lost update
        into a 412, which is retried against freshly read state.

        ``mutate`` returns whether it changed anything, so a no-op skips the
        upload entirely.

        The read is inside the retry boundary, not before it: ``_read_for_update``
        is itself two round trips, and a writer landing between them fails the
        pinned download's precondition. That is contention like any other, so it
        costs an attempt and re-reads rather than escaping as a raw 412.
        """
        _, precondition_failed = _gcs_precondition_errors()

        for _ in range(_MAX_WRITE_ATTEMPTS):
            blob = self._blob()
            try:
                data, generation = self._read_for_update(blob)
            except precondition_failed:
                continue  # someone else wrote mid-read; re-read from scratch
            if not mutate(data):
                return
            try:
                blob.upload_from_string(
                    json.dumps(data, indent=2),
                    content_type="application/json",
                    if_generation_match=generation,
                )
            except precondition_failed:
                continue  # someone else wrote first; re-read and reapply
            return

        raise WatermarkContentionError(
            f"gs://{self._bucket_name}/{self._key} was modified by another writer "
            f"on every one of {_MAX_WRITE_ATTEMPTS} attempts, so the watermark was "
            f"not saved. Retry the sync; if this repeats, the same watermark blob is "
            f"probably shared by more concurrent syncs than it can serialize."
        )

    def get(self, sync_name: str) -> str | None:
        return self._load().get(sync_name)

    def save(self, sync_name: str, value: str) -> None:
        def apply(data: dict[str, str]) -> bool:
            data[sync_name] = value
            return True

        self._write(apply)

    def delete(self, sync_name: str) -> None:
        def apply(data: dict[str, str]) -> bool:
            # nothing stored: skip the upload round trip entirely
            return data.pop(sync_name, None) is not None

        self._write(apply)


def _bq_client(project: str | None = None) -> Any:
    """Lazy BigQuery client — import only when needed."""
    try:
        # Submodule direct import for consistency with `_gcs_client` and
        # `_query_config` below — `from google.cloud import bigquery` would
        # also work today (google-cloud-bigquery ships `py.typed`), but
        # keeping all three import sites in the same shape avoids
        # reintroducing the #561 attribute-lookup failure mode if the
        # google package layout changes.
        from google.cloud.bigquery import Client  # type: ignore[import-untyped]
    except ImportError as e:
        raise ImportError(
            "BigQuery watermark storage requires: pip install drt-core[bigquery]"
        ) from e
    return Client(project=project)


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
        from google.cloud.bigquery import (  # type: ignore[import-untyped]
            QueryJobConfig,
            ScalarQueryParameter,
        )

        return QueryJobConfig(
            query_parameters=[ScalarQueryParameter(name, type_, val) for name, type_, val in params]
        )

    def get(self, sync_name: str) -> str | None:
        client = self._client()
        query = f"SELECT watermark_value FROM {self._table} WHERE sync_name = @sync_name"
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
        job_config = self._query_config(
            [
                ("sync_name", "STRING", sync_name),
                ("value", "STRING", value),
            ]
        )
        client.query(merge, job_config=job_config).result()

    def delete(self, sync_name: str) -> None:
        # Parameterised like `save` above — a sync name is user-supplied and
        # has no business being interpolated into SQL. DELETE on a row that
        # isn't there is already a no-op in BigQuery, so an unknown sync needs
        # no special case.
        self._ensure_table()
        client = self._client()
        sql = f"DELETE FROM {self._table} WHERE sync_name = @sync_name"
        job_config = self._query_config([("sync_name", "STRING", sync_name)])
        client.query(sql, job_config=job_config).result()
