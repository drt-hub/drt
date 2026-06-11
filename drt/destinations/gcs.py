"""GCS destination — upload records as CSV / JSON / JSONL / Parquet to Google Cloud Storage.

GCS is the natural pair for BigQuery users and a common Reverse-ETL
exchange point alongside S3 (#168) and Azure Blob (#170). This
destination serialises each sync batch into a single object and
uploads it via ``google-cloud-storage``.

Supported formats:

- ``csv`` — RFC 4180 CSV, stdlib ``csv.DictWriter``
- ``json`` — JSON array of objects
- ``jsonl`` — JSON Lines (one object per line, the data-lake standard)
- ``parquet`` — Parquet via ``pandas`` + ``pyarrow`` (requires the
  separate ``[parquet]`` extra; ``compression`` for csv/json/jsonl is
  ignored, see ``parquet_compression`` instead)

Naming: every sync writes one object. The blob name defaults to
``<prefix><UTC ISO8601 basic>.<ext>`` — timestamped so re-runs land at
a fresh object rather than overwriting (matches the S3 convention and
makes downstream "new files" polling trivial). Override with
``key_template`` to customise — the only supported placeholder is
``{timestamp}``. For per-sync routing, give each sync its own
``prefix``.

Authentication: by default, defers to Application Default Credentials
(``GOOGLE_APPLICATION_CREDENTIALS`` env var → ``gcloud auth
application-default login`` → GCE/GKE/Cloud Run service account),
which is the right shape for most real deployments. For explicit
overrides, provide ``credentials_path`` pointing at a service-account
JSON key.

Requires: ``pip install drt-core[gcs]`` (and ``[parquet]`` if using
``format: parquet``).

Example sync YAML:

    destination:
      type: gcs
      bucket: my-data-exports
      prefix: drt/users/
      format: jsonl
      compression: gzip
"""

from __future__ import annotations

from typing import Any

from drt.config.models import DestinationConfig, GCSDestinationConfig, SyncOptions
from drt.destinations._blob_serializer import build_object_key, serialise_records
from drt.destinations.base import SyncResult


class GCSDestination:
    """Upload records as a single object to a GCS bucket."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, GCSDestinationConfig)
        if not records:
            # Empty-source short-circuit — no google.cloud import, no
            # GCS call. Same shape as the other 26 registered
            # destinations (empty-batch contract suite, #604-#606).
            return SyncResult()

        result = SyncResult()

        try:
            body, content_type, content_encoding = serialise_records(
                records,
                format=config.format,
                compression=config.compression,
                parquet_compression=config.parquet_compression,
            )
        except Exception as e:
            # Includes the [parquet] extras ImportError path — matches
            # the established missing-driver row-failure behaviour the
            # SQL destinations use rather than crashing the whole sync.
            result.failed = len(records)
            result.errors.append(f"GCS destination serialisation failed: {e}")
            return result

        # Missing-extras ImportError (raised inside _client) must bubble
        # up rather than be recorded as a row failure — it's a deployment
        # mistake, not a transient upload error, and the engine should
        # surface it once at the top rather than silently re-fail every
        # batch.
        client = self._client(config)
        key = build_object_key(
            prefix=config.prefix,
            key_template=config.key_template,
            format=config.format,
            compression=config.compression,
        )
        bucket = client.bucket(config.bucket)
        blob = bucket.blob(key)
        if content_encoding is not None:
            # Set BEFORE upload — google-cloud-storage uses the property
            # at upload time to populate the object's Content-Encoding
            # metadata (downstream consumers see "gzip" and decompress
            # transparently).
            blob.content_encoding = content_encoding

        # Upload errors (network, auth, permissions) are recoverable —
        # log as row failures so the sync's other batches keep going.
        try:
            blob.upload_from_string(body, content_type=content_type)
            result.success = len(records)
        except Exception as e:
            result.failed = len(records)
            result.errors.append(f"GCS destination upload failed: {e}")

        return result

    # -- google-cloud-storage client -----------------------------------------

    @staticmethod
    def _client(config: GCSDestinationConfig) -> Any:
        try:
            from google.cloud import storage  # type: ignore[import-untyped]
        except ImportError as e:
            raise ImportError("GCS destination requires: pip install drt-core[gcs]") from e

        if config.credentials_path:
            # Explicit service-account JSON keyfile. Convenient for
            # CI / cron environments where ADC isn't available.
            return storage.Client.from_service_account_json(config.credentials_path)

        # Default: Application Default Credentials chain.
        # GOOGLE_APPLICATION_CREDENTIALS env var → gcloud
        # application-default → GCE/GKE/Cloud Run service account.
        return storage.Client(project=config.project_id) if config.project_id else storage.Client()
