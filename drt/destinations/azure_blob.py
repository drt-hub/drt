"""Azure Blob destination — upload records as CSV / JSON / JSONL / Parquet to Azure Blob Storage.

Completes the v0.8 cloud-storage trio (S3 #168 / GCS #169 / Azure
Blob #170). This destination serialises each sync batch into a single
blob and uploads it via ``azure-storage-blob``.

Supported formats:

- ``csv`` — RFC 4180 CSV, stdlib ``csv.DictWriter``
- ``json`` — JSON array of objects
- ``jsonl`` — JSON Lines (one object per line, the data-lake standard)
- ``parquet`` — Parquet via ``pandas`` + ``pyarrow`` (requires the
  separate ``[parquet]`` extra; ``compression`` for csv/json/jsonl is
  ignored, see ``parquet_compression`` instead)

Naming: every sync writes one blob. The blob name defaults to
``<prefix><UTC ISO8601 basic>.<ext>`` — timestamped so re-runs land at
a fresh blob rather than overwriting (matches the S3 / GCS convention
and makes downstream "new blobs" polling trivial). Override with
``key_template`` to customise — the only supported placeholder is
``{timestamp}``. For per-sync routing, give each sync its own
``prefix``.

Authentication: two paths.

1. **Connection string** (most common for CI / cron / non-Azure
   environments) — set ``connection_string_env`` to the name of an
   env var holding the storage-account connection string
   (``DefaultEndpointsProtocol=...``).

2. **DefaultAzureCredential** chain (Azure-hosted apps with managed
   identity, plus local dev via Azure CLI) — set ``account_url`` to
   the storage account's blob endpoint
   (``https://<account>.blob.core.windows.net``) and leave
   ``connection_string_env`` unset. Resolves env vars → managed
   identity → VS Code → Azure CLI → Azure PowerShell in order.

Requires: ``pip install drt-core[azure]`` (and ``[parquet]`` if using
``format: parquet``).

Example sync YAML:

    destination:
      type: azure_blob
      container: data-exports
      prefix: drt/users/
      format: jsonl
      compression: gzip
      connection_string_env: AZURE_STORAGE_CONNECTION_STRING
"""

from __future__ import annotations

from typing import Any

from drt.config.credentials import resolve_env
from drt.config.models import AzureBlobDestinationConfig, DestinationConfig, SyncOptions
from drt.destinations._blob_serializer import build_object_key, serialise_records
from drt.destinations.base import SyncResult


class AzureBlobDestination:
    """Upload records as a single blob to an Azure Blob Storage container."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, AzureBlobDestinationConfig)
        if not records:
            # Empty-source short-circuit — no azure import, no Azure
            # call. Same shape as the other 27 registered destinations
            # (empty-batch contract suite, #604-#606).
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
            result.errors.append(f"Azure Blob destination serialisation failed: {e}")
            return result

        # Missing-extras ImportError (raised inside _service_client)
        # must bubble up rather than be recorded as a row failure —
        # it's a deployment mistake, not a transient upload error, and
        # the engine should surface it once at the top rather than
        # silently re-fail every batch.
        service_client = self._service_client(config)
        key = build_object_key(
            prefix=config.prefix,
            key_template=config.key_template,
            format=config.format,
            compression=config.compression,
        )

        # ContentSettings carries Content-Type + Content-Encoding
        # metadata on the blob (the Azure equivalent of S3's
        # ContentType / ContentEncoding put_object kwargs).
        content_settings = self._content_settings(content_type, content_encoding)
        blob_client = service_client.get_blob_client(container=config.container, blob=key)

        # Upload errors (network, auth, permissions) are recoverable —
        # log as row failures so the sync's other batches keep going.
        # ``overwrite=True`` matches the default key shape (timestamped,
        # so the same key never recurs in practice) and is what users
        # who set ``key_template`` to a fixed name expect.
        try:
            blob_client.upload_blob(body, overwrite=True, content_settings=content_settings)
            result.success = len(records)
        except Exception as e:
            result.failed = len(records)
            result.errors.append(f"Azure Blob destination upload failed: {e}")

        return result

    # -- azure-storage-blob client -------------------------------------------

    @staticmethod
    def _service_client(config: AzureBlobDestinationConfig) -> Any:
        try:
            from azure.storage.blob import BlobServiceClient  # type: ignore[import-untyped]
        except ImportError as e:
            raise ImportError("Azure Blob destination requires: pip install drt-core[azure]") from e

        if config.connection_string_env:
            connection_string = resolve_env(None, config.connection_string_env)
            if not connection_string:
                raise ValueError(
                    f"Azure Blob destination connection_string_env "
                    f"'{config.connection_string_env}' is empty or unset"
                )
            return BlobServiceClient.from_connection_string(connection_string)

        # DefaultAzureCredential path: account_url is required.
        if not config.account_url:
            raise ValueError(
                "Azure Blob destination requires either connection_string_env "
                "or account_url (for DefaultAzureCredential)"
            )
        try:
            from azure.identity import DefaultAzureCredential  # type: ignore[import-untyped]
        except ImportError as e:
            raise ImportError("Azure Blob destination requires: pip install drt-core[azure]") from e
        return BlobServiceClient(
            account_url=config.account_url, credential=DefaultAzureCredential()
        )

    @staticmethod
    def _content_settings(content_type: str, content_encoding: str | None) -> Any:
        from azure.storage.blob import ContentSettings  # type: ignore[import-untyped]

        kwargs: dict[str, Any] = {"content_type": content_type}
        if content_encoding is not None:
            kwargs["content_encoding"] = content_encoding
        return ContentSettings(**kwargs)
