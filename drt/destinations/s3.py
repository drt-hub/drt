"""S3 destination — upload records as CSV / JSON / JSONL / Parquet to Amazon S3.

S3 is one of the most common Reverse-ETL exchange points: downstream
systems poll a bucket prefix for new files and pick them up on a
schedule. This destination serialises each sync batch into a single
file and uploads it via ``boto3``.

Supported formats:

- ``csv`` — RFC 4180 CSV, stdlib ``csv.DictWriter``
- ``json`` — JSON array of objects
- ``jsonl`` — JSON Lines (one object per line, the data-lake standard)
- ``parquet`` — Parquet via ``pandas`` + ``pyarrow`` (requires the
  separate ``[parquet]`` extra; ``compression`` for csv/json/jsonl is
  ignored, see ``parquet_compression`` instead)

Naming: every sync writes one file. The S3 key defaults to
``<prefix><UTC ISO8601 basic>.<ext>`` — timestamped so re-runs land at
a fresh key rather than overwriting (which matches the Census /
Hightouch convention and makes downstream "new files" polling
trivial). Override with ``key_template`` to customise — the only
supported placeholder is ``{timestamp}``. For per-sync routing, give
each sync its own ``prefix``.

Authentication: by default, defers to boto3's standard credential
chain (env vars → ``~/.aws/credentials`` → instance profile → IAM
role), which is the right shape for most real deployments. For
explicit overrides, provide one of:

- ``aws_profile``: a named profile in ``~/.aws/credentials``
- ``aws_access_key_id_env`` / ``aws_secret_access_key_env`` /
  ``aws_session_token_env``: env-var names to read credentials from

For S3-compatible services (MinIO, LocalStack, Cloudflare R2,
DigitalOcean Spaces) set ``endpoint_url``.

Requires: ``pip install drt-core[s3]`` (and ``[parquet]`` if using
``format: parquet``).

Example sync YAML:

    destination:
      type: s3
      bucket: my-data-exports
      prefix: drt/users/
      format: jsonl
      compression: gzip
      region: us-east-1
"""

from __future__ import annotations

import csv
import gzip
import io
import json
from datetime import datetime, timezone
from typing import Any

from drt.config.credentials import resolve_env
from drt.config.models import DestinationConfig, S3DestinationConfig, SyncOptions
from drt.destinations.base import SyncResult

_FORMAT_EXTENSIONS: dict[str, str] = {
    "csv": "csv",
    "json": "json",
    "jsonl": "jsonl",
    "parquet": "parquet",
}


class S3Destination:
    """Upload records as a single file to an S3 bucket."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, S3DestinationConfig)
        if not records:
            # Empty-source short-circuit — no boto3 client, no AWS call.
            # Same shape as the other 25 registered destinations
            # (empty-batch contract suite, #604-#606).
            return SyncResult()

        result = SyncResult()

        try:
            body, content_type, content_encoding = self._serialise(records, config)
        except Exception as e:
            result.failed = len(records)
            result.errors.append(f"S3 destination serialisation failed: {e}")
            return result

        # Missing-extras ImportError (raised inside _client) must bubble
        # up rather than be recorded as a row failure — it's a deployment
        # mistake, not a transient upload error, and the engine should
        # surface it once at the top rather than silently re-fail every
        # batch.
        client = self._client(config)
        key = self._build_key(config, sync_options)
        put_kwargs: dict[str, Any] = {
            "Bucket": config.bucket,
            "Key": key,
            "Body": body,
            "ContentType": content_type,
        }
        if content_encoding is not None:
            put_kwargs["ContentEncoding"] = content_encoding

        # Upload errors (network, auth, permissions) are recoverable —
        # log as row failures so the sync's other batches keep going.
        try:
            client.put_object(**put_kwargs)
            result.success = len(records)
        except Exception as e:
            result.failed = len(records)
            result.errors.append(f"S3 destination upload failed: {e}")

        return result

    # -- serialisation -------------------------------------------------------

    @staticmethod
    def _serialise(
        records: list[dict[str, Any]],
        config: S3DestinationConfig,
    ) -> tuple[bytes, str, str | None]:
        """Return ``(body_bytes, content_type, content_encoding | None)``.

        Parquet is binary and ignores the gzip flag — Parquet has its
        own column-level compression configured via
        ``parquet_compression``.
        """
        if config.format == "parquet":
            body = S3Destination._serialise_parquet(records, config)
            return body, "application/octet-stream", None

        if config.format == "csv":
            text = S3Destination._serialise_csv(records)
            content_type = "text/csv"
        elif config.format == "json":
            text = json.dumps(records, default=str)
            content_type = "application/json"
        else:  # jsonl
            text = "\n".join(json.dumps(r, default=str) for r in records)
            content_type = "application/x-ndjson"

        raw = text.encode("utf-8")
        if config.compression == "gzip":
            return gzip.compress(raw), content_type, "gzip"
        return raw, content_type, None

    @staticmethod
    def _serialise_csv(records: list[dict[str, Any]]) -> str:
        buf = io.StringIO()
        columns = list(records[0].keys())
        writer = csv.DictWriter(buf, fieldnames=columns)
        writer.writeheader()
        writer.writerows(records)
        return buf.getvalue()

    @staticmethod
    def _serialise_parquet(
        records: list[dict[str, Any]],
        config: S3DestinationConfig,
    ) -> bytes:
        try:
            import pandas as pd  # type: ignore[import-untyped]
            import pyarrow  # type: ignore[import-untyped]  # noqa: F401
        except ImportError as e:
            raise ImportError(
                "S3 destination with format: parquet requires pip install drt-core[parquet]"
            ) from e

        compression = config.parquet_compression if config.parquet_compression != "none" else None
        df = pd.DataFrame(records)
        buf = io.BytesIO()
        df.to_parquet(buf, engine="pyarrow", compression=compression, index=False)
        return buf.getvalue()

    # -- key naming ----------------------------------------------------------

    @staticmethod
    def _build_key(config: S3DestinationConfig, sync_options: SyncOptions) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        extension = _FORMAT_EXTENSIONS[config.format]
        if config.compression == "gzip" and config.format != "parquet":
            extension = f"{extension}.gz"

        if config.key_template:
            file_part = config.key_template.format(timestamp=timestamp)
            # If the template already supplies an extension, leave it
            # alone; otherwise append the format-derived one.
            if "." not in file_part.rsplit("/", 1)[-1]:
                file_part = f"{file_part}.{extension}"
            return f"{config.prefix}{file_part}" if config.prefix else file_part

        return f"{config.prefix}{timestamp}.{extension}"

    # -- boto3 client --------------------------------------------------------

    @staticmethod
    def _client(config: S3DestinationConfig) -> Any:
        try:
            import boto3  # type: ignore[import-untyped]
        except ImportError as e:
            raise ImportError("S3 destination requires: pip install drt-core[s3]") from e

        session_kwargs: dict[str, Any] = {}
        if config.aws_profile:
            session_kwargs["profile_name"] = config.aws_profile
        if config.region:
            session_kwargs["region_name"] = config.region

        access_key = (
            resolve_env(None, config.aws_access_key_id_env)
            if config.aws_access_key_id_env
            else None
        )
        secret_key = (
            resolve_env(None, config.aws_secret_access_key_env)
            if config.aws_secret_access_key_env
            else None
        )
        session_token = (
            resolve_env(None, config.aws_session_token_env)
            if config.aws_session_token_env
            else None
        )
        if access_key:
            session_kwargs["aws_access_key_id"] = access_key
        if secret_key:
            session_kwargs["aws_secret_access_key"] = secret_key
        if session_token:
            session_kwargs["aws_session_token"] = session_token

        session = boto3.session.Session(**session_kwargs)
        client_kwargs: dict[str, Any] = {}
        if config.endpoint_url:
            client_kwargs["endpoint_url"] = config.endpoint_url
        return session.client("s3", **client_kwargs)
