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

from typing import Any

from drt.config.credentials import resolve_env
from drt.config.models import DestinationConfig, S3DestinationConfig, SyncOptions
from drt.destinations._blob_serializer import build_object_key, serialise_records
from drt.destinations.base import SyncResult


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
            result.errors.append(f"S3 destination serialisation failed: {e}")
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
