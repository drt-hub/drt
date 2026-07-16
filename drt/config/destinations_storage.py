"""Object-store / file destination configs (#721 split from models.py).

S3 / GCS / Azure Blob object stores plus local Parquet / File writers. All are
members of the :data:`~drt.config.sync_options.DestinationConfig` union.
"""

from __future__ import annotations

from pathlib import PurePath
from typing import Literal

from drt.config.base import DescribableConfig


class ParquetDestinationConfig(DescribableConfig):
    type: Literal["parquet"]
    path: str  # output file or directory path, e.g. "output/data.parquet"
    partition_by: list[str] | None = None  # optional partition columns
    compression: Literal["snappy", "gzip", "zstd", "none"] = "snappy"

    def _describe_detail(self) -> str:
        return f"{self.path}"

    def describe_safe(self) -> str:
        # File name only — the directory part of a local path can carry a home
        # dir / username into a hosted docs site (#696 review, @Pawansingh3889).
        return f"{self.type} ({PurePath(self.path).name})"


class FileDestinationConfig(DescribableConfig):
    type: Literal["file"]
    path: str  # output file path, e.g. "output/data.csv"
    format: Literal["csv", "json", "jsonl"] = "csv"

    def _describe_detail(self) -> str:
        return f"{self.path}"

    def describe_safe(self) -> str:
        # File name only — see ParquetDestinationConfig.describe_safe.
        return f"{self.type} ({PurePath(self.path).name})"


class S3DestinationConfig(DescribableConfig):
    """S3 destination — upload records as CSV / JSON / JSONL / Parquet to S3."""

    type: Literal["s3"]
    bucket: str
    # Optional key prefix. The generated file name is appended to this prefix:
    # e.g. prefix="drt/users/" → "drt/users/20260605T123000Z.csv". For
    # per-sync routing, give each sync its own prefix.
    prefix: str = ""
    format: Literal["csv", "json", "jsonl", "parquet"] = "csv"
    # gzip-compress csv / json / jsonl uploads ("none" disables). Parquet
    # uses its native compression below; "gzip" here is ignored for parquet.
    compression: Literal["none", "gzip"] = "none"
    # Optional Parquet-specific compression (matches ParquetDestinationConfig).
    parquet_compression: Literal["snappy", "gzip", "zstd", "none"] = "snappy"
    region: str | None = None  # AWS region; defers to boto3 default if unset
    # AWS auth: by default, falls back to boto3's standard credential chain
    # (env vars, ~/.aws/credentials, instance profile, IAM role). Provide one
    # of the following for explicit overrides:
    aws_profile: str | None = None  # named profile in ~/.aws/credentials
    aws_access_key_id_env: str | None = None
    aws_secret_access_key_env: str | None = None
    aws_session_token_env: str | None = None
    # Optional endpoint URL — set when targeting an S3-compatible service
    # (MinIO, LocalStack, R2, etc.). None → real AWS S3.
    endpoint_url: str | None = None
    # Optional file-name template (Jinja2-free, supports one placeholder:
    # {timestamp} — UTC ISO 8601 basic format, e.g. "20260605T123000Z").
    # Default produces "<prefix><timestamp>.<ext>". For per-sync naming,
    # set ``prefix`` per sync (e.g. ``prefix: drt/active_users/``).
    key_template: str | None = None

    def _describe_detail(self) -> str:
        return f"s3://{self.bucket}/{self.prefix}"

    def describe_safe(self) -> str:
        # Prefix is the per-sync routing identity and stays; the bucket name is
        # dropped — public bucket names invite probing (#696 review,
        # @Pawansingh3889). Empty prefix renders type-only.
        return f"{self.type} ({self.prefix})" if self.prefix else str(self.type)


class GCSDestinationConfig(DescribableConfig):
    """GCS destination — upload records as CSV / JSON / JSONL / Parquet to Google Cloud Storage."""

    type: Literal["gcs"]
    bucket: str
    # Optional object-name prefix. The generated file name is appended:
    # e.g. prefix="drt/users/" → "drt/users/20260605T123000Z.csv". For
    # per-sync routing, give each sync its own prefix.
    prefix: str = ""
    format: Literal["csv", "json", "jsonl", "parquet"] = "csv"
    # gzip-compress csv / json / jsonl uploads ("none" disables). Parquet
    # uses its native compression below; "gzip" here is ignored for parquet.
    compression: Literal["none", "gzip"] = "none"
    # Optional Parquet-specific compression (matches ParquetDestinationConfig).
    parquet_compression: Literal["snappy", "gzip", "zstd", "none"] = "snappy"
    # GCP project to bill / scope the client to. Optional — the credentials
    # file usually carries one, and ADC will use the project from
    # ``gcloud config get-value project`` if neither is set.
    project_id: str | None = None
    # GCS auth: by default, falls back to Application Default Credentials
    # (GOOGLE_APPLICATION_CREDENTIALS env → gcloud
    # application-default → GCE/GKE/Cloud Run service account). For
    # explicit overrides, point at a service-account JSON keyfile:
    credentials_path: str | None = None
    # Optional file-name template (Jinja2-free, supports one placeholder:
    # {timestamp} — UTC ISO 8601 basic format, e.g. "20260605T123000Z").
    # Default produces "<prefix><timestamp>.<ext>". For per-sync naming,
    # set ``prefix`` per sync (e.g. ``prefix: drt/active_users/``).
    key_template: str | None = None

    def _describe_detail(self) -> str:
        return f"gs://{self.bucket}/{self.prefix}"

    def describe_safe(self) -> str:
        # Prefix is the per-sync routing identity and stays; the bucket name is
        # dropped — public bucket names invite probing (#696 review,
        # @Pawansingh3889). Empty prefix renders type-only.
        return f"{self.type} ({self.prefix})" if self.prefix else str(self.type)


class AzureBlobDestinationConfig(DescribableConfig):
    """Azure Blob destination — upload records as CSV / JSON / JSONL / Parquet."""

    type: Literal["azure_blob"]
    container: str
    # Optional blob-name prefix. The generated file name is appended:
    # e.g. prefix="drt/users/" → "drt/users/20260605T123000Z.csv". For
    # per-sync routing, give each sync its own prefix.
    prefix: str = ""
    format: Literal["csv", "json", "jsonl", "parquet"] = "csv"
    # gzip-compress csv / json / jsonl uploads ("none" disables). Parquet
    # uses its native compression below; "gzip" here is ignored for parquet.
    compression: Literal["none", "gzip"] = "none"
    # Optional Parquet-specific compression (matches ParquetDestinationConfig).
    parquet_compression: Literal["snappy", "gzip", "zstd", "none"] = "snappy"
    # Auth path 1: env-var name holding a storage-account connection
    # string (DefaultEndpointsProtocol=...). Most common shape for
    # non-Azure CI / cron deployments.
    connection_string_env: str | None = None
    # Auth path 2: storage account blob endpoint
    # (https://<account>.blob.core.windows.net) — when set without
    # connection_string_env, DefaultAzureCredential is used (env vars,
    # managed identity, Azure CLI, ...).
    account_url: str | None = None
    # Optional file-name template (Jinja2-free, supports one placeholder:
    # {timestamp} — UTC ISO 8601 basic format, e.g. "20260605T123000Z").
    # Default produces "<prefix><timestamp>.<ext>". For per-sync naming,
    # set ``prefix`` per sync (e.g. ``prefix: drt/active_users/``).
    key_template: str | None = None

    def _describe_detail(self) -> str:
        return f"{self.container}/{self.prefix}"

    def describe_safe(self) -> str:
        # Prefix is the per-sync routing identity and stays; the bucket name is
        # dropped — public bucket names invite probing (#696 review,
        # @Pawansingh3889). Empty prefix renders type-only.
        return f"{self.type} ({self.prefix})" if self.prefix else str(self.type)
