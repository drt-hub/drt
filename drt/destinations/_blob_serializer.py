"""Shared record-to-blob serialisation for object-storage destinations.

S3 (#168), GCS (#169), and Azure Blob (#170) all write the same wire
formats — CSV, JSON, JSONL, Parquet, optionally gzip-wrapped — and
follow the same `<prefix><timestamp>.<ext>` key-naming convention. The
only thing that varies across the three is the upload client.

This module centralises:

- ``serialise_records`` — records → ``(body_bytes, content_type, content_encoding)``
- ``build_object_key`` — ``(prefix, key_template, format, compression)`` → key string
- ``FORMAT_EXTENSIONS`` — format name → file extension

Keeping these pure and config-agnostic means each destination's module
stays a thin client-construction + upload shim. Behaviour is locked in
by the existing ``tests/unit/test_s3_destination.py`` suite plus the
sibling GCS/Azure suites that land alongside their destinations.
"""

from __future__ import annotations

import csv
import gzip
import io
import json
from datetime import datetime, timezone
from typing import Any

FORMAT_EXTENSIONS: dict[str, str] = {
    "csv": "csv",
    "json": "json",
    "jsonl": "jsonl",
    "parquet": "parquet",
}


def serialise_records(
    records: list[dict[str, Any]],
    *,
    format: str,
    compression: str,
    parquet_compression: str | None = None,
) -> tuple[bytes, str, str | None]:
    """Serialise ``records`` into the on-the-wire bytes for one upload.

    Returns ``(body_bytes, content_type, content_encoding)``. Parquet is
    binary and ignores ``compression`` — Parquet has its own column-level
    compression configured via ``parquet_compression``.
    """
    if format == "parquet":
        body = _serialise_parquet(records, parquet_compression)
        return body, "application/octet-stream", None

    if format == "csv":
        text = _serialise_csv(records)
        content_type = "text/csv"
    elif format == "json":
        text = json.dumps(records, default=str)
        content_type = "application/json"
    else:  # jsonl
        text = "\n".join(json.dumps(r, default=str) for r in records)
        content_type = "application/x-ndjson"

    raw = text.encode("utf-8")
    if compression == "gzip":
        return gzip.compress(raw), content_type, "gzip"
    return raw, content_type, None


def _serialise_csv(records: list[dict[str, Any]]) -> str:
    buf = io.StringIO()
    columns = list(records[0].keys())
    writer = csv.DictWriter(buf, fieldnames=columns)
    writer.writeheader()
    writer.writerows(records)
    return buf.getvalue()


def _serialise_parquet(
    records: list[dict[str, Any]],
    parquet_compression: str | None,
) -> bytes:
    try:
        import pandas as pd  # type: ignore[import-untyped]
        import pyarrow  # type: ignore[import-untyped]  # noqa: F401
    except ImportError as e:
        raise ImportError("Parquet format requires: pip install drt-core[parquet]") from e

    compression = (
        parquet_compression if parquet_compression and parquet_compression != "none" else None
    )
    df = pd.DataFrame(records)
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression=compression, index=False)
    return buf.getvalue()


def build_object_key(
    *,
    prefix: str | None,
    key_template: str | None,
    format: str,
    compression: str,
) -> str:
    """Compose the object key for a single sync's upload.

    Default shape: ``<prefix><UTC ISO8601 basic>.<ext>`` — timestamped
    so re-runs land at a fresh key instead of overwriting (the Census /
    Hightouch convention; downstream "new files" polling works
    trivially).

    ``key_template`` overrides the filename part; the only supported
    placeholder is ``{timestamp}``. If the template already supplies an
    extension, leave it alone.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    extension = FORMAT_EXTENSIONS[format]
    if compression == "gzip" and format != "parquet":
        extension = f"{extension}.gz"

    if key_template:
        file_part = key_template.format(timestamp=timestamp)
        if "." not in file_part.rsplit("/", 1)[-1]:
            file_part = f"{file_part}.{extension}"
        return f"{prefix}{file_part}" if prefix else file_part

    return f"{prefix or ''}{timestamp}.{extension}"
