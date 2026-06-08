"""Unit tests for the shared object-storage serializer.

The S3 destination's behaviour-locking suite already covers the end-to-end
path through ``serialise_records`` and ``build_object_key`` from the
caller side. These tests lock the module's own contract so the GCS and
Azure Blob destinations (#169, #170) can rely on it directly without
re-asserting wire-format details through their own boto-style mocks.
"""

from __future__ import annotations

import gzip
import json
import re
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.destinations._blob_serializer import (
    FORMAT_EXTENSIONS,
    build_object_key,
    serialise_records,
)

# ---------------------------------------------------------------------------
# serialise_records — text formats
# ---------------------------------------------------------------------------


class TestSerialiseRecordsText:
    def test_csv_emits_header_row_and_data_rows(self) -> None:
        records = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
        body, content_type, content_encoding = serialise_records(
            records, format="csv", compression="none"
        )

        text = body.decode("utf-8")
        # csv.DictWriter uses \r\n line terminators by default.
        assert text.splitlines() == ["id,name", "1,alice", "2,bob"]
        assert content_type == "text/csv"
        assert content_encoding is None

    def test_json_emits_array_of_objects(self) -> None:
        records = [{"id": 1}, {"id": 2}]
        body, content_type, content_encoding = serialise_records(
            records, format="json", compression="none"
        )

        assert json.loads(body.decode("utf-8")) == records
        assert content_type == "application/json"
        assert content_encoding is None

    def test_jsonl_emits_one_object_per_line(self) -> None:
        records = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
        body, content_type, content_encoding = serialise_records(
            records, format="jsonl", compression="none"
        )

        lines = body.decode("utf-8").split("\n")
        assert [json.loads(line) for line in lines] == records
        assert content_type == "application/x-ndjson"
        assert content_encoding is None

    def test_default_str_falls_back_for_non_json_native_types(self) -> None:
        """``default=str`` keeps datetime / Decimal / UUID flowing through.

        This mirrors the ``tojson_safe`` policy applied elsewhere in
        the codebase — silently coerce to the str repr rather than
        crashing the sync.
        """
        from datetime import date
        from decimal import Decimal

        records: list[dict[str, Any]] = [{"d": date(2026, 6, 8), "amount": Decimal("1.50")}]
        body, _, _ = serialise_records(records, format="json", compression="none")

        parsed = json.loads(body.decode("utf-8"))
        assert parsed == [{"d": "2026-06-08", "amount": "1.50"}]


# ---------------------------------------------------------------------------
# serialise_records — gzip wrapping
# ---------------------------------------------------------------------------


class TestSerialiseRecordsGzip:
    @pytest.mark.parametrize("fmt", ["csv", "json", "jsonl"])
    def test_text_formats_gzip_wrap_body_and_set_encoding(self, fmt: str) -> None:
        records = [{"id": 1, "name": "alice"}]
        body, _, content_encoding = serialise_records(records, format=fmt, compression="gzip")

        assert content_encoding == "gzip"
        # Body decompresses back to the uncompressed form.
        decompressed = gzip.decompress(body)
        plain_body, _, _ = serialise_records(records, format=fmt, compression="none")
        assert decompressed == plain_body

    def test_parquet_ignores_gzip_flag(self) -> None:
        """Parquet has its own column-level compression; the outer gzip
        flag is intentionally ignored to avoid double-compressing the
        binary body (and to keep ``ContentEncoding: gzip`` out of the
        put_object call for a non-gzipped payload)."""
        mock_df = MagicMock()

        def fake_to_parquet(buf: Any, **kwargs: Any) -> None:
            # Echo back the compression kwarg so we can assert on it.
            buf.write(b"PAR1" + repr(kwargs.get("compression")).encode() + b"PAR1")

        mock_df.to_parquet.side_effect = fake_to_parquet
        mock_pandas = MagicMock()
        mock_pandas.DataFrame.return_value = mock_df

        with patch.dict("sys.modules", {"pandas": mock_pandas, "pyarrow": MagicMock()}):
            body, content_type, content_encoding = serialise_records(
                [{"id": 1}],
                format="parquet",
                compression="gzip",
                parquet_compression="snappy",
            )

        assert content_type == "application/octet-stream"
        assert content_encoding is None
        # parquet_compression flows through; outer gzip is ignored.
        assert b"'snappy'" in body


# ---------------------------------------------------------------------------
# serialise_records — parquet missing-extras path
# ---------------------------------------------------------------------------


def test_parquet_without_extras_raises_with_install_hint() -> None:
    """No [parquet] extras → ImportError with the ``drt-core[parquet]`` hint.

    The S3 destination wraps this into a row-failure (see test_s3
    suite); GCS / Azure should mirror that behaviour. The hint string
    is contract — the destination wrappers depend on it.
    """
    with patch.dict("sys.modules", {"pandas": None, "pyarrow": None}):
        with pytest.raises(ImportError, match=r"drt-core\[parquet\]"):
            serialise_records([{"id": 1}], format="parquet", compression="none")


# ---------------------------------------------------------------------------
# build_object_key
# ---------------------------------------------------------------------------


class TestBuildObjectKey:
    @pytest.mark.parametrize(
        "fmt,expected_ext",
        [("csv", "csv"), ("json", "json"), ("jsonl", "jsonl"), ("parquet", "parquet")],
    )
    def test_default_key_uses_iso8601_basic_timestamp(self, fmt: str, expected_ext: str) -> None:
        key = build_object_key(
            prefix="drt/users/",
            key_template=None,
            format=fmt,
            compression="none",
        )

        # Shape: drt/users/<8-digit date>T<6-digit time>Z.<ext>
        assert re.fullmatch(rf"drt/users/\d{{8}}T\d{{6}}Z\.{expected_ext}", key)

    def test_gzip_appends_gz_suffix_for_text_formats(self) -> None:
        key = build_object_key(
            prefix="x/",
            key_template=None,
            format="jsonl",
            compression="gzip",
        )
        assert key.endswith(".jsonl.gz")

    def test_gzip_skipped_for_parquet(self) -> None:
        """Parquet's outer gzip flag is ignored — key extension stays plain
        ``.parquet`` to match the body content (no ContentEncoding header)."""
        key = build_object_key(
            prefix="x/",
            key_template=None,
            format="parquet",
            compression="gzip",
        )
        assert key.endswith(".parquet")
        assert ".gz" not in key

    def test_key_template_substitutes_timestamp_and_appends_extension(self) -> None:
        key = build_object_key(
            prefix="exports/",
            key_template="users-{timestamp}",
            format="csv",
            compression="none",
        )
        assert re.fullmatch(r"exports/users-\d{8}T\d{6}Z\.csv", key)

    def test_key_template_with_explicit_extension_is_preserved(self) -> None:
        """If the template already ends with a dot-something, the format-
        derived extension is NOT appended (avoids ``foo.txt.csv``)."""
        key = build_object_key(
            prefix="exports/",
            key_template="snapshot-{timestamp}.tsv",
            format="csv",
            compression="none",
        )
        assert re.fullmatch(r"exports/snapshot-\d{8}T\d{6}Z\.tsv", key)

    def test_no_prefix_yields_bare_filename(self) -> None:
        key = build_object_key(
            prefix=None,
            key_template=None,
            format="json",
            compression="none",
        )
        assert re.fullmatch(r"\d{8}T\d{6}Z\.json", key)


def test_format_extensions_covers_all_documented_formats() -> None:
    assert set(FORMAT_EXTENSIONS) == {"csv", "json", "jsonl", "parquet"}
