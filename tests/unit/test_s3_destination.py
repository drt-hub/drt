"""Unit tests for the S3 destination.

boto3 is mocked via ``sys.modules`` injection (same pattern as the
Snowflake destination tests). No real AWS account or boto3 install
required — tests verify the put_object call shape, key naming,
serialisation per format, gzip handling, credential threading, and
error paths.

The empty-batch test deliberately does **not** inject a boto3 mock:
this is the same implicit "no driver was imported" contract that the
SQL empty-batch tests use (#595). If the destination ever loses its
empty-source short-circuit, this test crashes with
``ModuleNotFoundError`` on CI's minimal install — the test passing
proves the short-circuit holds.
"""

from __future__ import annotations

import gzip
import json
import re
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.models import S3DestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.s3 import S3Destination


def _options() -> SyncOptions:
    return SyncOptions(mode="full", batch_size=100, on_error="skip")


def _config(**overrides: Any) -> S3DestinationConfig:
    defaults: dict[str, Any] = {
        "type": "s3",
        "bucket": "my-bucket",
        "prefix": "drt/users/",
        "format": "csv",
    }
    defaults.update(overrides)
    return S3DestinationConfig(**defaults)


def _mock_boto3_modules(client: MagicMock) -> dict[str, MagicMock]:
    """Build sys.modules entries that satisfy ``import boto3``."""
    session_mod = MagicMock()
    session_instance = MagicMock()
    session_instance.client.return_value = client
    session_mod.Session.return_value = session_instance

    boto3_mod = MagicMock()
    boto3_mod.session = session_mod
    return {"boto3": boto3_mod, "boto3.session": session_mod}


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class TestS3DestinationConfig:
    def test_minimal_valid(self) -> None:
        config = _config(prefix="")
        assert config.bucket == "my-bucket"
        assert config.format == "csv"
        assert config.compression == "none"
        assert config.region is None

    def test_describe(self) -> None:
        assert _config().describe() == "s3 (s3://my-bucket/drt/users/)"

    def test_all_formats_accepted(self) -> None:
        for fmt in ["csv", "json", "jsonl", "parquet"]:
            assert _config(format=fmt).format == fmt

    def test_invalid_format_rejected(self) -> None:
        with pytest.raises(Exception):
            _config(format="avro")

    def test_invalid_compression_rejected(self) -> None:
        with pytest.raises(Exception):
            _config(compression="bzip2")


# ---------------------------------------------------------------------------
# Empty batch — load([]) must short-circuit before any boto3 import
# ---------------------------------------------------------------------------


def test_empty_batch_returns_empty_sync_result_without_importing_boto3() -> None:
    """``load([])`` short-circuits before touching boto3 or AWS.

    No ``sys.modules`` patch — if ``S3Destination.load`` ever loses
    its empty-source short-circuit and reaches ``_client()``, the
    ``import boto3`` inside will fail with ``ModuleNotFoundError`` on
    CI's minimal install (no ``[s3]`` extras). The test passing
    therefore proves the short-circuit holds. Same implicit
    "no driver was imported" contract pattern as the SQL empty-batch
    suite (#595).
    """
    result = S3Destination().load([], _config(), _options())
    assert isinstance(result, SyncResult)
    assert result.success == 0
    assert result.failed == 0
    assert result.skipped == 0


# ---------------------------------------------------------------------------
# CSV upload — happy path
# ---------------------------------------------------------------------------


class TestCsvUpload:
    def test_uploads_csv_with_correct_put_object_shape(self) -> None:
        client = MagicMock()
        modules = _mock_boto3_modules(client)
        records = [
            {"id": 1, "name": "alice"},
            {"id": 2, "name": "bob"},
        ]

        with patch.dict("sys.modules", modules):
            result = S3Destination().load(records, _config(), _options())

        assert result.success == 2
        assert result.failed == 0
        client.put_object.assert_called_once()
        kwargs = client.put_object.call_args.kwargs
        assert kwargs["Bucket"] == "my-bucket"
        assert kwargs["ContentType"] == "text/csv"
        # No ContentEncoding header without gzip
        assert "ContentEncoding" not in kwargs
        body = kwargs["Body"]
        assert isinstance(body, bytes)
        text = body.decode("utf-8")
        assert text.splitlines()[0] == "id,name"
        assert "1,alice" in text
        assert "2,bob" in text

    def test_key_default_is_prefix_plus_timestamp_csv(self) -> None:
        client = MagicMock()
        modules = _mock_boto3_modules(client)

        with patch.dict("sys.modules", modules):
            S3Destination().load([{"id": 1}], _config(), _options())

        key = client.put_object.call_args.kwargs["Key"]
        # "drt/users/<UTC YYYYMMDDTHHMMSSZ>.csv"
        assert re.match(r"^drt/users/\d{8}T\d{6}Z\.csv$", key), key

    def test_key_template_override_with_timestamp_placeholder(self) -> None:
        client = MagicMock()
        modules = _mock_boto3_modules(client)
        config = _config(key_template="active_users-{timestamp}")

        with patch.dict("sys.modules", modules):
            S3Destination().load([{"id": 1}], config, _options())

        key = client.put_object.call_args.kwargs["Key"]
        # Template appends the format-derived ".csv" since the template
        # itself did not include one.
        assert re.match(r"^drt/users/active_users-\d{8}T\d{6}Z\.csv$", key), key

    def test_key_template_with_own_extension_is_respected(self) -> None:
        client = MagicMock()
        modules = _mock_boto3_modules(client)
        config = _config(key_template="snapshot.csv")

        with patch.dict("sys.modules", modules):
            S3Destination().load([{"id": 1}], config, _options())

        assert client.put_object.call_args.kwargs["Key"] == "drt/users/snapshot.csv"


# ---------------------------------------------------------------------------
# JSON / JSONL
# ---------------------------------------------------------------------------


class TestJsonFormats:
    def test_json_uploads_json_array(self) -> None:
        client = MagicMock()
        modules = _mock_boto3_modules(client)
        records = [{"id": 1}, {"id": 2}]

        with patch.dict("sys.modules", modules):
            S3Destination().load(records, _config(format="json"), _options())

        kwargs = client.put_object.call_args.kwargs
        assert kwargs["ContentType"] == "application/json"
        body = kwargs["Body"]
        assert isinstance(body, bytes)
        decoded = json.loads(body.decode("utf-8"))
        assert decoded == records
        assert kwargs["Key"].endswith(".json")

    def test_jsonl_uploads_one_object_per_line(self) -> None:
        client = MagicMock()
        modules = _mock_boto3_modules(client)
        records = [{"id": 1}, {"id": 2}]

        with patch.dict("sys.modules", modules):
            S3Destination().load(records, _config(format="jsonl"), _options())

        kwargs = client.put_object.call_args.kwargs
        assert kwargs["ContentType"] == "application/x-ndjson"
        body = kwargs["Body"]
        assert isinstance(body, bytes)
        lines = body.decode("utf-8").splitlines()
        assert [json.loads(line) for line in lines] == records
        assert kwargs["Key"].endswith(".jsonl")


# ---------------------------------------------------------------------------
# Gzip
# ---------------------------------------------------------------------------


class TestGzipCompression:
    def test_gzip_csv_sets_content_encoding_and_extension(self) -> None:
        client = MagicMock()
        modules = _mock_boto3_modules(client)
        records = [{"id": 1, "name": "alice"}]

        with patch.dict("sys.modules", modules):
            S3Destination().load(records, _config(compression="gzip"), _options())

        kwargs = client.put_object.call_args.kwargs
        assert kwargs["ContentType"] == "text/csv"
        assert kwargs["ContentEncoding"] == "gzip"
        assert kwargs["Key"].endswith(".csv.gz")
        # Body is gzip-compressed; decompress and verify
        body = gzip.decompress(kwargs["Body"]).decode("utf-8")
        assert "1,alice" in body


# ---------------------------------------------------------------------------
# Credentials + endpoint URL
# ---------------------------------------------------------------------------


class TestCredentialsAndEndpoint:
    def test_aws_profile_passed_to_session(self) -> None:
        client = MagicMock()
        modules = _mock_boto3_modules(client)
        config = _config(aws_profile="prod-readonly")

        with patch.dict("sys.modules", modules):
            S3Destination().load([{"id": 1}], config, _options())

        modules["boto3"].session.Session.assert_called_once()
        session_kwargs = modules["boto3"].session.Session.call_args.kwargs
        assert session_kwargs["profile_name"] == "prod-readonly"

    def test_region_passed_to_session(self) -> None:
        client = MagicMock()
        modules = _mock_boto3_modules(client)
        config = _config(region="eu-west-1")

        with patch.dict("sys.modules", modules):
            S3Destination().load([{"id": 1}], config, _options())

        session_kwargs = modules["boto3"].session.Session.call_args.kwargs
        assert session_kwargs["region_name"] == "eu-west-1"

    def test_env_var_credentials_passed_to_session(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MY_AWS_KEY", "AKIA_TEST")
        monkeypatch.setenv("MY_AWS_SECRET", "secret-test")
        monkeypatch.setenv("MY_AWS_TOKEN", "session-test")

        client = MagicMock()
        modules = _mock_boto3_modules(client)
        config = _config(
            aws_access_key_id_env="MY_AWS_KEY",
            aws_secret_access_key_env="MY_AWS_SECRET",
            aws_session_token_env="MY_AWS_TOKEN",
        )

        with patch.dict("sys.modules", modules):
            S3Destination().load([{"id": 1}], config, _options())

        kwargs = modules["boto3"].session.Session.call_args.kwargs
        assert kwargs["aws_access_key_id"] == "AKIA_TEST"
        assert kwargs["aws_secret_access_key"] == "secret-test"
        assert kwargs["aws_session_token"] == "session-test"

    def test_endpoint_url_passed_to_client(self) -> None:
        client = MagicMock()
        modules = _mock_boto3_modules(client)
        config = _config(endpoint_url="http://localhost:9000")

        with patch.dict("sys.modules", modules):
            S3Destination().load([{"id": 1}], config, _options())

        session = modules["boto3"].session.Session.return_value
        client_kwargs = session.client.call_args.kwargs
        assert client_kwargs["endpoint_url"] == "http://localhost:9000"

    def test_default_no_explicit_session_kwargs(self) -> None:
        """No profile / region / env-var overrides → empty session kwargs."""
        client = MagicMock()
        modules = _mock_boto3_modules(client)

        with patch.dict("sys.modules", modules):
            S3Destination().load([{"id": 1}], _config(), _options())

        session_kwargs = modules["boto3"].session.Session.call_args.kwargs
        # boto3 falls back to its standard credential chain when called
        # with no kwargs — this is the recommended default.
        assert session_kwargs == {}


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


class TestErrorPaths:
    def test_put_object_failure_records_failed_count(self) -> None:
        client = MagicMock()
        client.put_object.side_effect = RuntimeError("AccessDenied")
        modules = _mock_boto3_modules(client)
        records = [{"id": 1}, {"id": 2}]

        with patch.dict("sys.modules", modules):
            result = S3Destination().load(records, _config(), _options())

        assert result.success == 0
        assert result.failed == 2
        assert any("AccessDenied" in e for e in result.errors)

    def test_missing_boto3_raises_helpful_import_error(self) -> None:
        """Calling load() with records but no boto3 installed raises."""
        # Sentinel: ensure boto3 is NOT importable here. We patch sys.modules
        # so that ``import boto3`` raises ModuleNotFoundError immediately.
        with patch.dict("sys.modules", {"boto3": None}):
            with pytest.raises(ImportError, match=r"pip install drt-core\[s3\]"):
                S3Destination().load([{"id": 1}], _config(), _options())

    def test_serialisation_failure_records_row_errors_without_uploading(self) -> None:
        """Serialisation error → row failures, no boto3 client created.

        Any exception raised inside ``_serialise`` (bad pandas frame, encoding
        error, etc.) must be captured into ``result.errors`` and short-circuit
        BEFORE the upload phase, so no S3 client is ever constructed for a
        non-uploadable payload.
        """
        client = MagicMock()
        modules = _mock_boto3_modules(client)

        with patch.object(
            S3Destination,
            "_serialise",
            side_effect=RuntimeError("boom: bad row"),
        ):
            with patch.dict("sys.modules", modules):
                result = S3Destination().load(
                    [{"id": 1}, {"id": 2}], _config(), _options()
                )

        assert result.success == 0
        assert result.failed == 2
        assert any("boom: bad row" in e for e in result.errors)
        # Upload phase never ran — no put_object call.
        client.put_object.assert_not_called()


# ---------------------------------------------------------------------------
# Parquet ImportError path (no [parquet] extras installed)
# ---------------------------------------------------------------------------


def test_missing_pyarrow_for_parquet_raises_helpful_import_error() -> None:
    """``format: parquet`` without the [parquet] extras raises with the
    install hint.

    Surfaces in CI's minimal install (no [parquet] in the install line).
    The error message has to mention ``drt-core[parquet]`` so users know
    which extra to add.
    """
    client = MagicMock()
    modules = _mock_boto3_modules(client)
    # Block pandas + pyarrow imports inside _serialise_parquet.
    modules["pandas"] = None  # type: ignore[assignment]
    modules["pyarrow"] = None  # type: ignore[assignment]

    with patch.dict("sys.modules", modules):
        result = S3Destination().load(
            [{"id": 1}], _config(format="parquet"), _options()
        )

    # Serialisation failure path: ImportError from inside _serialise_parquet
    # is caught by the outer try/except in load(), recorded as row failures
    # with the helpful install hint preserved in result.errors. This matches
    # the established Postgres / MySQL / ClickHouse missing-driver behaviour
    # rather than crashing the whole sync — the user still gets a single
    # clear "install drt-core[parquet]" message in the failure record.
    assert result.success == 0
    assert result.failed == 1
    assert any("drt-core[parquet]" in e for e in result.errors)
    # No boto3 client was created — _serialise failed before _client() ran.
    client.put_object.assert_not_called()


# ---------------------------------------------------------------------------
# Parquet (only runs when [parquet] extras installed)
# ---------------------------------------------------------------------------


class TestParquetFormat:
    def test_parquet_uploads_binary_body_with_octet_stream(self) -> None:
        # pandas + pyarrow are installed locally for parquet tests; on CI
        # they're part of the [parquet] extra. This test verifies the actual
        # parquet binary (PAR1 magic bytes) — the orchestration-only sibling
        # below runs without [parquet] so CI covers the same code path.
        pytest.importorskip("pandas")
        pytest.importorskip("pyarrow")

        client = MagicMock()
        modules = _mock_boto3_modules(client)
        records = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]

        with patch.dict("sys.modules", modules):
            S3Destination().load(records, _config(format="parquet"), _options())

        kwargs = client.put_object.call_args.kwargs
        assert kwargs["ContentType"] == "application/octet-stream"
        assert "ContentEncoding" not in kwargs
        assert kwargs["Key"].endswith(".parquet")
        # Parquet body magic bytes: "PAR1" at start and end
        body = kwargs["Body"]
        assert body[:4] == b"PAR1"
        assert body[-4:] == b"PAR1"

    def test_parquet_orchestration_with_mocked_pandas_runs_on_ci(self) -> None:
        """Verify _serialise_parquet's orchestration without requiring [parquet].

        Sibling to the end-to-end test above: that one validates the produced
        binary (PAR1 magic bytes) and only runs when pandas/pyarrow are
        installed. This one mocks both libraries via ``sys.modules`` so it
        runs on CI's minimal install (no [parquet] extras), covering the
        ``compression = ... if ... else None`` branch + the
        ``pandas.DataFrame(records)`` / ``df.to_parquet(buf, engine=...,
        compression=..., index=False)`` orchestration that's otherwise
        skipped on CI. The split keeps the CI install line policy intact
        (.github/workflows/ci.yml#L41-L43 — "parquet remain opt-in") while
        closing the CI coverage hole for the parquet path.
        """
        client = MagicMock()
        modules = _mock_boto3_modules(client)

        # Mock pandas: DataFrame(records) → mock_df; mock_df.to_parquet writes
        # PAR1 magic bytes to the BytesIO buffer so the downstream assertions
        # on the put_object body remain meaningful even with mocks.
        mock_df = MagicMock()

        def fake_to_parquet(buf: Any, **_kwargs: Any) -> None:
            buf.write(b"PAR1" + b"\x00" * 16 + b"PAR1")

        mock_df.to_parquet.side_effect = fake_to_parquet
        mock_pandas = MagicMock()
        mock_pandas.DataFrame.return_value = mock_df
        modules["pandas"] = mock_pandas
        modules["pyarrow"] = MagicMock()

        records = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]

        with patch.dict("sys.modules", modules):
            result = S3Destination().load(
                records,
                _config(format="parquet", parquet_compression="snappy"),
                _options(),
            )

        assert result.success == 2
        # pandas.DataFrame called with the records
        mock_pandas.DataFrame.assert_called_once_with(records)
        # to_parquet called with the expected kwargs
        to_parquet_kwargs = mock_df.to_parquet.call_args.kwargs
        assert to_parquet_kwargs["engine"] == "pyarrow"
        assert to_parquet_kwargs["compression"] == "snappy"
        assert to_parquet_kwargs["index"] is False
        # Body uploaded as parquet
        put_kwargs = client.put_object.call_args.kwargs
        assert put_kwargs["ContentType"] == "application/octet-stream"
        assert "ContentEncoding" not in put_kwargs
        assert put_kwargs["Key"].endswith(".parquet")

    def test_parquet_compression_none_maps_to_none(self) -> None:
        """``parquet_compression: none`` translates to ``compression=None``
        for ``df.to_parquet`` rather than passing the literal string ``"none"``.
        """
        client = MagicMock()
        modules = _mock_boto3_modules(client)
        mock_df = MagicMock()

        def fake_to_parquet(buf: Any, **_kwargs: Any) -> None:
            buf.write(b"PAR1PAR1")

        mock_df.to_parquet.side_effect = fake_to_parquet
        mock_pandas = MagicMock()
        mock_pandas.DataFrame.return_value = mock_df
        modules["pandas"] = mock_pandas
        modules["pyarrow"] = MagicMock()

        with patch.dict("sys.modules", modules):
            S3Destination().load(
                [{"id": 1}],
                _config(format="parquet", parquet_compression="none"),
                _options(),
            )

        assert mock_df.to_parquet.call_args.kwargs["compression"] is None
