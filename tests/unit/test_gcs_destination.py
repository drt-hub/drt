"""Unit tests for the GCS destination.

``google-cloud-storage`` is mocked via ``sys.modules`` injection (same
pattern as the S3 destination tests). No real GCP account or
google-cloud-storage install required — tests verify the
``blob.upload_from_string`` call shape, blob naming, serialisation
per format, gzip handling, credential threading, and error paths.

The empty-batch test deliberately does **not** inject a google.cloud
mock: this is the same implicit "no driver was imported" contract that
the SQL empty-batch tests use (#595). If the destination ever loses
its empty-source short-circuit, this test crashes with
``ModuleNotFoundError`` on CI's minimal install — the test passing
proves the short-circuit holds.

Most of the wire-format (csv/json/jsonl/parquet, gzip, key naming) is
locked by ``tests/unit/test_blob_serializer.py`` — these tests focus
on the GCS-specific call shape (Content-Encoding via
``blob.content_encoding``, project_id + credentials_path threading,
upload error paths).
"""

from __future__ import annotations

import gzip
import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.models import GCSDestinationConfig, SyncOptions
from drt.destinations.gcs import GCSDestination


def _options() -> SyncOptions:
    return SyncOptions(mode="full", batch_size=100, on_error="skip")


def _config(**overrides: Any) -> GCSDestinationConfig:
    defaults: dict[str, Any] = {
        "type": "gcs",
        "bucket": "my-bucket",
        "prefix": "drt/users/",
        "format": "csv",
    }
    defaults.update(overrides)
    return GCSDestinationConfig(**defaults)


def _mock_gcs_modules(blob: MagicMock, client_factory: MagicMock | None = None) -> dict[str, Any]:
    """Build sys.modules entries that satisfy ``from google.cloud import storage``."""
    bucket = MagicMock()
    bucket.blob.return_value = blob

    client = MagicMock()
    client.bucket.return_value = bucket

    storage_mod = MagicMock()
    storage_mod.Client.return_value = client
    if client_factory is not None:
        storage_mod.Client = client_factory
    else:
        storage_mod.Client.from_service_account_json = MagicMock(return_value=client)

    google_mod = MagicMock()
    google_mod.cloud = MagicMock()
    google_mod.cloud.storage = storage_mod
    google_cloud_mod = MagicMock()
    google_cloud_mod.storage = storage_mod

    return {
        "google": google_mod,
        "google.cloud": google_cloud_mod,
        "google.cloud.storage": storage_mod,
        # Expose handles for assertions.
        "_client": client,
        "_bucket": bucket,
        "_blob": blob,
        "_storage_mod": storage_mod,
    }


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class TestGCSDestinationConfig:
    def test_minimal_valid(self) -> None:
        config = GCSDestinationConfig(type="gcs", bucket="b")
        assert config.bucket == "b"
        assert config.prefix == ""
        assert config.format == "csv"
        assert config.compression == "none"
        assert config.parquet_compression == "snappy"
        assert config.project_id is None
        assert config.credentials_path is None
        assert config.key_template is None

    def test_describe_uses_gs_scheme(self) -> None:
        config = GCSDestinationConfig(type="gcs", bucket="b", prefix="users/")
        assert config.describe() == "gcs (gs://b/users/)"


# ---------------------------------------------------------------------------
# Empty-batch short-circuit (no google.cloud import)
# ---------------------------------------------------------------------------


def test_empty_batch_returns_empty_sync_result_without_importing_google_cloud() -> None:
    """Implicit contract: empty source → no driver import, no GCS call.

    Mirrors the SQL empty-batch tests (#595). If this regresses, CI's
    minimal install (no [gcs] extras) crashes with
    ``ModuleNotFoundError`` before the assertion runs.
    """
    result = GCSDestination().load([], _config(), _options())
    assert result.success == 0
    assert result.failed == 0
    assert result.skipped == 0


# ---------------------------------------------------------------------------
# CSV upload
# ---------------------------------------------------------------------------


class TestCsvUpload:
    def test_csv_uploads_with_correct_blob_name_and_content_type(self) -> None:
        blob = MagicMock()
        modules = _mock_gcs_modules(blob)

        with patch.dict(
            "sys.modules", {k: v for k, v in modules.items() if k.startswith("google")}
        ):
            result = GCSDestination().load(
                [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
                _config(),
                _options(),
            )

        assert result.success == 2
        assert result.failed == 0

        # bucket("my-bucket") was called
        modules["_client"].bucket.assert_called_once_with("my-bucket")
        # blob name has the expected prefix + timestamp.csv shape
        bucket = modules["_bucket"]
        blob_name = bucket.blob.call_args[0][0]
        assert blob_name.startswith("drt/users/")
        assert blob_name.endswith(".csv")
        # upload_from_string carries text/csv
        call = blob.upload_from_string.call_args
        body = call[0][0]
        text = body.decode("utf-8")
        assert text.splitlines() == ["id,name", "1,alice", "2,bob"]
        assert call[1]["content_type"] == "text/csv"
        # No content-encoding for plain CSV
        assert blob.content_encoding != "gzip"


# ---------------------------------------------------------------------------
# JSON / JSONL formats
# ---------------------------------------------------------------------------


class TestJsonFormats:
    def test_json_format_uploads_array(self) -> None:
        blob = MagicMock()
        modules = _mock_gcs_modules(blob)

        with patch.dict(
            "sys.modules", {k: v for k, v in modules.items() if k.startswith("google")}
        ):
            GCSDestination().load([{"id": 1}, {"id": 2}], _config(format="json"), _options())

        call = blob.upload_from_string.call_args
        assert json.loads(call[0][0].decode("utf-8")) == [{"id": 1}, {"id": 2}]
        assert call[1]["content_type"] == "application/json"
        assert modules["_bucket"].blob.call_args[0][0].endswith(".json")

    def test_jsonl_format_uploads_one_per_line(self) -> None:
        blob = MagicMock()
        modules = _mock_gcs_modules(blob)

        with patch.dict(
            "sys.modules", {k: v for k, v in modules.items() if k.startswith("google")}
        ):
            GCSDestination().load(
                [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
                _config(format="jsonl"),
                _options(),
            )

        call = blob.upload_from_string.call_args
        lines = call[0][0].decode("utf-8").split("\n")
        assert [json.loads(line) for line in lines] == [
            {"id": 1, "name": "alice"},
            {"id": 2, "name": "bob"},
        ]
        assert call[1]["content_type"] == "application/x-ndjson"
        assert modules["_bucket"].blob.call_args[0][0].endswith(".jsonl")


# ---------------------------------------------------------------------------
# Gzip compression
# ---------------------------------------------------------------------------


class TestGzipCompression:
    def test_gzip_sets_content_encoding_and_gz_suffix(self) -> None:
        blob = MagicMock()
        modules = _mock_gcs_modules(blob)

        with patch.dict(
            "sys.modules", {k: v for k, v in modules.items() if k.startswith("google")}
        ):
            GCSDestination().load(
                [{"id": 1, "name": "alice"}],
                _config(format="jsonl", compression="gzip"),
                _options(),
            )

        # Blob content_encoding set BEFORE upload (set on the object,
        # not in upload_from_string kwargs — that's how
        # google-cloud-storage exposes it).
        assert blob.content_encoding == "gzip"
        # Key extension reflects gzip wrap
        assert modules["_bucket"].blob.call_args[0][0].endswith(".jsonl.gz")
        # Body is actually gzip-compressed and decompresses to JSONL
        body = blob.upload_from_string.call_args[0][0]
        decompressed = gzip.decompress(body).decode("utf-8")
        assert json.loads(decompressed) == {"id": 1, "name": "alice"}


# ---------------------------------------------------------------------------
# Credentials & client construction
# ---------------------------------------------------------------------------


class TestCredentialsAndProject:
    def test_default_uses_adc_with_no_project(self) -> None:
        """No credentials_path, no project_id → storage.Client() (ADC chain)."""
        blob = MagicMock()
        modules = _mock_gcs_modules(blob)

        with patch.dict(
            "sys.modules", {k: v for k, v in modules.items() if k.startswith("google")}
        ):
            GCSDestination().load([{"id": 1}], _config(), _options())

        modules["_storage_mod"].Client.assert_called_once_with()
        modules["_storage_mod"].Client.from_service_account_json.assert_not_called()

    def test_project_id_threaded_into_client(self) -> None:
        blob = MagicMock()
        modules = _mock_gcs_modules(blob)

        with patch.dict(
            "sys.modules", {k: v for k, v in modules.items() if k.startswith("google")}
        ):
            GCSDestination().load([{"id": 1}], _config(project_id="my-gcp-project"), _options())

        modules["_storage_mod"].Client.assert_called_once_with(project="my-gcp-project")

    def test_credentials_path_uses_from_service_account_json(self) -> None:
        blob = MagicMock()
        modules = _mock_gcs_modules(blob)

        with patch.dict(
            "sys.modules", {k: v for k, v in modules.items() if k.startswith("google")}
        ):
            GCSDestination().load(
                [{"id": 1}],
                _config(credentials_path="/path/to/sa.json"),
                _options(),
            )

        modules["_storage_mod"].Client.from_service_account_json.assert_called_once_with(
            "/path/to/sa.json"
        )
        # Default storage.Client() is NOT called when credentials_path given
        modules["_storage_mod"].Client.assert_not_called()


# ---------------------------------------------------------------------------
# Key naming
# ---------------------------------------------------------------------------


class TestKeyNaming:
    def test_key_template_substitutes_timestamp(self) -> None:
        blob = MagicMock()
        modules = _mock_gcs_modules(blob)

        with patch.dict(
            "sys.modules", {k: v for k, v in modules.items() if k.startswith("google")}
        ):
            GCSDestination().load(
                [{"id": 1}],
                _config(prefix="exports/", key_template="users-{timestamp}"),
                _options(),
            )

        key = modules["_bucket"].blob.call_args[0][0]
        assert key.startswith("exports/users-")
        assert key.endswith(".csv")

    def test_no_prefix_yields_bare_filename(self) -> None:
        blob = MagicMock()
        modules = _mock_gcs_modules(blob)

        with patch.dict(
            "sys.modules", {k: v for k, v in modules.items() if k.startswith("google")}
        ):
            GCSDestination().load([{"id": 1}], _config(prefix="", format="json"), _options())

        key = modules["_bucket"].blob.call_args[0][0]
        assert key.endswith(".json")
        assert "/" not in key


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


class TestErrorPaths:
    def test_upload_failure_recorded_as_row_failures(self) -> None:
        blob = MagicMock()
        blob.upload_from_string.side_effect = RuntimeError("403 Forbidden")
        modules = _mock_gcs_modules(blob)

        with patch.dict(
            "sys.modules", {k: v for k, v in modules.items() if k.startswith("google")}
        ):
            result = GCSDestination().load([{"id": 1}, {"id": 2}], _config(), _options())

        assert result.success == 0
        assert result.failed == 2
        assert any("403 Forbidden" in e for e in result.errors)
        assert any("GCS destination upload failed" in e for e in result.errors)

    def test_missing_google_cloud_raises_helpful_import_error(self) -> None:
        """No [gcs] extras → ImportError with the install hint.

        Unlike upload errors, missing-extras is a deployment mistake
        and should bubble up so the engine surfaces it once at the top.
        """
        # Block ``from google.cloud import storage`` entirely.
        with patch.dict("sys.modules", {"google.cloud.storage": None}):
            with pytest.raises(ImportError, match=r"pip install drt-core\[gcs\]"):
                GCSDestination().load([{"id": 1}], _config(), _options())

    def test_serialisation_failure_records_row_errors_without_uploading(self) -> None:
        blob = MagicMock()
        modules = _mock_gcs_modules(blob)

        with patch(
            "drt.destinations.gcs.serialise_records",
            side_effect=RuntimeError("boom: bad row"),
        ):
            with patch.dict(
                "sys.modules", {k: v for k, v in modules.items() if k.startswith("google")}
            ):
                result = GCSDestination().load([{"id": 1}, {"id": 2}], _config(), _options())

        assert result.success == 0
        assert result.failed == 2
        assert any("boom: bad row" in e for e in result.errors)
        # Upload phase never ran — no blob.upload_from_string call.
        blob.upload_from_string.assert_not_called()


# ---------------------------------------------------------------------------
# Parquet ImportError path (no [parquet] extras installed)
# ---------------------------------------------------------------------------


def test_missing_pyarrow_for_parquet_records_row_failure_with_install_hint() -> None:
    """``format: parquet`` without the [parquet] extras → row failure
    with the ``drt-core[parquet]`` install hint preserved.

    Matches the S3 destination's behaviour (the shared serialiser
    raises ImportError; both destinations catch it and record as a row
    failure rather than crashing the whole sync). The hint string is
    contract.
    """
    blob = MagicMock()
    modules = _mock_gcs_modules(blob)
    # Block pandas + pyarrow imports inside the shared serialiser.
    full_modules: dict[str, Any] = {
        **{k: v for k, v in modules.items() if k.startswith("google")},
        "pandas": None,
        "pyarrow": None,
    }

    with patch.dict("sys.modules", full_modules):
        result = GCSDestination().load([{"id": 1}], _config(format="parquet"), _options())

    assert result.success == 0
    assert result.failed == 1
    assert any("drt-core[parquet]" in e for e in result.errors)
    blob.upload_from_string.assert_not_called()


# ---------------------------------------------------------------------------
# Parquet (only runs when [parquet] extras installed)
# ---------------------------------------------------------------------------


class TestParquetFormat:
    def test_parquet_orchestration_with_mocked_pandas(self) -> None:
        """Verify parquet upload shape without depending on a real pandas/pyarrow.

        The real end-to-end parquet binary (PAR1 magic bytes, snappy
        compression) is already covered by the S3 destination's
        ``test_parquet_uploads_binary_body_with_octet_stream``
        — re-running it here would trigger the pyarrow C-extension
        double-registration error (``A type extension with name
        pandas.period already defined``) because two test classes
        would each register the same global pyarrow type extension.

        This test mocks pandas + pyarrow so the GCS orchestration
        (Content-Type, no Content-Encoding, .parquet extension,
        upload_from_string call shape) is verified without going
        through a second real Parquet round-trip.
        """
        blob = MagicMock()
        modules = _mock_gcs_modules(blob)

        mock_df = MagicMock()

        def fake_to_parquet(buf: Any, **_kwargs: Any) -> None:
            buf.write(b"PAR1" + b"\x00" * 16 + b"PAR1")

        mock_df.to_parquet.side_effect = fake_to_parquet
        mock_pandas = MagicMock()
        mock_pandas.DataFrame.return_value = mock_df

        google_modules = {k: v for k, v in modules.items() if k.startswith("google")}
        google_modules["pandas"] = mock_pandas
        google_modules["pyarrow"] = MagicMock()

        with patch.dict("sys.modules", google_modules):
            GCSDestination().load(
                [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
                _config(format="parquet"),
                _options(),
            )

        call = blob.upload_from_string.call_args
        assert call[1]["content_type"] == "application/octet-stream"
        # No content-encoding header for binary parquet
        assert blob.content_encoding != "gzip"
        # Key ends in .parquet
        assert modules["_bucket"].blob.call_args[0][0].endswith(".parquet")
        # Parquet body magic bytes
        body = call[0][0]
        assert body[:4] == b"PAR1"
        assert body[-4:] == b"PAR1"
