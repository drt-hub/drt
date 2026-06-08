"""Unit tests for the Azure Blob destination.

``azure-storage-blob`` + ``azure-identity`` are mocked via
``sys.modules`` injection (same pattern as the S3 / GCS destination
tests). No real Azure account or azure SDK install required — tests
verify the ``blob_client.upload_blob`` call shape, blob naming,
serialisation per format, gzip handling, credential threading, and
error paths.

The empty-batch test deliberately does **not** inject an azure mock:
this is the same implicit "no driver was imported" contract that the
SQL empty-batch tests use (#595). If the destination ever loses its
empty-source short-circuit, this test crashes with
``ModuleNotFoundError`` on CI's minimal install — the test passing
proves the short-circuit holds.

Most of the wire-format (csv/json/jsonl/parquet, gzip, key naming) is
locked by ``tests/unit/test_blob_serializer.py`` — these tests focus
on the Azure-specific call shape (Content-Type + Content-Encoding via
``ContentSettings``, connection_string_env vs DefaultAzureCredential
threading, ``overwrite=True`` policy, upload error paths).
"""

from __future__ import annotations

import gzip
import json
import os
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.models import AzureBlobDestinationConfig, SyncOptions
from drt.destinations.azure_blob import AzureBlobDestination


def _options() -> SyncOptions:
    return SyncOptions(mode="full", batch_size=100, on_error="skip")


def _config(**overrides: Any) -> AzureBlobDestinationConfig:
    defaults: dict[str, Any] = {
        "type": "azure_blob",
        "container": "my-container",
        "prefix": "drt/users/",
        "format": "csv",
        "connection_string_env": "AZ_CONN",
    }
    defaults.update(overrides)
    return AzureBlobDestinationConfig(**defaults)


def _mock_azure_modules(blob_client: MagicMock) -> dict[str, Any]:
    """Build sys.modules entries that satisfy the Azure imports.

    Covers ``from azure.storage.blob import BlobServiceClient`` and
    ``from azure.storage.blob import ContentSettings`` plus
    ``from azure.identity import DefaultAzureCredential`` (for the
    account_url + managed-identity path).
    """
    service_client = MagicMock()
    service_client.get_blob_client.return_value = blob_client

    blob_service_client_cls = MagicMock(return_value=service_client)
    blob_service_client_cls.from_connection_string = MagicMock(return_value=service_client)

    # ContentSettings echoes its kwargs back so tests can introspect
    # what content_type / content_encoding the destination set.
    def content_settings_factory(**kwargs: Any) -> MagicMock:
        cs = MagicMock()
        cs.kwargs = kwargs
        return cs

    content_settings_cls = MagicMock(side_effect=content_settings_factory)

    storage_blob_mod = MagicMock()
    storage_blob_mod.BlobServiceClient = blob_service_client_cls
    storage_blob_mod.ContentSettings = content_settings_cls

    azure_storage_mod = MagicMock()
    azure_storage_mod.blob = storage_blob_mod

    default_credential_instance = MagicMock()
    default_credential_cls = MagicMock(return_value=default_credential_instance)
    azure_identity_mod = MagicMock()
    azure_identity_mod.DefaultAzureCredential = default_credential_cls

    azure_mod = MagicMock()
    azure_mod.storage = azure_storage_mod
    azure_mod.identity = azure_identity_mod

    return {
        "azure": azure_mod,
        "azure.storage": azure_storage_mod,
        "azure.storage.blob": storage_blob_mod,
        "azure.identity": azure_identity_mod,
        # Expose handles for assertions.
        "_service_client": service_client,
        "_blob_client": blob_client,
        "_blob_service_client_cls": blob_service_client_cls,
        "_content_settings_cls": content_settings_cls,
        "_default_credential_cls": default_credential_cls,
    }


def _azure_only(modules: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in modules.items() if k.startswith("azure")}


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class TestAzureBlobDestinationConfig:
    def test_minimal_valid(self) -> None:
        config = AzureBlobDestinationConfig(
            type="azure_blob", container="c", connection_string_env="X"
        )
        assert config.container == "c"
        assert config.prefix == ""
        assert config.format == "csv"
        assert config.compression == "none"
        assert config.parquet_compression == "snappy"
        assert config.connection_string_env == "X"
        assert config.account_url is None
        assert config.key_template is None

    def test_describe(self) -> None:
        config = AzureBlobDestinationConfig(
            type="azure_blob", container="c", prefix="users/", connection_string_env="X"
        )
        assert config.describe() == "azure_blob (c/users/)"


# ---------------------------------------------------------------------------
# Empty-batch short-circuit (no azure import)
# ---------------------------------------------------------------------------


def test_empty_batch_returns_empty_sync_result_without_importing_azure() -> None:
    """Implicit contract: empty source → no driver import, no Azure call.

    Mirrors the SQL empty-batch tests (#595). If this regresses, CI's
    minimal install (no [azure] extras) crashes with
    ``ModuleNotFoundError`` before the assertion runs.
    """
    result = AzureBlobDestination().load([], _config(), _options())
    assert result.success == 0
    assert result.failed == 0
    assert result.skipped == 0


# ---------------------------------------------------------------------------
# CSV upload
# ---------------------------------------------------------------------------


class TestCsvUpload:
    def test_csv_uploads_with_correct_blob_name_and_content_settings(self) -> None:
        blob_client = MagicMock()
        modules = _mock_azure_modules(blob_client)

        with patch.dict("sys.modules", _azure_only(modules)):
            with patch.dict(os.environ, {"AZ_CONN": "DefaultEndpointsProtocol=https;..."}):
                result = AzureBlobDestination().load(
                    [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
                    _config(),
                    _options(),
                )

        assert result.success == 2
        assert result.failed == 0

        # service_client.get_blob_client(container, blob) called once
        modules["_service_client"].get_blob_client.assert_called_once()
        kwargs = modules["_service_client"].get_blob_client.call_args.kwargs
        assert kwargs["container"] == "my-container"
        assert kwargs["blob"].startswith("drt/users/")
        assert kwargs["blob"].endswith(".csv")

        # upload_blob called with body + overwrite=True + ContentSettings
        upload_call = blob_client.upload_blob.call_args
        body = upload_call[0][0]
        assert body.decode("utf-8").splitlines() == ["id,name", "1,alice", "2,bob"]
        assert upload_call[1]["overwrite"] is True
        # ContentSettings carries text/csv, no content_encoding for plain
        content_settings = upload_call[1]["content_settings"]
        assert content_settings.kwargs == {"content_type": "text/csv"}


# ---------------------------------------------------------------------------
# JSON / JSONL formats
# ---------------------------------------------------------------------------


class TestJsonFormats:
    def test_json_format_uploads_array(self) -> None:
        blob_client = MagicMock()
        modules = _mock_azure_modules(blob_client)

        with patch.dict("sys.modules", _azure_only(modules)):
            with patch.dict(os.environ, {"AZ_CONN": "x"}):
                AzureBlobDestination().load(
                    [{"id": 1}, {"id": 2}], _config(format="json"), _options()
                )

        upload_call = blob_client.upload_blob.call_args
        assert json.loads(upload_call[0][0].decode("utf-8")) == [{"id": 1}, {"id": 2}]
        cs = upload_call[1]["content_settings"]
        assert cs.kwargs["content_type"] == "application/json"
        assert modules["_service_client"].get_blob_client.call_args.kwargs["blob"].endswith(".json")

    def test_jsonl_format_uploads_one_per_line(self) -> None:
        blob_client = MagicMock()
        modules = _mock_azure_modules(blob_client)

        with patch.dict("sys.modules", _azure_only(modules)):
            with patch.dict(os.environ, {"AZ_CONN": "x"}):
                AzureBlobDestination().load(
                    [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
                    _config(format="jsonl"),
                    _options(),
                )

        upload_call = blob_client.upload_blob.call_args
        lines = upload_call[0][0].decode("utf-8").split("\n")
        assert [json.loads(line) for line in lines] == [
            {"id": 1, "name": "alice"},
            {"id": 2, "name": "bob"},
        ]
        cs = upload_call[1]["content_settings"]
        assert cs.kwargs["content_type"] == "application/x-ndjson"
        assert (
            modules["_service_client"].get_blob_client.call_args.kwargs["blob"].endswith(".jsonl")
        )


# ---------------------------------------------------------------------------
# Gzip compression
# ---------------------------------------------------------------------------


class TestGzipCompression:
    def test_gzip_sets_content_encoding_and_gz_suffix(self) -> None:
        blob_client = MagicMock()
        modules = _mock_azure_modules(blob_client)

        with patch.dict("sys.modules", _azure_only(modules)):
            with patch.dict(os.environ, {"AZ_CONN": "x"}):
                AzureBlobDestination().load(
                    [{"id": 1, "name": "alice"}],
                    _config(format="jsonl", compression="gzip"),
                    _options(),
                )

        upload_call = blob_client.upload_blob.call_args
        cs = upload_call[1]["content_settings"]
        # ContentSettings carries both content_type and content_encoding
        assert cs.kwargs["content_type"] == "application/x-ndjson"
        assert cs.kwargs["content_encoding"] == "gzip"
        # Blob key reflects gzip wrap
        blob_key = modules["_service_client"].get_blob_client.call_args.kwargs["blob"]
        assert blob_key.endswith(".jsonl.gz")
        # Body actually gzip-compressed and decompresses to JSONL
        body = upload_call[0][0]
        decompressed = gzip.decompress(body).decode("utf-8")
        assert json.loads(decompressed) == {"id": 1, "name": "alice"}


# ---------------------------------------------------------------------------
# Authentication paths
# ---------------------------------------------------------------------------


class TestAuth:
    def test_connection_string_path_uses_from_connection_string(self) -> None:
        blob_client = MagicMock()
        modules = _mock_azure_modules(blob_client)

        with patch.dict("sys.modules", _azure_only(modules)):
            with patch.dict(os.environ, {"AZ_CONN": "DefaultEndpointsProtocol=https;X"}):
                AzureBlobDestination().load([{"id": 1}], _config(), _options())

        # BlobServiceClient.from_connection_string called with the
        # resolved env var value
        modules["_blob_service_client_cls"].from_connection_string.assert_called_once_with(
            "DefaultEndpointsProtocol=https;X"
        )
        # DefaultAzureCredential NOT used on connection_string path
        modules["_default_credential_cls"].assert_not_called()

    def test_empty_connection_string_env_raises(self) -> None:
        """Connection-string env var resolving to empty string is a
        deployment misconfiguration, not a recoverable error."""
        blob_client = MagicMock()
        modules = _mock_azure_modules(blob_client)

        with patch.dict("sys.modules", _azure_only(modules)):
            # Env var unset (not in os.environ).
            with patch.dict(os.environ, {}, clear=False):
                os.environ.pop("AZ_CONN", None)
                with pytest.raises(ValueError, match="connection_string_env"):
                    AzureBlobDestination().load([{"id": 1}], _config(), _options())

    def test_account_url_path_uses_default_azure_credential(self) -> None:
        blob_client = MagicMock()
        modules = _mock_azure_modules(blob_client)

        with patch.dict("sys.modules", _azure_only(modules)):
            AzureBlobDestination().load(
                [{"id": 1}],
                _config(
                    connection_string_env=None,
                    account_url="https://acct.blob.core.windows.net",
                ),
                _options(),
            )

        # DefaultAzureCredential() instantiated and passed as credential
        modules["_default_credential_cls"].assert_called_once_with()
        modules["_blob_service_client_cls"].assert_called_once()
        ctor_kwargs = modules["_blob_service_client_cls"].call_args.kwargs
        assert ctor_kwargs["account_url"] == "https://acct.blob.core.windows.net"
        assert ctor_kwargs["credential"] is modules["_default_credential_cls"].return_value
        # from_connection_string NOT used on account_url path
        modules["_blob_service_client_cls"].from_connection_string.assert_not_called()

    def test_neither_auth_path_raises(self) -> None:
        """No connection_string_env AND no account_url is a config
        error — must raise rather than silently produce a broken
        client."""
        blob_client = MagicMock()
        modules = _mock_azure_modules(blob_client)

        with patch.dict("sys.modules", _azure_only(modules)):
            with pytest.raises(ValueError, match="connection_string_env or account_url"):
                AzureBlobDestination().load(
                    [{"id": 1}],
                    _config(connection_string_env=None, account_url=None),
                    _options(),
                )


# ---------------------------------------------------------------------------
# Key naming
# ---------------------------------------------------------------------------


class TestKeyNaming:
    def test_key_template_substitutes_timestamp(self) -> None:
        blob_client = MagicMock()
        modules = _mock_azure_modules(blob_client)

        with patch.dict("sys.modules", _azure_only(modules)):
            with patch.dict(os.environ, {"AZ_CONN": "x"}):
                AzureBlobDestination().load(
                    [{"id": 1}],
                    _config(prefix="exports/", key_template="users-{timestamp}"),
                    _options(),
                )

        blob_key = modules["_service_client"].get_blob_client.call_args.kwargs["blob"]
        assert blob_key.startswith("exports/users-")
        assert blob_key.endswith(".csv")

    def test_no_prefix_yields_bare_filename(self) -> None:
        blob_client = MagicMock()
        modules = _mock_azure_modules(blob_client)

        with patch.dict("sys.modules", _azure_only(modules)):
            with patch.dict(os.environ, {"AZ_CONN": "x"}):
                AzureBlobDestination().load(
                    [{"id": 1}], _config(prefix="", format="json"), _options()
                )

        blob_key = modules["_service_client"].get_blob_client.call_args.kwargs["blob"]
        assert blob_key.endswith(".json")
        assert "/" not in blob_key


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


class TestErrorPaths:
    def test_upload_failure_recorded_as_row_failures(self) -> None:
        blob_client = MagicMock()
        blob_client.upload_blob.side_effect = RuntimeError("403 AuthorizationPermissionMismatch")
        modules = _mock_azure_modules(blob_client)

        with patch.dict("sys.modules", _azure_only(modules)):
            with patch.dict(os.environ, {"AZ_CONN": "x"}):
                result = AzureBlobDestination().load([{"id": 1}, {"id": 2}], _config(), _options())

        assert result.success == 0
        assert result.failed == 2
        assert any("403 AuthorizationPermissionMismatch" in e for e in result.errors)
        assert any("Azure Blob destination upload failed" in e for e in result.errors)

    def test_missing_azure_blob_raises_helpful_import_error(self) -> None:
        """No [azure] extras → ImportError with the install hint.

        Unlike upload errors, missing-extras is a deployment mistake
        and should bubble up so the engine surfaces it once at the top.
        """
        with patch.dict("sys.modules", {"azure.storage.blob": None}):
            with pytest.raises(ImportError, match=r"pip install drt-core\[azure\]"):
                AzureBlobDestination().load([{"id": 1}], _config(), _options())

    def test_missing_azure_identity_on_account_url_path_raises_with_install_hint(self) -> None:
        """`account_url` + no `azure.identity` → ImportError with the
        `[azure]` install hint.

        Defensive coverage: ``azure-storage-blob`` and
        ``azure-identity`` ship together in the ``[azure]`` extra, so
        in practice you'd see either both or neither. This guards the
        edge case of a hand-rolled install where only
        ``azure-storage-blob`` was pulled in — the `connection_string_env`
        path keeps working, the `account_url` path raises with the
        same install hint as the top-level missing-extras path rather
        than emitting a cryptic ``ModuleNotFoundError: azure.identity``.
        """
        blob_client = MagicMock()
        modules = _mock_azure_modules(blob_client)
        # azure.storage.blob importable, azure.identity NOT.
        partial = {**_azure_only(modules), "azure.identity": None}

        with patch.dict("sys.modules", partial):
            with pytest.raises(ImportError, match=r"pip install drt-core\[azure\]"):
                AzureBlobDestination().load(
                    [{"id": 1}],
                    _config(
                        connection_string_env=None,
                        account_url="https://acct.blob.core.windows.net",
                    ),
                    _options(),
                )

    def test_serialisation_failure_records_row_errors_without_uploading(self) -> None:
        blob_client = MagicMock()
        modules = _mock_azure_modules(blob_client)

        with patch(
            "drt.destinations.azure_blob.serialise_records",
            side_effect=RuntimeError("boom: bad row"),
        ):
            with patch.dict("sys.modules", _azure_only(modules)):
                with patch.dict(os.environ, {"AZ_CONN": "x"}):
                    result = AzureBlobDestination().load(
                        [{"id": 1}, {"id": 2}], _config(), _options()
                    )

        assert result.success == 0
        assert result.failed == 2
        assert any("boom: bad row" in e for e in result.errors)
        # Upload phase never ran — no blob_client.upload_blob call.
        blob_client.upload_blob.assert_not_called()


# ---------------------------------------------------------------------------
# Parquet ImportError path (no [parquet] extras installed)
# ---------------------------------------------------------------------------


def test_missing_pyarrow_for_parquet_records_row_failure_with_install_hint() -> None:
    """``format: parquet`` without the [parquet] extras → row failure
    with the ``drt-core[parquet]`` install hint preserved.

    Matches the S3 / GCS destinations' behaviour (the shared serialiser
    raises ImportError; each destination catches it and records as a
    row failure rather than crashing the whole sync). The hint string
    is contract.
    """
    blob_client = MagicMock()
    modules = _mock_azure_modules(blob_client)
    full_modules: dict[str, Any] = {
        **_azure_only(modules),
        "pandas": None,
        "pyarrow": None,
    }

    with patch.dict("sys.modules", full_modules):
        with patch.dict(os.environ, {"AZ_CONN": "x"}):
            result = AzureBlobDestination().load([{"id": 1}], _config(format="parquet"), _options())

    assert result.success == 0
    assert result.failed == 1
    assert any("drt-core[parquet]" in e for e in result.errors)
    blob_client.upload_blob.assert_not_called()


# ---------------------------------------------------------------------------
# Parquet orchestration (mocked pandas — see GCS test docstring for the
# pyarrow C-extension double-registration rationale)
# ---------------------------------------------------------------------------


class TestParquetFormat:
    def test_parquet_orchestration_with_mocked_pandas(self) -> None:
        """Verify parquet upload shape without depending on real pandas/pyarrow.

        Real end-to-end PAR1 binary is already covered by the S3
        destination's parquet test; re-running it here would trigger
        the pyarrow C-extension double-registration error
        (``A type extension with name pandas.period already defined``).
        Mirrors the mocked-pandas pattern from the GCS sibling.
        """
        blob_client = MagicMock()
        modules = _mock_azure_modules(blob_client)

        mock_df = MagicMock()

        def fake_to_parquet(buf: Any, **_kwargs: Any) -> None:
            buf.write(b"PAR1" + b"\x00" * 16 + b"PAR1")

        mock_df.to_parquet.side_effect = fake_to_parquet
        mock_pandas = MagicMock()
        mock_pandas.DataFrame.return_value = mock_df

        full_modules = {
            **_azure_only(modules),
            "pandas": mock_pandas,
            "pyarrow": MagicMock(),
        }

        with patch.dict("sys.modules", full_modules):
            with patch.dict(os.environ, {"AZ_CONN": "x"}):
                AzureBlobDestination().load(
                    [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
                    _config(format="parquet"),
                    _options(),
                )

        upload_call = blob_client.upload_blob.call_args
        cs = upload_call[1]["content_settings"]
        assert cs.kwargs["content_type"] == "application/octet-stream"
        assert "content_encoding" not in cs.kwargs
        blob_key = modules["_service_client"].get_blob_client.call_args.kwargs["blob"]
        assert blob_key.endswith(".parquet")
        body = upload_call[0][0]
        assert body[:4] == b"PAR1"
        assert body[-4:] == b"PAR1"
