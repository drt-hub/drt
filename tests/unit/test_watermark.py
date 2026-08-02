"""Tests for watermark storage backends."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from drt.state.watermark import LocalWatermarkStorage


class TestLocalWatermarkStorage:
    def test_get_returns_none_when_no_state(self, tmp_path: Path) -> None:
        storage = LocalWatermarkStorage(tmp_path)
        assert storage.get("my_sync") is None

    def test_save_and_get_round_trip(self, tmp_path: Path) -> None:
        storage = LocalWatermarkStorage(tmp_path)
        storage.save("my_sync", "2026-04-15T10:00:00")
        assert storage.get("my_sync") == "2026-04-15T10:00:00"

    def test_save_overwrites_previous(self, tmp_path: Path) -> None:
        storage = LocalWatermarkStorage(tmp_path)
        storage.save("my_sync", "old")
        storage.save("my_sync", "new")
        assert storage.get("my_sync") == "new"

    def test_independent_sync_names(self, tmp_path: Path) -> None:
        storage = LocalWatermarkStorage(tmp_path)
        storage.save("sync_a", "value_a")
        storage.save("sync_b", "value_b")
        assert storage.get("sync_a") == "value_a"
        assert storage.get("sync_b") == "value_b"


class TestGCSWatermarkStorage:
    @patch("drt.state.watermark._gcs_client")
    def test_get_returns_none_when_blob_missing(
        self,
        mock_client: MagicMock,
    ) -> None:
        from drt.state.watermark import GCSWatermarkStorage

        bucket = mock_client.return_value.bucket.return_value
        blob = bucket.blob.return_value
        blob.exists.return_value = False

        storage = GCSWatermarkStorage(
            bucket="my-bucket",
            key="watermarks/sync.json",
        )
        assert storage.get("my_sync") is None

    @patch("drt.state.watermark._gcs_client")
    def test_save_uploads_json(self, mock_client: MagicMock) -> None:
        from drt.state.watermark import GCSWatermarkStorage

        bucket = mock_client.return_value.bucket.return_value
        blob = bucket.blob.return_value
        blob.exists.return_value = False

        storage = GCSWatermarkStorage(
            bucket="my-bucket",
            key="watermarks/sync.json",
        )
        storage.save("my_sync", "2026-04-15T10:00:00")

        call_args = blob.upload_from_string.call_args
        uploaded = json.loads(call_args[0][0])
        assert uploaded["my_sync"] == "2026-04-15T10:00:00"

    @patch("drt.state.watermark._gcs_client")
    def test_get_reads_existing_blob(self, mock_client: MagicMock) -> None:
        from drt.state.watermark import GCSWatermarkStorage

        bucket = mock_client.return_value.bucket.return_value
        blob = bucket.blob.return_value
        blob.exists.return_value = True
        blob.download_as_text.return_value = '{"my_sync": "2026-04-15"}'

        storage = GCSWatermarkStorage(
            bucket="my-bucket",
            key="watermarks/sync.json",
        )
        assert storage.get("my_sync") == "2026-04-15"


class TestBigQueryWatermarkStorage:
    def _make_storage(self) -> Any:
        from drt.state.watermark import BigQueryWatermarkStorage

        storage = BigQueryWatermarkStorage(
            project="my-project",
            dataset="my_dataset",
        )
        # Bypass _query_config which needs google.cloud.bigquery
        storage._query_config = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        return storage

    @patch("drt.state.watermark._bq_client")
    def test_get_returns_none_when_no_row(
        self,
        mock_client: MagicMock,
    ) -> None:
        mock_client.return_value.query.return_value.result.return_value = iter([])
        storage = self._make_storage()
        assert storage.get("my_sync") is None

    @patch("drt.state.watermark._bq_client")
    def test_get_returns_value_when_row_exists(
        self,
        mock_client: MagicMock,
    ) -> None:
        row = MagicMock()
        row.watermark_value = "2026-04-15T10:00:00"
        mock_client.return_value.query.return_value.result.return_value = iter([row])
        storage = self._make_storage()
        assert storage.get("my_sync") == "2026-04-15T10:00:00"

    @patch("drt.state.watermark._bq_client")
    def test_save_executes_merge(self, mock_client: MagicMock) -> None:
        storage = self._make_storage()
        storage.save("my_sync", "2026-04-15T10:00:00")

        call_args = mock_client.return_value.query.call_args_list
        merge_sql = call_args[-1][0][0]
        assert "MERGE" in merge_sql


class TestDelete:
    """#776: resetting a watermark needs a delete the Protocol never had.

    `WatermarkStorage` exposed only get/save, so `drt state reset` had no way
    to clear a stored watermark on any backend — the reason the issue calls
    out that hand-editing JSON "does nothing for remote backends".

    Deleting an unknown sync is a no-op rather than an error: reset is a
    recovery path, and a user recovering from a poisoned cursor should not
    have to know whether a watermark was ever written.
    """

    def test_local_delete_removes_only_that_sync(self, tmp_path: Path) -> None:
        storage = LocalWatermarkStorage(tmp_path)
        storage.save("a", "2026-01-01")
        storage.save("b", "2026-02-02")

        storage.delete("a")

        assert storage.get("a") is None
        assert storage.get("b") == "2026-02-02", "an unrelated sync was cleared"

    def test_local_delete_unknown_sync_is_a_noop(self, tmp_path: Path) -> None:
        storage = LocalWatermarkStorage(tmp_path)
        storage.save("a", "2026-01-01")

        storage.delete("never-synced")  # must not raise

        assert storage.get("a") == "2026-01-01"

    def test_local_delete_with_no_file_is_a_noop(self, tmp_path: Path) -> None:
        """Reset on a project that has never run must not create or crash."""
        LocalWatermarkStorage(tmp_path).delete("a")

    @patch("drt.state.watermark._gcs_client")
    def test_gcs_delete_rewrites_without_that_key(self, mock_client: MagicMock) -> None:
        from drt.state.watermark import GCSWatermarkStorage

        blob = mock_client.return_value.bucket.return_value.blob.return_value
        blob.exists.return_value = True
        blob.download_as_text.return_value = json.dumps({"a": "1", "b": "2"})

        GCSWatermarkStorage(bucket="bkt", key="w.json").delete("a")

        written = json.loads(blob.upload_from_string.call_args.args[0])
        assert written == {"b": "2"}

    @patch("drt.state.watermark._gcs_client")
    def test_gcs_delete_unknown_sync_is_a_noop(self, mock_client: MagicMock) -> None:
        from drt.state.watermark import GCSWatermarkStorage

        blob = mock_client.return_value.bucket.return_value.blob.return_value
        blob.exists.return_value = True
        blob.download_as_text.return_value = json.dumps({"a": "1"})

        GCSWatermarkStorage(bucket="bkt", key="w.json").delete("nope")

        # No upload at all: nothing was stored, so there is nothing to rewrite.
        # Skipping the round trip also means `state reset` on a sync that never
        # ran cannot fail on a network error.
        blob.upload_from_string.assert_not_called()

    @patch("drt.state.watermark._bq_client")
    def test_bigquery_delete_is_parameterised(self, mock_client: MagicMock) -> None:
        """The sync name must be a query parameter, not interpolated —
        matching how `save` builds its MERGE."""
        from drt.state.watermark import BigQueryWatermarkStorage

        storage = BigQueryWatermarkStorage(project="p", dataset="d")
        storage._query_config = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]

        storage.delete("my_sync")

        sql = mock_client.return_value.query.call_args.args[0]
        assert "DELETE" in sql.upper()
        assert "my_sync" not in sql, "the sync name was interpolated into SQL"
        assert "@sync_name" in sql
