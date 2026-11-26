"""Tests for watermark storage backends."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from drt.state.watermark import LocalWatermarkStorage


class _NotFound(Exception):
    """Stand-in for google.api_core.exceptions.NotFound."""


class _PreconditionFailed(Exception):
    """Stand-in for google.api_core.exceptions.PreconditionFailed (HTTP 412)."""


def _patch_gcs_errors() -> Any:
    """Supply the exception pair the conditional write path looks up.

    google-api-core arrives only with the `[gcs]` extra, which the test
    environment deliberately does not install.
    """
    return patch(
        "drt.state.watermark._gcs_precondition_errors",
        return_value=(_NotFound, _PreconditionFailed),
    )


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

    def test_save_uses_atomic_replace(self, tmp_path: Path) -> None:
        from drt.state import _atomic

        with patch("drt.state._atomic.os.replace", wraps=_atomic.os.replace) as replace:
            LocalWatermarkStorage(tmp_path).save("sync", "value")
        assert replace.call_count == 1


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
        # An absent object now surfaces as NotFound from reload(), since the
        # write path needs the generation and not just existence.
        blob.reload.side_effect = _NotFound("absent")

        storage = GCSWatermarkStorage(
            bucket="my-bucket",
            key="watermarks/sync.json",
        )
        with _patch_gcs_errors():
            storage.save("my_sync", "2026-04-15T10:00:00")

        call_args = blob.upload_from_string.call_args
        uploaded = json.loads(call_args[0][0])
        assert uploaded["my_sync"] == "2026-04-15T10:00:00"
        assert call_args.kwargs["if_generation_match"] == 0

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


class _FakeGCSObject:
    """A single GCS object with generation semantics.

    Generation ``0`` means "does not exist", matching the precondition value
    GCS uses for it. Every successful write bumps the generation, which is what
    makes a stale ``if_generation_match`` detectable.
    """

    def __init__(self, content: str | None = None) -> None:
        self.generation = 0 if content is None else 1
        self.content = content
        # Fires immediately before a write is applied, to land a competing
        # writer at exactly the point a real race would occur.
        self.before_upload: Any = None
        # Fires between reload() and the pinned download, the other point a
        # competing writer can land: the read is two round trips, not one.
        self.before_download: Any = None

    def land_competing_write(self, content: str) -> None:
        self.content = content
        self.generation += 1


class _FakeBlob:
    def __init__(self, store: _FakeGCSObject) -> None:
        self._store = store
        self.generation: int | None = None

    def exists(self) -> bool:
        return self._store.generation != 0

    def reload(self) -> None:
        if self._store.generation == 0:
            raise _NotFound("no such object")
        self.generation = self._store.generation

    def download_as_text(self, if_generation_match: int | None = None) -> str:
        if self._store.before_download is not None:
            self._store.before_download()
        if self._store.generation == 0:
            raise _NotFound("no such object")
        if if_generation_match is not None and if_generation_match != self._store.generation:
            raise _PreconditionFailed("generation mismatch on read")
        assert self._store.content is not None
        return self._store.content

    def upload_from_string(
        self,
        data: str,
        content_type: str | None = None,
        if_generation_match: int | None = None,
    ) -> None:
        if self._store.before_upload is not None:
            self._store.before_upload()
        if if_generation_match is not None and if_generation_match != self._store.generation:
            raise _PreconditionFailed("generation mismatch on write")
        self._store.generation += 1
        self._store.content = data


class TestGCSWatermarkConcurrency:
    """Concurrent writers must not discard each other's watermarks (#919)."""

    def _storage(self, store: _FakeGCSObject) -> Any:
        from drt.state.watermark import GCSWatermarkStorage

        storage = GCSWatermarkStorage(bucket="my-bucket", key="watermarks.json")
        storage._blob = lambda: _FakeBlob(store)  # type: ignore[method-assign]
        return storage

    def _patch_errors(self) -> Any:
        return _patch_gcs_errors()

    def test_save_pins_the_write_to_the_generation_it_read(self) -> None:
        store = _FakeGCSObject(json.dumps({"sync_a": "old"}))
        with self._patch_errors():
            self._storage(store).save("sync_a", "new")

        assert json.loads(store.content or "") == {"sync_a": "new"}
        assert store.generation == 2

    def test_first_write_requires_the_object_to_still_be_absent(self) -> None:
        store = _FakeGCSObject()
        with self._patch_errors():
            self._storage(store).save("sync_a", "first")

        assert json.loads(store.content or "") == {"sync_a": "first"}

    def test_concurrent_save_does_not_discard_the_other_writer(self) -> None:
        """The regression: two syncs finishing together, both keys must survive.

        Without a precondition the second upload wins outright and the first
        sync silently reverts to a stale cursor on its next run.
        """
        store = _FakeGCSObject(json.dumps({"sync_a": "old_a"}))

        def land_sync_b() -> None:
            store.before_upload = None  # once only
            store.land_competing_write(json.dumps({"sync_a": "old_a", "sync_b": "b_value"}))

        store.before_upload = land_sync_b

        with self._patch_errors():
            self._storage(store).save("sync_a", "new_a")

        assert json.loads(store.content or "") == {"sync_a": "new_a", "sync_b": "b_value"}

    def test_concurrent_delete_does_not_discard_the_other_writer(self) -> None:
        store = _FakeGCSObject(json.dumps({"sync_a": "a_value", "sync_b": "old_b"}))

        def land_sync_b() -> None:
            store.before_upload = None
            store.land_competing_write(json.dumps({"sync_a": "a_value", "sync_b": "new_b"}))

        store.before_upload = land_sync_b

        with self._patch_errors():
            self._storage(store).delete("sync_a")

        assert json.loads(store.content or "") == {"sync_b": "new_b"}

    def test_writer_landing_mid_read_is_retried_not_raised(self) -> None:
        """The read is two round trips, and the gap between them is racy.

        ``reload()`` reads the generation; the download is pinned to it. A
        competing writer landing in between fails that pin with a 412: the
        object still exists, so it is not the 404 the read guards against.
        Unhandled, it would escape ``save()`` as a raw ``PreconditionFailed``,
        which is the outcome ``WatermarkContentionError`` exists to prevent.
        """
        store = _FakeGCSObject(json.dumps({"sync_a": "old_a"}))

        def land_sync_b() -> None:
            store.before_download = None  # once only
            store.land_competing_write(json.dumps({"sync_a": "old_a", "sync_b": "b_value"}))

        store.before_download = land_sync_b

        with self._patch_errors():
            self._storage(store).save("sync_a", "new_a")

        assert json.loads(store.content or "") == {"sync_a": "new_a", "sync_b": "b_value"}

    def test_sustained_mid_read_contention_fails_loudly(self) -> None:
        """Losing every read must fail the same way as losing every write."""
        import pytest

        from drt.state.watermark import _MAX_WRITE_ATTEMPTS, WatermarkContentionError

        store = _FakeGCSObject(json.dumps({"sync_a": "old"}))
        reads = 0

        def always_lose() -> None:
            nonlocal reads
            reads += 1
            store.land_competing_write(json.dumps({"sync_a": f"other_{reads}"}))

        store.before_download = always_lose

        with self._patch_errors(), pytest.raises(WatermarkContentionError, match="not saved"):
            self._storage(store).save("sync_a", "mine")

        assert reads == _MAX_WRITE_ATTEMPTS

    def test_object_deleted_mid_read_starts_from_absent(self) -> None:
        """A delete in the read gap is a 404, not a 412, and is not contention.

        ``reload()`` succeeds and then the object goes away before the pinned
        download runs. There is no competing generation to lose to here, so the
        right answer is to treat the object as absent and write it fresh rather
        than to burn an attempt retrying against something that is gone.
        """
        store = _FakeGCSObject(json.dumps({"sync_a": "old_a", "sync_b": "old_b"}))

        def delete_the_object() -> None:
            store.before_download = None  # once only
            store.generation = 0
            store.content = None

        store.before_download = delete_the_object

        with self._patch_errors():
            self._storage(store).save("sync_a", "new_a")

        assert json.loads(store.content or "") == {"sync_a": "new_a"}
        # Generation 1, not 2: the write went out under if_generation_match=0
        # ("only if still absent"), so this is a fresh object's first version.
        assert store.generation == 1

    def test_unparseable_object_is_replaced_at_its_own_generation(self) -> None:
        """Unreadable contents are given up on, but the generation is not.

        Returning ``0`` instead of the generation just read would pin the write
        to "only if still absent" against an object that plainly exists, so
        every attempt would 412 and a self-healing case would raise
        ``WatermarkContentionError`` instead of overwriting the bad object.
        """
        store = _FakeGCSObject("{not json")

        with self._patch_errors():
            self._storage(store).save("sync_a", "new_a")

        assert json.loads(store.content or "") == {"sync_a": "new_a"}
        assert store.generation == 2  # pinned to the generation it read, and passed

    def test_delete_of_unknown_sync_writes_nothing(self) -> None:
        store = _FakeGCSObject(json.dumps({"sync_a": "a_value"}))
        with self._patch_errors():
            self._storage(store).delete("never_stored")

        assert store.generation == 1  # untouched, no upload round trip

    def test_sustained_contention_fails_loudly(self) -> None:
        """Giving up quietly would reintroduce exactly the bug being fixed."""
        import pytest

        from drt.state.watermark import _MAX_WRITE_ATTEMPTS, WatermarkContentionError

        store = _FakeGCSObject(json.dumps({"sync_a": "old"}))
        attempts = 0

        def always_lose() -> None:
            nonlocal attempts
            attempts += 1
            store.land_competing_write(json.dumps({"sync_a": f"other_{attempts}"}))

        store.before_upload = always_lose

        with self._patch_errors(), pytest.raises(WatermarkContentionError, match="not saved"):
            self._storage(store).save("sync_a", "mine")

        assert attempts == _MAX_WRITE_ATTEMPTS


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
        blob.generation = 7
        blob.download_as_text.return_value = json.dumps({"a": "1", "b": "2"})

        with _patch_gcs_errors():
            GCSWatermarkStorage(bucket="bkt", key="w.json").delete("a")

        written = json.loads(blob.upload_from_string.call_args.args[0])
        assert written == {"b": "2"}
        assert blob.upload_from_string.call_args.kwargs["if_generation_match"] == 7

    @patch("drt.state.watermark._gcs_client")
    def test_gcs_delete_unknown_sync_is_a_noop(self, mock_client: MagicMock) -> None:
        from drt.state.watermark import GCSWatermarkStorage

        blob = mock_client.return_value.bucket.return_value.blob.return_value
        blob.generation = 7
        blob.download_as_text.return_value = json.dumps({"a": "1"})

        with _patch_gcs_errors():
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
