"""Unit tests for generation-pinned GCS object operations."""

from __future__ import annotations

import sys
from types import ModuleType
from typing import Any

import pytest

from drt.state._objectstore import ObjectPreconditionError
from drt.state.gcs import GCSObjectClient


class FakeNotFound(Exception):
    pass


class FakePreconditionFailed(Exception):
    pass


def _install_google_exceptions(monkeypatch: pytest.MonkeyPatch) -> None:
    google = ModuleType("google")
    setattr(google, "__path__", [])
    cloud = ModuleType("google.cloud")
    setattr(cloud, "__path__", [])
    cloud_exceptions = ModuleType("google.cloud.exceptions")
    setattr(cloud_exceptions, "NotFound", FakeNotFound)
    api_core = ModuleType("google.api_core")
    setattr(api_core, "__path__", [])
    api_exceptions = ModuleType("google.api_core.exceptions")
    setattr(api_exceptions, "PreconditionFailed", FakePreconditionFailed)
    for name, module in {
        "google": google,
        "google.cloud": cloud,
        "google.cloud.exceptions": cloud_exceptions,
        "google.api_core": api_core,
        "google.api_core.exceptions": api_exceptions,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)


class FakeBlob:
    def __init__(self, bucket: FakeBucket, generation: int | None) -> None:
        self.bucket = bucket
        self.requested_generation = generation
        self.generation: int | None = generation

    def reload(self) -> None:
        if self.bucket.live_generation == 0:
            raise FakeNotFound
        self.generation = self.bucket.live_generation

    def download_as_bytes(self) -> bytes:
        if self.requested_generation != self.bucket.live_generation:
            raise FakeNotFound
        return self.bucket.body

    def upload_from_string(self, body: bytes, **kwargs: Any) -> None:
        token = kwargs["if_generation_match"]
        if token != self.bucket.live_generation:
            raise FakePreconditionFailed
        self.bucket.live_generation += 1
        self.bucket.body = body
        self.generation = self.bucket.live_generation


class FakeBucket:
    def __init__(self, *, race_first_versioned_read: bool = False) -> None:
        self.live_generation = 1
        self.body = b"generation one"
        self.race_first_versioned_read = race_first_versioned_read
        self.calls: list[tuple[str, int | None]] = []

    def blob(self, key: str, generation: int | None = None) -> FakeBlob:
        self.calls.append((key, generation))
        if generation is not None and self.race_first_versioned_read:
            self.race_first_versioned_read = False
            self.live_generation += 1
            self.body = b"generation two"
        return FakeBlob(self, generation)


class FakeStorageClient:
    def __init__(self, bucket: FakeBucket) -> None:
        self.fake_bucket = bucket

    def bucket(self, name: str) -> FakeBucket:
        assert name == "bucket"
        return self.fake_bucket

    def list_blobs(self, name: str, *, prefix: str) -> list[Any]:
        assert name == "bucket"
        return [type("BlobName", (), {"name": f"{prefix}a.jsonl"})()]


def test_versioned_read_retries_when_observed_generation_disappears(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_google_exceptions(monkeypatch)
    bucket = FakeBucket(race_first_versioned_read=True)
    client = GCSObjectClient("bucket", client=FakeStorageClient(bucket))

    body, token = client.read_for_update("state.json")

    assert (body, token) == (b"generation two", 2)
    assert bucket.calls == [
        ("state.json", None),
        ("state.json", 1),
        ("state.json", None),
        ("state.json", 2),
    ]


def test_metadata_not_found_returns_create_only_generation_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_google_exceptions(monkeypatch)
    bucket = FakeBucket()
    bucket.live_generation = 0
    client = GCSObjectClient("bucket", client=FakeStorageClient(bucket))

    assert client.read_for_update("missing") == (None, 0)
    assert bucket.calls == [("missing", None)]


def test_stale_upload_is_normalized_to_object_precondition_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_google_exceptions(monkeypatch)
    client = GCSObjectClient("bucket", client=FakeStorageClient(FakeBucket()))

    with pytest.raises(ObjectPreconditionError, match="stale GCS generation"):
        client.write_if("state.json", b"new", 0)


def test_successful_upload_returns_new_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_google_exceptions(monkeypatch)
    bucket = FakeBucket()
    client = GCSObjectClient("bucket", client=FakeStorageClient(bucket))

    assert client.write_if("state.json", b"new", 1) == 2
    assert bucket.body == b"new"


def test_list_keys_preserves_full_object_names() -> None:
    storage = FakeStorageClient(FakeBucket())
    client = GCSObjectClient("bucket", client=storage)
    assert client.list_keys("prefix/history/") == ["prefix/history/a.jsonl"]


def test_client_is_constructed_lazily_via_gcs_client_when_none_injected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No test above injects a client — every one of them bypasses the
    lazy-construction path drt/state/factory.py actually uses in production
    (``GCSObjectClient(bucket)``, no ``client=``). Cover that path directly
    rather than trusting patch-coverage on the surrounding methods.
    """
    sentinel = FakeStorageClient(FakeBucket())
    calls: list[None] = []

    def _fake_gcs_client() -> Any:
        calls.append(None)
        return sentinel

    monkeypatch.setattr("drt.state.gcs._gcs_client", _fake_gcs_client)
    client = GCSObjectClient("bucket")

    assert client._client() is sentinel
    assert client._client() is sentinel  # second call reuses it, doesn't reconstruct
    assert len(calls) == 1


def test_gcs_client_missing_sdk_raises_helpful_import_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "google.cloud.storage", None)
    from drt.state.gcs import _gcs_client

    with pytest.raises(ImportError, match=r"pip install drt-core\[gcs\]"):
        _gcs_client()


def test_read_for_update_raises_after_generation_keeps_changing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every one of the eight pinned-read attempts loses its race — the loop
    must give up loudly rather than retry forever or fall back to a wrong
    "absent" token."""
    _install_google_exceptions(monkeypatch)

    class AlwaysRacingBucket(FakeBucket):
        def blob(self, key: str, generation: int | None = None) -> FakeBlob:
            self.calls.append((key, generation))
            if generation is not None:
                # Every versioned read arrives to find the generation it
                # just observed already superseded.
                self.live_generation += 1
                self.body = f"generation {self.live_generation}".encode()
            return FakeBlob(self, generation)

    bucket = AlwaysRacingBucket()
    client = GCSObjectClient("bucket", client=FakeStorageClient(bucket))

    with pytest.raises(ObjectPreconditionError, match="changed during every pinned"):
        client.read_for_update("state.json")
