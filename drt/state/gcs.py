"""Google Cloud Storage adapter for the object-store state primitive (#756)."""

from __future__ import annotations

from typing import Any

from drt.state._objectstore import ObjectPreconditionError, Token


def _gcs_client() -> Any:
    """Construct a GCS client lazily so core installs need no Google SDK."""
    try:
        # Direct submodule import avoids typed namespace-package lookup issues;
        # see the same house pattern in drt.state.watermark (#561).
        from google.cloud.storage import Client  # type: ignore[import-untyped]
    except ImportError as exc:
        raise ImportError(
            "GCS state storage requires: pip install drt-core[gcs]"
        ) from exc
    return Client()


class GCSObjectClient:
    """Conditional object operations implemented with GCS generations.

    Reads deliberately select a version through ``blob(..., generation=g)``.
    A generation precondition on a normal download is not equivalent: if a
    writer wins between metadata reload and download, the SDK raises 412, while
    a versioned reference raises 404 when that exact revision is unavailable.
    Keeping those cases distinct prevents a raced read from masquerading as a
    genuinely absent object.
    """

    _READ_SNAPSHOT_ATTEMPTS = 8

    def __init__(self, bucket: str, *, client: Any | None = None) -> None:
        self._bucket_name = bucket
        self._injected_client = client

    def _client(self) -> Any:
        if self._injected_client is None:
            self._injected_client = _gcs_client()
        return self._injected_client

    def read_for_update(self, key: str) -> tuple[bytes | None, Token]:
        try:
            from google.cloud.exceptions import NotFound  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError(
                "GCS state storage requires: pip install drt-core[gcs]"
            ) from exc

        bucket = self._client().bucket(self._bucket_name)
        for _ in range(self._READ_SNAPSHOT_ATTEMPTS):
            current = bucket.blob(key)
            try:
                current.reload()
            except NotFound:
                # A 404 at metadata lookup means there is no live object. GCS
                # generation 0 is the create-only conditional-write token.
                return None, 0

            generation = current.generation
            if generation is None:  # pragma: no cover - SDK invariant after reload
                raise RuntimeError(f"GCS object '{key}' had no generation after reload")

            versioned = bucket.blob(key, generation=int(generation))
            try:
                return versioned.download_as_bytes(), int(generation)
            except NotFound:
                # This is a different 404: the generation observed above was
                # superseded before its pinned read. Re-read metadata rather
                # than returning an absent-object token for a present object.
                continue

        raise ObjectPreconditionError(
            f"GCS object '{key}' changed during every pinned snapshot read"
        )

    def write_if(self, key: str, body: bytes, token: Token) -> Token:
        try:
            from google.api_core.exceptions import (  # type: ignore[import-untyped]
                PreconditionFailed,
            )
        except ImportError as exc:
            raise ImportError(
                "GCS state storage requires: pip install drt-core[gcs]"
            ) from exc

        blob = self._client().bucket(self._bucket_name).blob(key)
        try:
            blob.upload_from_string(
                body,
                content_type="application/octet-stream",
                if_generation_match=int(token),
            )
        except PreconditionFailed as exc:
            raise ObjectPreconditionError(f"stale GCS generation for '{key}'") from exc
        if blob.generation is None:  # pragma: no cover - SDK invariant after upload
            blob.reload()
        return int(blob.generation)

    def list_keys(self, prefix: str) -> list[str]:
        blobs = self._client().list_blobs(self._bucket_name, prefix=prefix)
        return [str(blob.name) for blob in blobs]

