"""Amazon S3 adapter for the object-store state primitive (#756)."""

from __future__ import annotations

from typing import Any

from drt.config.credentials import resolve_env
from drt.state._objectstore import ObjectPreconditionError, Token


def _s3_client(
    *,
    region: str | None = None,
    endpoint_url: str | None = None,
    aws_profile: str | None = None,
    aws_access_key_id_env: str | None = None,
    aws_secret_access_key_env: str | None = None,
    aws_session_token_env: str | None = None,
) -> Any:
    """Construct an S3 client lazily so core installs need no AWS SDK."""
    try:
        import boto3  # type: ignore[import-untyped]
    except ImportError as exc:
        raise ImportError(
            "S3 state storage requires: pip install drt-core[s3]"
        ) from exc

    # Keep credential resolution byte-for-byte equivalent to S3Destination:
    # state storage and destination uploads must not expose two subtly
    # different AWS authentication vocabularies or precedence rules.
    session_kwargs: dict[str, Any] = {}
    if aws_profile:
        session_kwargs["profile_name"] = aws_profile
    if region:
        session_kwargs["region_name"] = region

    access_key = (
        resolve_env(None, aws_access_key_id_env) if aws_access_key_id_env else None
    )
    secret_key = (
        resolve_env(None, aws_secret_access_key_env)
        if aws_secret_access_key_env
        else None
    )
    session_token = (
        resolve_env(None, aws_session_token_env) if aws_session_token_env else None
    )
    if access_key:
        session_kwargs["aws_access_key_id"] = access_key
    if secret_key:
        session_kwargs["aws_secret_access_key"] = secret_key
    if session_token:
        session_kwargs["aws_session_token"] = session_token

    session = boto3.session.Session(**session_kwargs)
    client_kwargs: dict[str, Any] = {}
    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url
    return session.client("s3", **client_kwargs)


def _client_error_type() -> Any:
    """Load botocore's modeled error lazily with the same extras guidance."""
    try:
        from botocore.exceptions import ClientError  # type: ignore[import-untyped]
    except ImportError as exc:
        raise ImportError(
            "S3 state storage requires: pip install drt-core[s3]"
        ) from exc
    return ClientError


def _error_details(exc: Any) -> tuple[str, int | None]:
    response = exc.response
    error = response.get("Error", {})
    metadata = response.get("ResponseMetadata", {})
    status = metadata.get("HTTPStatusCode")
    return str(error.get("Code", "")), status if isinstance(status, int) else None


class S3ObjectClient:
    """Conditional object operations implemented with S3 ETags.

    S3 has no non-versioned equivalent of GCS's generation-addressed object
    reference. A read therefore observes the current ETag with ``HeadObject``
    and pins the following ``GetObject`` with ``IfMatch``. If another writer
    wins between those calls, S3 returns 412 (or 404 when the object was
    concurrently removed), so the complete snapshot read must be retried.

    Writes never fall back to an unconditional PUT. An S3-compatible endpoint
    that rejects conditional headers (for example with 501 NotImplemented)
    is allowed to fail loudly instead of silently degrading correctness.
    """

    _READ_SNAPSHOT_ATTEMPTS = 8

    def __init__(
        self,
        bucket: str,
        *,
        region: str | None = None,
        endpoint_url: str | None = None,
        aws_profile: str | None = None,
        aws_access_key_id_env: str | None = None,
        aws_secret_access_key_env: str | None = None,
        aws_session_token_env: str | None = None,
        client: Any | None = None,
    ) -> None:
        self._bucket_name = bucket
        self._client_options = {
            "region": region,
            "endpoint_url": endpoint_url,
            "aws_profile": aws_profile,
            "aws_access_key_id_env": aws_access_key_id_env,
            "aws_secret_access_key_env": aws_secret_access_key_env,
            "aws_session_token_env": aws_session_token_env,
        }
        self._injected_client = client

    def _client(self) -> Any:
        if self._injected_client is None:
            self._injected_client = _s3_client(**self._client_options)
        return self._injected_client

    def read_for_update(self, key: str) -> tuple[bytes | None, Token]:
        ClientError = _client_error_type()
        client = self._client()
        for _ in range(self._READ_SNAPSHOT_ATTEMPTS):
            try:
                metadata = client.head_object(Bucket=self._bucket_name, Key=key)
            except ClientError as exc:
                code, status = _error_details(exc)
                if status == 404 or code in {"404", "NoSuchKey", "NotFound"}:
                    # None is S3's create-only token; write_if translates it
                    # to the SDK's modeled IfNoneMatch="*" parameter.
                    return None, None
                raise

            etag = metadata.get("ETag")
            if not isinstance(etag, str):  # pragma: no cover - S3 SDK invariant
                raise RuntimeError(f"S3 object '{key}' had no ETag after HeadObject")

            try:
                response = client.get_object(
                    Bucket=self._bucket_name,
                    Key=key,
                    IfMatch=etag,
                )
            except ClientError as exc:
                code, status = _error_details(exc)
                if status in {404, 412} or code in {
                    "404",
                    "NoSuchKey",
                    "NotFound",
                    "PreconditionFailed",
                }:
                    # Unlike GCS's generation-addressed 404, S3 explicitly
                    # reports a stale IfMatch read as 412. A concurrent delete
                    # can produce 404 after the successful metadata lookup.
                    continue
                raise

            stream = response["Body"]
            try:
                body = stream.read()
            finally:
                close = getattr(stream, "close", None)
                if close is not None:
                    close()
            return body, etag

        raise ObjectPreconditionError(
            f"S3 object '{key}' changed during every ETag-pinned snapshot read"
        )

    def write_if(self, key: str, body: bytes, token: Token) -> Token:
        ClientError = _client_error_type()
        request: dict[str, Any] = {
            "Bucket": self._bucket_name,
            "Key": key,
            "Body": body,
            "ContentType": "application/octet-stream",
        }
        if token is None:
            request["IfNoneMatch"] = "*"
        else:
            request["IfMatch"] = str(token)

        try:
            response = self._client().put_object(**request)
        except ClientError as exc:
            code, status = _error_details(exc)
            if status in {409, 412} or code in {
                "ConditionalRequestConflict",
                "PreconditionFailed",
            }:
                raise ObjectPreconditionError(f"stale S3 ETag for '{key}'") from exc
            # In particular, do not swallow 501/NotImplemented from an
            # S3-compatible endpoint that cannot honor conditional writes.
            raise

        etag = response.get("ETag")
        if not isinstance(etag, str):  # pragma: no cover - S3 SDK invariant
            raise RuntimeError(f"S3 object '{key}' had no ETag after PutObject")
        return etag

    def list_keys(self, prefix: str) -> list[str]:
        paginator = self._client().get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self._bucket_name, Prefix=prefix)
        return [
            str(item["Key"])
            for page in pages
            for item in page.get("Contents", [])
        ]
