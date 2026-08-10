"""Unit tests for ETag-pinned S3 object operations."""

from __future__ import annotations

import sys
from types import ModuleType
from typing import Any

import pytest

from drt.state._objectstore import ObjectPreconditionError
from drt.state.s3 import S3ObjectClient


class FakeClientError(Exception):
    def __init__(self, code: str, status: int) -> None:
        super().__init__(code)
        self.response = {
            "Error": {"Code": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        }


def _install_botocore(monkeypatch: pytest.MonkeyPatch) -> None:
    botocore = ModuleType("botocore")
    setattr(botocore, "__path__", [])
    exceptions = ModuleType("botocore.exceptions")
    setattr(exceptions, "ClientError", FakeClientError)
    monkeypatch.setitem(sys.modules, "botocore", botocore)
    monkeypatch.setitem(sys.modules, "botocore.exceptions", exceptions)


class FakeBody:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.closed = False

    def read(self) -> bytes:
        return self.body

    def close(self) -> None:
        self.closed = True


class FakePaginator:
    def __init__(self, client: FakeS3Client) -> None:
        self.client = client

    def paginate(self, **kwargs: Any) -> list[dict[str, Any]]:
        self.client.calls.append(("list", kwargs))
        prefix = kwargs["Prefix"]
        keys = sorted(key for key in self.client.objects if key.startswith(prefix))
        midpoint = len(keys) // 2
        return [
            {"Contents": [{"Key": key} for key in keys[:midpoint]]},
            {"Contents": [{"Key": key} for key in keys[midpoint:]]},
        ]


class FakeS3Client:
    def __init__(self, *, race_reads: int = 0) -> None:
        self.objects: dict[str, tuple[bytes, str]] = {
            "state.json": (b"etag one", '"1"')
        }
        self.race_reads = race_reads
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.last_body: FakeBody | None = None
        self.next_head_error: FakeClientError | None = None
        self.next_get_error: FakeClientError | None = None
        self.next_put_error: FakeClientError | None = None

    def _advance(self, key: str) -> None:
        _, old_etag = self.objects[key]
        value = int(old_etag.strip('"')) + 1
        self.objects[key] = (f"etag {value}".encode(), f'"{value}"')

    def head_object(self, **kwargs: Any) -> dict[str, str]:
        self.calls.append(("head", kwargs))
        if self.next_head_error is not None:
            error = self.next_head_error
            self.next_head_error = None
            raise error
        key = kwargs["Key"]
        if key not in self.objects:
            raise FakeClientError("404", 404)
        return {"ETag": self.objects[key][1]}

    def get_object(self, **kwargs: Any) -> dict[str, FakeBody]:
        self.calls.append(("get", kwargs))
        if self.next_get_error is not None:
            error = self.next_get_error
            self.next_get_error = None
            raise error
        key = kwargs["Key"]
        if key not in self.objects:
            raise FakeClientError("NoSuchKey", 404)
        if self.race_reads:
            self.race_reads -= 1
            self._advance(key)
        body, etag = self.objects[key]
        if kwargs["IfMatch"] != etag:
            raise FakeClientError("PreconditionFailed", 412)
        self.last_body = FakeBody(body)
        return {"Body": self.last_body}

    def put_object(self, **kwargs: Any) -> dict[str, str]:
        self.calls.append(("put", kwargs))
        if self.next_put_error is not None:
            error = self.next_put_error
            self.next_put_error = None
            raise error
        key = kwargs["Key"]
        existing = self.objects.get(key)
        if kwargs.get("IfNoneMatch") == "*" and existing is not None:
            raise FakeClientError("PreconditionFailed", 412)
        if "IfMatch" in kwargs and (
            existing is None or kwargs["IfMatch"] != existing[1]
        ):
            raise FakeClientError("PreconditionFailed", 412)
        value = int(existing[1].strip('"')) + 1 if existing else 1
        etag = f'"{value}"'
        self.objects[key] = (kwargs["Body"], etag)
        return {"ETag": etag}

    def get_paginator(self, operation: str) -> FakePaginator:
        assert operation == "list_objects_v2"
        return FakePaginator(self)


def test_etag_pinned_read_retries_after_precondition_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_botocore(monkeypatch)
    storage = FakeS3Client(race_reads=1)
    client = S3ObjectClient("bucket", client=storage)

    body, token = client.read_for_update("state.json")

    assert (body, token) == (b"etag 2", '"2"')
    assert [name for name, _ in storage.calls] == ["head", "get", "head", "get"]
    assert storage.calls[1][1]["IfMatch"] == '"1"'
    assert storage.calls[3][1]["IfMatch"] == '"2"'
    assert storage.last_body is not None and storage.last_body.closed


def test_missing_object_returns_create_only_none_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_botocore(monkeypatch)
    storage = FakeS3Client()
    client = S3ObjectClient("bucket", client=storage)

    assert client.read_for_update("missing") == (None, None)
    assert storage.calls == [("head", {"Bucket": "bucket", "Key": "missing"})]


def test_read_for_update_raises_after_every_pinned_read_races(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_botocore(monkeypatch)
    storage = FakeS3Client(race_reads=S3ObjectClient._READ_SNAPSHOT_ATTEMPTS)
    client = S3ObjectClient("bucket", client=storage)

    with pytest.raises(ObjectPreconditionError, match="changed during every ETag-pinned"):
        client.read_for_update("state.json")

    assert [name for name, _ in storage.calls].count("get") == 8


@pytest.mark.parametrize("operation", ["head", "get"])
def test_non_snapshot_read_errors_are_not_misreported_as_contention(
    monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    _install_botocore(monkeypatch)
    storage = FakeS3Client()
    setattr(storage, f"next_{operation}_error", FakeClientError("AccessDenied", 403))
    client = S3ObjectClient("bucket", client=storage)

    with pytest.raises(FakeClientError, match="AccessDenied"):
        client.read_for_update("state.json")


@pytest.mark.parametrize("operation", ["head", "get"])
def test_missing_bucket_is_not_treated_as_missing_object(
    monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    """A misspelled/deleted/wrong-region bucket must raise, not read as empty
    state -- both share HTTP 404 with a missing key, but silently returning
    (None, None) here would make incremental sync think there is no prior
    watermark and replay the whole dataset every run."""
    _install_botocore(monkeypatch)
    storage = FakeS3Client()
    setattr(storage, f"next_{operation}_error", FakeClientError("NoSuchBucket", 404))
    client = S3ObjectClient("bucket", client=storage)

    with pytest.raises(FakeClientError, match="NoSuchBucket"):
        client.read_for_update("state.json")


def test_create_only_and_update_paths_use_the_modeled_put_conditions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_botocore(monkeypatch)
    storage = FakeS3Client()
    client = S3ObjectClient("bucket", client=storage)

    assert client.write_if("new.json", b"first", None) == '"1"'
    assert storage.calls[-1][1]["IfNoneMatch"] == "*"
    assert "IfMatch" not in storage.calls[-1][1]

    assert client.write_if("new.json", b"second", '"1"') == '"2"'
    assert storage.calls[-1][1]["IfMatch"] == '"1"'
    assert "IfNoneMatch" not in storage.calls[-1][1]


@pytest.mark.parametrize(
    ("code", "status"),
    [("PreconditionFailed", 412), ("ConditionalRequestConflict", 409)],
)
def test_conditional_put_failures_are_normalized(
    monkeypatch: pytest.MonkeyPatch, code: str, status: int
) -> None:
    _install_botocore(monkeypatch)
    storage = FakeS3Client()
    storage.next_put_error = FakeClientError(code, status)
    client = S3ObjectClient("bucket", client=storage)

    with pytest.raises(ObjectPreconditionError, match="stale S3 ETag"):
        client.write_if("state.json", b"new", '"1"')


def test_unsupported_conditional_writes_fail_loudly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_botocore(monkeypatch)
    storage = FakeS3Client()
    storage.next_put_error = FakeClientError("NotImplemented", 501)
    client = S3ObjectClient("bucket", client=storage)

    with pytest.raises(FakeClientError, match="NotImplemented"):
        client.write_if("state.json", b"new", '"1"')


def test_list_keys_preserves_full_names_across_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_botocore(monkeypatch)
    storage = FakeS3Client()
    storage.objects.update(
        {
            "prefix/history/b.jsonl": (b"", '"2"'),
            "prefix/history/a.jsonl": (b"", '"3"'),
            "other": (b"", '"4"'),
        }
    )
    client = S3ObjectClient("bucket", client=storage)

    assert client.list_keys("prefix/history/") == [
        "prefix/history/a.jsonl",
        "prefix/history/b.jsonl",
    ]


def test_client_is_constructed_lazily_via_s3_client_when_none_injected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel = FakeS3Client()
    calls: list[dict[str, Any]] = []

    def _fake_s3_client(**kwargs: Any) -> Any:
        calls.append(kwargs)
        return sentinel

    monkeypatch.setattr("drt.state.s3._s3_client", _fake_s3_client)
    client = S3ObjectClient(
        "bucket",
        region="eu-west-1",
        endpoint_url="http://localhost:4566",
        aws_profile="prod",
        aws_access_key_id_env="AWS_KEY_NAME",
        aws_secret_access_key_env="AWS_SECRET_NAME",
        aws_session_token_env="AWS_TOKEN_NAME",
    )

    assert client._client() is sentinel
    assert client._client() is sentinel
    assert calls == [
        {
            "region": "eu-west-1",
            "endpoint_url": "http://localhost:4566",
            "aws_profile": "prod",
            "aws_access_key_id_env": "AWS_KEY_NAME",
            "aws_secret_access_key_env": "AWS_SECRET_NAME",
            "aws_session_token_env": "AWS_TOKEN_NAME",
        }
    ]


def test_s3_client_matches_destination_session_and_endpoint_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_calls: list[dict[str, Any]] = []
    client_calls: list[tuple[str, dict[str, Any]]] = []
    sentinel = object()

    class FakeSession:
        def __init__(self, **kwargs: Any) -> None:
            session_calls.append(kwargs)

        def client(self, service: str, **kwargs: Any) -> Any:
            client_calls.append((service, kwargs))
            return sentinel

    boto3 = ModuleType("boto3")
    session = ModuleType("boto3.session")
    setattr(session, "Session", FakeSession)
    setattr(boto3, "session", session)
    monkeypatch.setitem(sys.modules, "boto3", boto3)
    monkeypatch.setitem(sys.modules, "boto3.session", session)
    monkeypatch.setenv("STATE_AWS_KEY", "key")
    monkeypatch.setenv("STATE_AWS_SECRET", "secret")
    monkeypatch.setenv("STATE_AWS_TOKEN", "token")

    from drt.state.s3 import _s3_client

    result = _s3_client(
        region="ap-northeast-1",
        endpoint_url="http://localhost:9000",
        aws_profile="state",
        aws_access_key_id_env="STATE_AWS_KEY",
        aws_secret_access_key_env="STATE_AWS_SECRET",
        aws_session_token_env="STATE_AWS_TOKEN",
    )

    assert result is sentinel
    assert session_calls == [
        {
            "profile_name": "state",
            "region_name": "ap-northeast-1",
            "aws_access_key_id": "key",
            "aws_secret_access_key": "secret",
            "aws_session_token": "token",
        }
    ]
    assert client_calls == [("s3", {"endpoint_url": "http://localhost:9000"})]


def test_s3_client_missing_sdk_raises_helpful_import_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "boto3", None)
    from drt.state.s3 import _s3_client

    with pytest.raises(ImportError, match=r"pip install drt-core\[s3\]"):
        _s3_client()


def test_client_error_type_missing_sdk_raises_helpful_import_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "botocore.exceptions", None)
    from drt.state.s3 import _client_error_type

    with pytest.raises(ImportError, match=r"pip install drt-core\[s3\]"):
        _client_error_type()
