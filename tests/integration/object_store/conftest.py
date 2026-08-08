"""Real GCS semantics through fake-gcs-server in Docker (#756)."""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from typing import Any

import pytest


def require_docker() -> None:
    """Skip when Docker's Python client is absent or its daemon is unreachable."""
    docker = pytest.importorskip("docker")
    try:
        client = docker.from_env()
        try:
            client.ping()
        finally:
            client.close()
    except docker.errors.DockerException as exc:
        pytest.skip(f"Docker is not reachable: {exc}")


@pytest.fixture(scope="session")
def fake_gcs() -> Iterator[tuple[Any, str]]:
    """Start fake-gcs-server over HTTP and return its SDK client and bucket."""
    require_docker()
    storage = pytest.importorskip("google.cloud.storage")
    from testcontainers.core.generic import DockerContainer

    container = (
        DockerContainer("fsouza/fake-gcs-server")
        .with_exposed_ports(4443)
        .with_command(["-scheme", "http"])
    )
    container.start()
    endpoint = f"http://{container.get_container_host_ip()}:{container.get_exposed_port(4443)}"
    previous_endpoint = os.environ.get("STORAGE_EMULATOR_HOST")
    os.environ["STORAGE_EMULATOR_HOST"] = endpoint
    try:
        client = storage.Client(project="drt-object-store-smoke")
        deadline = time.monotonic() + 15
        while True:
            try:
                list(client.list_buckets())
                break
            except Exception:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.1)
        bucket_name = "drt-object-store-smoke"
        client.create_bucket(bucket_name)
        yield client, bucket_name
    finally:
        if previous_endpoint is None:
            os.environ.pop("STORAGE_EMULATOR_HOST", None)
        else:
            os.environ["STORAGE_EMULATOR_HOST"] = previous_endpoint
        container.stop()


@pytest.fixture(scope="session")
def generation_preconditions_supported(fake_gcs: tuple[Any, str]) -> bool:
    """Foundational canary: reject false-green concurrency tests.

    The suite is meaningful only if the emulator enforces both stale upload
    preconditions and exact-generation reads. Skip every dependent test when
    either capability is missing.
    """
    from google.api_core.exceptions import PreconditionFailed
    from google.cloud.exceptions import NotFound

    from drt.state._objectstore import ObjectPreconditionError
    from drt.state.gcs import GCSObjectClient

    storage_client, bucket_name = fake_gcs
    client = GCSObjectClient(bucket_name, client=storage_client)
    key = "canary/generation.txt"
    first_generation = client.write_if(key, b"first", 0)
    second_generation = client.write_if(key, b"second", first_generation)
    try:
        client.write_if(key, b"stale", first_generation)
    except ObjectPreconditionError:
        pass
    else:
        pytest.skip(
            "fake-gcs-server does not enforce stale ifGenerationMatch uploads; "
            "concurrency results would be false-green"
        )

    old_version = storage_client.bucket(bucket_name).blob(
        key, generation=int(first_generation)
    )
    try:
        old_version.download_as_bytes()
    except (NotFound, PreconditionFailed):
        pass
    else:
        pytest.skip(
            "fake-gcs-server does not enforce exact-generation reads; "
            "pinned-read concurrency results would be false-green"
        )
    assert second_generation != first_generation
    return True


@pytest.fixture
def gcs_client(
    fake_gcs: tuple[Any, str], generation_preconditions_supported: bool
) -> Any:
    assert generation_preconditions_supported
    from drt.state.gcs import GCSObjectClient

    storage_client, bucket_name = fake_gcs
    return GCSObjectClient(bucket_name, client=storage_client)


@pytest.fixture(scope="session")
def localstack_s3() -> Iterator[tuple[Any, str]]:
    """Start LocalStack's S3 service and return its boto3 client and bucket."""
    require_docker()
    pytest.importorskip("boto3")
    from testcontainers.community.localstack import LocalStackContainer

    container = LocalStackContainer(
        image="localstack/localstack:4.14.0",
        region_name="us-east-1",
    ).with_services("s3")
    container.start()
    try:
        client = container.get_client("s3")
        deadline = time.monotonic() + 15
        while True:
            try:
                client.list_buckets()
                break
            except Exception:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.1)
        bucket_name = "drt-object-store-smoke"
        client.create_bucket(Bucket=bucket_name)
        yield client, bucket_name
    finally:
        container.stop()


@pytest.fixture(scope="session")
def s3_preconditions_supported(localstack_s3: tuple[Any, str]) -> bool:
    """Foundational canary: reject false-green S3 concurrency tests.

    The suite is meaningful only when LocalStack enforces create-only PUT,
    stale-ETag PUT, and ETag-pinned GET. A merely accepted conditional header
    is insufficient: silently ignoring one would turn lost updates green.
    """
    from botocore.exceptions import ClientError

    from drt.state._objectstore import ObjectPreconditionError
    from drt.state.s3 import S3ObjectClient

    storage_client, bucket_name = localstack_s3
    client = S3ObjectClient(bucket_name, client=storage_client)
    key = "canary/etag.txt"
    first_etag = client.write_if(key, b"first", None)
    try:
        client.write_if(key, b"duplicate create", None)
    except ObjectPreconditionError:
        pass
    else:
        pytest.skip(
            "LocalStack does not enforce PutObject IfNoneMatch='*'; "
            "concurrency results would be false-green"
        )

    second_etag = client.write_if(key, b"second", first_etag)
    try:
        client.write_if(key, b"stale", first_etag)
    except ObjectPreconditionError:
        pass
    else:
        pytest.skip(
            "LocalStack does not enforce stale PutObject IfMatch; "
            "concurrency results would be false-green"
        )

    try:
        storage_client.get_object(
            Bucket=bucket_name,
            Key=key,
            IfMatch=str(first_etag),
        )
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status != 412:
            raise
    else:
        pytest.skip(
            "LocalStack does not enforce stale GetObject IfMatch reads; "
            "pinned-read concurrency results would be false-green"
        )
    assert second_etag != first_etag
    return True


@pytest.fixture
def s3_client(
    localstack_s3: tuple[Any, str], s3_preconditions_supported: bool
) -> Any:
    assert s3_preconditions_supported
    from drt.state.s3 import S3ObjectClient

    storage_client, bucket_name = localstack_s3
    return S3ObjectClient(bucket_name, client=storage_client)
