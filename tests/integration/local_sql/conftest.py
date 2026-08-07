"""Docker availability gating for local SQL dialect smoke tests (#939)."""

from __future__ import annotations

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
