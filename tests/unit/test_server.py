"""Tests for the webhook trigger server."""

from __future__ import annotations

import json
import threading
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from drt import __version__
from drt.cli.server import _SyncLock, make_handler


def _run_server(
    token: str | None = None,
    project_dir: str = ".",
) -> tuple[ThreadingHTTPServer, threading.Thread, int]:
    """Start a server on a random port and return (server, thread, port)."""
    sync_lock = _SyncLock()
    handler = make_handler(token, sync_lock, project_dir)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, port


def _get(url: str, token: str | None = None) -> tuple[int, dict[str, Any]]:
    req = urllib.request.Request(url)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def _post(
    url: str, token: str | None = None
) -> tuple[int, dict[str, Any]]:
    req = urllib.request.Request(url, method="POST")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def test_health_endpoint() -> None:
    server, _, port = _run_server()
    try:
        status, body = _get(f"http://127.0.0.1:{port}/health")
        assert status == 200
        assert body == {"status": "ok", "version": __version__}
    finally:
        server.shutdown()


def test_unknown_get_returns_404() -> None:
    server, _, port = _run_server()
    try:
        status, body = _get(f"http://127.0.0.1:{port}/unknown")
        assert status == 404
        assert "error" in body
    finally:
        server.shutdown()


def test_post_without_auth_when_required_returns_401() -> None:
    server, _, port = _run_server(token="secret123")
    try:
        status, body = _post(f"http://127.0.0.1:{port}/sync/my_sync")
        assert status == 401
        assert "unauthorized" in body["error"]
    finally:
        server.shutdown()


def test_post_with_wrong_token_returns_401() -> None:
    server, _, port = _run_server(token="secret123")
    try:
        status, _ = _post(
            f"http://127.0.0.1:{port}/sync/my_sync", token="wrong"
        )
        assert status == 401
    finally:
        server.shutdown()


def test_post_missing_sync_name_returns_400() -> None:
    server, _, port = _run_server()
    try:
        status, body = _post(f"http://127.0.0.1:{port}/sync/")
        assert status == 400
        assert "sync name" in body["error"]
    finally:
        server.shutdown()


def test_post_sync_not_found_returns_404(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """run_drt_sync raises ValueError for unknown sync → 404."""
    # Create minimal project setup so load_project works
    import yaml

    (tmp_path / "drt_project.yml").write_text(
        yaml.dump({"name": "test", "version": "0.1", "profile": "default"})
    )
    creds = tmp_path / "drt_home"
    creds.mkdir()
    (creds / "profiles.yml").write_text(
        yaml.dump({"default": {"type": "duckdb", "database": ":memory:"}})
    )
    monkeypatch.setattr(
        "drt.config.credentials._config_dir",
        lambda override=None: override or creds,
    )
    (tmp_path / "syncs").mkdir()

    server, _, port = _run_server(project_dir=str(tmp_path))
    try:
        status, body = _post(f"http://127.0.0.1:{port}/sync/nonexistent")
        assert status == 404
        assert "nonexistent" in body["error"]
    finally:
        server.shutdown()


def test_concurrent_request_returns_423() -> None:
    """Second concurrent sync request should get 423 Locked."""
    sync_lock = _SyncLock()

    # Simulate running sync by holding the lock
    acquired = sync_lock.try_acquire()
    assert acquired

    handler = make_handler(None, sync_lock, ".")
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        status, body = _post(f"http://127.0.0.1:{port}/sync/any")
        assert status == 423
        assert "already running" in body["error"]
    finally:
        sync_lock.release()
        server.shutdown()


def test_sync_lock_releases_after_success() -> None:
    """After one sync completes, next request is not blocked."""
    with patch("drt.integrations._runner.run_drt_sync") as mock_run:
        mock_run.return_value = {
            "sync_name": "s",
            "status": "success",
            "rows_synced": 1,
            "rows_failed": 0,
            "duration_seconds": 0.1,
            "dry_run": False,
            "errors": [],
        }
        server, _, port = _run_server()
        try:
            status1, body1 = _post(f"http://127.0.0.1:{port}/sync/s")
            assert status1 == 200
            assert body1["status"] == "success"
            # Second call should also work (lock released)
            status2, _ = _post(f"http://127.0.0.1:{port}/sync/s")
            assert status2 == 200
        finally:
            server.shutdown()