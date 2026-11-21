"""Execute the documented Segment rest_api recipe YAML against pytest-httpserver (#1179)."""

from __future__ import annotations

import base64
import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml

from drt.config.credentials import SQLiteProfile
from drt.config.models import BasicAuth, RestApiDestinationConfig, SyncConfig
from drt.destinations.auth import AuthHandler
from drt.destinations.rest_api import RestApiDestination
from drt.engine.sync import run_sync

_REPO_ROOT = Path(__file__).resolve().parents[2]
_RECIPE_PATH = _REPO_ROOT / "docs" / "guides" / "recipes" / "segment.md"


class _StaticSource:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def extract(self, _query: str, _config: Any, **_kwargs: Any) -> Iterator[dict[str, Any]]:
        yield from self._rows


def _extract_first_yaml_block(markdown_path: Path) -> dict[str, Any]:
    text = markdown_path.read_text(encoding="utf-8")
    match = re.search(r"```yaml\s*\n(.*?)```", text, flags=re.DOTALL)
    assert match is not None, f"No ```yaml block found in {markdown_path}"
    loaded = yaml.safe_load(match.group(1))
    assert isinstance(loaded, dict)
    return loaded


def test_segment_batch_recipe_request_shape(
    httpserver: Any,
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    monkeypatch.setenv("SEGMENT_WRITE_KEY", "seg-write-key-test")
    monkeypatch.setenv("SEGMENT_WRITE_PASSWORD", "")
    received: list[tuple[dict[str, str], dict[str, Any]]] = []

    def handler(request: Any) -> Any:
        from werkzeug.wrappers import Response

        received.append((dict(request.headers), json.loads(request.data)))
        return Response(json.dumps({"success": True}), status=200, content_type="application/json")

    httpserver.expect_request("/v1/batch", method="POST").respond_with_handler(handler)

    raw_cfg = _extract_first_yaml_block(_RECIPE_PATH)
    raw_cfg["destination"]["url"] = httpserver.url_for("/v1/batch")
    raw_cfg["destination"]["max_records_per_request"] = 2
    sync = SyncConfig.model_validate(raw_cfg)
    assert isinstance(sync.destination, RestApiDestinationConfig)

    source = _StaticSource(
        [
            {
                "type": "track",
                "user_id": "u-1",
                "event_name": "Order Completed",
                "message_id": "msg-1",
                "occurred_at": "2026-11-16T01:00:00Z",
                "properties": {"total": 49.0, "currency": "AUD"},
            },
            {
                "type": "track",
                "user_id": "u-2",
                "event_name": "Order Completed",
                "message_id": "msg-2",
                "occurred_at": "2026-11-16T01:05:00Z",
                "properties": {"total": 19.5, "currency": "AUD"},
            },
            {
                "type": "track",
                "user_id": "u-3",
                "event_name": "Plan Upgraded",
                "message_id": "msg-3",
                "occurred_at": "2026-11-16T01:10:00Z",
                "properties": {"plan": "pro"},
            },
        ]
    )
    result = run_sync(
        sync,
        source,
        RestApiDestination(),
        SQLiteProfile(type="sqlite"),
        tmp_path,
    )

    assert result.success == 3
    assert result.failed == 0
    assert len(received) == 2  # 3 rows split by max_records_per_request=2

    expected_auth = "Basic " + base64.b64encode(b"seg-write-key-test:").decode("ascii")
    for headers, _ in received:
        assert headers.get("Authorization") == expected_auth
        assert headers.get("Content-Type") == "application/json"

    assert len(received[0][1]["batch"]) == 2
    assert len(received[1][1]["batch"]) == 1
    assert received[0][1]["batch"][0] == {
        "type": "track",
        "userId": "u-1",
        "event": "Order Completed",
        "messageId": "msg-1",
        "timestamp": "2026-11-16T01:00:00Z",
        "properties": {"total": 49.0, "currency": "AUD"},
    }
    assert received[1][1]["batch"][0]["messageId"] == "msg-3"
    httpserver.check_assertions()


def test_basic_auth_allows_empty_password_but_rejects_unset(monkeypatch: Any) -> None:
    auth = BasicAuth(
        type="basic",
        username_env="TEST_BASIC_USER",
        password_env="TEST_BASIC_PASS",
    )
    monkeypatch.setenv("TEST_BASIC_USER", "write-key-only")

    monkeypatch.delenv("TEST_BASIC_PASS", raising=False)
    with pytest.raises(ValueError, match="TEST_BASIC_PASS"):
        AuthHandler(auth).get_headers()

    monkeypatch.setenv("TEST_BASIC_PASS", "")
    headers = AuthHandler(auth).get_headers()
    expected = "Basic " + base64.b64encode(b"write-key-only:").decode("ascii")
    assert headers == {"Authorization": expected}
