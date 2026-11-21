"""Execute the Customer.io rest_api recipe YAML against pytest-httpserver (#1179)."""

from __future__ import annotations

import base64
import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import yaml

from drt.config.credentials import SQLiteProfile
from drt.config.models import RestApiDestinationConfig, SyncConfig
from drt.destinations.rest_api import RestApiDestination
from drt.engine.sync import run_sync

_REPO_ROOT = Path(__file__).resolve().parents[2]
_RECIPE_PATH = _REPO_ROOT / "docs" / "guides" / "recipes" / "customer_io.md"


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


def test_customer_io_recipe_request_shape(
    httpserver: Any,
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    monkeypatch.setenv("CUSTOMERIO_SITE_ID", "site-123")
    monkeypatch.setenv("CUSTOMERIO_API_KEY", "api-key-secret")
    received: list[tuple[dict[str, str], dict[str, Any]]] = []

    def handler(request: Any) -> Any:
        from werkzeug.wrappers import Response

        received.append((dict(request.headers), json.loads(request.data)))
        return Response(json.dumps({}), status=200, content_type="application/json")

    httpserver.expect_request("/api/v2/batch", method="POST").respond_with_handler(handler)

    raw_cfg = _extract_first_yaml_block(_RECIPE_PATH)
    raw_cfg["destination"]["url"] = httpserver.url_for("/api/v2/batch")
    raw_cfg["destination"]["max_records_per_request"] = 2
    sync = SyncConfig.model_validate(raw_cfg)
    assert isinstance(sync.destination, RestApiDestinationConfig)

    source = _StaticSource(
        [
            {
                "user_id": "u-1",
                "email": "ada@example.com",
                "plan": "pro",
                "updated_at": "2026-11-16T01:00:00Z",
            },
            {
                "user_id": "u-2",
                "email": "grace@example.com",
                "plan": "enterprise",
                "updated_at": "2026-11-16T01:05:00Z",
            },
            {
                "user_id": "u-3",
                "email": "linus@example.com",
                "plan": "free",
                "updated_at": "2026-11-16T01:10:00Z",
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
    assert len(received) == 2

    expected_auth = "Basic " + base64.b64encode(b"site-123:api-key-secret").decode("ascii")
    for headers, _ in received:
        assert headers.get("Authorization") == expected_auth
        assert headers.get("Content-Type") == "application/json"

    assert len(received[0][1]["batch"]) == 2
    assert len(received[1][1]["batch"]) == 1
    assert received[0][1]["batch"][0] == {
        "type": "person",
        "identifiers": {"id": "u-1"},
        "action": "identify",
        "attributes": {"email": "ada@example.com", "plan": "pro"},
    }
    assert received[1][1]["batch"][0]["identifiers"] == {"id": "u-3"}
    httpserver.check_assertions()
