"""Execute the documented Braze rest_api recipe YAML against pytest-httpserver (#1179)."""

from __future__ import annotations

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
_RECIPE_PATH = _REPO_ROOT / "docs" / "guides" / "recipes" / "braze.md"


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


def test_braze_recipe_request_shape(
    httpserver: Any,
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    monkeypatch.setenv("BRAZE_API_KEY", "braze-secret-key")
    received: list[tuple[dict[str, str], dict[str, Any]]] = []

    def handler(request: Any) -> Any:
        from werkzeug.wrappers import Response

        received.append((dict(request.headers), json.loads(request.data)))
        return Response(
            json.dumps({"attributes_processed": 2, "message": "success"}),
            status=201,
            content_type="application/json",
        )

    httpserver.expect_request("/users/track", method="POST").respond_with_handler(handler)

    raw_cfg = _extract_first_yaml_block(_RECIPE_PATH)
    raw_cfg["destination"]["url"] = httpserver.url_for("/users/track")
    raw_cfg["destination"]["max_records_per_request"] = 2
    sync = SyncConfig.model_validate(raw_cfg)
    assert isinstance(sync.destination, RestApiDestinationConfig)

    source = _StaticSource(
        [
            {
                "external_id": "usr-1",
                "email": "ada@example.com",
                "plan_tier": "pro",
                "updated_at": "2026-11-16T01:00:00Z",
            },
            {
                "external_id": "usr-2",
                "email": "grace@example.com",
                "plan_tier": "enterprise",
                "updated_at": "2026-11-16T01:05:00Z",
            },
            {
                "external_id": "usr-3",
                "email": "linus@example.com",
                "plan_tier": "free",
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

    for headers, _ in received:
        assert headers.get("Authorization") == "Bearer braze-secret-key"
        assert headers.get("Content-Type") == "application/json"

    assert len(received[0][1]["attributes"]) == 2
    assert len(received[1][1]["attributes"]) == 1
    assert received[0][1]["attributes"][0] == {
        "external_id": "usr-1",
        "email": "ada@example.com",
        "plan_tier": "pro",
        "_update_existing_only": False,
    }
    assert received[1][1]["attributes"][0]["external_id"] == "usr-3"
    httpserver.check_assertions()
