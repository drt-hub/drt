"""Tests that execute the documented rest_api recipe YAMLs against pytest-httpserver (#1179)."""

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
_RECIPES_DIR = _REPO_ROOT / "docs" / "guides" / "recipes"


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


def test_pagerduty_recipe_request_shape(
    httpserver: Any,
    tmp_path: Path,
) -> None:
    received: list[tuple[dict[str, str], dict[str, Any]]] = []

    def handler(request: Any) -> Any:
        from werkzeug.wrappers import Response

        received.append((dict(request.headers), json.loads(request.data)))
        return Response(
            json.dumps({"status": "success", "message": "Event processed", "dedup_key": "b-1"}),
            status=202,
            content_type="application/json",
        )

    httpserver.expect_request("/v2/enqueue", method="POST").respond_with_handler(handler)

    raw_cfg = _extract_first_yaml_block(_RECIPES_DIR / "pagerduty.md")
    raw_cfg["destination"]["url"] = httpserver.url_for("/v2/enqueue")
    sync = SyncConfig.model_validate(raw_cfg)
    assert isinstance(sync.destination, RestApiDestinationConfig)

    source = _StaticSource(
        [
            {
                "routing_key": "pd-routing-key-test",
                "dedup_key": "breach-orders-2026-11-16",
                "event_action": "trigger",
                "summary": "SLA breach on orders: 45m late",
                "source": "orders_pipeline",
                "severity": "error",
                "detected_at": "2026-11-16T01:00:00Z",
            }
        ]
    )
    result = run_sync(
        sync,
        source,
        RestApiDestination(),
        SQLiteProfile(type="sqlite"),
        tmp_path,
    )

    assert result.success == 1
    assert result.failed == 0
    assert len(received) == 1

    headers, payload = received[0]
    assert headers.get("Content-Type") == "application/json"
    assert payload == {
        "routing_key": "pd-routing-key-test",
        "event_action": "trigger",
        "dedup_key": "breach-orders-2026-11-16",
        "payload": {
            "summary": "SLA breach on orders: 45m late",
            "source": "orders_pipeline",
            "severity": "error",
        },
    }
    httpserver.check_assertions()
