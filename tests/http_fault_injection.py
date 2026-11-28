"""Shared HTTP fault injection for non-idempotent create tests (ADR 0023).

The harness is deliberately blind by default: every configured write request
creates a new remote object. Keyed vendor endpoints can opt into deduplication
with ``replay_mode`` and ``dedup_key``. It records the side effect before either
dropping the response or returning a proxy-style 5xx, reproducing the
ambiguous-success window that a mock which raises before recording cannot
exercise.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any, Literal

import httpx
import pytest

FaultMode = Literal["commit-then-drop", "commit-then-5xx"]
ReplayMode = Literal["blind-default", "keyed"]
ResponseFactory = Callable[[httpx.Request, int], httpx.Response]
FallbackHandler = Callable[[httpx.Request, "BlindCreateFaultHarness"], httpx.Response]
DedupKey = Callable[[dict[str, Any]], object]
RequestDecoder = Callable[[httpx.Request], dict[str, Any]]


def mixpanel_event_dedup_key(payload: dict[str, Any]) -> object:
    """Model Mixpanel's full event deduplication tuple for each imported event."""
    return tuple(
        (
            item["event"],
            item["properties"]["distinct_id"],
            item["properties"]["time"],
            item["properties"]["$insert_id"],
        )
        for item in payload["items"]
    )


def _decode_json_object(request: httpx.Request) -> dict[str, Any]:
    decoded = json.loads(request.content)
    if not isinstance(decoded, dict):
        raise AssertionError("create payload must be a JSON object")
    return decoded


class BlindCreateFaultHarness:
    """In-memory vendor state plus a commit-before-failure HTTP transport."""

    def __init__(
        self,
        create_path: str,
        *,
        create_method: str = "POST",
        mode: FaultMode,
        fallback: FallbackHandler,
        create_response: ResponseFactory,
        replay_mode: ReplayMode = "blind-default",
        dedup_key: DedupKey | None = None,
        decode_request: RequestDecoder = _decode_json_object,
    ) -> None:
        self.create_path = create_path
        self.create_method = create_method
        self.mode = mode
        self.replay_mode = replay_mode
        self.fallback = fallback
        self.create_response = create_response
        self.dedup_key = dedup_key
        self.decode_request = decode_request
        self.remote_objects: list[dict[str, Any]] = []
        self.requests: list[httpx.Request] = []
        self.create_attempts = 0
        self._fault_injected = False

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.method != self.create_method or request.url.path != self.create_path:
            return self.fallback(request, self)

        self.create_attempts += 1
        decoded = self.decode_request(request)
        existing = None
        if self.replay_mode == "keyed":
            if self.dedup_key is None:
                raise AssertionError("keyed replay mode requires dedup_key")
            key = self.dedup_key(decoded)
            existing = next(
                (item for item in self.remote_objects if self.dedup_key(item) == key),
                None,
            )
        if existing is None:
            object_id = len(self.remote_objects) + 1
            self.remote_objects.append({**decoded, "id": object_id})
        else:
            object_id = int(existing["id"])

        if not self._fault_injected:
            self._fault_injected = True
            if self.mode == "commit-then-drop":
                raise httpx.ReadError("response dropped after remote commit", request=request)
            return httpx.Response(503, text="proxy lost upstream response", request=request)
        return self.create_response(request, object_id)


def install_mock_transport(
    monkeypatch: pytest.MonkeyPatch,
    handler: Callable[[httpx.Request], httpx.Response],
) -> None:
    """Route clients created during a test through ``handler`` without a socket."""
    real_client = httpx.Client

    def client_factory(*args: Any, **kwargs: Any) -> httpx.Client:
        kwargs["transport"] = httpx.MockTransport(handler)
        return real_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", client_factory)
