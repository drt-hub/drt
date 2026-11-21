"""Shared HTTP fault injection for non-idempotent create tests (ADR 0023).

The harness is deliberately blind by default: every POST creates a new remote
object. It records the side effect before either dropping the response or
returning a proxy-style 5xx, reproducing the ambiguous-success window that a
mock which raises before recording cannot exercise.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any, Literal

import httpx
import pytest

FaultMode = Literal["commit-then-drop", "commit-then-5xx"]
ReplayMode = Literal["blind-default"]
ResponseFactory = Callable[[httpx.Request, int], httpx.Response]
FallbackHandler = Callable[[httpx.Request, "BlindCreateFaultHarness"], httpx.Response]


class BlindCreateFaultHarness:
    """In-memory vendor state plus a commit-before-failure HTTP transport."""

    def __init__(
        self,
        create_path: str,
        *,
        mode: FaultMode,
        fallback: FallbackHandler,
        create_response: ResponseFactory,
        replay_mode: ReplayMode = "blind-default",
    ) -> None:
        self.create_path = create_path
        self.mode = mode
        self.replay_mode = replay_mode
        self.fallback = fallback
        self.create_response = create_response
        self.remote_objects: list[dict[str, Any]] = []
        self.requests: list[httpx.Request] = []
        self.create_attempts = 0
        self._fault_injected = False

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.method != "POST" or request.url.path != self.create_path:
            return self.fallback(request, self)

        self.create_attempts += 1
        decoded = json.loads(request.content)
        if not isinstance(decoded, dict):
            raise AssertionError("create payload must be a JSON object")
        object_id = len(self.remote_objects) + 1
        self.remote_objects.append({**decoded, "id": object_id})

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
