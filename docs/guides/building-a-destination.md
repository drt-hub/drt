# Building a Destination Connector

This guide walks through adding a new destination connector to drt, step by step. By the end you will have a working connector with config validation, error handling, and tests.

We will build a fictional **Webhook** destination as our running example -- a generic HTTP POST sender that pushes each row as JSON to a URL. The same pattern applies to databases, SaaS APIs, and message queues.

## Overview

Adding a destination requires four changes:

| Step | File(s) | What you add |
|------|---------|-------------|
| 1. Config model | `drt/config/models.py` | Pydantic model for YAML config |
| 2. Destination class | `drt/destinations/webhook.py` | `load()` implementation |
| 3. CLI registration | `drt/cli/main.py` | isinstance branch |
| 4. Tests | `tests/unit/test_webhook_destination.py` | Unit tests |

## Prerequisites

```bash
git clone https://github.com/drt-hub/drt.git && cd drt
uv sync --extra dev        # or: pip install -e ".[dev]"
make test                  # verify everything passes before you start
```

---

## Step 1: Config Model

Open `drt/config/models.py` and add your config class. Every destination config must have a `type` field with a `Literal` value that matches the YAML `type:` key.

```python
class WebhookDestinationConfig(BaseModel):
    type: Literal["webhook"]
    url: str | None = None
    url_env: str | None = None
    method: str = "POST"
    headers: dict[str, str] = {}
    body_template: str | None = None  # Jinja2 template; if None, sends raw row JSON

    def describe(self) -> str:
        return f"{self.type} ({self.url or self.url_env})"

    @model_validator(mode="after")
    def _check_url(self) -> "WebhookDestinationConfig":
        if not self.url and not self.url_env:
            raise ValueError("Either url or url_env is required.")
        return self
```

Key patterns:
- **`type: Literal["webhook"]`** -- discriminator for the Pydantic union.
- **`_env` fields** -- let users reference env vars instead of hardcoding secrets.
- **`describe()`** -- used by `drt list` CLI output.
- **`@model_validator`** -- validate at parse time, not at runtime.

Then register it in the `DestinationConfig` union at the bottom of the same file:

```python
DestinationConfig = Annotated[
    RestApiDestinationConfig
    | SlackDestinationConfig
    | DiscordDestinationConfig
    # ... existing destinations ...
    | WebhookDestinationConfig,   # <-- add yours here
    Field(discriminator="type"),
]
```

At this point, `drt validate` will accept YAML with `type: webhook`.

---

## Step 2: Destination Class

Create `drt/destinations/webhook.py`. Your class must implement a `load()` method matching the `Destination` Protocol in `drt/destinations/base.py`:

```python
from __future__ import annotations

import json
from typing import Any

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import (
    DestinationConfig,
    RetryConfig,
    SyncOptions,
    WebhookDestinationConfig,
)
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter
from drt.destinations.retry import with_retry
from drt.destinations.row_errors import RowError
from drt.templates.renderer import render_template

_DEFAULT_RETRY = RetryConfig(
    max_attempts=3,
    initial_backoff=1.0,
    retryable_status_codes=(429, 500, 502, 503, 504),
)


class WebhookDestination:

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, WebhookDestinationConfig)

        url = resolve_env(config.url, config.url_env)
        if not url:
            raise ValueError(
                "Webhook destination: provide url or set url_env."
            )

        result = SyncResult()
        rate_limiter = RateLimiter(sync_options.rate_limit.requests_per_second)
        retry = sync_options.retry or _DEFAULT_RETRY

        with httpx.Client(timeout=30.0) as client:
            for i, record in enumerate(records):
                rate_limiter.acquire()
                try:
                    if config.body_template:
                        body = json.loads(
                            render_template(config.body_template, record)
                        )
                    else:
                        body = record

                    _url, _body, _method = url, body, config.method
                    _headers = config.headers

                    def do_request() -> httpx.Response:
                        resp = client.request(
                            _method, _url, json=_body, headers=_headers
                        )
                        resp.raise_for_status()
                        return resp

                    with_retry(do_request, retry)
                    result.success += 1

                except httpx.HTTPStatusError as e:
                    result.failed += 1
                    result.row_errors.append(
                        RowError(
                            batch_index=i,
                            record_preview=json.dumps(record, default=str)[:200],
                            http_status=e.response.status_code,
                            error_message=e.response.text[:500],
                        )
                    )
                    if sync_options.on_error == "fail":
                        return result

                except Exception as e:
                    result.failed += 1
                    result.row_errors.append(
                        RowError(
                            batch_index=i,
                            record_preview=json.dumps(record, default=str)[:200],
                            http_status=None,
                            error_message=str(e),
                        )
                    )
                    if sync_options.on_error == "fail":
                        return result

        return result
```

### Key patterns to follow

1. **`assert isinstance(config, ...)`** -- narrows the union type for type checkers.
2. **`resolve_env()`** -- resolves env var secrets. Always prefer this over raw `os.environ`.
3. **`RateLimiter`** -- respects the user rate_limit setting. One-liner to add.
4. **`with_retry()`** -- exponential backoff on transient HTTP errors.
5. **`RowError`** -- capture per-row failures with `batch_index`, a truncated `record_preview` (max 200 chars), optional `http_status`, and `error_message`.
6. **`on_error` handling** -- if `"fail"`, return immediately on first error. If `"skip"` (default), log the error and continue.
7. **Empty records** -- return `SyncResult()` early if `records` is empty (optional but clean).

### Database destinations

For database connectors, the pattern differs slightly:
- Lazy-import the driver (e.g. `psycopg2`, `pymysql`) and raise a helpful `ImportError` if missing.
- Use a `_connect()` static method to build the connection.
- Use `try/finally` to guarantee `conn.close()`.
- Roll back on row errors, re-open the cursor, and continue (for `on_error="skip"`).

See `drt/destinations/postgres.py` for the reference implementation.

---

## Step 3: CLI Registration

Open `drt/cli/main.py` and add your destination to `_get_destination()`:

```python
from drt.destinations.webhook import WebhookDestination

# Inside _get_destination():
if isinstance(dest, WebhookDestinationConfig):
    return WebhookDestination()
```

Add it before the final `raise ValueError(...)` line. No plugin registry needed.

---

## Step 4: Tests

Create `tests/unit/test_webhook_destination.py`. For HTTP destinations, use `pytest-httpserver` which is already in dev dependencies.

```python
import pytest
from pytest_httpserver import HTTPServer

from drt.config.models import SyncOptions, WebhookDestinationConfig
from drt.destinations.webhook import WebhookDestination


def _options(**kwargs) -> SyncOptions:
    return SyncOptions(**kwargs)


class TestWebhookDestination:
    def test_success(self, httpserver: HTTPServer) -> None:
        httpserver.expect_request(
            "/ingest", method="POST"
        ).respond_with_json({"ok": True}, status=200)
        config = WebhookDestinationConfig(
            type="webhook", url=httpserver.url_for("/ingest")
        )
        result = WebhookDestination().load(
            [{"id": 1, "name": "Alice"}], config, _options()
        )
        assert result.success == 1
        assert result.failed == 0

    def test_on_error_skip(self, httpserver: HTTPServer) -> None:
        httpserver.expect_ordered_request(
            "/ingest", method="POST"
        ).respond_with_json({"error": "bad"}, status=500)
        httpserver.expect_ordered_request(
            "/ingest", method="POST"
        ).respond_with_json({"ok": True}, status=200)
        config = WebhookDestinationConfig(
            type="webhook", url=httpserver.url_for("/ingest")
        )
        opts = _options(on_error="skip", retry={"max_attempts": 1})
        result = WebhookDestination().load(
            [{"id": 1}, {"id": 2}], config, opts
        )
        assert result.failed == 1
        assert result.success == 1
        assert result.row_errors[0].http_status == 500

    def test_on_error_fail_stops_early(self, httpserver: HTTPServer) -> None:
        httpserver.expect_request(
            "/ingest", method="POST"
        ).respond_with_json({"error": "bad"}, status=500)
        config = WebhookDestinationConfig(
            type="webhook", url=httpserver.url_for("/ingest")
        )
        opts = _options(on_error="fail", retry={"max_attempts": 1})
        result = WebhookDestination().load(
            [{"id": 1}, {"id": 2}], config, opts
        )
        assert result.failed == 1
        assert result.success == 0
        assert result.total == 1

    def test_missing_url_raises(self) -> None:
        with pytest.raises(ValueError, match="Either url or url_env"):
            WebhookDestinationConfig(type="webhook")


class TestWebhookConfig:
    def test_url_env_is_valid(self) -> None:
        config = WebhookDestinationConfig(
            type="webhook", url_env="MY_WEBHOOK_URL"
        )
        assert config.url_env == "MY_WEBHOOK_URL"

    def test_describe(self) -> None:
        config = WebhookDestinationConfig(
            type="webhook", url="https://example.com/hook"
        )
        assert "webhook" in config.describe()
```

Run the tests:

```bash
uv run pytest tests/unit/test_webhook_destination.py -v
```

### Database destination tests

For database connectors, mock the connection instead of using httpserver:

```python
from unittest.mock import MagicMock, patch

@patch("drt.destinations.mydb.MyDbDestination._connect")
def test_success(self, mock_connect):
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    # ... test load() ...
    mock_conn.close.assert_called_once()  # verify cleanup
```

See `tests/unit/test_postgres_destination.py` for the full pattern.

---

## Step 5: Verify

Before opening a PR, run the full suite:

```bash
make lint       # ruff + mypy
make test       # all tests
```

## Checklist

- [ ] Config model in `drt/config/models.py` with `type: Literal["..."]`
- [ ] Config added to `DestinationConfig` union
- [ ] Config has `describe()` method
- [ ] Config has validators for required fields
- [ ] Destination class in `drt/destinations/`
- [ ] `load()` signature matches the `Destination` Protocol
- [ ] Uses `resolve_env()` for secrets
- [ ] Uses `RateLimiter` and `with_retry()` (HTTP destinations)
- [ ] Uses `try/finally` for connection cleanup (database destinations)
- [ ] Builds `RowError` on per-row failures
- [ ] Respects `on_error` ("fail" returns early, "skip" continues)
- [ ] isinstance branch in `_get_destination()` in `drt/cli/main.py`
- [ ] Tests cover: success, skip, fail-stops-early, missing config
- [ ] `make lint` passes
- [ ] `make test` passes

## Shared Utilities Reference

| Module | What it does |
|--------|-------------|
| `drt.config.credentials.resolve_env(value, env_var)` | Resolve a secret from explicit value or env var |
| `drt.destinations.rate_limiter.RateLimiter` | Token-bucket rate limiter |
| `drt.destinations.retry.with_retry(fn, config)` | Exponential backoff on transient errors |
| `drt.destinations.auth.AuthHandler` | Resolves `AuthConfig` to HTTP headers |
| `drt.destinations.row_errors.RowError` | Structured per-row error record |
| `drt.templates.renderer.render_template(tpl, row)` | Jinja2 template rendering with `{{ row.field }}` |
