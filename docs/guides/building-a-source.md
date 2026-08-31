# Building a Source Connector

This guide walks through adding a new source connector *in-tree* (contributed directly to drt-core), step by step. By the end you will have a working connector with a typed credential profile, registry dispatch, connection testing, and unit tests.

An out-of-tree source registered through the `drt.sources` entry point uses the same source-method convention. Its profile class is loaded directly from `profiles.yml` rather than added to drt-core's built-in union; see [Third-Party Plugins](plugins.md).

We will build a fictional **Acme Analytics** API source as the running example. The same shape applies to databases, warehouses, and other APIs.

## Overview

Adding an in-tree source requires four changes:

| Step | File(s) | What you add |
|------|---------|-------------|
| 1. Profile model | `drt/config/profiles.py` | Dataclass for credentials and the built-in union member |
| 2. Source class | `drt/sources/acme.py` | `extract()` and `test_connection()` implementations |
| 3. Loading and registration | `drt/config/credentials.py`, `drt/connectors/registry.py` | YAML construction, saving, and connector registry wiring |
| 4. Tests and docs | `tests/unit/test_acme_source.py`, `docs/connectors/acme.md` | Unit, contract, and user-facing coverage |

## Prerequisites

```bash
git clone https://github.com/drt-hub/drt.git && cd drt
uv sync --extra dev        # or: pip install -e ".[dev]"
make test                  # verify everything passes before you start
```

---

## Step 1: Profile Model

Open `drt/config/profiles.py` and add a dataclass. Every source profile has a `type` field with a `Literal` value matching the `type:` key in `profiles.yml`, plus `describe()` for CLI output.

```python
from dataclasses import dataclass
from typing import Literal


@dataclass
class AcmeProfile:
    type: Literal["acme"]
    base_url: str = "https://api.acme.example"
    token_env: str = "ACME_TOKEN"

    def describe(self) -> str:
        return f"{self.type} ({self.base_url})"
```

Add `AcmeProfile` to the `ProfileConfig` union at the bottom of that file. The union deliberately enumerates built-ins; third-party plugins do not edit it.

Keep secrets in `_env` fields and resolve them only when the connector runs. Do not read environment variables or make network calls while constructing the profile.

---

## Step 2: Source Class

Create `drt/sources/acme.py`. The class implements the stable `Source` Protocol in `drt/sources/base.py`:

```python
from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import httpx

from drt.config.credentials import AcmeProfile, ProfileConfigLike, resolve_env


class AcmeSource:
    def extract(
        self,
        query: str,
        config: ProfileConfigLike,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        assert isinstance(config, AcmeProfile)

        token = resolve_env(None, config.token_env)
        if not token:
            raise ValueError(f"Acme token env var {config.token_env!r} is not set")

        with httpx.Client(
            base_url=config.base_url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30.0,
        ) as client:
            response = client.get("/rows", params={"query": query})
            response.raise_for_status()
            for row in response.json()["rows"]:
                yield dict(row)

    def test_connection(self, config: ProfileConfigLike) -> bool:
        assert isinstance(config, AcmeProfile)

        token = resolve_env(None, config.token_env)
        if not token:
            return False
        try:
            response = httpx.get(
                f"{config.base_url}/health",
                headers={"Authorization": f"Bearer {token}"},
                timeout=10.0,
            )
            return response.is_success
        except httpx.HTTPError:
            return False
```

Key patterns:

1. `config: ProfileConfigLike` keeps the method aligned with the stable `Source` Protocol.
2. `assert isinstance(config, AcmeProfile)` is the first executable line and narrows the structural type for checked access to `base_url` and `token_env`.
3. `extract()` yields dictionaries instead of materializing the whole result, so engine memory follows the batch size.
4. `query_tags` remains keyword-only with a `None` default. Use it when the service has native request metadata; otherwise it is safe to ignore.
5. `test_connection()` returns `False` for connection/query failures, matching the frozen source-side contract.
6. Optional drivers belong behind a lazy import with a helpful `ImportError` naming the required extra.

### Incremental sources

API sources that can receive a resolved watermark directly may also implement `IncrementalSource`:

```python
def extract_incremental(
    self,
    query: str,
    config: ProfileConfigLike,
    cursor_value: str | None,
    *,
    query_tags: dict[str, str] | None = None,
) -> Iterator[dict[str, Any]]:
    assert isinstance(config, AcmeProfile)
    # Send cursor_value to the remote API and yield matching rows.
    ...
```

SQL sources normally consume the rendered watermark predicate through `query` and do not need this optional capability.

---

## The `ProfileConfigLike` Convention for Plugins

`ProfileConfigLike` is structural: a profile satisfies it by exposing a string-compatible `type` and `describe() -> str`. It does not need to inherit from a drt class. This is what allows a profile supplied by an installed plugin to cross the same stable `Source` boundary as a built-in profile.

The safe implementation pattern is exact and intentional:

```python
@dataclass
class MyPluginProfile:
    type: str
    endpoint: str

    def describe(self) -> str:
        return f"{self.type} ({self.endpoint})"


class MyPluginSource:
    def extract(
        self,
        query: str,
        config: ProfileConfigLike,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        assert isinstance(config, MyPluginProfile)
        # config is MyPluginProfile here, so endpoint is typed.
        yield from fetch_rows(config.endpoint, query)

    def test_connection(self, config: ProfileConfigLike) -> bool:
        assert isinstance(config, MyPluginProfile)
        return ping(config.endpoint)
```

Do **not** annotate the method parameter directly as `config: MyPluginProfile` when the class is meant to satisfy `Source`. Method parameters are checked contravariantly: a `Source` promises it can accept any `ProfileConfigLike`, while that narrower signature promises only `MyPluginProfile`, so static type checkers correctly reject it as a structural `Source`. Accept the Protocol and narrow immediately inside the body. Apply the same rule to `extract()`, `test_connection()`, and `extract_incremental()`.

The first-line assertion also fails close to the connector boundary if registry wiring accidentally pairs the source with the wrong profile class. Every built-in source follows this convention.

---

## Step 3: Loading and Registry Wiring

For an in-tree connector, add matching `load_profile()` and `save_profile()` branches in `drt/config/credentials.py`. Preserve connector-specific required-field checks and defaults in those branches, and add `AcmeProfile` to the module's imports and `__all__` re-exports.

Then update `_register_all_connectors()` in `drt/connectors/registry.py`:

```python
from drt.config.credentials import AcmeProfile
from drt.sources.acme import AcmeSource

# Alongside the other built-in registrations:
register_source("acme", AcmeProfile, AcmeSource)
```

The CLI, MCP server, and integrations all resolve sources through this registry; there is no separate `isinstance` branch to add in `drt/cli/main.py`. If the connector is offered by `drt init`, add it to the init wizard separately.

### Out-of-tree registration

A plugin registers its own profile and source classes from a zero-argument entry-point callable:

```python
# my_acme_plugin/__init__.py
def register() -> None:
    from drt.connectors.registry import register_source
    from .profile import MyPluginProfile
    from .source import MyPluginSource

    register_source("acme_plugin", MyPluginProfile, MyPluginSource)
```

```toml
[project.entry-points."drt.sources"]
acme_plugin = "my_acme_plugin:register"
```

The profile dataclass must accept the YAML `type=` keyword and provide `describe()`. The rest of its constructor fields should match the keys operators write under that named profile.

---

## Step 4: Tests

Create `tests/unit/test_acme_source.py` and cover at least:

- successful extraction and streaming iteration;
- authentication, HTTP, and malformed-response failures;
- `test_connection()` success and failure;
- profile defaults, required fields, load/save symmetry, and secret handling;
- registry resolution and `isinstance(AcmeSource(), Source)`;
- incremental cursor handling, if implemented;
- forwarding or safely ignoring `query_tags`.

Use mocked clients or the repository's HTTP test server; unit tests must not contact the real service. Add the source to the existing registry/profile consistency assertions, then run:

```bash
ruff check drt tests
mypy drt
pytest tests/unit/ -q
```

Finally add `docs/connectors/acme.md`, declare any optional dependency extra in `pyproject.toml`, and include the connector in the README source table.
