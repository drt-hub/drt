[English](./CONTRIBUTING.md) | [日本語](./CONTRIBUTING.ja.md)

# Contributing to drt

Thank you for your interest in contributing!

> **Want to grow with the project?** drt has a [contributor ladder](./GOVERNANCE.md#roles): every merged PR builds toward becoming a Triage Collaborator, and from there to Owner. Roles are earned by sustained, quality contribution — and the criteria are public.

## Development Setup

### Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

### Clone and Install

```bash
git clone https://github.com/drt-hub/drt.git
cd drt
```

**With uv (recommended):**

```bash
uv sync --extra dev --extra bigquery
```

### Pre-commit hooks

You can optionally use [pre-commit](https://pre-commit.com/) to run ruff and mypy before each commit. This is optional — you can also use `make lint` and `make fmt` directly.

```bash
uv pip install pre-commit
pre-commit install
```

To run on all files manually:

```bash
pre-commit run --all-files
```

**With pip:**

```bash
pip install -e ".[dev,bigquery]"
```

Or use the Makefile shortcut:

```bash
make dev
```

## Running Tests

```bash
make test       # run all tests (pytest)
make lint       # ruff + mypy
make fmt        # auto-format (ruff format + fix)
```

You can also run commands directly:

```bash
uv run pytest
uv run ruff check drt tests
uv run mypy drt
```

## Branch Naming Convention

| Prefix   | When to use                                 |
| -------- | ------------------------------------------- |
| `feat/`  | New features or connectors                  |
| `fix/`   | Bug fixes                                   |
| `docs/`  | Documentation changes                       |
| `chore/` | Maintenance, dependency updates, CI changes |

Example: `feat/snowflake-source`, `fix/empty-batch-rest-api`, `docs/quickstart-update`

## Branch Strategy

drt uses **GitHub Flow** — all development happens on feature branches that merge directly into `main`.

- `main` is always in a releasable state
- No `develop` or `release` branches
- Releases are marked with tags (`v0.2.0`, `v0.3.0`, …)

## Commit Signing (Required)

The `main` branch requires **signed commits** to protect against supply chain attacks. All PRs are merged with **Squash & merge**, and GitHub automatically signs the squash commit — so **you don't need to set up signing just to contribute**.

However, if you push directly to protected branches or want your commits to show the "Verified" badge, set up SSH signing:

```bash
# Use your existing SSH key (or generate one with: ssh-keygen -t ed25519)
git config --global gpg.format ssh
git config --global user.signingkey ~/.ssh/id_ed25519.pub
git config --global commit.gpgsign true
```

Then add the same key as a **Signing Key** on [GitHub SSH settings](https://github.com/settings/keys).

## Submitting Changes

1. Fork the repository
2. Create a branch following the naming convention above: `git checkout -b feat/your-feature`
3. Make your changes with tests
4. Run `make lint` and `make test` to verify everything passes
5. Open a Pull Request and fill out the PR template

> **Merge strategy:** All PRs are merged with **Squash & merge**. Your branch commits are squashed into a single commit on `main`, so individual WIP commits don't need to be cleaned up. GitHub signs the squash commit automatically.

## Picking up an Issue

drt uses a lightweight **soft assignment** model so contributors don't step on each other:

- Comment on the issue saying you'd like to work on it. A maintainer will assign it to you.
- **Limit: 1–2 issues at a time per contributor.** Finish (or open a draft PR) before grabbing more.
- **Stale rule: 14 days of no progress → unassigned.** A friendly nudge will come first. Pick it back up anytime by commenting again.
- **Larger features** (new integrations, extensions, big refactors): post a short design comment first and wait for maintainer feedback before implementing — saves rework.
- **Small/quick issues** (typos, single-line fixes): no need to ask — open the PR directly. First PR wins.

This is to keep momentum, not to gatekeep. If you're unsure, just ask in the issue.

## Pull Request Checklist

- [ ] Tests pass (`make test`)
- [ ] Linter passes (`make lint`)
- [ ] `CHANGELOG.md` updated (if user-facing change)
- [ ] New connectors include tests under `tests/` and an example under `examples/`

## Commit Style

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add Snowflake source
fix: handle empty batch in REST API destination
docs: update quickstart example
chore: bump dependencies
```

## Contributor Recognition

We recognize all types of contributions using the [all-contributors](https://allcontributors.org/) specification.

**How to get recognized:**

If you contribute code, documentation, organize discussions, or help in any way, you can be added to the contributors list by commenting on your Issue or PR:

```
@all-contributors please add @username for <contribution-type>
```

**Contribution types include:**
- `code` — Pull requests with code changes
- `doc` — Documentation, blog posts, tutorials
- `review` — Code reviews and feedback
- `ideas` — Feature suggestions and design discussions
- `bug` — Bug reports and issue triage
- `test` — Test creation and improvements
- `maintenance` — Maintenance and DevOps

See the [emoji key](https://allcontributors.org/docs/en/emoji-key) for the complete list of 33+ contribution types.

**Note on bot PRs:** The all-contributors bot will automatically open a Pull Request to update the README contributors list. These PRs are safe to merge once CI passes and the visual grid layout is verified — no full review needed. Just verify the names and avatars look correct!

All contributors are listed in the [README Contributors section](./README.md#contributors-) with an all-contributors badge tracking the total count.

## Your First Connector — Step-by-Step Tutorial

This walkthrough builds a complete destination connector from scratch. We'll create a simple **Console** destination that prints records to stdout — a useful debugging tool and a template for real connectors.

By the end you'll have touched the core files needed to add a new connector. Before opening a PR, also check the checklist above for any additional required updates, such as adding an example under `examples/`.

### Step 1: Define the config model

Open `drt/config/models.py`. Add your config class after the existing destinations:

```python
class ConsoleDestinationConfig(BaseModel):
    type: Literal["console"]
    pretty: bool = True  # pretty-print JSON output
```

Then add it to the `DestinationConfig` union:

```python
DestinationConfig = Annotated[
    RestApiDestinationConfig
    | SlackDestinationConfig
    # ... existing destinations ...
    | ConsoleDestinationConfig,  # <-- add here
    Field(discriminator="type"),
]
```

**Key rules:**

- `type` must be a `Literal` — this is how Pydantic discriminates between destination types
- Use `_env` suffix fields for secrets (e.g. `api_key_env: str | None = None`)
- Add a `@model_validator` if fields depend on each other (see `PostgresDestinationConfig` for an example)

### Step 2: Implement the destination

Create `drt/destinations/console.py`:

```python
"""Console destination — prints records to stdout for debugging.

No extra dependencies required.

Example sync YAML:

    destination:
      type: console
      pretty: true
"""

from __future__ import annotations

import json
from typing import Any

from drt.config.models import ConsoleDestinationConfig, DestinationConfig, SyncOptions
from drt.destinations.base import SyncResult


class ConsoleDestination:
    """Print records to stdout."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, ConsoleDestinationConfig)
        if not records:
            return SyncResult()

        result = SyncResult()
        indent = 2 if config.pretty else None

        for record in records:
            try:
                print(json.dumps(record, indent=indent, default=str))
                result.success += 1
            except Exception as e:
                result.failed += 1
                result.errors.append(str(e))

        return result
```

**Key rules:**

- The class does **not** inherit from anything — it implements the `Destination` Protocol
- `load()` must accept `records`, `config`, and `sync_options` with exact signatures
- Use `assert isinstance(config, YourConfig)` for type narrowing
- Return `SyncResult` — track `success` and `failed`, use `errors` for general/batch-level failures, and use `row_errors` only when you have row-level context to report
- Handle empty records as an early return
- Use `try/finally` to close connections (see `PostgresDestination` for the pattern)

### Step 3: Register in the CLI

Open `drt/cli/main.py`. Three changes:

**a) Add to `TYPE_CHECKING` imports (top of file):**

```python
if TYPE_CHECKING:
    from drt.destinations.console import ConsoleDestination
    # ... existing imports ...
```

**b) Add to `_get_destination()` return type:**

```python
def _get_destination(sync: SyncConfig) -> (
    RestApiDestination
    # ... existing types ...
    | ConsoleDestination  # <-- add here
):
```

**c) Add the isinstance check inside `_get_destination()`:**

```python
    if isinstance(dest, ConsoleDestinationConfig):
        from drt.destinations.console import ConsoleDestination
        return ConsoleDestination()
```

Also add `ConsoleDestinationConfig` to the lazy imports inside the function body.

**Why lazy imports?** Destinations may have heavy optional dependencies (e.g. `clickhouse-connect`). Importing them at module level would force every user to install every extra. Lazy imports inside the function keep startup fast.

### Step 4: Write tests

Create `tests/unit/test_console_destination.py`:

```python
"""Unit tests for Console destination."""

from __future__ import annotations

from typing import Any

from drt.config.models import ConsoleDestinationConfig, SyncOptions
from drt.destinations.console import ConsoleDestination


def _options(**kwargs: Any) -> SyncOptions:
    return SyncOptions(**kwargs)


def _config(**overrides: Any) -> ConsoleDestinationConfig:
    defaults: dict[str, Any] = {"type": "console", "pretty": True}
    defaults.update(overrides)
    return ConsoleDestinationConfig(**defaults)


class TestConsoleDestinationConfig:
    def test_valid_config(self) -> None:
        config = _config()
        assert config.type == "console"
        assert config.pretty is True

    def test_pretty_false(self) -> None:
        config = _config(pretty=False)
        assert config.pretty is False


class TestConsoleDestinationLoad:
    def test_success(self, capsys) -> None:
        records = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
        result = ConsoleDestination().load(records, _config(), _options())
        assert result.success == 2
        assert result.failed == 0
        output = capsys.readouterr().out
        assert "alice" in output

    def test_empty_records(self) -> None:
        result = ConsoleDestination().load([], _config(), _options())
        assert result.success == 0
        assert result.failed == 0

    def test_error_handling(self, monkeypatch) -> None:
        # Force json.dumps to fail
        import json

        def _raise(*a, **kw):
            raise ValueError("boom")

        monkeypatch.setattr(json, "dumps", _raise)
        result = ConsoleDestination().load([{"id": 1}], _config(), _options())
        assert result.failed == 1
        assert "boom" in result.errors[0]
```

**Test patterns to follow:**

- `_config()` helper with defaults — easy to override per test
- `_options()` wraps `SyncOptions` with kwargs
- Test config validation, success path, empty records, and error handling
- For HTTP destinations, use `pytest-httpserver` (see `test_teams_destination.py`)
- For DB destinations, mock the connection (see `test_postgres_destination.py`)

### Step 5: Add to pyproject.toml (if needed)

If your destination needs an extra dependency:

```toml
[project.optional-dependencies]
clickhouse = ["clickhouse-connect>=0.7.0"]  # example with a real dependency
```

Users install with: `pip install drt-core[clickhouse]`

If your destination uses only stdlib or core dependencies, skip this step.

### Step 6: Update CHANGELOG.md

Add an entry under `[Unreleased] > Added`:

```markdown
- **Console destination** (#NNN): Print sync records to stdout for debugging. No extra dependencies. N unit tests.
```

### Step 7: Submit your PR

```bash
git checkout -b feat/console-destination
git add drt/destinations/console.py drt/config/models.py drt/cli/main.py \
        tests/unit/test_console_destination.py CHANGELOG.md
git commit -m "feat: add Console destination connector" -m "Closes #NNN"
make lint   # ruff + mypy must pass
make test   # all tests must pass
git push origin feat/console-destination
```

Open a PR with:

- What it does (1-2 sentences)
- `Closes #NNN`
- Checklist items checked

### Quick reference — files to touch

```
New connector checklist:
  drt/config/models.py           → add YourDestinationConfig + add to union
  drt/destinations/your_dest.py  → implement load() method
  drt/cli/main.py                → TYPE_CHECKING import + return type + isinstance check
  tests/unit/test_your_dest.py   → config + load tests
  pyproject.toml                 → add [your_dest] extra (if external deps)
  CHANGELOG.md                   → document under [Unreleased]
```

Do **not** change the `Source` or `Destination` protocol signatures without prior discussion — these are stable interfaces designed for a future Rust rewrite.

## Code of Conduct

Be kind, be constructive. We follow the [Contributor Covenant](https://www.contributor-covenant.org/).

## Updating AI Skills

drt ships Claude Code skills via the plugin marketplace (`skills/drt/`). When you update skill content, users only receive the update if the plugin version is bumped.

**Rule: bump the version in all three places whenever any `SKILL.md` changes:**

```bash
# 1. skills/drt/.claude-plugin/plugin.json
# 2. .claude-plugin/marketplace.json  (plugin entry version)
# 3. .claude-plugin/plugin.json       (repo-level version)
```

Keep the version in sync with `pyproject.toml` (e.g. if releasing `0.4.0`, set all plugin versions to `0.4.0`).

If you add a **new skill**, also add an entry to `skills/drt/.claude-plugin/plugin.json` if needed, and document it in `README.md` and `docs/llm/CONTEXT.md`.

## Contributor Recognition

We recognize all types of contributions — code, documentation, design, ideas, reviews, and more — using the [all-contributors](https://allcontributors.org/) specification.

**How to get recognized:**

If you contribute code, documentation, organize discussions, or help in any way, you can be added to the contributors list by commenting on your Issue or PR:

```
@all-contributors please add @username for <contribution-type>
```

**Contribution types include:**

- `code` — Pull requests with code changes
- `doc` — Documentation, blog posts, tutorials
- `review` — Code reviews and feedback
- `ideas` — Feature suggestions and design discussions
- `bug` — Bug reports and issue triage
- `test` — Test creation and improvements
- `maintenance` — Maintenance and DevOps

See the [emoji key](https://allcontributors.org/docs/en/emoji-key) for the complete list of 33+ contribution types.

**Note on bot PRs:** The all-contributors bot will automatically open a Pull Request to update the README contributors list. These PRs are safe to merge once CI passes and the visual grid layout is verified — no full review needed. Just verify the names and avatars look correct!

All contributors are listed in the [README Contributors section](./README.md#contributors-) with an all-contributors badge tracking the total count.
