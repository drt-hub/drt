[English](./CONTRIBUTING.md) | [日本語](./CONTRIBUTING.ja.md)

# Contributing to drt

Thank you for your interest in contributing!

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

| Prefix | When to use |
|--------|-------------|
| `feat/` | New features or connectors |
| `fix/` | Bug fixes |
| `docs/` | Documentation changes |
| `chore/` | Maintenance, dependency updates, CI changes |

Example: `feat/snowflake-source`, `fix/empty-batch-rest-api`, `docs/quickstart-update`

## Branch Strategy

drt uses **GitHub Flow** — all development happens on feature branches that merge directly into `main`.

- `main` is always in a releasable state
- No `develop` or `release` branches
- Releases are marked with tags (`v0.2.0`, `v0.3.0`, …)

## Submitting Changes

1. Fork the repository
2. Create a branch following the naming convention above: `git checkout -b feat/your-feature`
3. Make your changes with tests
4. Run `make lint` and `make test` to verify everything passes
5. Open a Pull Request and fill out the PR template

> **Merge strategy:** All PRs are merged with **Squash & merge**. Your branch commits are squashed into a single commit on `main`, so individual WIP commits don't need to be cleaned up.

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

## Adding a Connector

See `drt/sources/base.py` and `drt/destinations/base.py` for the Protocol interfaces.
Implement the protocol, add tests under `tests/`, and add an example under `examples/`.

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
