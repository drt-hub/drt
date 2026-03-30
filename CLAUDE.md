# CLAUDE.md — AI Agent Context for drt

This file gives AI agents (Claude Code, Cursor, etc.) the context needed to work effectively in this codebase.

## What is drt?

**drt** (data reverse tool) is a CLI tool that syncs data from a data warehouse (BigQuery) to external services via declarative YAML configuration. Think of it as the reverse of dlt: `dlt` loads data *into* a DWH; `drt` activates data *out of* a DWH.

**Tagline:** "Reverse ETL for the code-first data stack."

## Architecture

```
Config Parser → Source (BigQuery) → Sync Engine → Destination (REST API)
                                                         ↓
                                                   State Manager
```

Key design principle: **module boundaries are drawn for future Rust rewrite (PyO3)**. The `engine/sync.py` module is the primary Rust candidate — keep it pure (no I/O side effects beyond protocol calls).

## Package Layout

```
drt/
├── cli/          # Typer CLI commands
├── config/       # Pydantic models + YAML parser
├── sources/      # Source Protocol + BigQuery impl
├── destinations/ # Destination Protocol + REST API impl
├── engine/       # Sync orchestration (future Rust core)
├── state/        # Local JSON state persistence
└── templates/    # Jinja2 renderer (future MiniJinja/Rust)
```

## Protocols (critical interfaces)

- `Source.extract(query, config) -> Iterator[dict]`
- `Destination.load(records, config) -> SyncResult`
- `StateManager.get_last_sync / save_sync`

These interfaces are stable. New sources/destinations implement these protocols — do not change the signatures.

## Development Commands

```bash
make dev      # install with dev + bigquery extras
make test     # pytest
make lint     # ruff + mypy
make fmt      # ruff format + fix
```

## Current Status

- **v0.2 released** — Incremental sync, retry config, 53 tests passing
- CLI fully wired: `init`, `run`, `list`, `validate`, `status`
- Sources: BigQuery, DuckDB, PostgreSQL
- Destinations: REST API, Slack, GitHub Actions, HubSpot
- Integration tests use `pytest-httpserver` (no real HTTP mocking)

## What NOT to do

- Do not add a GUI or web UI — this is a CLI-first tool
- Do not add RBAC or multi-tenancy — small team / personal use
- Do not change Source/Destination protocol signatures without discussion
- Do not add heavy dependencies to core — extras (`[bigquery]`, `[mcp]`) exist for a reason

## Roadmap Reference

See the roadmap table in README.md. The short version:
- v0.1 ✅: BigQuery → REST API working end-to-end
- v0.2 ✅: Incremental sync + retry from config
- v0.3: MCP Server (`uvx drt mcp run`) + AI Skills for Claude Code + LLM-readable docs
- v1.x: Rust engine via PyO3
