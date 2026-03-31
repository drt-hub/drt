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

- `Source.extract(query: str, config: ProfileConfig) -> Iterator[dict]`
- `Destination.load(records: list[dict], config: DestinationConfig, sync_options: SyncOptions) -> SyncResult`
- `StateManager.get_last_sync / save_sync`

These interfaces are stable. Implementations must match the Protocol signature exactly (use union types `ProfileConfig` / `DestinationConfig`, then `assert isinstance()` to narrow).

**Type safety rules:**
- `type: ignore` is prohibited except for external library stub issues (`no-untyped-call`). CI enforces this.
- Protocol signatures may be improved for type safety via PR (not a breaking change if runtime behavior is unchanged).
- New sources/destinations must pass `mypy --strict` without `type: ignore`.

## Development Commands

```bash
make dev      # install with dev + bigquery extras
make test     # pytest
make lint     # ruff + mypy
make fmt      # ruff format + fix
```

## Current Status

- **v0.3.4 released** — Redshift source connector (`drt-core[redshift]`)
- CLI fully wired: `init`, `run`, `list`, `validate`, `status`, `mcp run`
- Sources: BigQuery, DuckDB, PostgreSQL, Redshift
- Destinations: REST API, Slack, GitHub Actions, HubSpot
- MCP Server: `drt mcp run` via `drt-core[mcp]` (FastMCP)
- 84 tests, integration tests use `pytest-httpserver` (no real HTTP mocking)

## What NOT to do

- Do not add a GUI or web UI — this is a CLI-first tool
- Do not add RBAC or multi-tenancy — small team / personal use
- Do not add `type: ignore` — CI will reject it (except `no-untyped-call` for external libraries)
- Do not add heavy dependencies to core — extras (`[bigquery]`, `[mcp]`) exist for a reason

## Roadmap Reference

See the roadmap table in README.md. The short version:
- v0.1 ✅: BigQuery → REST API working end-to-end
- v0.2 ✅: Incremental sync + retry from config
- v0.3 ✅: MCP Server + AI Skills for Claude Code + LLM-readable docs + row-level errors + security hardening + Redshift source
- v0.4: Dagster integration + Google Sheets destination + dbt post-hook + examples
- v0.5: Snowflake source + CSV/JSON destination + test coverage
- v0.6: Salesforce destination + Airflow integration
- v1.x: Rust engine via PyO3
