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

Implementations use `assert isinstance(config, SpecificConfig)` for type narrowing. `type: ignore` is only allowed for external library issues.

## Development Commands

```bash
make dev      # install with dev + bigquery extras
make test     # pytest
make lint     # ruff + mypy
make fmt      # ruff format + fix
```

## Current Status

- **v0.4.3 released** — ClickHouse source, Discord CLI fix, SQLite in init wizard, README.ja.md (community contributions)
- CLI fully wired: `init`, `run`, `list`, `validate`, `status`, `mcp run`
- Sources: BigQuery, DuckDB, PostgreSQL, Redshift, SQLite, ClickHouse
- Destinations: REST API, Slack, Discord, GitHub Actions, HubSpot, Google Sheets, PostgreSQL, MySQL
- Integrations: MCP Server (`drt-core[mcp]`), dagster-drt, dbt manifest reader
- 170+ tests, integration tests use `pytest-httpserver`

## What NOT to do

- Do not add a GUI or web UI — this is a CLI-first tool
- Do not add RBAC or multi-tenancy — small team / personal use
- Do not add `type: ignore` — only allowed for external library issues (`no-untyped-call`, `import-untyped`)
- Do not add heavy dependencies to core — extras (`[bigquery]`, `[mcp]`) exist for a reason

## Roadmap Reference

**SSoT: [GitHub Milestones](https://github.com/drt-hub/drt/milestones)** — all issues are tracked there.

- v0.1 ✅: BigQuery → REST API working end-to-end
- v0.2 ✅: Incremental sync + retry from config
- v0.3 ✅: MCP Server + AI Skills for Claude Code + LLM-readable docs + row-level errors + security hardening + Redshift source
- v0.4 ✅: Google Sheets / PostgreSQL / MySQL destinations + dagster-drt + dbt manifest reader + type safety overhaul
- [v0.5](https://github.com/drt-hub/drt/milestone/2): Snowflake source + CSV/JSON + Parquet destinations + test coverage + Docker
- [v0.6](https://github.com/drt-hub/drt/milestone/3): Salesforce + Airflow integration + Jira / Twilio / Intercom destinations
- [v0.7](https://github.com/drt-hub/drt/milestone/4): DWH destinations (Snowflake / BigQuery / ClickHouse / Databricks) + Cloud storage (S3 / GCS / Azure Blob)
- [v0.8](https://github.com/drt-hub/drt/milestone/5): Lakehouse sources (Delta Lake / Apache Iceberg)
- v1.x: Rust engine via PyO3

**Good First Issues:** https://github.com/drt-hub/drt/issues?q=is%3Aopen+label%3A%22good+first+issue%22
