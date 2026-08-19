# CLAUDE.md -- AI Agent Context for drt

This file gives AI agents (Claude Code, Cursor, etc.) the context needed to work effectively in this codebase.

## What is drt?

**drt** (data reverse tool) is a CLI tool that syncs data from a data warehouse (BigQuery) to external services via declarative YAML configuration. Think of it as the reverse of dlt: `dlt` loads data *into* a DWH; `drt` activates data *out of* a DWH.

**Tagline:** "Reverse ETL for the code-first data stack."

## Architecture

```
Config Parser -> Source (BigQuery) -> Sync Engine -> Destination (REST API)
                                                         \
                                                       State Manager
```

Key design principle: **module boundaries are drawn for future Rust rewrite (PyO3)**. The `engine/sync.py` module is the primary Rust candidate -- keep it pure (no I/O side effects beyond protocol calls). Logging, state persistence, OTel spans, and any other observability/persistence side effect MUST flow through `drt.engine.observer.SyncObserver`. Direct `logger.*`, `state_manager.save_sync(...)`, or `watermark_storage.save(...)` calls inside `engine/sync.py` are guarded by `tests/unit/test_engine_observer.py` boundary checks and will fail CI.

## Package Layout

```
drt/
  cli/          # Typer CLI commands
  config/       # Pydantic models + YAML parser
  connectors/   # Connector registry -- auto-discovery of sources/destinations
  sources/      # Source Protocol + BigQuery impl
  destinations/ # Destination Protocol + REST API impl
  engine/       # Sync orchestration (future Rust core)
  state/        # Local JSON state persistence
  templates/    # Jinja2 renderer (future MiniJinja/Rust)
```

## Protocols (critical interfaces)

- `Source.extract(query: str, config: ProfileConfig) -> Iterator[dict]`
- `Destination.load(records: list[dict], config: DestinationConfig, sync_options: SyncOptions) -> SyncResult`
- `StateManager.get_last_sync / save_sync`

Connector dispatch uses a centralized registry (`drt/connectors/registry.py`) -- adding a new connector requires registering it there, not editing `main.py`. Implementations use `assert isinstance(config, SpecificConfig)` for type narrowing. `type: ignore` is only allowed for external library issues.

## Development Commands

```bash
make dev      # install with dev + bigquery extras
make test     # pytest
make lint     # ruff + mypy
make fmt      # ruff format + fix
```

`local_sql_smoke` covers real Postgres/MySQL dialect behaviour with ephemeral
testcontainers and runs in the normal PR test job; it skips when Docker is not
available. ClickHouse and other local dialects are outside this initial scope.
Cloud-warehouse tests remain under `dwh_smoke` and require `DRT_SMOKE_*` secrets.

## Current Status

- **v0.9.0** (latest) -- state-location backends (GCS/S3), `run_id` correlation, `drt serve` delivery contract, secret provider URIs, tracked+scoped mirror on all SQL destinations. No breaking changes, drop-in upgrade from v0.8.5. (An accidental repo-wide `make fmt` sweep from #909 was reverted in #910 -- do not run `make fmt`.)
- **Shipped releases:** see [CHANGELOG.md](CHANGELOG.md) or [GitHub Releases](https://github.com/drt-hub/drt/releases)

### Connector Inventory

- **Sources:** BigQuery, DuckDB, PostgreSQL, Redshift, SQLite, ClickHouse, Snowflake, MySQL, Databricks, SQL Server, Delta Lake, Apache Iceberg
- **Destinations:** REST API, Slack, Discord, Microsoft Teams, GitHub Actions, HubSpot, Google Sheets, PostgreSQL, MySQL, ClickHouse, Snowflake, Parquet, CSV/JSON/JSONL, Jira, Linear, SendGrid, Notion, Twilio, Intercom, Email SMTP, Salesforce Bulk, Google Ads, Staged Upload, Amplitude, S3, GCS, Azure Blob, BigQuery, Elasticsearch, Databricks Delta, Klaviyo, Airtable, Mixpanel
- **Integrations:** MCP Server (`drt-core[mcp]`), dagster-drt, Airflow, Prefect, dbt manifest reader
- **Tests:** 833+, integration tests use `pytest-httpserver`

## What NOT to do

- Do not add a GUI or web UI -- this is a CLI-first tool
- Do not add RBAC or multi-tenancy -- small team / personal use
- Do not add `type: ignore` -- only allowed for external library issues (`no-untyped-call`, `import-untyped`)
- Do not add heavy dependencies to core -- extras (`[bigquery]`, `[mcp]`) exist for a reason
- Do not run `make fmt` -- an accidental repo-wide sweep from #909 was reverted in #910

## Roadmap Reference

**SSoT for upcoming releases: [ROADMAP.md](ROADMAP.md)** -- each version has Theme / Scope / Out of scope / Target / Progress link.

- **Shipped releases:** see [CHANGELOG.md](CHANGELOG.md) or [GitHub Releases](https://github.com/drt-hub/drt/releases)
- **Issue-level tracking:** [GitHub Milestones](https://github.com/drt-hub/drt/milestones)
- **Good First Issues:** https://github.com/drt-hub/drt/issues?q=is%3Aopen+label%3A%22good+first+issue%22

When scope shifts between versions, update ROADMAP.md first, then re-label issues to match.
