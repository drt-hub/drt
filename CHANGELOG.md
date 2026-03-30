# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-03-30

### Added

#### MCP Server
- `drt mcp run` — start a FastMCP server (stdio transport) for Claude Desktop, Cursor, and any MCP-compatible client
- 5 MCP tools: `drt_list_syncs`, `drt_run_sync`, `drt_get_status`, `drt_validate`, `drt_get_schema`
- Install: `pip install drt-core[mcp]`

#### AI Skills for Claude Code
- `.claude/commands/drt-create-sync.md` — `/drt-create-sync` skill: generate sync YAML from user intent
- `.claude/commands/drt-debug.md` — `/drt-debug` skill: diagnose and fix failing syncs
- `.claude/commands/drt-init.md` — `/drt-init` skill: guide through project initialization
- `.claude/commands/drt-migrate.md` — `/drt-migrate` skill: migrate from Census/Hightouch to drt

#### LLM-readable Docs
- `docs/llm/CONTEXT.md` — architecture, key concepts, state file format (optimized for LLM consumption)
- `docs/llm/API_REFERENCE.md` — all config fields with types, defaults, and full YAML examples

#### Row-level Error Details
- `RowError` dataclass: `batch_index`, `record_preview` (200-char PII-safe), `http_status`, `error_message`, `timestamp`
- `drt run --verbose` and `drt status --verbose` show per-row error details
- `RestApiDestination` now populates `row_errors` on each failure

### Tests
- 82 tests total (up from 53 in v0.2)
- MCP server tests auto-skip when `fastmcp` not installed

## [0.2.0] - 2026-03-30

### Added

#### Incremental Sync
- `sync.mode: incremental` — watermark-based incremental sync using a `cursor_field`
- Saves `last_cursor_value` in `.drt/state.json` after each run
- Injects `WHERE {cursor_field} > '{last_cursor_value}'` automatically on next run
- Works with both `ref('table')` and raw SQL models

#### Retry Configuration
- `sync.retry` is now fully configurable per-sync in YAML (`max_attempts`, `initial_backoff`, `backoff_multiplier`, `max_backoff`, `retryable_status_codes`)
- Previously used a hardcoded default; now reads from `SyncOptions.retry`

### Fixed
- Removed duplicate `RetryConfig` dataclass from `destinations/retry.py` (was shadowing the Pydantic model in `config/models.py`)

### Tests
- 6 new unit tests for incremental sync (resolver + engine)
- Integration test suite cleaned up: removed monkey-patching of internal `_DEFAULT_RETRY`

## [0.1.1] - 2026-03-29

### Fixed

- `drt --version` now correctly displays the installed package version (e.g. `0.1.1`) instead of the stale hardcoded value `0.1.0.dev0`. Version is now read dynamically via `importlib.metadata`.

## [0.1.0] - 2026-03-28

### Added

#### CLI
- `drt init` — interactive project wizard (supports BigQuery, DuckDB, PostgreSQL)
- `drt run` — run all syncs or a specific sync (`--select`)
- `drt run --dry-run` — preview without writing data
- `drt list` — list sync definitions
- `drt validate` — validate sync YAML configs
- `drt status` — show recent sync run results

#### Sources
- BigQuery (`pip install drt-core[bigquery]`)
- DuckDB (`pip install drt-core[duckdb]`)
- PostgreSQL (`pip install drt-core[postgres]`)

#### Destinations
- REST API (core) — generic HTTP with Jinja2 body templates, auth, rate limiting, retry
- Slack Incoming Webhook (core)
- GitHub Actions `workflow_dispatch` trigger (core)
- HubSpot Contacts / Deals / Companies upsert (core)

#### Configuration
- `profiles.yml` credential management (dbt-style, stored in `~/.drt/`)
- Declarative sync YAML with Jinja2 templating
- Auth: Bearer token, API key, Basic auth
- Rate limiting and exponential backoff retry
- `on_error: skip | fail` per sync
