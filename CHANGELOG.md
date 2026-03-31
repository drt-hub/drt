# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-03-31

### Added

- **Google Sheets destination** (#64): Overwrite or append mode. Service account or ADC auth. Install: `pip install drt-core[sheets]`
- **PostgreSQL destination** (#81): Upsert via `INSERT ... ON CONFLICT DO UPDATE`. Row-level error handling
- **MySQL destination** (#83): Upsert via `INSERT ... ON DUPLICATE KEY UPDATE`. Row-level error handling
- **dagster-drt integration** (#63): `integrations/dagster-drt/` package. Expose drt syncs as Dagster assets with `drt_assets()`
- **dbt manifest reader** (#65): `drt.integrations.dbt.resolve_ref_from_manifest()` resolves `ref()` from `target/manifest.json`
- **Google Sheets example** (#94): `examples/duckdb_to_google_sheets/`
- **dbt usage guide**: `docs/guides/using-with-dbt.md`

### Refactored

- **Type safety overhaul** (#80, #110): Eliminated 13 `type: ignore` annotations. `Destination.load()` config type `object` → `DestinationConfig`. `Source.extract()` config type narrowed to `ProfileConfig`. `DetailedSyncResult` removed in favor of `SyncResult`. All sources/destinations use `assert isinstance()` for type narrowing
- **Redshift lazy import** (#78): `RedshiftSource` no longer crashes when `psycopg2` is not installed

### Tests

- 136 tests total (up from 101 in v0.3.4)

## [0.3.4] - 2026-03-30

### Added

- **Redshift source** (#76, closes #20): Amazon Redshift connector via psycopg2. New `RedshiftProfile` with host/port/dbname/user/password_env/schema fields (port defaults to 5439). `drt init` wizard updated to support `redshift` source type. Install: `pip install drt-core[redshift]`.

## [0.3.3] - 2026-03-30

### Fixed

- **SQL injection** (#42): `cursor_field` now validated as a safe SQL identifier; `last_cursor_value` escaped with standard `''` quoting in incremental WHERE clauses
- **row_errors lost** (#43): `run_sync()` now aggregates `row_errors` across all batches
- **Numeric cursor comparison** (#44): Incremental cursor uses numeric comparison (`float()`) when possible — fixes `"9" > "10"` regression for integer/timestamp cursors
- **HTTP timeout** (#45): `httpx.Client(timeout=30.0)` added to all destinations (REST API, Slack, HubSpot, GitHub Actions) — prevents indefinite hangs
- **BasicAuth empty credentials** (#46): `BasicAuth` now raises `ValueError` when `username_env`/`password_env` are not set (was silently sending empty credentials)
- **Corrupted state.json** (#47): `JSONDecodeError` on corrupted `.drt/state.json` is caught; prints warning to stderr and resets to empty state instead of crashing all syncs
- **Slack retry** (#48): `SlackDestination` now uses `with_retry` — 429 rate limit responses are retried with backoff
- **Incremental cursor_field validation** (#49): `SyncOptions` raises `ValidationError` if `mode: incremental` is set without `cursor_field`
- **RateLimiter ZeroDivisionError** (#50): `RateLimiter.acquire()` returns immediately when `requests_per_second <= 0`

### Added

- **Destination unit tests** (#51): 10 new unit tests for `SlackDestination`, `HubSpotDestination`, `GitHubActionsDestination` (84 tests total)

### Refactored

- **DetailedSyncResult unification** (#52): Slack, HubSpot, and GitHub Actions destinations now use `DetailedSyncResult` + `RowError` — consistent row-level error reporting across all destinations

## [0.3.2] - 2026-03-30

### Fixed

- `load_profile()` が `profiles.yml` の `location` フィールドを `BigQueryProfile` に渡していなかった問題を修正。常にデフォルト `"US"` が使われていた。 ([#58](https://github.com/drt-hub/drt/issues/58), [#59](https://github.com/drt-hub/drt/pull/59))

## [0.3.1] - 2026-03-30

### Fixed

- **BigQuery location**: `profiles.yml` now supports a `location` field (e.g. `"EU"`, `"asia-northeast1"`); passed to `bigquery.Client()` so queries route to the correct regional endpoint. Defaults to `"US"` for backwards compatibility. ([#54](https://github.com/drt-hub/drt/issues/54))
- `drt init` wizard now prompts for dataset location when configuring a BigQuery profile.

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
