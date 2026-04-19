# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## dagster-drt

> `dagster-drt` is published as a separate PyPI package with its own version.

### [0.2.0] - 2026-04-04 (dagster-drt)

- **API alignment with dagster-dbt / dagster-dlt patterns** (#176, #177, #178, #179)
- **`@drt_assets` decorator** — wraps `@multi_asset(can_subset=True)`, replacing the old `drt_assets()` function
- **`build_drt_asset_specs()`** — pure spec generation, decoupled from execution. Enables Dagster Pipes remote execution (#175)
- **`DagsterDrtResource`** — `ConfigurableResource` with `.run()` yielding `MaterializeResult` per sync. Auto-resolves `project_dir` from `@drt_assets` metadata
- **`DagsterDrtTranslator.get_asset_spec()`** — new primary override point (per-attribute methods deprecated)
- **`partitions_def`, `backfill_policy`, `pool`** support on `@drt_assets`
- **Asset `kinds`** metadata — `{"drt", "<destination_type>"}` visible in Dagster UI
- **Group name conflict detection** — raises if translator and decorator both set group names
- **Subset execution** — `DagsterDrtResource.run()` respects `context.selected_asset_keys`
- Old `drt_assets()` function renamed to `drt_assets_legacy()` with deprecation warning
- Requires `drt-core>=0.4.1`, `dagster>=1.6`

### [0.1.0] - 2026-04-01 (dagster-drt)

- First PyPI release (`pip install dagster-drt`)
- **DagsterDrtTranslator** (#127): Customise asset keys, group names, deps, and metadata (follows dagster-dbt pattern)
- **DrtConfig with dry_run** (#126): RunConfig controllable from Dagster UI, plus `drt_assets(dry_run=True)` for build-time defaults
- **MaterializeResult** (#128): Assets return structured metadata (rows_synced, rows_failed, rows_skipped, dry_run, row_errors_count)
- Requires `drt-core>=0.4.1`

---

## drt-core

## [Unreleased]

### Added

- **`drt sources` and `drt destinations` CLI commands** (#223): List available source and destination connectors with descriptions. Rich table formatting for clean terminal output. Foundation for future auto-discovery features.
- **SQL Server source connector** (#91): Extract data from Microsoft SQL Server using pure-Python `pymssql`. Supports host, port, database, user, password_env, schema. Install: `pip install drt-core[sqlserver]`.
- **Databricks source connector** (#88): Extract data from Databricks SQL Warehouse using `databricks-sql-connector`. Supports Unity Catalog, access token auth. Install: `pip install drt-core[databricks]`.
- **Webhook trigger endpoint** (#218): New `drt serve` command starts a lightweight HTTP server (stdlib `http.server`) so you can trigger syncs via `POST /sync/<name>`. Includes health check, bearer token auth, and single-sync concurrency control (423 on parallel requests). Guide: `docs/guides/using-webhook-trigger.md`.
- **Prefect integration** (#213): Built-in `run_drt_sync()` helper and `drt_sync_task` for Prefect 2.x/3.x. No extra package needed — included in drt-core. Shares the runner with Airflow integration via `drt.integrations._runner`.
- **Airflow integration** (#70): Built-in `run_drt_sync()` helper and `DrtRunOperator` for Apache Airflow. No extra package needed — included in drt-core.
- **Google Ads destination** (#217): Upload offline click conversions. Supports partial failure handling and OAuth2 auth.
- **Staged Upload destination** (#258): Async bulk-upload APIs (e.g. Amazon Marketing Cloud, Salesforce Bulk API). Declarative 3-phase YAML config: Stage (file upload) → Trigger (job kick) → Poll (completion wait). Supports CSV, JSON, JSONL. New `StagedDestination` Protocol.
- **OAuth2 Client Credentials auth** (#259): Token exchange with caching for REST API destination.
- **`drt init --from-dbt`** (#215): Generate sync YAML scaffolds from dbt `manifest.json`.
- **`--output json` for validate/list** (#230): Structured JSON output for `drt validate` and `drt list`.
- **MCP Server: `drt_list_connectors`** (#262): New tool listing all available sources and destinations.
- **MCP Server: improved `drt_validate`** (#262): Per-file error reporting via `load_syncs_safe()`.
- **BigQuery → Discord example** (#266): Alert pipeline that queries BigQuery for recent error rows and posts a Discord notification per row via Incoming Webhook using incremental sync. Includes `examples/bigquery_to_discord/`.
- **Notion destination** (#38): Append rows to a Notion database via the Notion API. Supports `properties_template` (Jinja2 → JSON for page properties), `rich_text` fallback for records without a template, retry with backoff, rate limiting (3 req/s). Contributed by @armorbreak001, with credit to @PFCAaron12 and @pureqin.
- **Twilio SMS destination** (#159): Send SMS per row via Twilio Messages API. Basic auth (Account SID + Auth Token), Jinja2 templates for message body and per-row phone number (`to_template`), E.164 format validation. Contributed by @PFCAaron12.
- **Intercom destination** (#163): Create/update contacts via Intercom REST API v2. Bearer token auth, Jinja2 `properties_template` for flexible contact attribute mapping. Contributed by @PFCAaron12.
- **`--log-format json` flag** (#275): Structured JSON logging for `drt run`. Emits one JSON object per sync event (start, complete, error) to stderr. Separate from `--output json` (final result). Contributed by @Khush-domadia.
- **GOVERNANCE.md** (#310): Formalized contributor ladder (Contributor → Triage Collaborator → Owner), soft-assignment policy (first-PR-wins), lazy consensus decision model, and 14-day stale rule.

### Fixed

- **MySQL destination**: auto-serialize `dict`/`list` values to JSON strings before passing to pymysql (#311). Also shipped in [0.5.1](#051---2026-04-14).

## [0.5.5] - 2026-04-16

### Added

- **`drop_match_columns`** (#347): Automatically remove lookup match columns from INSERT after FK resolution. Prevents `Unknown column` errors when the destination table doesn't have the match columns. Enabled by default; set `drop_match_columns: false` to opt out.

## [0.5.4] - 2026-04-16

### Added

- **`destination_lookup`** (#345): Resolve foreign key values by querying the destination database during sync. When syncing related tables, child tables can now reference parent table auto-increment IDs without triggers or denormalized schemas. Supports MySQL, PostgreSQL, and ClickHouse destinations. Configure via `lookups` field in destination YAML with `on_miss: skip | fail | null`. Guide: `docs/guides/destination-lookup.md`.

## [0.5.1] - 2026-04-14

### Fixed

- **MySQL destination**: auto-serialize `dict`/`list` values to JSON strings before passing to pymysql (#311). Fixes BigQuery → MySQL reverse ETL where BigQuery JSON columns come back as Python `dict`/`list`. Backward compatible — strings, ints, and other types pass through unchanged.

## [0.5.0] - 2026-04-13

### Added

#### Sources
- **Snowflake source connector** (#162): Extract data from Snowflake using `snowflake-connector-python`. Supports account, user, password/password_env, database, schema, warehouse, and optional role. Install: `pip install drt-core[snowflake]`
- **MySQL source connector** (#19): Extract data from MySQL databases using pymysql. Supports host, port, dbname, user, password via env var. Backtick quoting for table names. Install: `pip install drt-core[mysql]`

#### Destinations
- **ClickHouse destination** (#166): Insert rows via `clickhouse-connect` (HTTP). Supports connection string, HTTPS via `secure` flag. Install: `pip install drt-core[clickhouse]`
- **Parquet file destination** (#171): Write to local Parquet files with snappy/gzip/zstd compression and partition columns. Install: `pip install drt-core[parquet]`
- **CSV/JSON/JSONL file destination** (#67): Write to local files using stdlib csv/json. No extra dependencies
- **Microsoft Teams destination** (#85): Incoming Webhook with plain text and Adaptive Card payloads
- **Jira destination** (#158): Create/update Jira issues via REST API v3 with Jinja2 templates
- **Linear destination** (#195): Create Linear issues via GraphQL API with Jinja2 templates
- **SendGrid email destination** (#194): Transactional emails via SendGrid v3 Mail Send API

#### CLI
- **`drt test` command** (#141): Post-sync data validation. Supports `row_count` (min/max) and `not_null` (columns) tests for DB destinations (PostgreSQL, MySQL, ClickHouse)
- **`--output json` flag** (#142): Structured JSON output for `drt run` and `drt status`. Designed for CI/scripting use
- **`--profile` CLI override** (#238): Runtime profile switching via `--profile` flag or `DRT_PROFILE` env var. Precedence: flag > env var > drt_project.yml
- **Improved `drt validate` errors** (#104): User-friendly error messages with YAML field paths instead of raw Pydantic tracebacks. Shows ✓/✗ per sync file
- **Dry-run summary** (#219): Enhanced `--dry-run` shows Source, Destination, Rows to sync, and Sync mode

#### Multi-environment support
- **`${VAR}` env var substitution** (#240): Use `${VAR}` syntax in `model:` field for environment-specific SQL queries
- **dbt manifest resolution** (#239): `ref('model')` now resolves from dbt `target/manifest.json` when available. Resolution order: SQL file > dbt manifest > profile-based expansion
- **`secrets.toml`** (#143): Local secret management via `.drt/secrets.toml` (dlt-like pattern). Resolution order: explicit value > env var > secrets.toml

#### Infrastructure
- **Dockerfile and docker-compose** (#161): `python:3.12-slim` image with `DRT_EXTRAS` build arg, non-root user
- **Codecov integration** (#103): Coverage badge, PR reports. Patch checks set to informational
- **Pre-commit hooks** (#105): ruff + mypy
- **Python 3.13 support** (#225): Added to CI matrix and classifiers
- **`duration_seconds` in SyncResult** (#226): Track sync execution time

### Tests
- 382+ tests (up from 170+ in v0.4.3)
- Source and destination protocol contract tests (#209, #210)
- Slack Block Kit tests (#97), state persistence tests (#100)
- CLI validate error case tests (#98)
- Codecov coverage at 64%

## [0.4.3] - 2026-04-02

### Added

- **ClickHouse source connector** (#156 by @msarwal345): High-performance source using `clickhouse-connect` (HTTP interface). Supports host, port, database, user, and password/password_env. Includes `examples/clickhouse_to_rest/`.
- **SQLite in `drt init` wizard** (#153): Users can now select `sqlite` as a source type during project initialization
- **README.ja.md** (#150 by @Ami-3110): Japanese translation of README with language toggle
- **Fractional rps test** (#144 by @Pranavv157): Additional RateLimiter test for non-integer request rates

### Fixed

- **Discord destination not wired in CLI** (#152): `type: discord` syncs now work correctly — was raising `ValueError` since v0.4.2
- **API_REFERENCE.md** (#154): Added missing SQLite source and Discord destination config examples
- **drt-init skill** (#155): Updated source type list to include all supported sources (redshift, sqlite)

## [0.4.2] - 2026-04-02

### Added

- **SQLite source connector** (#146 by @PFCAaron12): Zero-dependency source using Python's built-in `sqlite3` — ideal for testing, prototyping, and local development
- **Discord webhook destination** (#147 by @xingzihai): Send messages and rich embeds to Discord channels via webhooks, following the same pattern as the Slack destination

### Improved

- **Redshift unit tests** (#145 by @HoudaBelhad): Replaced fake test double with real `RedshiftSource` + mock-based tests covering connection, query execution, error handling, and protocol compatibility
- **RateLimiter boundary tests** (#125 by @kipra19821810-cloud): Added 11 boundary-value tests covering zero/negative rps, large values, rapid calls, and state management — regression tests for the v0.3.3 ZeroDivisionError fix

### Fixed

- **psycopg2 lazy import**: Moved top-level `from psycopg2 import sql` to method-level import, fixing CI failures in environments without psycopg2 installed

### Community

🎉 This release features contributions from **4 community members** — thank you @PFCAaron12, @xingzihai, @HoudaBelhad, and @kipra19821810-cloud!

## [0.4.1] - 2026-04-01

### Added

- **Upsert sync mode** (#130): `mode: upsert` for explicit intent in YAML — behaves like `mode: full` with `upsert_key`
- **SSL/TLS for DB destinations** (#131): Optional `ssl` config for PostgreSQL and MySQL with `ca_env`, `cert_env`, `key_env`
- **Connection string support** (#132): `connection_string_env` for PostgreSQL and MySQL — alternative to individual host/port/dbname params
- **dagster-drt: DagsterDrtTranslator** (#127): Customise asset keys, group names, deps, and metadata (follows dagster-dbt pattern)
- **dagster-drt: DrtConfig with dry_run** (#126): RunConfig controllable from Dagster UI, plus `drt_assets(dry_run=True)` for build-time defaults
- **dagster-drt: MaterializeResult** (#128): Assets return structured metadata (rows_synced, rows_failed, rows_skipped, dry_run, row_errors_count)

### Fixed

- **MySQL type: ignore** (#133): Replaced `# type: ignore[import-untyped]` with `types-PyMySQL` dev dependency

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
