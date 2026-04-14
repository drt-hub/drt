# drt — LLM Context

This document is optimized for LLM consumption. It gives you the full context needed to help users configure, debug, and extend drt.

## What is drt?

**drt** (data reverse tool) is a CLI tool that syncs data from a data warehouse to external services, declaratively via YAML.

```
dlt (load into DWH) → dbt (transform) → drt (activate out of DWH)
```

- **Category:** Reverse ETL
- **Tagline:** "Reverse ETL for the code-first data stack"
- **Install:** `pip install drt-core` or `uv add drt-core`
- **Package name:** `drt-core` (PyPI) — CLI command is `drt`
- **Current version:** v0.5.0

## What drt is NOT

- Not a data loader (that's dlt)
- Not a transformer (that's dbt)
- Not a scheduler — it runs via CLI or cron, not a built-in scheduler
- Not a SaaS — fully self-hosted OSS (Apache 2.0)

## Architecture

```
drt_project.yml          # project config (source profile)
syncs/*.yml              # one file per sync definition

CLI (drt run)
  → Config Parser        # parse + validate YAML via Pydantic
  → Source               # extract rows from DWH
  → Engine (sync.py)     # batch, orchestrate, track cursor
  → Destination          # load rows to external service
  → State Manager        # persist last run result to .drt/state.json
```

## Project Structure

```
my-project/
├── drt_project.yml       # required: project name + source profile
├── syncs/
│   ├── notify_slack.yml  # one sync per file
│   └── update_hubspot.yml
└── syncs/models/
    └── active_users.sql  # optional: custom SQL (overrides ref())
```

## Sources (where data comes from)

| Source | Extra | Notes |
|--------|-------|-------|
| BigQuery | `drt-core[bigquery]` | Uses ADC or keyfile. Supports `location` (e.g. `"EU"`, `"asia-northeast1"`) |
| DuckDB | (core) | Local `.duckdb` file |
| SQLite | (core) | Built-in `sqlite3`, no extra dependencies. Local `.sqlite` files or `:memory:` |
| PostgreSQL | `drt-core[postgres]` | Connection string via env |
| Redshift | `drt-core[redshift]` | PostgreSQL wire protocol via psycopg2. Supports `schema` (search_path). Port defaults to 5439. |
| ClickHouse | `drt-core[clickhouse]` | HTTP interface via `clickhouse-connect`. Supports host, port, database, user, password_env. |
| Snowflake | `drt-core[snowflake]` | Supports account, user, password_env, database, schema, warehouse, role |
| MySQL | `drt-core[mysql]` | Uses pymysql. Supports host, port, dbname, user, password_env |
| Databricks | `drt-core[databricks]` | SQL Warehouse via databricks-sql-connector. Supports Unity Catalog, access_token_env |

Source is configured in `~/.drt/profiles.yml` (dbt-style):

```yaml
default:
  type: bigquery
  project: my-gcp-project
  dataset: analytics
  location: US             # optional: "US" (default), "EU", "asia-northeast1", etc.
```

## Destinations (where data goes)

| Destination | `type` value | Notes |
|-------------|-------------|-------|
| REST API (generic) | `rest_api` | Any HTTP endpoint |
| Slack Webhook | `slack` | Incoming webhook, Block Kit support |
| Discord Webhook | `discord` | Plain text or rich embeds via webhook URL |
| Microsoft Teams | `teams` | Incoming Webhook, Adaptive Card support |
| GitHub Actions | `github_actions` | workflow_dispatch trigger |
| HubSpot CRM | `hubspot` | Contacts / Deals / Companies upsert |
| Google Sheets | `google_sheets` | Overwrite or append. Requires `drt-core[sheets]` |
| PostgreSQL (upsert) | `postgres` | INSERT ... ON CONFLICT DO UPDATE. Requires `drt-core[postgres]` |
| MySQL (upsert) | `mysql` | INSERT ... ON DUPLICATE KEY UPDATE. Requires `drt-core[mysql]` |
| ClickHouse | `clickhouse` | HTTP client via clickhouse-connect. Requires `drt-core[clickhouse]` |
| Parquet file | `parquet` | Local Parquet files. Requires `drt-core[parquet]` |
| CSV/JSON/JSONL file | `file` | Local files, no extra dependencies |
| Jira | `jira` | Create/update issues via REST API v3 |
| Linear | `linear` | Create issues via GraphQL API |
| SendGrid | `sendgrid` | Transactional emails via v3 Mail Send API |
| Google Ads | `google_ads` | Offline click conversion upload |

## CLI Commands

```bash
drt init                          # interactive project wizard
drt list                          # list sync definitions
drt validate                      # validate all sync YAMLs
drt run                           # run all syncs
drt run --select <sync-name>      # run one sync
drt run --dry-run                 # preview without writing data
drt run --verbose                 # show row-level error details on failure
drt run --output json             # structured JSON output for CI/scripting
drt run --profile prd             # override profile (or DRT_PROFILE env var)
drt test                          # run post-sync validation tests
drt test --select <sync-name>     # test a specific sync
drt status                        # show recent sync results
drt status --output json          # JSON output for status
drt mcp run                       # start MCP server (requires drt-core[mcp])
```

## MCP Server

drt exposes its operations as MCP tools so LLMs can trigger syncs, check status, and validate configs without a terminal.

```bash
pip install drt-core[mcp]
drt mcp run   # starts stdio MCP server
```

### Available MCP tools

| Tool | Description |
|------|-------------|
| `drt_list_syncs` | Returns all sync definitions (name, model, destination type, mode) |
| `drt_run_sync(sync_name, dry_run=False)` | Runs a sync; returns success/failed counts and errors |
| `drt_get_status(sync_name=None)` | Returns last run result(s); omit sync_name for all |
| `drt_validate()` | Validates all sync YAMLs; returns valid list and errors dict |
| `drt_get_schema(schema_type="sync")` | Returns JSON Schema for "sync" or "project" config |

The MCP server reads from the current working directory (the drt project root).

## Orchestration

drt provides built-in helpers for Airflow and Prefect (no separate package needed), plus a first-class Dagster integration (`dagster-drt` on PyPI).

- **Airflow**: `drt.integrations.airflow` — `run_drt_sync()` + `DrtRunOperator`. See `docs/guides/using-with-airflow.md`.
- **Prefect**: `drt.integrations.prefect` — `run_drt_sync()` + `drt_sync_task`. See `docs/guides/using-with-prefect.md`.
- **Dagster**: `pip install dagster-drt`. See below.

## Orchestration: dagster-drt

Community-maintained Dagster integration. Install: `pip install dagster-drt`

```python
from dagster import AssetExecutionContext, Definitions
from dagster_drt import drt_assets, DagsterDrtResource, DagsterDrtTranslator

# Basic usage — @drt_assets decorator + DagsterDrtResource
@drt_assets(project_dir="path/to/drt-project")
def my_syncs(context: AssetExecutionContext, drt: DagsterDrtResource):
    yield from drt.run(context=context)

defs = Definitions(
    assets=[my_syncs],
    resources={"drt": DagsterDrtResource(project_dir="path/to/drt-project")},
)

# Pipes-based remote execution (Cloud Run, K8s, etc.)
from dagster_drt import build_drt_asset_specs
specs = build_drt_asset_specs(project_dir=".")
# Use specs with @multi_asset + PipesClient
```

## AI Skills for Claude Code

Four skills available via the Claude Code plugin marketplace:

```bash
/plugin marketplace add drt-hub/drt
/plugin install drt@drt-hub
```

| Skill | File | Purpose |
|-------|------|---------|
| `drt-create-sync` | `skills/drt/skills/drt-create-sync/SKILL.md` | Generate sync YAML from user intent |
| `drt-debug` | `skills/drt/skills/drt-debug/SKILL.md` | Diagnose and fix failing syncs |
| `drt-init` | `skills/drt/skills/drt-init/SKILL.md` | Guide through project initialization |
| `drt-migrate` | `skills/drt/skills/drt-migrate/SKILL.md` | Migrate from Census/Hightouch to drt |

Slash command versions also available in `.claude/commands/` for manual installation.

## Key Concepts

### Sync Modes

**Full sync** (default): Extract all rows and send to destination on every run.

**Incremental sync**: Extract only new/updated rows using a watermark column.
- Set `sync.mode: incremental` and `sync.cursor_field: <column>`
- `cursor_field` is **required** when `mode: incremental` — omitting it raises a validation error
- `cursor_field` must be a valid SQL identifier (letters, digits, underscores, dots only)
- drt saves `last_cursor_value` in `.drt/state.json` after each run
- Next run automatically injects `WHERE <cursor_field> > '<last_value>'`
- Cursor comparison uses numeric ordering when possible (handles integer/float cursors correctly)

**Upsert mode**: Semantic alias for `mode: full` when `upsert_key` is set. Makes YAML intent explicit.
- Set `sync.mode: upsert` — behaves identically to `mode: full`

### Model Reference

The `model` field in a sync can be:
- `ref('table_name')` — expands to `SELECT * FROM <dataset>.<table_name>`
- Raw SQL — `SELECT id, email FROM analytics.users WHERE active = true`
- drt checks `syncs/models/<name>.sql` first; if found, uses that file's content

### Jinja2 Templates

Destination configs support Jinja2 templating with `{{ row.<field> }}`:

```yaml
body_template: |
  {"text": "New user: {{ row.name }} ({{ row.email }})"}
```

The `row` variable contains all columns from the current record as a dict.

### Error Handling

- `on_error: fail` (default) — stop the entire sync on first failure
- `on_error: skip` — log the error, continue with remaining records

### Rate Limiting

```yaml
sync:
  rate_limit:
    requests_per_second: 10  # default: 10
```

### Retry

```yaml
sync:
  retry:
    max_attempts: 3           # default: 3
    initial_backoff: 1.0      # seconds
    backoff_multiplier: 2.0   # exponential: 1s, 2s, 4s...
    max_backoff: 60.0         # cap at 60s
    retryable_status_codes: [429, 500, 502, 503, 504]
```

## State File

`.drt/state.json` stores the result of the last run per sync:

```json
{
  "notify_slack": {
    "sync_name": "notify_slack",
    "last_run_at": "2026-03-30T12:00:00",
    "records_synced": 42,
    "status": "success",
    "last_cursor_value": "2026-03-30T11:59:00"
  }
}
```
