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
- **Current version:** v0.4.0

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
| PostgreSQL | `drt-core[postgres]` | Connection string via env |
| Redshift | `drt-core[redshift]` | PostgreSQL wire protocol via psycopg2. Supports `schema` (search_path). Port defaults to 5439. |

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
| Slack Webhook | `slack` | Incoming webhook |
| GitHub Actions | `github_actions` | workflow_dispatch trigger |
| HubSpot CRM | `hubspot` | Contacts / Deals / Companies upsert |
| Google Sheets | `google_sheets` | Overwrite or append. Requires `drt-core[sheets]` |
| PostgreSQL (upsert) | `postgres` | INSERT ... ON CONFLICT DO UPDATE. Requires `drt-core[postgres]` |
| MySQL (upsert) | `mysql` | INSERT ... ON DUPLICATE KEY UPDATE. Requires `drt-core[mysql]` |

## CLI Commands

```bash
drt init                          # interactive project wizard
drt list                          # list sync definitions
drt validate                      # validate all sync YAMLs
drt run                           # run all syncs
drt run --select <sync-name>      # run one sync
drt run --dry-run                 # preview without writing data
drt run --verbose                 # show row-level error details on failure
drt status                        # show recent sync results
drt status --verbose              # show per-row error details
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
