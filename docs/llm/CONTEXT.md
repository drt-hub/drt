# drt — LLM Context

This document is optimized for LLM consumption. It gives you the full context needed to help users configure, debug, and extend drt.

## What is drt?

**drt** (data reverse tool) is a CLI tool that syncs data from a data warehouse to external services, declaratively via YAML.

```
dlt (load into DWH) → dbt (transform) → drt (activate out of DWH)
```

- **Category:** Reverse ETL
- **Tagline:** "Reverse ETL as code — no UI, no lock-in, no per-row bill."
- **Install:** `pip install drt-core` or `uv add drt-core`
- **Package name:** `drt-core` (PyPI) — CLI command is `drt`
- **Current version:** v1.1.0

## What drt is NOT

drt's competitive set is commercial reverse-ETL tools (Census, Hightouch,
RudderStack Reverse ETL, and similar), not dlt or dbt — those are adjacent
pipeline stages. Against that competitive set, four things are deliberately
excluded from `drt-core`, not merely unbuilt — see
[ADR 0011](../adr/0011-subtraction-positioning-vs-reverse-etl.md):

- Not a data loader (that's dlt)
- Not a transformer (that's dbt)
- Not a scheduler — it runs via CLI or cron, not a built-in scheduler
- Not a SaaS / hosted runtime — fully self-hosted OSS (Apache 2.0); data
  goes straight from your warehouse to the destination, never through a
  drt-hosted intermediary, and there is no per-row bill
- Not a UI or dashboard — config-as-code (YAML, git-reviewable) is the
  product, not a placeholder for one
- Not an audience/segmentation builder — building the record set to sync is
  a SQL/dbt-modeling problem; drt syncs exactly what the sync config's
  `model` field points it at (raw SQL or a dbt-style reference)
- Not a gatekept connector catalog: installed `drt.sources` and
  `drt.destinations` plugins register types that can be named directly in
  profiles/sync YAML (#997). `drt plugins list` reports every discovered
  entry point and isolates a broken plugin instead of taking down the CLI.

## Architecture

```
drt_project.yml          # project config (source profile, vars:)
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
├── drt_project.yml       # required: project name + source profile (+ optional vars:)
├── syncs/
│   ├── notify_slack.yml  # one sync per file
│   └── update_hubspot.yml
└── syncs/models/
    └── active_users.sql  # optional: custom SQL (overrides ref())
```

## Sources (where data comes from)

| Source | Extra | Notes |
|--------|-------|-------|
| BigQuery | `drt-core[bigquery]` | Uses ADC or keyfile. Supports `location` and `managed_schema` |
| DuckDB | (core) | Local `.duckdb` file |
| SQLite | (core) | Built-in `sqlite3`, no extra dependencies. Local `.sqlite` files or `:memory:` |
| PostgreSQL | `drt-core[postgres]` | Supports `managed_schema` for drt-owned bookkeeping tables |
| Redshift | `drt-core[redshift]` | PostgreSQL wire protocol via psycopg2. Supports `schema` (search_path). Port defaults to 5439. |
| ClickHouse | `drt-core[clickhouse]` | HTTP interface via `clickhouse-connect`. Supports host, port, database, user, password_env. |
| Snowflake | `drt-core[snowflake]` | Supports account, user, private_key_env (key-pair, preferred — #737) / password_env, database, schema, warehouse, role, managed_schema |
| MySQL | `drt-core[mysql]` | Uses pymysql. Supports host, port, dbname, user, password_env |
| Databricks | `drt-core[databricks]` | SQL Warehouse via databricks-sql-connector. Supports Unity Catalog, access_token_env, managed_schema |
| SQL Server | `drt-core[sqlserver]` | Microsoft SQL Server via pure-Python pymssql. Supports host, port, database, user, password_env |

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
| Zendesk | `zendesk` | Users / organizations upsert |
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
| Meta Conversions | `meta_conversions` | Batched server-side Pixel conversion events |
| Staged Upload | `staged_upload` | Async bulk APIs: file upload → job trigger → poll |
| Notion | `notion` | Append rows to Notion databases |
| Twilio SMS | `twilio` | Send SMS per row via Twilio Messages API |
| Intercom | `intercom` | Create/update contacts via Intercom REST API v2 |
| Email SMTP | `email_smtp` | Send emails via SMTP (plain text or HTML) |
| Salesforce Bulk API 2.0 | `salesforce_bulk` | Upsert via Bulk API 2.0 with CSV serialization |
| Snowflake | `snowflake` | INSERT / MERGE upsert. Requires `drt-core[snowflake]` |
| Databricks | `databricks` | INSERT / MERGE, replace, and mirror into Delta tables. Requires `drt-core[databricks]` |
| BigQuery | `bigquery` | INSERT / MERGE, load-job replace, and mirror. Requires `drt-core[bigquery]` |
| Amplitude | `amplitude` | Identify API (user properties) or HTTP V2 API (events). No extra dependencies. |

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
drt run --log-format json         # structured JSON logging to stderr
drt run --profile prd             # override profile (or DRT_PROFILE env var)
drt run --select 'users_*'        # glob selection (#771)
drt run --select tag:<tag>        # run syncs matching a tag (repeat --select to union)
drt run --select destination:hubspot  # select by destination type (#771)
drt run --exclude <name-or-selector>  # subtract from the selection (#771)
drt run --select state:modified --state <manifest.json>  # run definitions changed since a schema-v3 docs manifest (#772); also works with build/test/validate
drt run --failed                  # re-run only syncs whose last status != success (#773; record-level replay is `drt retry`)
drt run --limit 10                # sampled run (#774): extract at most N rows; watermark does NOT advance; refused for mirror/replace
drt run --fail-fast               # stop scheduling after first failure (#775); remaining syncs report status=skipped; also on drt test
drt run --threads 4               # parallel sync execution
drt run --cursor-value '2026-01-01 00:00:00'  # override watermark cursor for backfill
# drt run also writes target/drt/run_results.json (#778) -- dbt run_results.json-style durable per-invocation record, independent of --output (written in text mode too), for every invocation that resolves a sync list (including no-op runs) -- NOT written for a preflight failure before syncs are known (bad project/profile/vars, --diff without --dry-run), matching dbt's own run_results.json. Reuses the same per-sync entries --output json's syncs array builds, minus raw error text (dropped, not redacted -- error_type/error_stage/error_suggestion stay). --target-path <dir> relocates it (default target/drt/, deliberately not dbt's own target/)
drt test                          # run post-sync validation tests
drt test --select <sync-name>     # test a specific sync
drt test --store-failures         # sample up to N failing rows/failed test (#779); sync.mask applied
drt test --unit                   # run unit_tests fixture rows through transforms; no credentials/network (#780)
drt build                         # run + test per sync in one pass (#777); test failure = sync failed (no rollback); sequential
drt build --select tag:crm --fail-fast
drt sources                       # list available source connectors
drt destinations                  # list available destination connectors
drt plugins list                  # list installed entry-point plugins and load failures (#297/#997)
drt status                        # show recent sync results
drt status --output json          # JSON output for status
drt mcp run                       # start MCP server (requires drt-core[mcp])
drt serve --port 8080             # HTTP webhook endpoint — POST /sync/<name> answers 202 + run id (poll GET /runs/<id>, or ?wait=true for the result); same-sync triggers coalesce, different syncs run concurrently, nothing accepted is dropped (#854). Auth: --auth none|bearer|hmac|oidc, applied to every route except GET /health (GET /runs/<id> included; under hmac a POST signs its raw body while a GET signs the request path under a derived key, so a signature is bound to one run id and cannot be replayed as a POST, #936)
drt serve --auth oidc --oidc-audience <aud> --oidc-email <email>  # OIDC JWT verification for Pub/Sub push (#903) — needs drt-core[serve-oidc]; verifies signature against Google's rotating public keys, requires email_verified:true for --oidc-email, fails closed (401) if the extra isn't installed. Google-specific: no --oidc-issuer override, since verify_oauth2_token only ever fetches Google's own certs
drt docs generate                 # static docs site to target/docs/ (html; also --format mermaid|json|dbt-exposures). dbt-exposures prints deterministic ref()-only dbt exposure YAML to stdout (#781). Destination labels are docs-safe by default (#696): object identity (table/channel/sheet/bucket) stays, endpoints/hosts/phones/emails do not
drt docs generate --full-labels   # verbatim describe() labels + unredacted error text — trusted/internal hosting only (#696/#698)
drt docs generate --history-depth 20  # recent runs per sync embedded in the manifest from .drt/history (schema v2, #698; default 10, 0 disables, --no-state omits)
drt docs generate --inline        # html only: emit the whole catalog as ONE self-contained navigable HTML object — inlined CSS/JS + in-page (#hash) navigation (Elementary single-file model), zero sub-resource AND zero inter-object requests — so it renders and navigates on an authenticated object store (GCS storage.cloud.google.com / S3 presigned URLs) where per-object auth breaks the multi-file output's assets and cross-links (#818/#821). Display byte-identical to the default; default output stays multi-file
drt deploy github-actions --schedule "40 3 * * *"  # scaffold .github/workflows/drt-sync.yml — drt-action wired, extras inferred, required secrets enumerated (#785)
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
| `drt_run_test(sync_name=None, unit=False)` | Runs destination tests, or offline `unit_tests` when `unit=True` |
| `drt_get_status(sync_name=None)` | Returns last run result(s); omit sync_name for all |
| `drt_state_show(sync_name=None)` | Returns stored watermark and last-run state |
| `drt_state_reset(...)` | Explicitly resets watermark, run, and/or tracked-mirror state |
| `drt_get_history(sync_name=None, limit=20)` | Returns past execution entries |
| `drt_validate()` | Validates all sync YAMLs; returns valid list and errors dict |
| `drt_get_schema(schema_type="sync")` | Returns JSON Schema for "sync" or "project" config |
| `drt_list_connectors()` | Lists all available sources and destinations |
| `drt_dlq(sync_name=None)` | Inspects failed records persisted for replay |
| `drt_retry(sync_name, ...)` | Replays or clears a sync's DLQ |
| `drt_get_manifest(...)` | Returns the machine-readable docs/lineage manifest |
| `drt_list_profiles()` | Lists credential profile names and source types without secrets |
| `drt_test_profile(name)` | Tests a source profile connection |
| `drt_doctor()` | Returns structured environment diagnostics |

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

The v0.4 API also includes:

- `build_drt_change_sensor()` for event-driven runs from metadata-only Delta
  Lake, Iceberg, Snowflake, or SQL Server change signals. Snowflake requires
  both `watch_table=` and an explicit `minimum_interval_seconds=`; SQL Server
  requires `watch_table=` to validate table-level Change Tracking.
- Plain `@op` execution through `DagsterDrtResource.run(context=...,
  sync_names=[...])`, which emits asset materializations without requiring an
  `@drt_assets` definition.
- `DrtEventIterator`, returned by `run()`, with chainable
  `.fetch_row_count()` source-side verification.
- `DrtSyncComponent` for declarative `defs.yaml` assets and `dg scaffold defs`.

## AI Skills for Claude Code

Five skills available via the Claude Code plugin marketplace:

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
| `drt-troubleshoot` | `skills/drt/skills/drt-troubleshoot/SKILL.md` | Walk a setup through end-to-end diagnosis |

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
- **Template variable**: Use `{{ cursor_value }}` (or `{{ watermark }}`) in model SQL for flexible WHERE placement. When present, auto-injection is skipped.
- **Remote watermark storage**: For stateless environments (e.g., Cloud Run Jobs), set `sync.watermark.storage` to `gcs` or `bigquery` to persist cursor values externally instead of `.drt/state.json`.
- **Overlap window** (#759): `sync.watermark.lag` re-reads a window behind the stored watermark (`"1 hour"` for timestamp cursors — same grammar as `freshness.max_age` — or a positive int for numeric cursors) so late-arriving rows are re-synced. Applies only to storage-sourced watermarks (never `--cursor-value` or `default_value`), and the persisted watermark itself is never lagged. Overlap rows are re-sent every run, so the destination must tolerate duplicates (e.g. `upsert_key`).
- **REST API source** (#767): set `incremental: {start_param: updated_since}` on the `rest_api` profile — the engine hands the watermark to the source, which sends it as a request query param so the API filters server-side. Without `start_param`, `mode: incremental` re-extracts the full endpoint every run (warning logged).

**Snapshot-diff incremental** (#755): For a model without a reliable cursor, set
`sync.mode: upsert` (or `mirror`), `sync.incremental_strategy: diff`, and a
destination `upsert_key`. Postgres, Snowflake, Databricks, and BigQuery sources
materialize the full model under the profile's `managed_schema`, compare it to
the last successful baseline in warehouse SQL, and send only added/changed
rows. `sync.diff.hash_columns` is `all` by default or an explicit non-empty
column list. Removed keys can drive `sync.mirror.strategy: diff`. The baseline
advances only after a successful, non-dry-run, unlimited run; `--limit` never
promotes it. This opt-in strategy needs source-warehouse write privileges and
same-sync runs must not overlap.

**Upsert mode**: Semantic alias for `mode: full` when `upsert_key` is set. Makes YAML intent explicit.
- Set `sync.mode: upsert` — behaves identically to `mode: full`

**Replace mode**: Rebuild the destination table from the full source snapshot.
- Set `sync.mode: replace`
- `upsert_key` is not required (no conflict resolution needed)
- Useful for junction/mapping tables where deleted source rows must be removed from destination
- Supported by Postgres, MySQL, ClickHouse, Snowflake, Databricks, and
  BigQuery. BigQuery uses load/copy jobs rather than `TRUNCATE TABLE`.
- `replace_strategy: swap` uses a staging/shadow table for an atomic cutover
  on supported warehouse destinations.

**Mirror mode** (#340 — v0.7.7): Upsert every source row, then DELETE destination rows whose `upsert_key` tuple was not observed in the source — application-side differential delete. Lighter than `replace` (no TRUNCATE / re-insert), heavier than `upsert` (extra DELETE pass).
- Set `sync.mode: mirror`
- `destination.upsert_key` is **required** (used to identify which rows to DELETE)
- Supported destinations: Postgres, MySQL, ClickHouse, Snowflake,
  Databricks, and BigQuery.
- **`strategy: destination` (default):** compares with the destination and is
  correct when drt owns the whole target. `scope: [parent_id]` can narrow
  deletes to parents observed in this run.
- **`strategy: tracked`:** deletes only rows drt previously synced, using
  `_drt_synced_keys`; safe for co-written tables. It can be combined with
  `scope` on Postgres, MySQL, Snowflake, ClickHouse, and Databricks, but is
  unsupported on BigQuery.
- **`strategy: diff`:** deletes exactly the removal keys produced by
  `incremental_strategy: diff`; supported on every mirror-capable SQL
  destination. It requires that incremental strategy and rejects `scope`
  because the source removal set is already exact.
- Safety: if the source produces no batches with records, the DELETE is skipped — a transient empty source can't wipe the destination

### Managed bookkeeping and shared state

Postgres, Snowflake, Databricks, and BigQuery source profiles expose
`managed_schema` (default `_drt`). It is a schema/dataset owned by drt for
opt-in bookkeeping such as snapshot-diff baselines and warehouse-backed
state; it does not change how `model:` SQL resolves tables. Operators may let
drt create its tables or pre-provision them and grant only table-level access.

At project level, `state.backend: warehouse` plus `connection_profile` stores
run state, history, and the DLQ as `_drt_runs`, `_drt_history`, and `_drt_dlq`
under that profile's `managed_schema`. All four warehouses above are
supported. Postgres additionally supports `state.idempotency` and
`state.audit_trail`; those two capabilities fail loudly on the other three.

Secrets can remain outside env vars: any `*_env` field can name an
`aws-sm://`, `gcp-sm://`, or `vault://` provider URI (with the matching
optional extra). Resolution order is explicit YAML, environment, local
secrets file, then provider URI.

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
