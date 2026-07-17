---
name: drt-migrate
description: >
  Help migrate from Census, Hightouch, Polytomic, or custom scripts to drt.
  Use this skill when a user wants to replace their existing Reverse ETL tool
  with drt, or asks how to replicate a sync they had in another tool.
---

Help the user migrate from an existing Reverse ETL tool (Census, Hightouch, Polytomic, or custom scripts) to drt.

## Steps

1. Ask the user to share their existing sync configuration (screenshot, YAML, JSON, or description).

2. Map their existing config to drt equivalents using the tables below.

3. Generate a valid `syncs/<name>.yml` for each sync.

4. Note any features that need manual setup (auth env vars, profiles.yml).

## Concept Mapping

### Census / Hightouch → drt

| Census / Hightouch concept | drt equivalent |
|---------------------------|----------------|
| Source (BigQuery model) | `model: ref('table')` or raw SQL |
| Destination connection | `destination.type` + auth config |
| Sync behavior: Full | `sync.mode: full` (every run, no dedup) |
| Sync behavior: Append (incremental) | `sync.mode: incremental` + `cursor_field` |
| Sync behavior: Mirror (upsert + delete-removed) | `sync.mode: mirror` (v0.7.7+) + `upsert_key` — supported on postgres / mysql / clickhouse / snowflake / databricks (v0.7.9). Tune deletes via `sync.mirror` (v0.7.10, postgres / mysql only): `strategy: tracked` (#686, delete only rows drt itself synced — safe when the app also writes the table) and `scope: [col]` (#687, restrict deletes to observed parents) |
| Sync behavior: Replace (overwrite table) | `sync.mode: replace` (TRUNCATE + INSERT, zero-downtime via `replace_strategy: swap` on supported DWHs) |
| Field mappings (rename columns) | `sync.field_mappings: {source_column: destination_field}` (v0.7.9, #415) — first-class column rename applied just before the destination; use instead of aliasing in SQL |
| Field mappings (compute / reshape a value) | `body_template` / `properties_template` (Jinja2) |
| PII masking on a synced column | `sync.mask: {field: hash \| redact}` or `{field: {strategy: truncate, length: N}}` (v0.7.10, #427/#660) — obscures a field just before the destination without touching the source SQL |
| Run schedule | `drt run` via cron, CI, Dagster, Airflow, or Prefect |
| Error notifications | `failure_alerts` (Slack / webhook, v0.7.0+) — fires on sync-level failures |
| Per-row error policy | `on_error: skip` (continue past failures) vs `on_error: fail` (stop at first) |

### Sync-mode picking guide (which `sync.mode` matches the source semantic)

| User wants | drt mode | Notes |
|------------|----------|-------|
| "Re-send everything every run" | `full` | Default. Idempotent destinations only — REST API / Slack / file outputs. |
| "Append new rows since last run" | `incremental` + `cursor_field` | Watermark-based. `--cursor-value` overrides for backfill. |
| "Upsert by key" | `upsert` + `upsert_key` | Census's most common "Update" shape. |
| "Upsert by key AND delete rows removed from source" | `mirror` + `upsert_key` | Census's "Full Sync with Deletion" / Hightouch's "Mirror" semantic. v0.7.7+, postgres / mysql / clickhouse / snowflake / databricks. Source key cardinality fits in memory. On a table the app also writes, use `sync.mirror.strategy: tracked` (postgres / mysql). |
| "Overwrite the destination table each run" | `replace` | TRUNCATE + INSERT. Set `replace_strategy: swap` (Postgres / MySQL / ClickHouse / Snowflake / Databricks) for zero-downtime via staging-table swap. |

### Auth migration

| Old tool style | drt equivalent |
|---------------|----------------|
| Stored API key in UI | `token_env: MY_TOKEN` + `export MY_TOKEN=...` (never hardcode — `drt validate` flags hardcoded secrets) |
| OAuth app | Use token from OAuth flow → `token_env` |
| Service account JSON | Set `GOOGLE_APPLICATION_CREDENTIALS` for BigQuery source |
| Connection string | Source profile field in `~/.drt/profiles.yml` (env-var-substituted via `${VAR}`) |

## Output Format

For each sync, output:

```yaml
# syncs/<name>.yml
name: <name>
description: "Migrated from <tool>"
model: ref('<table>')   # or raw SQL

destination:
  type: <type>
  # ... fields

sync:
  mode: full   # or incremental / upsert / mirror / replace
  # ...
```

Then summarize:
- What manual steps are needed (env vars, `~/.drt/profiles.yml`)
- Any features the old tool had that drt doesn't support yet (flag these clearly)
- Whether the migration changes semantics (e.g. Census "Full Sync with Deletion" → drt `mirror` is the same shape; Census "Full Sync without Deletion" → drt `full` keeps stale rows in the destination, so the user may want `upsert` instead)

## Reference

- `docs/llm/API_REFERENCE.md` — all destination types and fields
- `docs/connectors/` — per-destination details (auth, supported modes)
- `examples/postgres_to_postgres_mirror/` — runnable example for the `mirror` mode shape
