
Help the user migrate from an existing Reverse ETL tool (Census, Hightouch, Polytomic, or custom scripts) to drt.

## Steps

1. Ask the user to share their existing sync configuration (screenshot, YAML, JSON, or description).

2. If they do not have a drt project yet, scaffold one first — the syncs need a
   project and a profile to live in:
   ```bash
   mkdir my-drt-project && cd my-drt-project
   drt init                  # interactive: project name, source type, connection fields
   drt profile test <name>   # prove the warehouse credential works before migrating anything
   ```
   Use the `/drt-init` skill if they want to be walked through it.

3. Map their existing config to drt equivalents using the tables below.

4. Generate a valid `syncs/<name>.yml` for each sync.

5. Note any features that need manual setup (auth env vars, profiles.yml).

6. **Run the first migrated sync as a preview, not a send.** This is the step
   that matters most in a migration: the old tool is still running, the
   destination has live records, and a mismapped `sync.mode` or `upsert_key`
   writes to production on the first invocation.
   ```bash
   drt validate                          # catches schema errors and hardcoded secrets
   drt run --select <name> --dry-run     # no data written
   drt run --select <name> --dry-run --diff   # record-level preview on queryable destinations
   drt run --select <name> --limit 10    # first real send, capped at 10 rows
   ```
   Only after the diff looks right should they run it uncapped. Flag explicitly
   that `mode: mirror` and `replace` **delete or truncate** destination rows, and
   that `--limit` is refused for both — so those two modes go straight from
   `--dry-run --diff` to a full run, with no sampled middle step. For
   snapshot-diff incremental, `--limit` is allowed in upsert mode but never
   promotes the full snapshot baseline.

## Concept Mapping

### Census / Hightouch → drt

| Census / Hightouch concept | drt equivalent |
|---------------------------|----------------|
| Source (BigQuery model) | `model: ref('table')` or raw SQL |
| Destination connection | `destination.type` + auth config |
| Sync behavior: Full | `sync.mode: full` (every run, no dedup) |
| Sync behavior: Append (incremental) | Cursor: `sync.mode: incremental` + `cursor_field`. No reliable cursor on a Postgres/Snowflake/Databricks/BigQuery source: `mode: upsert` + `incremental_strategy: diff` + destination `upsert_key` (source snapshots in `managed_schema`). |
| Sync behavior: Mirror (upsert + delete-removed) | `sync.mode: mirror` + `upsert_key` — Postgres / MySQL / ClickHouse / Snowflake / Databricks / BigQuery. `strategy: destination` compares with the target; `tracked` protects co-written rows and combines with `scope` on all except BigQuery; `diff` deletes exact removals produced by `incremental_strategy: diff` on all six and rejects `scope`. |
| Sync behavior: Replace (overwrite table) | `sync.mode: replace` (TRUNCATE + INSERT, zero-downtime via `replace_strategy: swap` on supported DWHs) |
| Field mappings (rename columns) | `sync.field_mappings: {source_column: destination_field}` (v0.7.9, #415) — first-class column rename applied just before the destination; use instead of aliasing in SQL |
| Field mappings (compute / reshape a value) | `sync.computed_fields: {name: "<jinja>"}` (#763) — derived columns for **every** destination, applied before `field_mappings` / `mask`; a single-expression template keeps the value's Python type. `body_template` / `properties_template` remain for shaping the whole payload of the destinations that have them |
| PII masking on a synced column | `sync.mask: {field: hash \| redact}` or `{field: {strategy: truncate, length: N}}` (v0.7.10, #427/#660) — obscures a field just before the destination without touching the source SQL |
| Run schedule | `drt run` via cron, CI, Dagster, Airflow, or Prefect |
| Error notifications | `failure_alerts` (Slack / webhook, v0.7.0+) — fires on sync-level failures |
| Per-row error policy | `on_error: skip` (continue past failures) vs `on_error: fail` (stop at first) |

### Sync-mode picking guide (which `sync.mode` matches the source semantic)

| User wants | drt mode | Notes |
|------------|----------|-------|
| "Re-send everything every run" | `full` | Default. Idempotent destinations only — REST API / Slack / file outputs. |
| "Append new rows since last run" | `incremental` + `cursor_field` | Watermark-based. `--cursor-value` overrides for backfill. |
| "Send added/changed rows without a cursor" | `upsert` + `incremental_strategy: diff` + `upsert_key` | Postgres/Snowflake/Databricks/BigQuery sources. Needs writable `managed_schema`; same-sync runs must not overlap; `--limit` never advances the baseline. |
| "Upsert by key" | `upsert` + `upsert_key` | Census's most common "Update" shape. |
| "Upsert by key AND delete rows removed from source" | `mirror` + `upsert_key` | Census's "Full Sync with Deletion" / Hightouch's "Mirror" semantic. All six SQL warehouse destinations. Use `tracked` for co-written tables (not BigQuery), or pair source diff with `mirror.strategy: diff` for exact removal keys. |
| "Overwrite the destination table each run" | `replace` | Full rebuild on all six SQL warehouse destinations. `replace_strategy: swap` uses an atomic shadow-table cutover, including BigQuery copy jobs. |

### Auth migration

| Old tool style | drt equivalent |
|---------------|----------------|
| Stored API key in UI | `token_env: MY_TOKEN` + `export MY_TOKEN=...` (never hardcode — `drt validate` flags hardcoded secrets) |
| OAuth app | Use token from OAuth flow → `token_env` |
| Service account JSON | Set `GOOGLE_APPLICATION_CREDENTIALS` for BigQuery source |
| Connection string | Source profile field in `~/.drt/profiles.yml` (env-var-substituted via `${VAR}`) |
| Managed state/history in the old service | `state.backend: warehouse` + `connection_profile`; Postgres/Snowflake/Databricks/BigQuery persist run state, history, and DLQ under the profile's `managed_schema` |
| Cloud secret reference | Put an `aws-sm://`, `gcp-sm://`, or `vault://` URI in the relevant `*_env` field; install the matching extra |

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
