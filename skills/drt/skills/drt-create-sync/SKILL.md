---
name: drt-create-sync
description: >
  Generate a drt sync YAML configuration file. Use this skill whenever a user
  wants to create a new drt sync, connect a data warehouse table to an external
  service, or set up a Reverse ETL pipeline with drt.
---

Create a drt sync YAML configuration file for the user.

## Steps

1. Ask the user for the following (or infer from context if already provided):
   - **Source table or SQL**: what data to sync (e.g. `ref('new_users')` or a SQL query)
   - **Destination**: where to send it (Slack, Discord, Microsoft Teams, REST API, HubSpot, GitHub Actions, Google Sheets, PostgreSQL, MySQL, ClickHouse, Snowflake, Databricks Delta Lake, BigQuery, Parquet, CSV/JSON/JSONL, Amazon S3, Google Cloud Storage (GCS), Azure Blob Storage, Jira, Linear, SendGrid, Amplitude, Mixpanel, Klaviyo, Notion, Twilio, Intercom, Zendesk, Google Ads, Email SMTP, Elasticsearch/OpenSearch, Staged Upload (async bulk APIs), Salesforce Bulk, Airtable, or other)
   - **Sync mode**: full (every run), incremental (watermark-based, needs cursor column), upsert (dedup by key), replace (TRUNCATE + INSERT for full table refresh), or mirror (upsert + DELETE rows whose `upsert_key` was not observed in the source — differential delete, requires `upsert_key`; supported on postgres / mysql / clickhouse / snowflake as of v0.7.7, databricks as of v0.7.9). Mirror deletes can be tuned via `sync.mirror` (v0.7.10, postgres / mysql only): `strategy: tracked` deletes only rows drt itself previously synced (state kept in a drt-managed `_drt_synced_keys` table in the destination — safe when the application also writes to the table; first run baselines, no deletes), and `scope: [parent_id]` restricts deletes to rows whose scope-column values appeared in this run (stateless fit for parent+child regeneration). `strategy` and `scope` are not combinable yet
   - **Frequency intent**: helps set `batch_size` and `rate_limit`
   - **Column renames (optional)**: if source column names differ from destination field names, use `sync.field_mappings: {source_column: destination_field}` (#415) instead of aliasing in SQL — applied just before the destination, so `cursor_field` / lookups use source names while `upsert_key` / destination columns use the mapped names
   - **PII masking (optional)**: to obscure a field before it reaches the destination without touching the source SQL, use `sync.mask` (v0.7.10, #427/#660). Flat form for parameter-less strategies — `sync.mask: {email: hash, ssn: redact}` (`hash` = SHA-256 hex, `redact` = `[REDACTED]`); object form for `truncate` — `sync.mask: {name: {strategy: truncate, length: 2}}` (keeps the first N chars). Runs at the same seam as `field_mappings` (after the rename), so mask keys reference the destination-facing field name; nulls pass through, works on every destination. See `docs/guides/pii-masking.md`
   - **Project vars (optional)**: values that differ between environments (a lookback window, a campaign tag) can live in a `vars:` block in `drt_project.yml` and be referenced as `{{ var('name') }}` / `{{ var('name', default) }}` in the model SQL and in YAML string fields (v0.8.0, #783). Override per run with `drt run --vars 'lookback_days: 1'` (precedence: `--vars` > `DRT_VAR_<NAME>` env > project `vars:`). An undefined var with no default is a `drt validate` error

2. Generate a valid sync YAML using the exact field names from `docs/llm/API_REFERENCE.md`.

3. Output the YAML in a code block and suggest where to save it: `syncs/<name>.yml`

4. Show the command to validate and run it:
   ```bash
   drt validate
   drt run --select <name> --dry-run
   drt run --select <name>
   ```

## Rules

- Use `type: bearer` + `token_env` (never hardcode tokens)
- Default `on_error: skip` for Slack/webhooks, `on_error: fail` for critical syncs
- For incremental mode, always include `cursor_field`
- Use `ref('table_name')` when the source is a single DWH table; raw SQL when filtering or joining
- Jinja2 templates use `{{ row.<column_name> }}` — column names must come from the user

## Reference

See `docs/llm/API_REFERENCE.md` for all fields, types, and defaults.
