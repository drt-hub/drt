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
   - **Match policy (optional)**: `sync.match_policy` (v0.8.1, #757) narrows the upsert write path (`mode: full` / `upsert` / `incremental`) to one side — `update_only` touches only rows that already exist in the destination (no-match rows are **skipped, not created** — the CRM enrichment case: push warehouse-computed fields into records reps already made), `create_only` inserts only rows that don't yet exist (existing left untouched). Skips are counted in `SyncResult.skipped` / `skipped_no_match` (shown as `… N skipped (M no match)`), never errors. Rejected for `mode: replace` / `mirror`; fails fast on destinations that don't implement it. Supported on **Postgres** and **HubSpot** as of v0.8.1; other SaaS / SQL destinations follow
   - **Frequency intent**: helps set `batch_size` and `rate_limit`
   - **Column renames (optional)**: if source column names differ from destination field names, use `sync.field_mappings: {source_column: destination_field}` (#415) instead of aliasing in SQL — applied just before the destination, so `cursor_field` / lookups / `computed_fields` use source names while `upsert_key` / destination columns use the mapped names
   - **Derived columns (optional)**: if the destination needs a shape the warehouse model shouldn't own (a concatenated `full_name`, E.164 phone, epoch-millis timestamp, an environment stamp), use `sync.computed_fields: {field_name: "<jinja>"}` (#763) rather than adding a destination-specific column to the dbt mart. Reads source column names as `{{ row.col }}`; same Jinja env / filters / `StrictUndefined` as `body_template`. Transform order is `computed_fields` → `field_mappings` → `mask`. A **single-expression** template keeps the Python value's type (`{{ row.n * 1000 }}` → `5000`, not `"5000"`); anything with surrounding text renders as a string. A computed field can never read another one (order-independent). Writing an existing column name replaces it in place and reads the original value. ⚠️ A null passed *through a filter* renders as the string `"None"` — write `{{ row.phone or '' | replace('-','') }}`. See `docs/guides/computed-fields.md`
   - **PII masking (optional)**: to obscure a field before it reaches the destination without touching the source SQL, use `sync.mask` (v0.7.10, #427/#660). Flat form for parameter-less strategies — `sync.mask: {email: hash, ssn: redact}` (`hash` = SHA-256 hex, `redact` = `[REDACTED]`); object form for `truncate` — `sync.mask: {name: {strategy: truncate, length: 2}}` (keeps the first N chars). Runs at the same seam as `field_mappings` (after the rename), so mask keys reference the destination-facing field name; nulls pass through, works on every destination. See `docs/guides/pii-masking.md`
   - **Project vars (optional)**: values that differ between environments (a lookback window, a campaign tag) can live in a `vars:` block in `drt_project.yml` and be referenced as `{{ var('name') }}` / `{{ var('name', default) }}` in the model SQL and in YAML string fields (v0.8.0, #783). Override per run with `drt run --vars 'lookback_days: 1'` (precedence: `--vars` > `DRT_VAR_<NAME>` env > project `vars:`). An undefined var with no default is a `drt validate` error

2. Generate a valid sync YAML using the exact field names from `docs/llm/API_REFERENCE.md`.

3. Output the YAML in a code block and suggest where to save it: `syncs/<name>.yml`

4. Show the commands to check, preview, then run it:
   ```bash
   drt validate                          # YAML schema check
   drt list                              # confirm the new sync is discovered
   drt run --select <name> --dry-run     # preview — no data written
   drt run --select <name> --limit 10    # real send, capped at 10 rows
   drt run --select <name>               # full run
   ```
   `drt list` is worth the extra line: sync discovery is glob-based, so a file
   saved outside `syncs/` or with a mismatched `name:` validates cleanly and then
   silently never runs.

5. If the sync declares a `tests:` block, show how to run it after the sync:
   ```bash
   drt test --select <name>    # post-sync validation (row counts, freshness, unique, custom SQL)
   drt build --select <name>   # run + test in one pass
   ```

6. If the sync uses `field_mappings` and/or `mask` (anything that reshapes a record before it
   reaches the destination), **offer** to generate a `sync.unit_tests` block too (#780) — one or
   two fixture rows through the transform, verified with zero credentials and zero network:
   ```yaml
   unit_tests:
     - name: <describes what it checks>
       given:
         - { <source columns with example values> }
       expect:
         - { <destination-facing columns, after field_mappings/mask> }
   ```
   `given` uses the sync's **source** column names; `expect` uses the **destination-facing**
   names (after the rename and the mask both run) — see `docs/guides/sync-unit-tests.md` for the
   full ordering rule. Optional, not generated by default — offer it, and skip silently if the
   user declines or the sync has no transforms worth verifying offline. If accepted, add to the
   command list from step 4:
   ```bash
   drt test --unit --select <name>   # verify the transform, before ever touching the destination
   ```

## Rules

- Use `type: bearer` + `token_env` (never hardcode tokens)
- Default `on_error: skip` for Slack/webhooks, `on_error: fail` for critical syncs
- For incremental mode, always include `cursor_field`
- Use `ref('table_name')` when the source is a single DWH table; raw SQL when filtering or joining
- Jinja2 templates use `{{ row.<column_name> }}` — column names must come from the user

## Reference

See `docs/llm/API_REFERENCE.md` for all fields, types, and defaults.
