
Create a drt sync YAML configuration file for the user.

## Steps

1. Ask the user for the following (or infer from context if already provided):
   - **Source table or SQL**: what data to sync (e.g. `ref('new_users')` or a SQL query)
   - **Destination**: where to send it (Slack, Discord, Microsoft Teams, REST API, HubSpot, GitHub Actions, Google Sheets, PostgreSQL, MySQL, ClickHouse, Snowflake, Databricks Delta Lake, BigQuery, Parquet, CSV/JSON/JSONL, Amazon S3, Google Cloud Storage (GCS), Azure Blob Storage, Jira, Linear, SendGrid, Amplitude, Mixpanel, Klaviyo, Meta Conversions API, Notion, Twilio, Intercom, Zendesk, Google Ads, Email SMTP, Elasticsearch/OpenSearch, Staged Upload (async bulk APIs), Salesforce Bulk, Airtable, or other)
   - **Sync mode**: full (every run), incremental (cursor/watermark-based), upsert (dedup by key), replace (full-table rebuild), or mirror (upsert + delete removed rows; requires `upsert_key`). Replace/mirror are supported on Postgres, MySQL, ClickHouse, Snowflake, Databricks, and BigQuery. Mirror `strategy: destination` (default) compares with the whole target; `strategy: tracked` deletes only rows drt previously synced and can combine with `scope` on all five non-BigQuery warehouse destinations; `strategy: diff` deletes exact removals from snapshot-diff incremental and works on all six, but rejects `scope`. BigQuery does not support `tracked`.
   - **Incremental strategy**: use the default cursor strategy (`mode: incremental` + `cursor_field`) when the model has a reliable monotonic column. For a Postgres, Snowflake, Databricks, or BigQuery source without one, offer `mode: upsert` (or `mirror`) + `incremental_strategy: diff` + destination `upsert_key`; optionally set `diff.hash_columns` to a non-empty list instead of `all`. This materializes full snapshots in the source profile's `managed_schema`, needs warehouse create/write/drop privileges, and same-sync runs must not overlap. `--limit` never promotes the snapshot baseline.
   - **Match policy (optional)**: `sync.match_policy` (v0.8.1, #757) narrows the upsert write path (`mode: full` / `upsert` / `incremental`) to one side — `update_only` touches only rows that already exist in the destination (no-match rows are **skipped, not created** — the CRM enrichment case: push warehouse-computed fields into records reps already made), `create_only` inserts only rows that don't yet exist (existing left untouched). Skips are counted in `SyncResult.skipped` / `skipped_no_match` (shown as `… N skipped (M no match)`), never errors. Rejected for `mode: replace` / `mirror`; fails fast on destinations that don't implement it. Supported on **Postgres** and **HubSpot** as of v0.8.1; other SaaS / SQL destinations follow
   - **Frequency intent**: helps set `batch_size` and `rate_limit`
   - **Column renames (optional)**: if source column names differ from destination field names, use `sync.field_mappings: {source_column: destination_field}` (#415) instead of aliasing in SQL — applied just before the destination, so `cursor_field` / lookups / `computed_fields` use source names while `upsert_key` / destination columns use the mapped names
   - **Derived columns (optional)**: if the destination needs a shape the warehouse model shouldn't own (a concatenated `full_name`, E.164 phone, epoch-millis timestamp, an environment stamp), use `sync.computed_fields: {field_name: "<jinja>"}` (#763) rather than adding a destination-specific column to the dbt mart. Reads source column names as `{{ row.col }}`; same Jinja env / filters / `StrictUndefined` as `body_template`. Transform order is `computed_fields` → `field_mappings` → `mask`. A **single-expression** template keeps the Python value's type (`{{ row.n * 1000 }}` → `5000`, not `"5000"`); anything with surrounding text renders as a string. A computed field can never read another one (order-independent). Writing an existing column name replaces it in place and reads the original value. ⚠️ A null passed *through a filter* renders as the string `"None"` — write `{{ (row.phone or '') | replace('-','') }}`. See `docs/guides/computed-fields.md`
   - **PII masking (optional)**: to obscure a field before it reaches the destination without touching the source SQL, use `sync.mask` (v0.7.10, #427/#660). Flat form for parameter-less strategies — `sync.mask: {email: hash, ssn: redact}` (`hash` = SHA-256 hex, `redact` = `[REDACTED]`); object form for `truncate` — `sync.mask: {name: {strategy: truncate, length: 2}}` (keeps the first N chars). Runs at the same seam as `field_mappings` (after the rename), so mask keys reference the destination-facing field name; nulls pass through, works on every destination. See `docs/guides/pii-masking.md`
   - **Bookkeeping columns (optional)**: to mark *when* a row synced, *which run* produced it, or *which sync* owns it, use `sync.metadata_columns: {synced_at: _drt_synced_at, run_id: _drt_run_id, sync_name: _drt_sync_name}` (#762) — any subset of the three. Plain dict enrichment at the engine seam (not DDL), so the target column must already exist on the destination. Runs **last** of every transform, after `mask` — the opposite end from `computed_fields`, since these are engine bookkeeping values, not derived from source data. See `docs/guides/metadata-columns.md`
   - **Project vars (optional)**: values that differ between environments (a lookback window, a campaign tag) can live in a `vars:` block in `drt_project.yml` and be referenced as `{{ var('name') }}` / `{{ var('name', default) }}` in the model SQL and in YAML string fields (v0.8.0, #783). Override per run with `drt run --vars 'lookback_days: 1'` (precedence: `--vars` > `DRT_VAR_<NAME>` env > project `vars:`). An undefined var with no default is a `drt validate` error
   - **Operational persistence (optional)**: Postgres, Snowflake, Databricks, and BigQuery profiles accept `managed_schema: _drt`. Project `state: {backend: warehouse, connection_profile: <profile>}` stores run state, history, and DLQ there. This is separate from `sync.watermark.storage`. Any `*_env` field may instead contain an `aws-sm://`, `gcp-sm://`, or `vault://` secret-provider URI when the matching extra is installed.
   - **Retry safety / quota identity (optional)**: REST APIs can send `native_idempotency_key` through `native_idempotency_header`; Google Ads maps it to `orderId`. Do not claim protection for destinations where `drt validate` says the field is unwired. For `staged_upload`, set public YAML field `rate_limit_key` to a stable, non-secret quota identity when regional hosts share a vendor quota or independent accounts share a host.
   - **Sparse positional outputs**: Google Sheets fixes its columns from the first batch's first-seen key union. A field first appearing in a later batch fails with `column mismatch`; increase `sync.batch_size` so sparse fields appear in batch one. For historical Klaviyo events, use `backfill: true` (revision `2026-07-15` or newer) to avoid firing live flows, then turn it off for live incremental events.

2. Generate a valid sync YAML using the exact field names from `docs/llm/API_REFERENCE.md`.

3. Output the YAML in a code block and suggest where to save it: `syncs/<name>.yml`

4. Show the commands to check, preview, then run it:
   ```bash
   drt validate                          # YAML schema check
   drt list                              # confirm the new sync is discovered
   drt run --select <name> --dry-run --diff  # preview actions/field changes — no data written
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

6. If the sync uses `computed_fields`, `field_mappings`, and/or `mask` (anything that reshapes a record before it
   reaches the destination), **offer** to generate a top-level `unit_tests` block too (#780) — one or
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
- For snapshot-diff incremental, use `mode: upsert|mirror`, omit `cursor_field`, and include a destination `upsert_key`
- Use `ref('table_name')` when the source is a single DWH table; raw SQL when filtering or joining
- Jinja2 templates use `{{ row.<column_name> }}` — column names must come from the user

## Reference

See `docs/llm/API_REFERENCE.md` for all fields, types, and defaults.
