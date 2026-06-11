
Create a drt sync YAML configuration file for the user.

## Steps

1. Ask the user for the following (or infer from context if already provided):
   - **Source table or SQL**: what data to sync (e.g. `ref('new_users')` or a SQL query)
   - **Destination**: where to send it (Slack, Discord, Microsoft Teams, REST API, HubSpot, GitHub Actions, Google Sheets, PostgreSQL, MySQL, ClickHouse, Snowflake, Databricks Delta Lake, Parquet, CSV/JSON/JSONL, Amazon S3, Google Cloud Storage (GCS), Jira, Linear, SendGrid, Amplitude, Mixpanel, Notion, Twilio, Intercom, Zendesk, Google Ads, Email SMTP, Staged Upload (async bulk APIs), Salesforce Bulk, or other)
   - **Sync mode**: full (every run), incremental (watermark-based, needs cursor column), upsert (dedup by key), replace (TRUNCATE + INSERT for full table refresh), or mirror (upsert + DELETE rows whose `upsert_key` was not observed in the source — differential delete, requires `upsert_key`; supported on postgres / mysql / clickhouse / snowflake as of v0.7.7, databricks as of v0.7.9)
   - **Frequency intent**: helps set `batch_size` and `rate_limit`
   - **Column renames (optional)**: if source column names differ from destination field names, use `sync.field_mappings: {source_column: destination_field}` (#415) instead of aliasing in SQL — applied just before the destination, so `cursor_field` / lookups use source names while `upsert_key` / destination columns use the mapped names

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
