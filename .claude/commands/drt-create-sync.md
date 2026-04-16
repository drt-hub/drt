
Create a drt sync YAML configuration file for the user.

## Steps

1. Ask the user for the following (or infer from context if already provided):
   - **Source table or SQL**: what data to sync (e.g. `ref('new_users')` or a SQL query)
   - **Destination**: where to send it (Slack, Discord, Microsoft Teams, REST API, HubSpot, GitHub Actions, Google Sheets, PostgreSQL, MySQL, ClickHouse, Parquet, CSV/JSON/JSONL, Jira, Linear, SendGrid, Staged Upload (async bulk APIs), or other)
   - **Sync mode**: full (every run), incremental (watermark-based, needs cursor column), upsert (dedup by key), or replace (TRUNCATE + INSERT for full table refresh)
   - **Frequency intent**: helps set `batch_size` and `rate_limit`

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
