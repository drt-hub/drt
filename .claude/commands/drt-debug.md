
Debug a failing drt sync.

## Steps

1. Start with the environment-level triage:
   ```bash
   drt doctor   # checks env vars, profile file, python/extras, common gotchas (v0.7+)
   ```
   This catches the bulk of "it doesn't even start" cases before you read code.

2. If `drt doctor` is clean, ask the user to share (or read from context):
   - The error output from `drt run` or `drt status`
   - The sync YAML (`syncs/<name>.yml`)
   - The `drt_project.yml` if relevant

3. Reproduce with more signal — pick the right verbosity:
   - `drt run --select <name> --verbose` — row-level error details
   - `drt run --select <name> --dry-run` — config parses, no data sent
   - `drt run --select <name> --dry-run --diff` — record-level preview (added/updated/deleted) for queryable destinations (v0.7.1+)
   - `drt run --output json` / `drt status --output json` — structured output for CI / scripting (v0.7+)
   - `drt run --log-format json` — JSON Lines logs to stderr (separate from `--output`)
   - `drt run --select <name> --limit 10` — really load ≤10 rows (#774) to reproduce with a small, inspectable send; watermark won't advance (refused for `mode: mirror` / `replace`)
   - `drt run --failed` — re-run only the syncs whose last status wasn't `success` (#773) — the tight loop while fixing one red sync in a larger project
   - `drt run --fail-fast` — stop after the first failure (#775) instead of running the whole set when the cause is systemic

4. Diagnose the root cause using the patterns below.

5. Suggest a concrete fix with the corrected YAML or command.

## Common Error Patterns

### Auth errors (401, 403)
- **Cause**: `token_env` or `value_env` env var not set, or token has wrong permissions.
- **Fix**: Check `echo $MY_TOKEN`, verify token scopes. For HubSpot, confirm Private App has CRM write scope. For GitHub, confirm `actions: write`.
- **Hardcoded-secret detection (v0.7.5+)**: if `drt validate` flags `hardcoded secret detected`, the YAML literally contains a token instead of `token_env`. Move it to an env var and reference it.

### Rate limit (429)
- **Cause**: Sending too fast for the destination's limits.
- **Fix**: Lower `sync.rate_limit.requests_per_second`. HubSpot max: 9 req/s. GitHub Actions: 5 req/s.
- **Also**: Add retry config — 429 is retryable by default; in v0.7+ you can also set a per-destination retry override.
- **Recover the rows that still failed**: enable the dead letter queue (`sync.dlq.enabled: true` + `on_error: skip`, v0.7.9+) so per-record failures persist to `.drt/dlq/<sync>.jsonl` instead of being dropped, then `drt retry <sync>` re-sends just those once the limit clears. `drt status` shows the queue depth.

### Connection errors / timeouts
- **Cause**: Wrong URL, network issue, or destination is down.
- **Fix**: Verify `url` with `curl -X POST <url>` manually. `drt run --dry-run` confirms config parses correctly without sending traffic.

### Template errors
- **Cause**: `{{ row.field_name }}` references a column that doesn't exist in the source.
- **Fix**: `drt run --dry-run` previews rows; confirm column names match the template. For `datetime` / `Decimal` / `UUID` columns flowing into a REST API `body_template`, use the `tojson_safe` filter (v0.7.6+) rather than `CAST(... AS STRING)` in SQL.

### Engine-stage attribution (v0.7.6+)
- **Cause**: `ErrorFormatter` now attaches a stage tag to every error (`extract` / `load` / `finalize` / `validate`). If the surfaced error mentions a stage you didn't expect (e.g. a `finalize` failure on a SQL destination's mirror mode), the bug is in that stage's code path — not in the load loop.
- **Fix**: Stage tag narrows the search. For `finalize` failures on `sync.mode: mirror` (v0.7.7+), check `upsert_key` is set and the source key cardinality is small enough to hold in memory.

### Incremental sync not filtering
- **Cause**: `mode: incremental` set but no saved cursor yet (first run syncs all rows).
- **Fix**: Expected on the first run. Check `drt status` after first run — `last_cursor_value` should be set. To replay or backfill, use `drt run --cursor-value '<value>' --select <name>` (v0.6.2+).

### `on_error: fail` stopping early
- **Cause**: Default behavior for some destinations — first failure stops the sync.
- **Fix**: Change to `on_error: skip` to continue past failures and see the full error count via `drt run --verbose` or `drt status`.
- **Recover the failures**: pair `on_error: skip` with `sync.dlq.enabled: true` (v0.7.9+) so the skipped records land in `.drt/dlq/<sync>.jsonl`; `drt retry <sync>` replays just them after you fix the root cause. To re-run whole syncs that failed (not individual records), use `drt run --failed` (#773). See `docs/guides/dead-letter-queue.md`.

### Profile not found
- **Cause**: `~/.drt/profiles.yml` missing or profile name mismatch.
- **Fix**: `cat ~/.drt/profiles.yml`, verify the profile name matches `drt_project.yml`. Override per-run with `drt run --profile <name>` or the `DRT_PROFILE` env var.

### SQL destinations: qualified-identifier errors (`schema.table`)
- **Cause**: Older versions mis-quoted `schema.table` as a single identifier — e.g. ClickHouse `Code: 62` on `db.table`, Postgres syntax errors on `marketing.events`.
- **Fix**: Already fixed in v0.7.3 (Postgres `_quote_ident` #498), v0.7.4 (MySQL #514), v0.7.8 (ClickHouse #610). Upgrade to the latest patch release and the qualified form parses correctly.

### Snowflake mirror mode: MERGE-only write path
- **Cause**: `sync.mode: mirror` forces the MERGE write path regardless of `config.mode` (v0.7.7+).
- **Fix**: This is intentional. Don't rely on `replace` semantics for Snowflake mirror — Snowflake's mirror doesn't have a swap path.

### Slack / webhook delivery failed (silent)
- **Cause**: Webhook URL returned 4xx but the upstream sync still succeeded; you missed it.
- **Fix**: Configure failure alerts (`failure_alerts` block in `drt_project.yml`, v0.7.0+ via #414) — Slack / webhook notifications fire when any sync run hits a hard failure.

## Tools to escalate to

- `drt test --select <name>` — runs the post-sync validation tests (freshness / unique / accepted_values) declared in the sync YAML; use this when "the sync says success but the data looks wrong".
- `drt_run_test` MCP tool (v0.7.5+) — same as `drt test` but callable from Claude/Cursor without leaving the chat.
- OTel traces (v0.7+, Phase 1+2 shipped) — set `observability.otel.endpoint` in `drt_project.yml` and `pip install drt-core[otel]` to capture spans. Phase 3 engine spans (`drt.sync.run` / `drt.sync.extract` / `drt.sync.load`) shipped v0.7.10 (`pip install drt-core[otel]`).

## Telemetry

drt ships anonymous opt-in telemetry (PostHog Cloud EU, off by default, v0.7.2+). If a user is debugging in an offline / regulated environment and wants to be sure no analytics fire, set `DO_NOT_TRACK=1` or `telemetry.enabled: false` in `~/.drt/config.yml`. drt also honours the `DO_NOT_TRACK` standard env var with no config.

## Reference

- `docs/llm/CONTEXT.md` for architecture and key concepts.
- `docs/llm/API_REFERENCE.md` for all config fields.
- `docs/connectors/` for per-connector details (auth, sync modes, gotchas).
