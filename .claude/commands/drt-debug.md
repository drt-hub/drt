Debug a failing drt sync.

## Steps

1. Ask the user to share (or read from context):
   - The error output from `drt run` or `drt status`
   - The sync YAML (`syncs/<name>.yml`)
   - The `drt_project.yml` if relevant

2. Diagnose the root cause using the patterns below.

3. Suggest a concrete fix with the corrected YAML or command.

## Common Error Patterns

### Auth errors (401, 403)
- **Cause**: `token_env` or `value_env` env var not set, or token has wrong permissions
- **Fix**: Check `echo $MY_TOKEN`, verify token scopes. For HubSpot, confirm Private App has CRM write scope. For GitHub, confirm `actions: write`.

### Rate limit (429)
- **Cause**: Sending too fast for the destination's limits
- **Fix**: Lower `sync.rate_limit.requests_per_second`. HubSpot max: 9 req/s. GitHub Actions: 5 req/s.
- **Also**: Add retry config — 429 is retryable by default.

### Connection errors / timeouts
- **Cause**: Wrong URL, network issue, or destination is down
- **Fix**: Verify `url` with `curl -X POST <url>` manually. Check `drt run --dry-run` to confirm config parses correctly.

### Template errors
- **Cause**: `{{ row.field_name }}` references a column that doesn't exist in the source
- **Fix**: Run `drt run --dry-run` to preview rows, confirm column names match the template.

### Incremental sync not filtering
- **Cause**: `mode: incremental` set but no saved cursor yet (first run syncs all rows)
- **Fix**: This is expected on the first run. Check `drt status` after first run — `last_cursor_value` should be set.

### `on_error: fail` stopping early
- **Cause**: Default behavior — first failure stops the sync
- **Fix**: Change to `on_error: skip` to continue past failures and see full error count.

### Profile not found
- **Cause**: `~/.drt/profiles.yml` missing or profile name mismatch
- **Fix**: Check `cat ~/.drt/profiles.yml`, verify the profile name matches `drt_project.yml`.

## Reference

See `docs/llm/CONTEXT.md` for architecture and key concepts.
See `docs/llm/API_REFERENCE.md` for all config fields.
