Help the user migrate from an existing Reverse ETL tool (Census, Hightouch, Polytomic, or custom scripts) to drt.

## Steps

1. Ask the user to share their existing sync configuration (screenshot, YAML, JSON, or description).

2. Map their existing config to drt equivalents using the table below.

3. Generate a valid `syncs/<name>.yml` for each sync.

4. Note any features that need manual setup (auth env vars, profiles.yml).

## Concept Mapping

### Census / Hightouch → drt

| Census / Hightouch concept | drt equivalent |
|---------------------------|----------------|
| Source (BigQuery model) | `model: ref('table')` or raw SQL |
| Destination connection | `destination.type` + auth config |
| Sync behavior: Full | `sync.mode: full` |
| Sync behavior: Append | `sync.mode: incremental` + `cursor_field` |
| Field mappings | `body_template` / `properties_template` (Jinja2) |
| Run schedule | `drt run` via cron or CI |
| Error notifications | `on_error: skip` + `drt status` |

### Auth migration

| Old tool style | drt equivalent |
|---------------|----------------|
| Stored API key in UI | `token_env: MY_TOKEN` + `export MY_TOKEN=...` |
| OAuth app | Use token from OAuth flow → `token_env` |
| Service account JSON | Set `GOOGLE_APPLICATION_CREDENTIALS` for BigQuery source |

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
  mode: full   # or incremental
  # ...
```

Then summarize:
- What manual steps are needed (env vars, `~/.drt/profiles.yml`)
- Any features the old tool had that drt doesn't support yet (flag these clearly)

## Reference

See `docs/llm/API_REFERENCE.md` for all destination types and fields.
