# Triggering drt syncs via HTTP

`drt serve` starts a lightweight HTTP endpoint so you can trigger syncs from webhooks, CI systems, or other orchestrators.

No extra dependencies — stdlib only.

## Start the server

```bash
drt serve --port 8080 --token-env DRT_WEBHOOK_TOKEN
```

- `--host` (default `127.0.0.1`)
- `--port` (default `8080`)
- `--token-env` (default `DRT_WEBHOOK_TOKEN`) — env var holding the bearer token. Empty or unset means no auth (local dev only).

```bash
export DRT_WEBHOOK_TOKEN="$(openssl rand -hex 32)"
drt serve --port 8080
```

## Endpoints

### `GET /health`

```bash
curl http://localhost:8080/health
# {"status": "ok", "version": "0.6.0-dev"}
```

### `POST /sync/<name>`

Trigger a sync by name. Optional `?dry_run=true` for preview.

```bash
curl -X POST http://localhost:8080/sync/sync_users \
  -H "Authorization: Bearer $DRT_WEBHOOK_TOKEN"
```

Response (success):
```json
{
  "sync_name": "sync_users",
  "status": "success",
  "rows_synced": 42,
  "rows_failed": 0,
  "duration_seconds": 1.5,
  "dry_run": false,
  "errors": []
}
```

### Status codes

| Code | Meaning |
|------|---------|
| 200  | sync succeeded |
| 207  | sync partial or failed (result body has details) |
| 400  | sync name missing from URL |
| 401  | bearer token missing or wrong (when auth enabled) |
| 404  | sync name not found in project |
| 423  | another sync is already running (one at a time) |
| 500  | unexpected error |

## Use cases

### GitHub webhook → run sync on push

```yaml
# .github/workflows/trigger-drt.yml
on:
  push:
    branches: [main]

jobs:
  trigger:
    runs-on: ubuntu-latest
    steps:
      - run: |
          curl -X POST $DRT_HOST/sync/sync_users \
            -H "Authorization: Bearer ${{ secrets.DRT_WEBHOOK_TOKEN }}"
```

### dbt Cloud job completion → sync downstream

Add a post-job webhook in dbt Cloud pointing to `https://your-drt-host/sync/sync_users` with the bearer token.

## Design notes

- **One sync at a time** — concurrent requests return 423 Locked. This keeps state consistent without needing a queue or DB.
- **No persistent state** beyond what `StateManager` already does (`.drt/state.json`).
- **Stdlib only** — `http.server.ThreadingHTTPServer`, no FastAPI/uvicorn. Keeps drt-core dependency-free.

For production, run behind a reverse proxy (nginx, Caddy) for TLS and rate limiting.