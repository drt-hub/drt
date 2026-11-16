# Iterable (`/api/users/bulkUpdate` & `/api/events/trackBulk`)

Sync user profiles or bulk events from your warehouse to [Iterable's `/api/users/bulkUpdate`](https://api.iterable.com/api/docs#users_bulkUpdate) and [`/api/events/trackBulk`](https://api.iterable.com/api/docs#events_trackBulk) using [`rest_api`](../../connectors/rest-api.md).

## Prerequisites

Create an Iterable Server-side API key and export it (or store it in `.drt/secrets.toml`):

```bash
export ITERABLE_API_KEY="your-iterable-api-key"
```

Use `https://api.eu.iterable.com` if your Iterable project is hosted in the EU data center.

## Sync YAML (Users Bulk Update)

Save as `syncs/iterable_users.yml`:

```yaml
name: iterable_users
description: Sync user profile attributes to Iterable /api/users/bulkUpdate
model: models/iterable_users.sql

destination:
  type: rest_api
  url: https://api.iterable.com/api/users/bulkUpdate
  method: POST
  headers:
    Content-Type: application/json
  auth:
    type: api_key
    header: Api-Key
    value_env: ITERABLE_API_KEY
  body_mode: batch
  max_records_per_request: 100
  error_path: failedUpdates
  batch_template: |
    {
      "users": [
        {% for row in rows %}
        {
          "userId": "{{ row.user_id }}",
          "email": "{{ row.email }}",
          "dataFields": {
            "plan": "{{ row.plan }}"
          }
        }{% if not loop.last %},{% endif %}
        {% endfor %}
      ]
    }

sync:
  mode: incremental
  cursor_field: updated_at
  batch_size: 100
  rate_limit:
    requests_per_second: 10
  on_error: skip
  dlq:
    enabled: true
```

To sync custom events instead, point `url` to `https://api.iterable.com/api/events/trackBulk` and replace the top-level `"users"` array in `batch_template` with `"events"` (`email` / `userId`, `eventName`, `id` for deduplication, `createdAt`, `dataFields`).

## Limits & Gotchas

- **Authentication header:** Iterable uses `Api-Key: <key>` (`auth.type: api_key`, `header: Api-Key`), not `Authorization: Bearer`.
- **EU endpoint:** Use `https://api.eu.iterable.com/api/...` for EU-hosted projects.
- **Batch size & rate limits:** Iterable caps `/api/users/bulkUpdate` and `/api/events/trackBulk` per-request record counts and payload sizes; check the [Iterable API Docs](https://api.iterable.com/api/docs) for current limits.
- **Partial failures (`failedUpdates`):** `/api/users/bulkUpdate` reports failed records in `failedUpdates` (on `/api/events/trackBulk`, check `disallowedEventNames` / `failCount`).
- **Event deduplication:** On `/api/events/trackBulk`, include a deterministic `id` field per event so retries do not create duplicate events.
