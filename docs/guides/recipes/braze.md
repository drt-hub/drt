# Braze (`/users/track`)

> Request shape verified against a local mock; not verified against the live API.

Sync user attributes or custom events from your warehouse to [Braze's `/users/track` endpoint](https://www.braze.com/docs/api/endpoints/user_data/post_user_track/) using [`rest_api`](../../connectors/rest-api.md).

## Prerequisites

Create a Braze REST API key with `users.track` permission and export it (or store it in `.drt/secrets.toml`):

```bash
export BRAZE_API_KEY="your-braze-rest-api-key"
```

Replace `rest.iad-01.braze.com` in the URL below with your [Braze REST endpoint](https://www.braze.com/docs/api/basics/#endpoints) for your cluster.

## Sync YAML

Save as `syncs/braze_users.yml`:

```yaml
name: braze_users
description: Sync user attributes to Braze /users/track
model: models/braze_user_attributes.sql

destination:
  type: rest_api
  url: https://rest.iad-01.braze.com/users/track
  method: POST
  headers:
    Content-Type: application/json
  auth:
    type: bearer
    token_env: BRAZE_API_KEY
  body_mode: batch
  max_records_per_request: 75
  error_path: errors
  batch_template: |
    {
      "attributes": [
        {% for row in rows %}
        {
          "external_id": "{{ row.external_id }}",
          "email": "{{ row.email }}",
          "plan_tier": "{{ row.plan_tier }}",
          "_update_existing_only": false
        }{% if not loop.last %},{% endif %}
        {% endfor %}
      ]
    }

sync:
  mode: incremental
  cursor_field: updated_at
  batch_size: 75
  rate_limit:
    requests_per_second: 10
  on_error: skip
  dlq:
    enabled: true
```

## Limits & Gotchas

- **Cluster-specific base URL:** Braze assigns different REST API hosts per instance (`rest.iad-01.braze.com`, `rest.fra-01.braze.eu`, etc.). Check your dashboard and [Braze API Endpoints](https://www.braze.com/docs/api/basics/#endpoints).
- **Per-request object cap (75 total):** `/users/track` caps each request at **75 objects total** combined across `attributes`, `events`, and `purchases`; keep `max_records_per_request: 75` or lower ([Braze `/users/track` Request Parameters](https://www.braze.com/docs/api/endpoints/user_data/post_user_track/#request-parameters)).
- **Rate limits & data points:** Braze enforces per-endpoint rate limits ([Braze `/users/track` Rate Limit](https://www.braze.com/docs/api/endpoints/user_data/post_user_track/#rate-limit)) and bills custom attributes/events as [data points](https://www.braze.com/docs/user_guide/data/data_points/)—only include changed columns in your model query and set `_update_existing_only: true` if you don't want to create new profiles.
- **Per-record errors (`error_path: errors`):** For non-fatal per-object validation failures, Braze returns an `errors` array in the JSON response body alongside processed counts (see [Braze `/users/track` Responses](https://www.braze.com/docs/api/endpoints/user_data/post_user_track/#responses) and [Braze API Errors](https://www.braze.com/docs/api/errors/)); `error_path: errors` surfaces those messages in `drt`'s DLQ/error summary.
