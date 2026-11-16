# Customer.io (Track API v2 `batch`)

Identify customers or send events from your warehouse to [Customer.io's Track API v2 (`/api/v2/batch`)](https://customer.io/docs/api/track/#operation/batch) using [`rest_api`](../../connectors/rest-api.md).

## Prerequisites

Customer.io Track API authenticates with **HTTP Basic Auth** using your **Site ID** as the username and your **Tracking API Key** as the password:

```bash
export CUSTOMERIO_SITE_ID="your-site-id"
export CUSTOMERIO_API_KEY="your-tracking-api-key"
```

Use `https://track-eu.customer.io/api/v2/batch` if your workspace is in the EU region.

## Sync YAML

Save as `syncs/customer_io_people.yml`:

```yaml
name: customer_io_people
description: Sync customer attributes to Customer.io Track API v2
model: models/customer_profiles.sql

destination:
  type: rest_api
  url: https://track.customer.io/api/v2/batch
  method: POST
  headers:
    Content-Type: application/json
  auth:
    type: basic
    username_env: CUSTOMERIO_SITE_ID
    password_env: CUSTOMERIO_API_KEY
  body_mode: batch
  max_records_per_request: 100
  error_path: errors
  batch_template: |
    {
      "batch": [
        {% for row in rows %}
        {
          "type": "person",
          "identifiers": {
            "id": "{{ row.user_id }}"
          },
          "action": "identify",
          "attributes": {
            "email": "{{ row.email }}",
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

## Limits & Gotchas

- **EU data center URL:** Use `https://track-eu.customer.io/api/v2/batch` for EU workspaces instead of `track.customer.io`.
- **Batch payload & item size caps:** `/api/v2/batch` enforces a maximum total request size and per-item payload size; keep `max_records_per_request` conservative (e.g., `100` or lower) and check the [Customer.io Track API Batch docs](https://customer.io/docs/api/track/#operation/batch) for current limits.
- **Rate limits & partial failures:** Customer.io returns `207 Multi-Status` or `400` with an `errors` array when individual items in a batch fail validation; `error_path: errors` surfaces those per-item errors in `drt`. See the [Customer.io Track API Rate Limits](https://customer.io/docs/api/track/#section/Limits) for current quotas.
