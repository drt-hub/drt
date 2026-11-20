# Customer.io (Track API v2 `batch`)

> Request shape verified against a local mock; not verified against the live API.

Identify customers or send events from your warehouse to [Customer.io's Track API v2 (`/api/v2/batch`)](https://docs.customer.io/api/track/#operation/batch) using [`rest_api`](../../connectors/rest-api.md).

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

- **EU data center URL:** Use `https://track-eu.customer.io/api/v2/batch` for EU workspaces instead of `track.customer.io` ([Customer.io Track API Regions](https://docs.customer.io/api/track/#section/Regions)).
- **Batch payload & item size caps:** `/api/v2/batch` caps total request size at **500 KB** and individual items at **32 KB** ([Customer.io Track API `/api/v2/batch`](https://docs.customer.io/api/track/#operation/batch)); keep `max_records_per_request` conservative (e.g., `100` or lower) so batches stay under 500 KB.
- **Rate limits & partial failures:** Per the [Customer.io Track API `/api/v2/batch` response docs](https://docs.customer.io/api/track/#operation/batch), partial batch validation failures return `207 Multi-Status` with an `errors` array (while malformed requests return `400`); setting `error_path: errors` surfaces those per-item errors in `drt`. See [Customer.io Track API Limits](https://docs.customer.io/api/track/#section/Limits) for current rate quotas.
