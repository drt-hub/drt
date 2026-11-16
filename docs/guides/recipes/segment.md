# Segment (HTTP Tracking API `batch`)

Send warehouse events or user traits to [Segment's HTTP Tracking API](https://segment.com/docs/connections/sources/catalog/libraries/server/http-api/) (`/v1/batch`) using [`rest_api`](../../connectors/rest-api.md).

## Prerequisites

Segment authenticates HTTP Tracking API requests with **HTTP Basic Auth** where your **Write Key** is the username and the password is empty (`""`):

```bash
export SEGMENT_WRITE_KEY="your-segment-write-key"
export SEGMENT_WRITE_PASSWORD=""
```

## Sync YAML

Save as `syncs/segment_events.yml`:

```yaml
name: segment_events
description: Send warehouse product events to Segment HTTP Tracking API
model: models/product_events.sql

destination:
  type: rest_api
  url: https://api.segment.io/v1/batch
  method: POST
  headers:
    Content-Type: application/json
  auth:
    type: basic
    username_env: SEGMENT_WRITE_KEY
    password_env: SEGMENT_WRITE_PASSWORD
  body_mode: batch
  max_records_per_request: 100
  batch_template: |
    {
      "batch": [
        {% for row in rows %}
        {
          "type": "{{ row.type | default('track') }}",
          "userId": "{{ row.user_id }}",
          "event": "{{ row.event_name }}",
          "messageId": "{{ row.message_id }}",
          "timestamp": "{{ row.occurred_at }}",
          "properties": {{ row.properties | tojson }}
        }{% if not loop.last %},{% endif %}
        {% endfor %}
      ]
    }

sync:
  mode: incremental
  cursor_field: occurred_at
  batch_size: 100
  rate_limit:
    requests_per_second: 10
  on_error: skip
  dlq:
    enabled: true
```

## Limits & Gotchas

- **Basic Auth with empty password:** Segment expects `Basic base64("<write_key>:")`. Keep `SEGMENT_WRITE_PASSWORD=""` (an empty string) in your environment or `.drt/secrets.toml`.
- **Batch payload size limits:** Segment enforces a maximum total payload size per `/v1/batch` request and a per-message size cap; keep `max_records_per_request` conservative (e.g., `100` or lower if `properties` are large) and check the [Segment HTTP API Batch docs](https://segment.com/docs/connections/sources/catalog/libraries/server/http-api/#batch) for current limits.
- **Deduplication via `messageId`:** Always include a deterministic `messageId` per row so Segment deduplicates retries within its deduplication window instead of double-counting events.
- **Partial failures:** `/v1/batch` returns `200 OK` on accepted batches and does not return per-record error arrays for valid JSON batches, so `error_path` is not used here.
