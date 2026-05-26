# Amplitude Destination

> Sync user properties or events from your warehouse to Amplitude via the Identify API or HTTP V2 API.

## YAML Example — user properties (Identify)

```yaml
destination:
  type: amplitude
  api_key_env: AMPLITUDE_API_KEY
  endpoint: identify
  user_id_field: user_id
  properties_template: |
    {
      "ltv_segment": "{{ row.ltv_segment }}",
      "plan": "{{ row.plan }}"
    }
```

## YAML Example — events (HTTP V2)

```yaml
destination:
  type: amplitude
  api_key_env: AMPLITUDE_API_KEY
  endpoint: event
  user_id_field: user_id
  event_type_field: event_name
  time_field: event_time
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"amplitude"` | — | Required |
| `api_key` | string \| null | null | Amplitude project API key (direct value) |
| `api_key_env` | string \| null | `AMPLITUDE_API_KEY` | Env var containing API key |
| `region` | `default\|eu` | `default` | API region (`api2.amplitude.com` vs `api.eu.amplitude.com`) |
| `endpoint` | `identify\|event` | `identify` | Identify API or HTTP V2 events API |
| `user_id_field` | string | `user_id` | Source column for Amplitude `user_id` |
| `device_id_field` | string \| null | null | Source column for Amplitude `device_id` |
| `event_type_field` | string \| null | null | Source column for `event_type` (required for `endpoint: event` unless `event_type` set) |
| `event_type` | string \| null | null | Constant event name (alternative to `event_type_field`) |
| `time_field` | string \| null | null | Source column for event `time` (milliseconds) |
| `insert_id_field` | string \| null | null | Source column for deduplication `insert_id` |
| `properties_template` | string \| null | null | Jinja2 template rendering a JSON object merged into `user_properties` or `event_properties` |
| `batch_size` | int | `1000` | Records per API request (clamped to 1–1000) |
| `min_id_length` | int \| null | null | Passed to Amplitude `options.min_id_length` when set |
| `retry` | RetryConfig \| null | null | Destination-level retry override |

## Authentication

Copy your project API key from Amplitude **Settings → Projects**, then:

```bash
export AMPLITUDE_API_KEY="your-api-key"
```

The API key is sent in the JSON request body (Amplitude does not use Bearer auth).

## Common Patterns

**Sync LTV segments and plan tier (user properties):**

```yaml
destination:
  type: amplitude
  api_key_env: AMPLITUDE_API_KEY
  endpoint: identify
  user_id_field: user_id
```

Warehouse columns not mapped to identity fields become `user_properties` automatically.

**Backfill historical events from SQL:**

```yaml
destination:
  type: amplitude
  endpoint: event
  event_type: warehouse_backfill
  user_id_field: user_id
  time_field: event_timestamp_ms
sync:
  rate_limit:
    requests_per_second: 10
```

**EU data residency:**

```yaml
destination:
  type: amplitude
  region: eu
  api_key_env: AMPLITUDE_API_KEY_EU
```

## Notes

- Use separate sync YAMLs for identify vs events (one `endpoint` per destination config).
- Amplitude requires `user_id` or `device_id` on each row; `user_id` defaults to minimum 5 characters unless `min_id_length` is set.
- `insert_id` is auto-generated from row content when not provided, enabling safe retries without duplicate events.
- For large backfills, set `rate_limit.requests_per_second` conservatively (e.g. 10) to avoid 429 throttling.
- See [HTTP V2 API](https://amplitude.com/docs/apis/analytics/http-v2) and [Identify API](https://amplitude.com/docs/apis/analytics/identify) for payload details.
