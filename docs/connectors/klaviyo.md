# Klaviyo Destination

> Upsert profiles or track behavioral events in Klaviyo — sync DWH customer segments and warehouse-computed events to the email/SMS marketing platform. Core connector — no extra install (uses `httpx`).

## YAML Example

```yaml
destination:
  type: klaviyo
  api_key_env: KLAVIYO_API_KEY
  email_field: email
  properties_template: |
    {"ltv_segment": "{{ row.ltv_segment }}", "plan": "{{ row.plan }}"}
  list_id_env: KLAVIYO_LIST_ID     # optional
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"klaviyo"` | — | Required |
| `api_key` / `api_key_env` | string \| null | `api_key_env: KLAVIYO_API_KEY` | Private API key (`Authorization: Klaviyo-API-Key …`). Prefer the `_env` form. One is required. |
| `endpoint` | `"profile"` \| `"event"` | `"profile"` | Upsert a profile or send a behavioral event. |
| `email_field` | string | `"email"` | Row field used as the profile identifier. |
| `metric_name_field` | string \| null | null | Row field holding the event/metric name (maximum 128 characters) for `endpoint: event`. Required unless `metric_name` is set. |
| `metric_name` | string \| null | null | Constant event/metric name (maximum 128 characters) for `endpoint: event`. Alternative to `metric_name_field`. |
| `time_field` | string \| null | null | Row field holding the event timestamp. Omitted from the payload when unset or null; Klaviyo then defaults to the current time. |
| `value_field` | string \| null | null | Row field holding the event's numeric `value`. Omitted when unset or null. |
| `unique_id_field` | string \| null | null | Row field holding Klaviyo's event deduplication key (`unique_id`). Required for `endpoint: event`; omitted when the configured row value is null. |
| `properties_template` | string \| null | null | Jinja2 JSON template → custom profile/event `properties`. When omitted, profile mode sends all row fields except `email_field`; event mode also excludes configured metric/time/value/unique-ID control fields. Event payloads always include `properties` (at least `{}`). |
| `list_id` / `list_id_env` | string \| null | null | For `endpoint: profile`, add each upserted profile to this Klaviyo list. |
| `revision` | string | `"2024-10-15"` | Klaviyo API revision (sent as the `revision` header). |
| `retry` | RetryConfig \| null | null | Per-destination override of `sync.retry`. |
| `rate_limit` | RateLimitConfig \| null | null | Per-destination override of `sync.rate_limit`. |

## Authentication

Create a [private API key](https://www.klaviyo.com/settings/account/api-keys) with the profile/list or event write access needed by the selected endpoint:

```bash
export KLAVIYO_API_KEY="pk_..."
```

## How upsert works

Each row is upserted **by email**, one record per request:

1. `POST /api/profiles/` to create the profile.
2. On `409` (the email already exists), the existing profile id is read from the error's `meta.duplicate_profile_id`, and the profile is updated with `PATCH /api/profiles/{id}/`.
3. If `list_id` is set, the profile is added via `POST /api/lists/{list_id}/relationships/profiles/`.

Per-record failures land in `result.row_errors` with the HTTP status (`on_error: skip` continues / `fail` stops).

## Event tracking

`endpoint: event` sends one JSON:API event per row to `POST /api/events/`. This lets a warehouse-computed behavior such as an abandoned cart or plan upgrade trigger a Klaviyo flow. A successful create returns `202 Accepted`.

```yaml
destination:
  type: klaviyo
  api_key_env: KLAVIYO_API_KEY
  endpoint: event
  email_field: email
  metric_name_field: event_name
  time_field: occurred_at       # optional
  value_field: amount           # optional
  unique_id_field: event_id     # required stable deduplication key
  properties_template: |
    {"cart_id": "{{ row.cart_id }}", "plan": "{{ row.plan }}"}
```

Every event needs a non-empty email and metric name. Set either `metric_name_field` for a per-row name or `metric_name` for one constant name. `unique_id_field` is required: [Klaviyo documents](https://developers.klaviyo.com/en/reference/events_api_overview) that an omitted `unique_id` defaults to the event timestamp truncated to one second, which can silently discard distinct same-profile/metric events in that second and makes an ambiguous request retry non-idempotent. Use a stable source ID for each logical event. `time` and `value` are optional and omitted when their configured row value is null; date/datetime values are sent as ISO-8601 strings and numeric values are sent as numbers. Without a template, configured event control fields are excluded from `properties`; custom templates remain explicit. Event `properties` is always sent, using `{}` when there are no properties.

## Rate limiting

**Vendor limits depend on the selected endpoint:**

- `endpoint: profile`: 75 requests/second burst, 700/minute steady.
- `endpoint: event`: 350 requests/second burst, 3500/minute steady.

drt applies **no endpoint-specific automatic cap** here — set one explicitly:

```yaml
destination:
  type: klaviyo
  rate_limit:
    requests_per_second: 10
    burst: 75                # optional: match Klaviyo's burst allowance
```

`destination.rate_limit` beats `sync.rate_limit`, which beats the default of 10/s.

The limiter is shared per **account** (API key) — `list_id` is deliberately excluded, since the quota is account-wide. Several syncs into one account concurrently (`drt run --threads 4`) pace through one bucket instead of one bucket each. When they request different rates, the lowest wins for both.

## Notes

- Core connector — no `pip install` extras needed.
- Calls are **per profile or event** — set `sync.rate_limit.requests_per_second` to respect the selected endpoint's limit.
- Each row must include a non-empty `email_field` value; rows without one are recorded as errors.
- `sync.mode: mirror` is not implemented — follow-up.
- `--dry-run` is honoured — `destination.load()` is never called when dry_run is on.
