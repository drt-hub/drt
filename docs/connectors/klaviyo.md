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
  retry_creates: false             # duplicate-risk escape hatch; keep false
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
| `time_field` | string \| null | null | Row field holding the event timestamp. A warehouse `TIMESTAMP`/`DATETIME` value is sent as ISO-8601; a date-only `DATE` value is rejected because Klaviyo requires a time component. Omitted from the payload when unset or null; Klaviyo then defaults to the current time. |
| `value_field` | string \| null | null | Row field holding the event's numeric `value`. Omitted when unset or null. |
| `unique_id_field` | string \| null | null | Row field holding Klaviyo's event deduplication key (`unique_id`). Required for `endpoint: event`; omitted when the configured row value is null. |
| `properties_template` | string \| null | null | Jinja2 JSON template → custom profile/event `properties`. When omitted, profile mode sends all row fields except `email_field`; event mode also excludes configured metric/time/value/unique-ID control fields. Event payloads always include `properties` (at least `{}`). |
| `list_id` / `list_id_env` | string \| null | null | For `endpoint: profile`, add each upserted profile to this Klaviyo list. |
| `backfill` | boolean | `false` | For `endpoint: event` only. When `true`, suppresses live flow/automation triggers for the event — use during a first full sync or a cursor-override replay of historical rows so existing flows don't re-send customer-facing messages for events that already happened. The event still counts toward metrics/segmentation. Rejected at config time if set with `endpoint: profile`. |
| `revision` | string | `"2026-07-15"` | Klaviyo API revision (sent as the `revision` header). Must be `"2026-07-15"` or later when `backfill: true` (enforced at config validation) — that's the revision Klaviyo introduced the field at. |
| `retry_creates` | boolean | `false` | After an ambiguous `create_only` profile or list-membership POST and an exact reconciliation miss, allow another blind POST. This can duplicate a profile/relationship; native profile upserts do not need it. |
| `retry` | RetryConfig \| null | null | Per-destination override of `sync.retry`. |
| `rate_limit` | RateLimitConfig \| null | null | Per-destination override of `sync.rate_limit`. |

## Authentication

Create a [private API key](https://www.klaviyo.com/settings/account/api-keys) with the profile/list or event write access needed by the selected endpoint. Profile `update_only` / `create_only` reconciliation needs `profiles:read`; list-membership reconciliation needs `lists:read` and `profiles:read`, in addition to the write scopes:

```bash
export KLAVIYO_API_KEY="pk_..."
```

## How upsert works

Each profile row is identified **by email**, one record per request. The three
`sync.match_policy` values use different API shapes:

- `upsert` (default) uses Klaviyo's documented server-side [Create or Update Profile](https://developers.klaviyo.com/en/v2026-07-15/reference/create_or_update_profile) endpoint, `POST /api/profile-import`. The same non-empty email and payload are reused across attempts, so a retry updates the same logical profile instead of creating another one.
- `create_only` uses `POST /api/profiles/`; `409` is a policy skip. An ambiguous response is reconciled with the documented exact-email [Get Profiles filter](https://developers.klaviyo.com/en/v2026-07-15/reference/get_profiles): one ID is accepted, multiple IDs are refused, and a miss fails delivery-unknown by default.
- `update_only` performs that exact lookup first; one ID receives `PATCH /api/profiles/{id}/`, no match is a policy skip, and multiple matches are refused.

If `list_id` is set, the profile is added via `POST /api/lists/{list_id}/relationships/profiles/`. Because Klaviyo does not document replay idempotency for that POST, an ambiguous response is reconciled through [Get Profile IDs for List](https://developers.klaviyo.com/en/v2026-07-15/reference/get_profile_ids_for_list) using the same exact email. One expected profile ID is accepted; multiple, mismatched, or missing IDs fail closed.

Per-record failures land in `result.row_errors` with the HTTP status (`on_error: skip` continues / `fail` stops).

## Retry safety and operator recovery

| Operation | Classification | Replay behavior |
|---|---|---|
| `POST /api/profile-import` (`upsert`) | Native keyed upsert by non-empty email | 429, proven pre-send connection failures, transport errors, and configured 5xx responses retry normally with the same payload. Klaviyo documents 200 for update and 201 for create. |
| `POST /api/profiles/` (`create_only`) | Reconciliable create | 429 and proven pre-send connection failures retry. A read/write/response transport error or configured 5xx triggers exact-email lookup, never an immediate second create. |
| Profile search and `PATCH /api/profiles/{id}/` | Idempotent read / ID-addressed update | Normal retry policy. |
| List-membership POST | Reconciliable relationship create; vendor replay idempotency unverified | An ambiguous response triggers exact list-membership lookup; a miss fails closed. |
| `POST /api/events/` | Native keyed event | The required non-empty `unique_id_field` is reused with the same profile and metric. Klaviyo documents that tuple as its idempotency key, so ambiguous attempts retry normally and produce one logical event. |

When a RowError says delivery is unknown, assume the first POST may have worked. Check the exact email (and the configured list, if applicable), allow time for the result to become visible, remove/merge any duplicates, and only then use `drt retry` or manually replay the row. Klaviyo [documents delayed GET Profiles visibility for asynchronous profile-affecting calls](https://developers.klaviyo.com/en/v2024-10-15/docs/collect_email_and_sms_consent_via_api); absence immediately after an ambiguous write is therefore not proof that the write failed. The warehouse delivery ledger cannot resolve a response lost inside one `load()` call.

`destination.retry_creates: true` restores the legacy blind retry after a reconciliation miss and emits a duplicate-risk warning. It does not change native profile-import retries, safe searches, or ID-addressed updates.

This retry behavior is verified only against the shared in-memory HTTP fault-injection harness. No live Klaviyo account was available for verification.

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

Every event needs a non-empty email and metric name. Set either `metric_name_field` for a per-row name or `metric_name` for one constant name. `unique_id_field` is required: [Klaviyo documents](https://developers.klaviyo.com/en/reference/events_api_overview) that an omitted `unique_id` defaults to the event timestamp truncated to one second, which can silently discard distinct same-profile/metric events in that second and makes an ambiguous request retry non-idempotent. Use a stable source ID for each logical event. `time` and `value` are optional and omitted when their configured row value is null; datetime values are sent as ISO-8601 strings, date-only values are rejected, and numeric values are sent as numbers. Use a warehouse `TIMESTAMP`/`DATETIME` source column—not a `DATE` column—for `time_field`. Without a template, configured event control fields are excluded from `properties`; custom templates remain explicit. Event `properties` is always sent, using `{}` when there are no properties.

### Safe historical replay (`backfill`)

Sending historical rows through `endpoint: event` — a first full sync of years of past orders, or a `--cursor-value` override re-processing old warehouse rows — replays each one as a live event by default, which can re-trigger existing Klaviyo flows and send customers duplicate/backdated emails or texts for things that already happened. Set `backfill: true` for that run to suppress flow triggering while the events still land for metrics and segmentation:

```yaml
destination:
  type: klaviyo
  api_key_env: KLAVIYO_API_KEY
  endpoint: event
  email_field: email
  metric_name_field: event_name
  unique_id_field: event_id
  backfill: true   # historical replay — don't re-fire flows
```

Turn `backfill` back off (the default) once the historical load is done, so future incremental syncs of genuinely new events trigger flows normally.

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
