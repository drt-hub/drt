# Klaviyo Destination

> Upsert profiles into Klaviyo (v3 API) — sync DWH customer segments (LTV, churn risk, plan) to the email/SMS marketing platform. Core connector — no extra install (uses `httpx`).

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
| `email_field` | string | `"email"` | Row field used as the profile identifier. |
| `properties_template` | string \| null | null | Jinja2 JSON template → custom profile `properties`. When omitted, **all row fields except `email_field`** are sent as custom properties. |
| `list_id` / `list_id_env` | string \| null | null | When set, each upserted profile is added to this Klaviyo list. |
| `revision` | string | `"2024-10-15"` | Klaviyo API revision (sent as the `revision` header). |
| `retry` | RetryConfig \| null | null | Per-destination override of `sync.retry`. |

## Authentication

Create a [private API key](https://www.klaviyo.com/settings/account/api-keys) with profile + list write access:

```bash
export KLAVIYO_API_KEY="pk_..."
```

## How upsert works

Each row is upserted **by email**, one record per request:

1. `POST /api/profiles/` to create the profile.
2. On `409` (the email already exists), the existing profile id is read from the error's `meta.duplicate_profile_id`, and the profile is updated with `PATCH /api/profiles/{id}/`.
3. If `list_id` is set, the profile is added via `POST /api/lists/{list_id}/relationships/profiles/`.

Per-record failures land in `result.row_errors` with the HTTP status (`on_error: skip` continues / `fail` stops).

## Rate limiting

**Vendor limit:** 75 requests/second (burst) on the profiles endpoint, 700/minute steady. drt applies **no automatic cap** here — set one explicitly:

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
- Calls are **per profile** — set `sync.rate_limit.requests_per_second` to respect Klaviyo's limit (75 req/s).
- Each row must include a non-empty `email_field` value; rows without one are recorded as errors.
- `sync.mode: mirror` and event tracking (`/api/events/`) are not implemented — follow-ups.
- `--dry-run` is honoured — `destination.load()` is never called when dry_run is on.
