# Google Ads Destination

> Upload offline conversions to Google Ads (conversion action) per row. Core connector — no extra install.

## YAML Example

```yaml
destination:
  type: google_ads
  customer_id: "1234567890"                                    # no hyphens
  conversion_action: "customers/1234567890/conversionActions/456"
  gclid_field: gclid                     # row field with the click ID
  conversion_time_field: conversion_time # row field with the timestamp
  conversion_value_field: value          # optional row field with the value
  currency_code: USD
  developer_token_env: GOOGLE_ADS_DEVELOPER_TOKEN
  auth:
    type: oauth2_client_credentials
    # client id / secret / refresh token via env — see rest-api.md
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"google_ads"` | — | Required |
| `customer_id` | string | — | Google Ads customer ID (digits only, no hyphens). **Required** |
| `conversion_action` | string | — | Conversion action resource name (`customers/<id>/conversionActions/<id>`). **Required** |
| `gclid_field` | string | `"gclid"` | Row field holding the Google click ID. |
| `conversion_time_field` | string | `"conversion_time"` | Row field holding the conversion timestamp. |
| `conversion_value_field` | string \| null | null | Optional row field holding the conversion value. |
| `currency_code` | string | `"USD"` | Currency for the conversion value. |
| `developer_token_env` | string | `"GOOGLE_ADS_DEVELOPER_TOKEN"` | Env var holding the Google Ads developer token. Sent when set; no longer required (Google's Cloud-project-based access model made this header optional and server-ignored). |
| `auth` | AuthConfig \| null | null | Typically `oauth2_client_credentials` (client id/secret + refresh token). |
| `retry` | RetryConfig \| null | null | Per-destination override of `sync.retry`. |
| `native_idempotency_key` | string \| null | null | Jinja template rendered per row (`{{ row.<field> }}`, `{{ sync_name }}`) and sent as ClickConversion's `orderId` field. Makes retries of the same row safe: Google recognizes a reused `orderId` and returns `ORDER_ID_ALREADY_IN_USE` instead of reprocessing it, which drt counts as a successful delivery. |
| `cloud_project_id` | string \| null | null | The Google Cloud project that owns this config's OAuth client credentials — the real API quota boundary under Google's Cloud-project-based access model. Optional: when set, `drt run --threads N` on two configs sharing one project shares one rate-limit bucket even with different developer tokens or OAuth clients; when unset, the bucket falls back to `developer_token_env`'s name instead, which is only correct if configs sharing a project also share that name (see Notes). |

## Authentication

You need an **OAuth2** client. A **developer token** (from your Google Ads manager account) is sent as a request header when configured, but is no longer required — Google's Cloud-project-based access model made it optional and server-ignored:

```bash
export GOOGLE_ADS_DEVELOPER_TOKEN="..."
```

See [rest-api.md](rest-api.md) for the `oauth2_client_credentials` auth block (client id / secret / refresh-token env vars).

## Notes

- Core connector — no `pip install` extras needed.
- Each row becomes one offline conversion upload; `gclid` + `conversion_time` are required per conversion.
- Conversions can take time to appear in the Google Ads UI (standard attribution delay).
- Google no longer accepts *new* adopters of offline click-conversion imports via this endpoint (`ConversionUploadService.UploadClickConversions`) — since 2026-06-15, a developer token with no prior conversion-import activity gets `CUSTOMER_NOT_ALLOWLISTED_FOR_THIS_FEATURE`. Existing adopters are unaffected during Google's transition to the newer Data Manager API; there is no drt-side workaround for a newly-allowlisted account.
- `native_idempotency_key` becomes the conversion's `orderId`, which Google rejects if it looks like personally-identifiable information (`ORDER_ID_CONTAINS_PII`) — don't template it from an email address or phone number, even though those are common `upsert_key` choices elsewhere.
- `order_id`/`native_idempotency_key` isn't permitted at all when the conversion action uses an external attribution model (`ORDER_ID_NOT_PERMITTED_FOR_EXTERNALLY_ATTRIBUTED_CONVERSION_ACTION`) — leave the field unset for those conversion actions.
- Without `cloud_project_id`, the rate-limit bucket falls back to `developer_token_env`'s *name* — configs that leave it at the shared default (or otherwise use the same name) correctly share one conservative bucket, but two configs naming *different* env vars (even both empty/tokenless) get separate limiters even if they belong to the same real Cloud project, risking a real `429` if run concurrently. Set `cloud_project_id` explicitly whenever configs sharing one Cloud project don't already share one `developer_token_env` name — it's the only setting that reflects the real quota boundary regardless of how tokens are named.
