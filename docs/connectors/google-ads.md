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
