# Meta Conversions Destination

> Send server-side conversion events from a warehouse to a Meta Pixel through the Conversions API. Core connector — no extra install (uses `httpx`).

## YAML Example

```yaml
destination:
  type: meta_conversions
  pixel_id: "123456789012345"
  access_token_env: META_CONVERSIONS_ACCESS_TOKEN
  api_version: v25.0
  action_source: website
  event_name: Purchase          # or event_name_field: event_name
  event_time_field: occurred_at # Unix seconds; current time when unset
  event_id_field: event_id      # required: retries need a stable deduplication id
  event_source_url_field: page_url # required when action_source is website
  email_field: email
  phone_field: phone
  client_ip_address_field: client_ip
  client_user_agent_field: user_agent # required when action_source is website
  fbc_field: fbc
  fbp_field: fbp
  value_field: revenue
  currency: USD
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"meta_conversions"` | — | Required. |
| `pixel_id` | string | — | Meta Pixel/data-source id. **Required.** |
| `access_token` / `access_token_env` | string \| null | `access_token_env: META_CONVERSIONS_ACCESS_TOKEN` | Long-lived Conversions API token. Prefer the `_env` form. One must resolve at runtime. |
| `api_version` | string | `"v25.0"` | Graph API version in the request path. Configurable so it can be advanced without a drt release. |
| `action_source` | string | `"website"` | Meta event origin. Common values include `website`, `app`, `phone_call`, `chat`, `email`, `physical_store`, `system_generated`, and `other`. |
| `event_name` | string \| null | null | Fixed Meta standard/custom event name. Exactly one of this and `event_name_field` is required. |
| `event_name_field` | string \| null | null | Row field containing the event name. Exactly one of this and `event_name` is required. |
| `event_time_field` | string \| null | null | Row field containing a Unix timestamp in seconds. When unset, drt uses the current time. Meta accepts times up to seven days old. |
| `event_id_field` | string | — | Row field containing a stable conversion id. **Required** so Meta can deduplicate a retry when the first response is lost. Use the browser Pixel event id for Pixel + Conversions API deduplication. |
| `event_source_url_field` | string \| null | null | Row field containing the page URL where the event occurred. **Required when `action_source` is `website` (including its default), and every website-event row must resolve it to a non-empty value.** Optional for other action sources. |
| `email_field` | string \| null | null | Row field mapped to hashed `user_data.em`. At least one of the six customer-information mappings (`email_field` through `fbp_field`) must be configured, and every row must resolve at least one mapping to a non-empty value. |
| `phone_field` | string \| null | null | Row field mapped to hashed `user_data.ph`; counts toward the required customer-information mapping. |
| `client_ip_address_field` | string \| null | null | Row field mapped unchanged to `user_data.client_ip_address`; counts toward the required customer-information mapping. |
| `client_user_agent_field` | string \| null | null | Row field mapped unchanged to `user_data.client_user_agent`; counts toward the required customer-information mapping. **Required when `action_source` is `website` (including its default), and every website-event row must resolve it to a non-empty value.** Optional for other action sources. |
| `fbc_field` | string \| null | null | Row field mapped unchanged to `user_data.fbc`; counts toward the required customer-information mapping. |
| `fbp_field` | string \| null | null | Row field mapped unchanged to `user_data.fbp`; counts toward the required customer-information mapping. |
| `value_field` | string \| null | null | Optional row field mapped to `custom_data.value`. |
| `currency` | string | `"USD"` | Currency paired with `custom_data.value`. |
| `retry` | RetryConfig \| null | null | Per-destination override of `sync.retry`. |
| `rate_limit` | RateLimitConfig \| null | null | Per-destination override of `sync.rate_limit`. |

## Authentication

Generate a long-lived Conversions API access token for the Pixel and keep it outside YAML:

```bash
export META_CONVERSIONS_ACCESS_TOKEN="..."
```

drt sends the resolved token in the `Authorization: Bearer <token>` header on
`POST https://graph.facebook.com/<api_version>/<pixel_id>/events`; it is never
placed in the request URL or query parameters. This is not an OAuth2
client-credentials exchange.
`access_token_env` also accepts a [secret-provider URI](../guides/secret-provider-uris.md).

## Hashing and normalization

drt applies Meta's required normalization before hashing matching identifiers:

- Email (`em`): trim leading/trailing whitespace, lowercase, UTF-8 encode, then SHA-256 hex digest. The digest is sent in a one-item list.
- Phone (`ph`): remove every non-ASCII digit, UTF-8 encode the resulting digits (including country code), then SHA-256 hex digest. The digest is sent in a one-item list.

`client_ip_address`, `client_user_agent`, `fbc`, and `fbp` are **not hashed**.
They are forwarded as plain text when their field mappings are configured.
Pre-hashing or double-hashing those four values is incorrect.

## Batching and errors

drt sends up to 1000 events in each request. Local mapping failures (for
example, an empty configured event name, event id, or customer-information
field, an event time older than seven days, a non-finite conversion value, or
any payload value that cannot be serialized to strict JSON) are attributed to
that row. HTTP 400 responses are retried only when Meta's
error envelope explicitly sets `error.is_transient` to `true`; missing, false,
malformed, and non-JSON classifications fail immediately.
Meta's synchronous response reports an aggregate `events_received` count but
does not document an event-indexed partial-failure array, so request failures
and acknowledgement-count mismatches fail the submitted batch atomically; drt
does not guess which event failed.

## Rate limiting

drt applies one limiter slot per HTTP batch and acquires again on every retry.
The process-wide limiter is shared per `pixel_id`, so concurrent syncs into the
same Pixel use one request budget. Configure a limit appropriate for the Meta
account when needed:

```yaml
destination:
  type: meta_conversions
  pixel_id: "123456789012345"
  event_name: Purchase
  event_id_field: event_id
  event_source_url_field: page_url
  client_user_agent_field: user_agent
  email_field: email
  rate_limit:
    requests_per_second: 5
```

`destination.rate_limit` beats `sync.rate_limit`, which beats drt's default.

## Notes

- Core connector — no `pip install` extras needed.
- Every row must provide a non-empty stable value in `event_id_field`; invalid rows are not sent. The value should match the browser Pixel event id when using Meta's 48-hour Pixel+CAPI deduplication window.
- Meta Graph API versions expire on a roughly two-year cycle. Review the [Meta version schedule](https://developers.facebook.com/docs/graph-api/changelog/versions/) periodically and override or bump `api_version` before `v25.0` expires.
- This connector covers the core synchronous Pixel events endpoint. Meta's Payload Helper, Events Manager UI workflows, and app-event-specific fields beyond the generic configurable `action_source` are out of scope.
