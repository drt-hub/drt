# SendGrid Destination

> Send a templated email per row (or add contacts to a list) via the SendGrid API. Core connector — no extra install.

## YAML Example

```yaml
destination:
  type: sendgrid
  from_email: noreply@example.com
  from_name: Example
  subject_template: "Welcome, {{ row.name }}"
  body_template: "Hi {{ row.name }}, thanks for signing up!"
  to_email_field: email          # row field holding the recipient address
  auth:
    type: bearer
    token_env: SENDGRID_API_KEY
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"sendgrid"` | — | Required |
| `from_email` | string | — | Verified sender address. **Required** |
| `from_name` | string \| null | null | Sender display name. |
| `subject_template` | string | — | Jinja2 template for the email subject. **Required** |
| `body_template` | string | — | Jinja2 template for the email body. **Required** |
| `to_email_field` | string | `"email"` | Row field containing the recipient address. |
| `list_ids` | list[str] \| null | null | When set, contacts are added to these SendGrid marketing list IDs (contact-upsert mode) instead of (or in addition to) sending mail. |
| `auth` | Bearer | bearer | Token auth — set `token_env` to your SendGrid API key. |
| `retry_creates` | boolean | `false` | Allow a blind Mail Send replay after an ambiguous failure. Can send duplicate email and logs a warning. |
| `retry` | RetryConfig \| null | null | Per-destination override of `sync.retry`. |

## Authentication

Create an API key in **Settings → API Keys** (Mail Send and/or Marketing scopes):

```bash
export SENDGRID_API_KEY="SG.xxxxx"
```

## Rate limiting

**Vendor limit:** plan-dependent — SendGrid meters the v3 Mail Send endpoint per API key (commonly ~600 requests/minute, i.e. 10/s, on lower tiers). Check your plan. drt applies **no automatic cap** here — set one explicitly:

```yaml
destination:
  type: sendgrid
  rate_limit:
    requests_per_second: 10
    burst: 20                # optional: let idle time bank up to 20 requests
```

`destination.rate_limit` beats `sync.rate_limit`, which beats the default of 10/s.

The limiter is shared per **API key**: the sending quota belongs to the key, not the From address, so two syncs sending as different senders on one key share one bucket even under `drt run --threads 4`. When they request different rates, the lowest wins for both.

## Retry safety

`POST /v3/mail/send` is a **blind side effect**. SendGrid's documented
[`batch_id`](https://www.twilio.com/docs/sendgrid/for-developers/sending-email/scheduling-parameters)
groups scheduled sends for pause/cancellation, while
[`custom_args`](https://www.twilio.com/docs/sendgrid/for-developers/sending-email/unique-arguments)
are tracking metadata; neither is documented as a replay-dedup key for Mail Send.
After a dropped response or retryable 5xx, drt returns a delivery-unknown
`RowError` after one request. Assume the email may have been delivered, inspect
SendGrid activity before replay, and do not auto-resend.

HTTP 429 and proven pre-send connection failures remain retryable.
`retry_creates: true` restores the legacy blind second send and logs a
duplicate-email warning. The warehouse ledger cannot resolve a response lost
inside this request. See [retry safety](../guides/retry-safety.md). Vendor
contract rechecked 2026-11-23.

## Notes

- Core connector — no `pip install` extras needed.
- The sender address/domain must be verified in SendGrid first, or sends are rejected.
- One API call per row; use `sync.rate_limit` to respect your plan's limits.
