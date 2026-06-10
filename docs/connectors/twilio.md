# Twilio SMS Destination

> Send SMS messages via the Twilio REST API — one message per source row,
> with Jinja2-templated recipient number and body.

## YAML Example

```yaml
destination:
  type: twilio
  account_sid_env: TWILIO_ACCOUNT_SID
  auth_token_env: TWILIO_AUTH_TOKEN
  from_number: "+15551234567"
  to_template: "{{ row.phone }}"
  message_template: "Hi {{ row.name }}, your order {{ row.order_id }} has shipped."
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"twilio"` | — | Required |
| `account_sid` | string \| null | null | Account SID (direct value) |
| `account_sid_env` | string \| null | null | Env var holding the Account SID |
| `auth_token` | string \| null | null | Auth token (direct value) |
| `auth_token_env` | string \| null | null | Env var holding the auth token |
| `from_number` | string | — | Twilio sending number in **E.164** format (`+15551234567`) |
| `to_template` | string | — | Jinja2 template → recipient number (E.164) |
| `message_template` | string | — | Jinja2 template → SMS body |
| `retry` | object \| null | null | Per-destination retry override |

One of `account_sid` / `account_sid_env` and one of `auth_token` /
`auth_token_env` are required (enforced at config load).

## Authentication

Twilio uses HTTP Basic auth: the Account SID is the username and the
auth token is the password. Find both in the
[Twilio Console](https://console.twilio.com/). Store them in env vars:

```bash
export TWILIO_ACCOUNT_SID="ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
export TWILIO_AUTH_TOKEN="your-auth-token"
```

Requests go to `https://api.twilio.com/2010-04-01/Accounts/<SID>/Messages.json`.

## Common Patterns

**Recipient from a source column** (must already be E.164):
```yaml
to_template: "{{ row.phone_e164 }}"
```

**Conditional body** with Jinja2:
```yaml
message_template: >
  {% if row.status == 'shipped' %}Your order shipped!{% else %}Order received.{% endif %}
```

## Notes

- (core) — no extra install required.
- Both `from_number` and the rendered `to_template` must be **E.164**
  (`+<country><number>`, no spaces or dashes) or Twilio rejects the
  message.
- One API call per row. SMS is billed per message — use `--dry-run` to
  preview the rendered recipients/bodies before a real run.
- Per-row send failures (invalid number, unsubscribed recipient) are
  recorded in `result.row_errors`; `on_error: skip` continues the batch.
- For high volume, set `rate_limit.requests_per_second` to match your
  Twilio account's messaging throughput.

## References

- [Twilio Messages API](https://www.twilio.com/docs/sms/api/message-resource)
- [E.164 formatting](https://www.twilio.com/docs/glossary/what-e164)
