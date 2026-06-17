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
| `retry` | RetryConfig \| null | null | Per-destination override of `sync.retry`. |

## Authentication

Create an API key in **Settings → API Keys** (Mail Send and/or Marketing scopes):

```bash
export SENDGRID_API_KEY="SG.xxxxx"
```

## Notes

- Core connector — no `pip install` extras needed.
- The sender address/domain must be verified in SendGrid first, or sends are rejected.
- One API call per row; use `sync.rate_limit` to respect your plan's limits.
