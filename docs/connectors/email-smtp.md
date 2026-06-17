# Email (SMTP) Destination

> Send a templated email per sync run via any SMTP server. Core connector — no extra install.

## YAML Example

```yaml
destination:
  type: email_smtp
  host: smtp.gmail.com
  port: 587
  use_tls: true
  sender: alerts@example.com
  recipients: [team@example.com, oncall@example.com]
  subject_template: "drt sync: {{ rows | length }} new rows"
  body_template: "{% for row in rows %}- {{ row.name }} ({{ row.email }})\n{% endfor %}"
  username_env: SMTP_USER
  password_env: SMTP_PASSWORD
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"email_smtp"` | — | Required |
| `host` | string | — | SMTP server hostname. **Required** |
| `port` | int | `587` | SMTP port (587 for STARTTLS, 465 for implicit TLS, 25 for plain). |
| `sender` | string | — | From address. **Required** |
| `recipients` | list[str] | — | To addresses. **Required** |
| `subject_template` | string | — | Jinja2 template for the subject. **Required** |
| `body_template` | string | — | Jinja2 template for the body. Has access to the whole `rows` batch (not just one `row`). **Required** |
| `use_tls` | bool | `true` | Use STARTTLS. |
| `username` / `username_env` | string \| null | null | SMTP username (prefer the `_env` form). |
| `password` / `password_env` | string \| null | null | SMTP password (prefer the `_env` form). |

## Authentication

```bash
export SMTP_USER="alerts@example.com"
export SMTP_PASSWORD="app-specific-password"
```

For Gmail / Google Workspace, use an [App Password](https://support.google.com/accounts/answer/185833), not your account password.

## Notes

- Core connector — no `pip install` extras needed.
- Unlike the per-row webhook destinations, the email destination renders **one message per batch** — `body_template` iterates over `rows`. Good for digest-style alerts.
- For per-row alerting to chat, see [Slack](slack.md) / [Discord](discord.md) / [Teams](teams.md).
