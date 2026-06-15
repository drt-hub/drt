# Discord Destination

> Post each row to a Discord channel via an incoming webhook. Core connector — no extra install.

## YAML Example

```yaml
destination:
  type: discord
  webhook_url_env: DISCORD_WEBHOOK      # env var holding the webhook URL
  message_template: "New signup: {{ row.name }} ({{ row.email }})"
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"discord"` | — | Required |
| `webhook_url` | string \| null | null | Webhook URL inline (not recommended — prefer the env form). |
| `webhook_url_env` | string \| null | null | Env var holding the webhook URL. One of `webhook_url` / `webhook_url_env` is required. |
| `message_template` | string | `"{{ row }}"` | Jinja2 template rendered per row. Plain text, or a JSON `embeds` payload (see `embeds`). |
| `embeds` | bool | `false` | When `true`, `message_template` is treated as a JSON payload with an `embeds` array (rich messages) instead of plain text. |
| `retry` | RetryConfig \| null | null | Per-destination override of `sync.retry`. |

## Authentication

Create a webhook in **Server Settings → Integrations → Webhooks**, then expose its URL via an env var:

```bash
export DISCORD_WEBHOOK="https://discord.com/api/webhooks/.../..."
```

## Templates

- **Plain text** — `message_template: "New user: {{ row.name }}"`.
- **Embeds** — set `embeds: true` and render a full payload:

  ```yaml
  embeds: true
  message_template: '{"embeds": [{"title": "{{ row.name }}", "description": "{{ row.email }}"}]}'
  ```

## Notes

- Core connector — no `pip install` extras needed.
- One HTTP POST per row; use `sync.rate_limit` to stay within Discord's webhook limits.
- Sibling webhook destinations: [Slack](slack.md), [Microsoft Teams](teams.md).
