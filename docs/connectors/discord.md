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
| `retry_creates` | boolean | `false` | Allow a blind webhook replay after an ambiguous failure. Can post duplicate messages/embeds and logs a warning. |
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

## Rate limiting

**Vendor limit:** Discord meters each webhook route separately — roughly 5 requests/second per webhook, with 429s carrying a `retry_after`. drt applies **no automatic cap** here — set one explicitly:

```yaml
destination:
  type: discord
  webhook_url_env: DISCORD_WEBHOOK_URL
  rate_limit:
    requests_per_second: 5
    burst: 5                 # optional: allow a short catch-up burst after idle time
```

`destination.rate_limit` beats `sync.rate_limit`, which beats the default of 10/s.

The limiter is shared per **webhook URL**, so several syncs posting to the same webhook concurrently (`drt run --threads 4`) pace through one bucket instead of one bucket each. Different webhooks never throttle each other. When two syncs share a webhook but request different rates, the lowest wins for both.

## Retry safety

Discord's [Execute Webhook](https://docs.discord.com/developers/resources/webhook#execute-webhook)
is a **blind message create**. Its documented request parameters expose no
idempotency key; `wait` asks for server confirmation but does not deduplicate a
replay. After a dropped response or retryable 5xx, drt returns a
delivery-unknown `RowError` after one POST. Assume the message or embed may be
visible; inspect the channel before replay and do not auto-resend.

HTTP 429 and proven pre-send connection failures remain retryable.
`retry_creates: true` restores the legacy blind second POST and logs a
duplicate-message warning. See [retry safety](../guides/retry-safety.md). Vendor
contract rechecked 2026-11-23.

## Notes

- Core connector — no `pip install` extras needed.
- One HTTP POST per row; use `sync.rate_limit` to stay within Discord's webhook limits.
- Sibling webhook destinations: [Slack](slack.md), [Microsoft Teams](teams.md).
