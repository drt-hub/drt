# Microsoft Teams Destination

> Post messages to a Microsoft Teams channel via an Incoming Webhook.
> Supports plain text / MessageCard and Adaptive Card JSON payloads.

## YAML Example

```yaml
destination:
  type: teams
  webhook_url_env: TEAMS_WEBHOOK_URL
  message_template: "🔔 New signup: **{{ row.name }}** ({{ row.email }})"
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"teams"` | — | Required |
| `webhook_url` | string \| null | null | Webhook URL (direct value) |
| `webhook_url_env` | string \| null | null | Env var containing the webhook URL |
| `message_template` | string | `"{{ row }}"` | Jinja2 template for the message body |
| `adaptive_card` | bool | `false` | If true, treat `message_template` as an Adaptive Card JSON payload |
| `retry` | object \| null | null | Per-destination retry override (see `sync.retry`) |

## Authentication

Create an **Incoming Webhook** connector on the target Teams channel
(channel → ⋯ → Connectors → Incoming Webhook), then store the URL in an
env var:

```bash
export TEAMS_WEBHOOK_URL="https://outlook.office.com/webhook/..."
```

> Prefer `webhook_url_env` over the inline `webhook_url` — `drt validate`
> flags hardcoded secrets (v0.7.5+).

## Common Patterns

**Plain text alert:**
```yaml
message_template: "🚨 **Alert:** {{ row.message }} (severity: {{ row.level }})"
```

**Adaptive Card** (richer layout — the template is the full Adaptive
Card JSON; drt wraps it in the `attachments` envelope):
```yaml
adaptive_card: true
message_template: |
  {
    "type": "AdaptiveCard",
    "version": "1.4",
    "body": [
      { "type": "TextBlock", "size": "Medium", "weight": "Bolder",
        "text": "{{ row.name }} just signed up" },
      { "type": "TextBlock", "text": "Email: {{ row.email }}", "wrap": true }
    ]
  }
```

## Notes

- (core) — no extra install required.
- One POST per row. For real-time alerts use `batch_size: 1`; for
  bursty syncs set `rate_limit.requests_per_second` to stay under the
  channel's webhook throttling.
- Webhook delivery failures surface as row errors; `on_error: skip`
  (default for webhook-style destinations) keeps the rest of the batch
  going.
- The webhook URL is channel-scoped — one URL per channel. Use a
  sync-specific webhook to route different syncs to different channels.

## References

- [Teams Incoming Webhooks](https://learn.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook)
- [Adaptive Cards](https://adaptivecards.io/)
