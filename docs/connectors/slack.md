# Slack Destination

> Send messages to Slack via Incoming Webhook. Supports plain text and Block Kit.

## YAML Example

```yaml
destination:
  type: slack
  webhook_url_env: SLACK_WEBHOOK_URL
  message_template: ":bell: New signup: *{{ row.name }}* ({{ row.email }})"
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"slack"` | — | Required |
| `webhook_url` | string \| null | null | Webhook URL (direct value) |
| `webhook_url_env` | string \| null | null | Env var containing webhook URL |
| `message_template` | string | `"{{ row }}"` | Jinja2 template for message content |
| `block_kit` | bool | `false` | If true, treat template as Block Kit JSON |

## Authentication

Create an [Incoming Webhook](https://api.slack.com/messaging/webhooks) in your Slack workspace, then set the env var:

```bash
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/T.../B.../xxx"
```

## Common Patterns

**Plain text alert:**
```yaml
message_template: ":rotating_light: *Alert:* {{ row.message }} (severity: {{ row.level }})"
```

**Rich message with Block Kit:**
```yaml
block_kit: true
message_template: |
  {
    "blocks": [
      {
        "type": "section",
        "text": {
          "type": "mrkdwn",
          "text": "*{{ row.name }}* just signed up!\nEmail: {{ row.email }}"
        }
      }
    ]
  }
```

## Notes

- Slack rate limits: ~1 message/second per webhook. Set `rate_limit.requests_per_second: 1` in sync config
- Use `batch_size: 1` for real-time alerts
- Webhook URL should be stored in env var, not hardcoded in YAML