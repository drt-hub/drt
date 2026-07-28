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

## Rate limiting

**Vendor limit:** ~1 message/second per webhook (Slack allows short bursts, then throttles). drt applies **no automatic cap** here — set one explicitly:

```yaml
destination:
  type: slack
  webhook_url_env: SLACK_WEBHOOK_URL
  rate_limit:
    requests_per_second: 1
    burst: 5                 # optional: allow a short catch-up burst after idle time
```

`destination.rate_limit` beats `sync.rate_limit`, which beats the default of 10/s.

The limiter is shared per **webhook URL**, so several syncs posting to the same webhook concurrently (`drt run --threads 4`) pace through one bucket instead of one bucket each. Different webhooks never throttle each other. When two syncs share a webhook but request different rates, the lowest wins for both.

## Notes

- Slack rate limits: ~1 message/second per webhook. Set `rate_limit.requests_per_second: 1` in sync config
- Use `batch_size: 1` for real-time alerts
- Webhook URL should be stored in env var, not hardcoded in YAML