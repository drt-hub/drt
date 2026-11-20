# PagerDuty (Events API v2)

> Request shape verified against a local mock; not verified against the live API.

Trigger or resolve PagerDuty incidents from a warehouse condition (such as SLA breaches, failed pipeline checks, or anomaly rows) using [`rest_api`](../../connectors/rest-api.md) and [PagerDuty Events API v2](https://developer.pagerduty.com/api-reference/368ae3d938c9e-send-an-event-to-pagerduty).

## Prerequisites

Obtain your 32-character PagerDuty Events API v2 **Integration / Routing Key** from your PagerDuty service integration settings. PagerDuty expects `routing_key` in the JSON request body (not as an HTTP header), so provide it as a `routing_key` column in your model (for example from a service-routing mapping table or warehouse secret).

## Sync YAML

Save as `syncs/pagerduty_alerts.yml`:

```yaml
name: pagerduty_alerts
description: Trigger PagerDuty alerts for warehouse SLA breaches
model: models/sla_breaches.sql

destination:
  type: rest_api
  url: https://events.pagerduty.com/v2/enqueue
  method: POST
  headers:
    Content-Type: application/json
  body_mode: record
  body_template: |
    {
      "routing_key": "{{ row.routing_key }}",
      "event_action": "{{ row.event_action | default('trigger') }}",
      "dedup_key": "{{ row.dedup_key }}",
      "payload": {
        "summary": "{{ row.summary }}",
        "source": "{{ row.source }}",
        "severity": "{{ row.severity | default('error') }}"
      }
    }

sync:
  mode: incremental
  cursor_field: detected_at
  rate_limit:
    requests_per_second: 2
  on_error: skip
  dlq:
    enabled: true
```

Example `models/sla_breaches.sql`:

```sql
SELECT
  routing_key,
  breach_id AS dedup_key,
  'trigger' AS event_action,
  CONCAT('SLA breach on ', pipeline_name, ': ', delay_minutes, 'm late') AS summary,
  pipeline_name AS source,
  'error' AS severity,
  detected_at
FROM analytics.pipeline_sla_breaches
```

## Limits & Gotchas

- **One event per request:** PagerDuty Events API v2 `/v2/enqueue` accepts a single event object per `POST` ([Send an event to PagerDuty](https://developer.pagerduty.com/api-reference/368ae3d938c9e-send-an-event-to-pagerduty)), so keep `body_mode: record`.
- **Routing key in JSON body:** `/v2/enqueue` authenticates via the `routing_key` field inside the JSON body rather than an HTTP header ([Send an event to PagerDuty](https://developer.pagerduty.com/api-reference/368ae3d938c9e-send-an-event-to-pagerduty)).
- **Idempotency via `dedup_key`:** Always pass a deterministic `dedup_key` (up to 255 characters) so retries or repeated incremental runs update the same open incident instead of paging on-call multiple times ([Alert De-duplication](https://developer.pagerduty.com/docs/ZG9jOjExMDI5NTgw-events-api-v2-overview#alert-de-duplication)).
- **Rate limits & payload caps:** Events API v2 enforces per-integration rate limits and a **512 KB** event payload limit; keep `rate_limit.requests_per_second` conservative and check the [PagerDuty Events API v2 Rate Limits](https://developer.pagerduty.com/docs/ZG9jOjExMDI5NTgw-events-api-v2-overview#rate-limiting) and [API Reference](https://developer.pagerduty.com/api-reference/368ae3d938c9e-send-an-event-to-pagerduty) for current quotas.
- **`202 Accepted` responses:** PagerDuty returns `202 Accepted` on valid events ([Send an event to PagerDuty](https://developer.pagerduty.com/api-reference/368ae3d938c9e-send-an-event-to-pagerduty)); `rest_api` treats all `2xx` statuses as successful deliveries.
