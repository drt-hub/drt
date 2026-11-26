# Mixpanel Destination

> Sync user profiles or events from your warehouse to Mixpanel via the
> `/engage` (profile-set) or `/import` (events) APIs.

## YAML Example — user profiles (people_set)

```yaml
destination:
  type: mixpanel
  endpoint: people_set
  project_token_env: MIXPANEL_TOKEN
  distinct_id_field: user_id
  properties_template: |
    {
      "plan": "{{ row.plan }}",
      "signup_source": "{{ row.source }}"
    }
```

## YAML Example — events (import)

```yaml
destination:
  type: mixpanel
  endpoint: import_events
  project_id: "1234567"
  service_account_username_env: MIXPANEL_SA_USERNAME
  service_account_secret_env: MIXPANEL_SA_SECRET
  distinct_id_field: user_id
  event_name: signup_completed
  time_field: event_time
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"mixpanel"` | — | Required |
| `endpoint` | `people_set\|import_events` | `people_set` | `/engage` profile-set or `/import` events |
| `region` | `default\|eu` | `default` | API region (`api.mixpanel.com` vs `api-eu.mixpanel.com`) |
| `project_token` | string \| null | null | Project token for `people_set` (direct value) |
| `project_token_env` | string \| null | `MIXPANEL_TOKEN` | Env var containing the project token |
| `project_id` | string \| null | null | Numeric project id, required for `import_events` |
| `service_account_username` | string \| null | null | Service account username for `import_events` |
| `service_account_username_env` | string \| null | `MIXPANEL_SA_USERNAME` | Env var containing the service account username |
| `service_account_secret` | string \| null | null | Service account secret for `import_events` |
| `service_account_secret_env` | string \| null | `MIXPANEL_SA_SECRET` | Env var containing the service account secret |
| `distinct_id_field` | string | `distinct_id` | Source column for the Mixpanel distinct id |
| `event_name_field` | string \| null | null | Source column for the event name (`import_events`) |
| `event_name` | string \| null | null | Constant event name (alternative to `event_name_field`) |
| `time_field` | string \| null | null | Source column for event `time` (Unix seconds); defaults to now |
| `insert_id_field` | string \| null | null | Source column for `$insert_id`; derived deterministically if unset |
| `properties_template` | string \| null | null | Jinja2 template rendering a JSON object merged into the profile `$set` or the event properties |
| `batch_size` | int | `2000` | Records per API request (clamped to 1–2000, Mixpanel's limit) |
| `retry` | RetryConfig \| null | null | Destination-level retry override |

## Authentication

**`people_set`** uses your **project token** (Mixpanel **Settings →
Project Settings**), carried inside each record — no auth header:

```bash
export MIXPANEL_TOKEN="your-project-token"
```

**`import_events`** uses a **service account** (Mixpanel **Organization
Settings → Service Accounts**) plus the numeric `project_id`:

```bash
export MIXPANEL_SA_USERNAME="your-service-account.mp-service-account"
export MIXPANEL_SA_SECRET="your-service-account-secret"
```

## Retry safety

- **`endpoint: import_events` is keyed.** Each event carries `$insert_id`: your
  `insert_id_field` value, otherwise a SHA-256 of the row and event name, so the same
  row always gets the same id. After a lost response drt re-sends the *same* batch
  and Mixpanel deduplicates on event name, `distinct_id`, `time` and `$insert_id`
  together ([event deduplication](https://docs.mixpanel.com/reference/event-deduplication)).
  Mixpanel's docs also say retries are safe only if the resent event is identical,
  which is why drt re-sends the same payload.
- **Set `time_field` if you rely on deduplication across runs or DLQ replays.**
  Without it the event time is the time drt built the payload, so a later run builds
  a different `time` and Mixpanel treats it as a different event. Within one run's
  retry the payload is reused, so the retry is safe either way.
- **`endpoint: people_set` is idempotent:** `$set` writes the same property values on
  a replay.

See [retry safety](../guides/retry-safety.md). Vendor contract rechecked 2026-11-26.

## Notes

- Both endpoints batch up to **2000 records** per request.
- For `import_events`, each event gets a deterministic `$insert_id`
  (derived from the row contents when `insert_id_field` is unset), so
  re-running the same sync does **not** double-count events in Mixpanel.
- EU data residency: set `region: eu` to route to
  `api-eu.mixpanel.com`.

## References

- [Set profile properties (`/engage`)](https://developer.mixpanel.com/reference/profile-set)
- [Import events (`/import`)](https://developer.mixpanel.com/reference/import-events)
