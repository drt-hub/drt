# Zendesk Destination

> Upsert users or organizations into Zendesk Support.

## YAML Example

```yaml
destination:
  type: zendesk
  subdomain_env: ZENDESK_SUBDOMAIN
  email_env: ZENDESK_EMAIL
  api_token_env: ZENDESK_API_TOKEN
  object: user
  id_field: zendesk_user_id
  custom_fields_template: |
    {
      "health_score": "{{ row.health_score }}",
      "plan": "{{ row.plan }}"
    }
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"zendesk"` | - | Required |
| `subdomain` | string \| null | null | Zendesk subdomain, such as `acme` for `acme.zendesk.com` |
| `subdomain_env` | string \| null | null | Env var containing the subdomain |
| `email` | string \| null | null | Zendesk user email |
| `email_env` | string \| null | null | Env var containing the Zendesk user email |
| `api_token` | string \| null | null | Zendesk API token |
| `api_token_env` | string \| null | null | Env var containing the API token |
| `object` | `user\|organization` | `user` | Zendesk object type to upsert |
| `id_field` | string \| null | null | Source row field containing a Zendesk `id` |
| `custom_fields_template` | string \| null | null | Jinja2 template that renders a JSON object of custom fields |
| `retry` | RetryConfig \| null | null | Destination-level retry override |

## Authentication

Create an API token in Zendesk, then set:

```bash
export ZENDESK_SUBDOMAIN="acme"
export ZENDESK_EMAIL="admin@example.com"
export ZENDESK_API_TOKEN="..."
```

## Common Patterns

**Upsert users by email or external ID:**

```yaml
destination:
  type: zendesk
  subdomain_env: ZENDESK_SUBDOMAIN
  email_env: ZENDESK_EMAIL
  api_token_env: ZENDESK_API_TOKEN
  object: user
```

Rows are sent as Zendesk user objects. Include `email`, `external_id`, `name`, `tags`, `phone`, or any other Zendesk user fields in the source query.

**Map a warehouse ID column to Zendesk `id`:**

```yaml
destination:
  type: zendesk
  subdomain_env: ZENDESK_SUBDOMAIN
  email_env: ZENDESK_EMAIL
  api_token_env: ZENDESK_API_TOKEN
  object: organization
  id_field: zendesk_organization_id
```

The source-only `zendesk_organization_id` column is copied to Zendesk `id` and removed from the outgoing payload.

**Custom user fields:**

```yaml
destination:
  type: zendesk
  subdomain_env: ZENDESK_SUBDOMAIN
  email_env: ZENDESK_EMAIL
  api_token_env: ZENDESK_API_TOKEN
  object: user
  custom_fields_template: |
    {
      "plan_tier": "{{ row.plan }}",
      "health_score": "{{ row.health_score }}"
    }
```

For users, custom fields are attached as `user_fields`. For organizations, they are attached as `organization_fields`.

## Notes

- User upserts use Zendesk's `users/create_or_update_many` endpoint in batches of 100.
- Organization upserts use Zendesk's `organizations/create_or_update` endpoint per row.
- drt caps the default request rate at 11 requests per second, below Zendesk's common 700 requests/minute account limit.
- `custom_fields_template` must render a JSON object.
