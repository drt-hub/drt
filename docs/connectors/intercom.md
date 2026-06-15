# Intercom Destination

> Upsert contacts into Intercom via the API. Core connector — no extra install.

## YAML Example

```yaml
destination:
  type: intercom
  properties_template: |
    {
      "role": "user",
      "email": "{{ row.email }}",
      "name": "{{ row.name }}",
      "custom_attributes": {"plan": "{{ row.plan }}"}
    }
  auth:
    type: bearer
    token_env: INTERCOM_TOKEN
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"intercom"` | — | Required |
| `properties_template` | string | — | Jinja2 template rendering a JSON contact payload (see the [Intercom contacts API](https://developers.intercom.com/docs/references/rest-api/api.intercom.io/contacts/)). **Required** |
| `auth` | AuthConfig | — | Authentication block (typically `bearer`). **Required** |
| `retry` | RetryConfig \| null | null | Per-destination override of `sync.retry`. |

## Authentication

Create an access token in the Intercom **Developer Hub** (or use an app token):

```bash
export INTERCOM_TOKEN="dG9rZW4..."
```

```yaml
auth:
  type: bearer
  token_env: INTERCOM_TOKEN
```

See [rest-api.md](rest-api.md) for the full `auth:` block shapes (bearer / basic / api-key).

## Notes

- Core connector — no `pip install` extras needed.
- `properties_template` must render valid JSON; include `email` (or another identifier) so Intercom can match/create the contact.
- One API call per row; use `sync.rate_limit` to respect Intercom's rate limits.
