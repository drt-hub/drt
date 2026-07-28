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

## Rate limiting

**Vendor limit:** commonly 1,000 requests/minute per workspace (~16/s) on the REST API; plan- and endpoint-dependent. drt applies **no automatic cap** here — set one explicitly:

```yaml
destination:
  type: intercom
  rate_limit:
    requests_per_second: 10
    burst: 20                # optional: let idle time bank up to 20 requests
```

`destination.rate_limit` beats `sync.rate_limit`, which beats the default of 10/s.

The limiter is shared per **workspace**, identified by the access token, so several syncs writing to one workspace concurrently (`drt run --threads 4`) pace through one bucket instead of one bucket each. When they request different rates, the lowest wins for both.

## Notes

- Core connector — no `pip install` extras needed.
- `properties_template` must render valid JSON; include `email` (or another identifier) so Intercom can match/create the contact.
- One API call per row; use `sync.rate_limit` to respect Intercom's rate limits.
