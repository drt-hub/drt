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
  retry_creates: false
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"intercom"` | — | Required |
| `properties_template` | string | — | Jinja2 template rendering a JSON contact payload (see the [Intercom contacts API](https://developers.intercom.com/docs/references/rest-api/api.intercom.io/contacts/)). **Required** |
| `auth` | AuthConfig | — | Authentication block (typically `bearer`). **Required** |
| `retry_creates` | boolean | `false` | After an ambiguous create and an exact reconciliation miss, allow another blind `POST`. This can create duplicate contacts; reconciliation itself does not require this option |
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

## Match policy

Intercom supports all three `sync.match_policy` values from [#757](https://github.com/drt-hub/drt/issues/757):

```yaml
sync:
  mode: upsert
  match_policy: update_only   # upsert (default) | update_only | create_only

destination:
  type: intercom
  properties_template: |
    {
      "external_id": "{{ row.customer_id }}",
      "email": "{{ row.email }}",
      "custom_attributes": {"health_score": {{ row.health_score }}}
    }
  auth:
    type: bearer
    token_env: INTERCOM_TOKEN
```

- `upsert` creates a contact, then follows Intercom's documented duplicate `409` response to update the existing contact by its Intercom ID.
- `create_only` uses the create endpoint but treats that duplicate response as a normal skip, so an existing contact is never overwritten.
- `update_only` updates a rendered Intercom `id` directly. Without `id`, drt searches by the rendered `external_id` and/or `email`; no match is skipped, while multiple matches fail instead of updating an arbitrary contact. Prefer `external_id` because it is unique and stable.

Policy skips increment `skipped` and `skipped_no_match`; they are not row errors. The engine rejects `match_policy` with `mode: replace` or `mirror`. An `update_only` row that needs a search makes two API calls (search, then update), and each call passes through the destination's rate limiter.

## Errors and retries

- Exact contact searches and ID-addressed `PUT` updates retry transient failures with exponential backoff. Search uses Intercom's exact `=` operator on the rendered `external_id`, `email`, or both.
- A create retries HTTP 429 and proven connection-establishment failures because Intercom did not accept the operation. If a response is lost or a retryable 5xx is returned after `POST /contacts`, delivery is unknown: drt searches by the same exact rendered identifier(s) before deciding what to do next.
- One reconciliation match is accepted as the created contact. In `upsert` mode drt then applies the normal ID-addressed update; in `create_only` mode it records success without updating. Multiple matches are a row error listing their Intercom IDs. No match fails closed because Intercom documents that a newly created contact can take a few minutes to appear in search.
- A missing bearer token fails the batch. Other failures follow `sync.on_error`: `fail` stops after the row, while `skip` records a row error and continues.

### Retry safety and operator recovery

| Operation | Classification | Protection and remaining risk |
|---|---|---|
| `POST /contacts/search`; `PUT /contacts/{id}` | Idempotent read / addressed update | Ordinary configured retries are safe: search has no write side effect and PUT targets the same Intercom contact ID |
| `POST /contacts` with non-empty `external_id` and/or `email` | Reconciliable create | Exact equality search protects the immediate ambiguous attempt. Intercom native idempotency-key support is unverified. A blind replay can create two contacts when the first commit is not yet searchable |
| `POST /contacts` without `external_id` or `email` | Blind create | No automatic replay after a possibly sent attempt; there is no exact reconciliation key |

When a RowError says create delivery is unknown, assume the first `POST` may have worked. Search Intercom by the exact rendered `external_id` and/or `email`, allow several minutes for indexing, and merge/remove duplicates if necessary before using `drt retry` or manually replaying the row. The warehouse delivery ledger cannot resolve this immediate window because it is marked only after a confirmed response. Set `destination.retry_creates: true` only if availability is worth the duplicate-contact risk; drt warns whenever that opt-in causes a blind replay.

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
- `properties_template` must render valid JSON. For `update_only`, include a non-empty `id`, `external_id`, or `email`; for create paths, include the identifier Intercom requires for that contact role.
- Most rows make one API call. `update_only` makes a search call first unless the payload includes Intercom's `id`; ambiguous create delivery adds a reconciliation search and may add an ID-addressed update. Use `sync.rate_limit` to respect Intercom's rate limits.

## References

- [Intercom: Create contact](https://developers.intercom.com/docs/references/rest-api/api.intercom.io/contacts/createcontact)
- [Intercom: Search contacts](https://developers.intercom.com/docs/references/rest-api/api.intercom.io/contacts/searchcontacts)
