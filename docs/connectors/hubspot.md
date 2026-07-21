# HubSpot Destination

> Upsert contacts, deals, or companies into HubSpot CRM.

## YAML Example

```yaml
destination:
  type: hubspot
  object_type: contacts
  id_property: email
  properties_template: |
    {
      "email": "{{ row.email }}",
      "firstname": "{{ row.first_name }}",
      "lastname": "{{ row.last_name }}",
      "company": "{{ row.company }}"
    }
  auth:
    type: bearer
    token_env: HUBSPOT_TOKEN
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"hubspot"` | — | Required |
| `object_type` | `contacts\|deals\|companies` | `contacts` | CRM object type |
| `id_property` | string | `email` | Property used for upsert deduplication |
| `properties_template` | string \| null | null | Jinja2 template for properties JSON |
| `auth` | BearerAuth | — | Bearer token auth |

## Authentication

Create a [Private App](https://developers.hubspot.com/docs/api/private-apps) in HubSpot with CRM scopes, then:

```bash
export HUBSPOT_TOKEN="pat-na1-xxxxxxxx"
```

```yaml
auth:
  type: bearer
  token_env: HUBSPOT_TOKEN
```

## Common Patterns

**Upsert contacts by email:**
```yaml
object_type: contacts
id_property: email
properties_template: '{"email": "{{ row.email }}", "firstname": "{{ row.name }}"}'
```

**Create deals:**
```yaml
object_type: deals
id_property: dealname
properties_template: |
  {
    "dealname": "{{ row.deal_name }}",
    "amount": "{{ row.amount }}",
    "pipeline": "default",
    "dealstage": "{{ row.stage }}"
  }
```

**Without template (send record fields as-is):**
```yaml
object_type: contacts
id_property: email
# record fields are sent directly as HubSpot properties
```

**Enrich existing contacts only — never create (`match_policy: update_only`, [#757](https://github.com/drt-hub/drt/issues/757)):**
```yaml
sync:
  mode: upsert
  match_policy: update_only   # upsert (default) | update_only | create_only
destination:
  type: hubspot
  object_type: contacts
  id_property: email
```

The classic reverse-ETL activation case: push warehouse-computed scores/traits into contacts your reps already created in HubSpot, **without spraying a new contact for every warehouse row**. `update_only` PATCHes by `id_property` directly — a record with no HubSpot match (404) is **skipped**, never created. `create_only` is the inverse: POST only, and a record that already exists (409) is **skipped**, so a seed audience is never overwritten. Skipped rows are counted in the run's `skipped` total (`drt run` prints `… N skipped`), not as errors. The default `upsert` is unchanged (POST, then PATCH on 409).

## Notes

- HubSpot rate limit: ~100 requests/10s for private apps. drt caps at 9 req/s automatically
- Upsert: POST with `idProperty` deduplicates. On 409 Conflict, drt retries as PATCH. `sync.match_policy: update_only` / `create_only` narrow this to one side (see Common Patterns above)
- `properties_template` must produce a JSON object of HubSpot property names