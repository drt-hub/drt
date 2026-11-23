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
  retry_creates: false
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"hubspot"` | — | Required |
| `object_type` | string | `contacts` | Standard CRM object name (`contacts`, `companies`, or `deals`) or a custom object's fully qualified name / object type ID |
| `id_property` | string | `email` | Property used for upsert deduplication |
| `properties_template` | string \| null | null | Jinja2 template for properties JSON |
| `auth` | BearerAuth | — | Bearer token auth |
| `retry_creates` | boolean | `false` | After an ambiguous create and an exact reconciliation miss, allow another blind `POST`. This can create a duplicate object; reconciliation itself does not require this option |

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

## Errors and retries

- Batch reads, exact reconciliation searches, and ID-addressed `PATCH` updates keep ordinary configured retries. A create also retries HTTP 429 and proven connection-establishment failures because the operation was not accepted or no request bytes were sent.
- A lost response or retryable 5xx after `POST /crm/v3/objects/{object_type}` is delivery-unknown. When the rendered properties contain a non-empty configured `id_property`, drt makes one bounded search using HubSpot's exact `EQ` filter. One object is accepted as delivered; `upsert` then applies an update to that returned HubSpot object ID, while `create_only` records success without updating. Multiple object IDs are refused as a row error.
- A search miss fails closed because [HubSpot documents that newly created or updated CRM objects may take a few moments to appear in search](https://developers.hubspot.com/docs/api-reference/latest/crm/search-the-crm#limits). A create whose rendered properties omit `id_property` or give it an empty value is blind and cannot be reconciled; it also fails closed after an ambiguous attempt.
- The existing match-policy responses remain unchanged: a definite create `409` becomes an `idProperty`-addressed update for `upsert` or a policy skip for `create_only`; `update_only` uses an `idProperty`-addressed PATCH and treats `404` as a policy skip.

### Retry safety and operator recovery

HubSpot's [CRM search API](https://developers.hubspot.com/docs/api-reference/latest/crm/search-the-crm) accepts property filters across standard and custom CRM objects. drt uses `EQ`, requests at most two results, and requires exactly one ID before it considers ambiguous delivery reconciled.

| Operation | Classification | Protection and remaining risk |
|---|---|---|
| Batch read; `POST .../{object_type}/search`; `PATCH .../{object_id}` or `PATCH .../{id_property}?idProperty=...` | Idempotent read / addressed update | Ordinary retries remain enabled. Updates target the same record ID or configured unique property |
| Object `POST` with a non-empty configured `id_property` value | Reconciliable create | One exact search protects the immediate ambiguous attempt. HubSpot native idempotency-key support is unverified; index lag makes a search miss non-authoritative |
| Object `POST` without a non-empty configured `id_property` value | Blind create | No automatic replay after a possibly sent attempt because there is no exact reconciliation key |

When a RowError says create delivery is unknown, assume the first POST may have worked. Wait for HubSpot's search index, search the same object type by the exact rendered `id_property`, and merge/remove duplicates if necessary before using `drt retry` or manually replaying the row. The warehouse delivery ledger cannot resolve a response lost inside one `load()` call because it marks only confirmed success. Set `destination.retry_creates: true` only when availability is worth the duplicate-object risk; drt warns whenever that opt-in authorizes a blind replay.

This retry behavior is verified against the shared HTTP fault-injection mock only. No live HubSpot account was available for verification.

**Fill only empty HubSpot properties (`write_policy: fill_empty`, [#1238](https://github.com/drt-hub/drt/issues/1238)):**

```yaml
sync:
  mode: upsert
  write_policy: fill_empty
  write_policy_overrides:
    lifecycle_stage: overwrite
destination:
  type: hubspot
  object_type: contacts
  id_property: email
```

For batches that contain fill-only properties, drt renders groups of at most
100 records, then reads their current HubSpot values through
`crm/v3/objects/{object_type}/batch/read`.
An existing property's `null`, empty string, or spaces-only string is filled;
`"0"`, numeric `0`, and `false` are values and are kept. Non-empty fill-only
properties are removed from the PATCH, while overwrite properties and
`id_property` remain. New objects are POSTed with every rendered property. The
same behavior works with `mode: full`, `upsert`, or `incremental`, and composes
with `match_policy: update_only` / `create_only`.

The read is fail-closed: after retries, any non-200/207 response, absent body
status or a status other than exactly `COMPLETE`, missing or malformed `results`,
or batch error other than `OBJECT_NOT_FOUND` records row errors and writes none
of that read's records. Each `OBJECT_NOT_FOUND` must carry a non-empty ID list
containing only requested inputs; one error may group multiple IDs. A 207 partial
response is accepted only when every absent result has a correlated error; those
IDs are treated as new. The batch-read and PATCH-by-`idProperty` APIs require
`id_property` to be `hs_object_id` or a HubSpot property whose values are
unique. A non-unique choice such as `dealname` makes the fill-empty read fail,
producing row errors and no writes.

Override names are lowercase and case-sensitive. When the private-app token can
read schemas, drt checks them once per object type and resolved token against
`crm/v3/properties/{object_type}` before writing. If that endpoint is unavailable,
unusable, or returns 401/403/404 (a token may lack the schemas scope), validation
is skipped with one warning and the sync continues. Repeated IDs are
first-writer-wins within one destination `load()` call. Duplicates split across
separate engine batches rely on HubSpot returning the first write on the next
read, so a small stale-read window is possible. If every non-ID property is kept,
the write is skipped without a no-match error. `SyncResult.kept_values` counts
kept properties and is summed into the run result. A result that cannot be
correlated back to its requested `id_property` also fails closed.

HubSpot is not a `QueryableDestination`, so `drt plan` and
`drt run --dry-run --diff` remain unsupported for it; [#472](https://github.com/drt-hub/drt/issues/472)
tracks API-backed diff support. This behavior is verified against HTTP mocks
only, not a live HubSpot account.

## Rate limiting

**Vendor limit:** ~100 requests / 10s for private apps. **drt caps this destination at 9 req/s** and will not exceed it even if you configure a higher number.

The limiter is shared per **portal** — keyed on the access token, *not* on `object_type`. Syncing `contacts` and `companies` into the same portal concurrently (`drt run --threads 4`) paces them through one bucket, matching how HubSpot actually meters the quota. Before v0.8.4 each sync built its own limiter, so four parallel syncs put 4x the intended rate on one portal.

Override to go **lower** than the cap:

```yaml
destination:
  type: hubspot
  object_type: contacts
  rate_limit:
    requests_per_second: 4   # gentler than the 9/s cap
    burst: 8                 # optional: let idle time bank up to 8 requests
```

`destination.rate_limit` beats `sync.rate_limit`, which beats the default of 10/s. When two syncs share a portal but request different rates, the lowest wins for both.

Rate limiting counts logical operations: one properties lookup, batch read, or
row write consumes one limiter slot. Retries within that operation do not consume
another slot, and the default-policy POST-409-to-PATCH fallback remains one
logical write. An ambiguous create additionally consumes one search slot and,
for `upsert`, one ID-addressed update slot.

> **Syncing to more than one portal? Use `auth.token_env`, not an inline `auth.token`.**
> Pacing is per portal, and drt identifies the portal by the *name* of the env var holding the token — it deliberately never derives the key from the token value itself. Two destinations that inline a literal token are therefore indistinguishable to the limiter and share one bucket, so they pace against each other even when they point at different portals. The failure mode is a slower sync, never a 429; giving each portal its own env var gives each its own bucket.
>
> ```yaml
> destination:
>   type: hubspot
>   auth:
>     type: bearer
>     token_env: HUBSPOT_TOKEN_EU   # per-portal bucket
> ```

## Notes

- HubSpot rate limit: ~100 requests/10s for private apps. drt caps at 9 req/s automatically
- Upsert: POST with `idProperty` deduplicates. On 409 Conflict, drt follows with PATCH. `sync.match_policy: update_only` / `create_only` narrow this to one side (see Common Patterns above)
- `properties_template` must produce a JSON object of HubSpot property names
