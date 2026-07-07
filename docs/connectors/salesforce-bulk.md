# Salesforce Bulk Destination

> Load source rows into a Salesforce object via the **Bulk API 2.0** — each
> sync opens a bulk job, uploads the batch as CSV, and polls it to completion.

## YAML Example

```yaml
destination:
  type: salesforce_bulk
  instance_url_env: SF_INSTANCE_URL      # e.g. https://mycompany.my.salesforce.com
  object_name: Contact
  operation: upsert
  external_id_field: External_Id__c
  client_id_env: SF_CLIENT_ID
  client_secret_env: SF_CLIENT_SECRET
  username_env: SF_USERNAME
  password_env: SF_PASSWORD
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"salesforce_bulk"` | — | Required |
| `instance_url` | string \| null | null | Salesforce instance URL (direct value) |
| `instance_url_env` | string \| null | null | Env var holding the instance URL |
| `object_name` | string | — | Target sObject, e.g. `Contact`, `Account` |
| `operation` | `insert` \| `update` \| `upsert` \| `delete` | `upsert` | Bulk operation |
| `external_id_field` | string | `Id` | Upsert key field (required for `upsert`) |
| `poll_timeout_seconds` | int | `3600` | Max time to wait for the job to finish |
| `poll_interval_seconds` | int | `30` | Delay between job-status polls |
| `client_id_env` | string | — | Env var → connected-app consumer key |
| `client_secret_env` | string | — | Env var → connected-app consumer secret |
| `username_env` | string | — | Env var → Salesforce username |
| `password_env` | string | — | Env var → password (+ security token if required) |

One of `instance_url` / `instance_url_env` is required (enforced at config load).

## Authentication

Uses the OAuth 2.0 **username-password flow** against a Salesforce
[connected app](https://help.salesforce.com/s/articleView?id=sf.connected_app_create.htm).
Create the connected app, enable OAuth, and store its consumer key/secret plus
the integration user's credentials in env vars:

```bash
export SF_INSTANCE_URL="https://mycompany.my.salesforce.com"
export SF_CLIENT_ID="3MVG9..."
export SF_CLIENT_SECRET="1A2B3C..."
export SF_USERNAME="integration@mycompany.com"
export SF_PASSWORD="password+securitytoken"
```

> If your org enforces IP restrictions, append the user's **security token** to
> the password (`passwordSECURITYTOKEN`).

## Common Patterns

**Upsert on an external ID** (idempotent re-runs keyed on your own column):
```yaml
operation: upsert
external_id_field: External_Id__c
```

**Insert-only** (fail on duplicates):
```yaml
operation: insert
```

**Delete by record Id**:
```yaml
operation: delete
external_id_field: Id
```

## Notes

- (core) — no extra install required (uses `httpx`).
- Source column names must match the target sObject's API field names
  (use `sync.field_mappings` to rename columns to Salesforce API names).
- The batch is uploaded as a single Bulk API 2.0 job; drt polls
  `poll_interval_seconds` apart up to `poll_timeout_seconds`. Raise the timeout
  for very large loads.
- Per-record failures reported by the job are surfaced in
  `result.row_errors`; `on_error: skip` continues, `on_error: fail` aborts.
- Use `--dry-run` to preview the row count and rendered payload before a real
  load.

## References

- [Salesforce Bulk API 2.0](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/bulk_api_2_0.htm)
- [OAuth username-password flow](https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_username_password_flow.htm)
