# Linear Destination

> Create Linear issues from source rows via the Linear GraphQL API
> (`issueCreate` mutation).

## YAML Example

```yaml
destination:
  type: linear
  team_id_env: LINEAR_TEAM_ID
  title_template: "{{ row.title }}"
  description_template: "Reported by {{ row.reporter }}\n\n{{ row.body }}"
  auth:
    type: bearer
    token_env: LINEAR_API_KEY
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"linear"` | — | Required |
| `team_id` | string \| null | null | Linear team id (direct value) |
| `team_id_env` | string \| null | null | Env var holding the team id |
| `title_template` | string | — | Jinja2 template → issue title |
| `description_template` | string | — | Jinja2 template → issue description (Markdown) |
| `label_ids` | list[str] | `[]` | Label ids to attach to every created issue |
| `assignee_id` | string \| null | null | User id to assign every created issue to |
| `auth` | bearer | bearer | Bearer auth — set `token_env` to your Linear API key |
| `retry_creates` | bool | `false` | `true` restores blind replay of an ambiguous issue create (duplicate risk); see [Retry safety](#retry-safety) |
| `retry` | object \| null | null | Per-destination retry override |

## Authentication

Linear uses a personal API key as a bearer token. Create one in Linear
under Settings → Security & access → Personal API keys:

```bash
export LINEAR_API_KEY="lin_api_xxxxxxxxxxxxxxxx"
export LINEAR_TEAM_ID="your-team-uuid"
```

Requests `POST` to `https://api.linear.app/graphql`.

## Finding ids

`team_id`, `label_ids`, and `assignee_id` are Linear UUIDs, not the
human-readable names. Fetch them once via the GraphQL API:

```graphql
query { teams { nodes { id name } } }
query { issueLabels { nodes { id name } } }
query { users { nodes { id name } } }
```

## Common Patterns

**Tag every synced issue and auto-assign:**
```yaml
label_ids: ["<bug-label-uuid>", "<from-sync-uuid>"]
assignee_id: "<triage-user-uuid>"
```

**Markdown body** (Linear renders Markdown in descriptions):
```yaml
description_template: |
  **Severity:** {{ row.severity }}

  {{ row.details }}
```

## Retry safety

`issueCreate` is not documented to deduplicate a retried create (a client-supplied issue `id` appears in third-party connector docs but its duplicate behaviour is unverified, so drt does not rely on it), so it is a **blind create**. After a dropped response or a
retryable 5xx, drt returns a delivery-unknown `RowError` after one request: assume
an issue may already exist, check Linear before replaying, and do not auto-resend.
HTTP 429 and connection failures proven to happen before the request was sent
remain retryable.

`retry_creates: true` restores the legacy second attempt after an ambiguous
write and logs a duplicate-risk warning. See [retry safety](../guides/retry-safety.md).
Vendor contract rechecked 2026-11-25.

## Notes

- (core) — no extra install required.
- Create-only — this destination opens new issues; it does not update
  existing ones.
- The mutation's success flag is checked per row; a `success: false`
  response or transport error is recorded in `result.row_errors`, and
  `on_error: skip` continues the batch.
- `label_ids` / `assignee_id` must be valid for the target `team_id` or
  Linear rejects the mutation.

## References

- [Linear GraphQL API](https://developers.linear.app/docs/graphql/working-with-the-graphql-api)
- [`issueCreate` mutation](https://developers.linear.app/docs/graphql/working-with-the-graphql-api/the-graphql-schema)
