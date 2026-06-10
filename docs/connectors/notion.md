# Notion Destination

> Append rows to a Notion database as new pages via the Notion API
> (`POST /v1/pages`). Page properties are built from a Jinja2-rendered
> JSON template.

## YAML Example

```yaml
destination:
  type: notion
  database_id: "your-notion-database-id"
  properties_template: |
    {
      "Name":   { "title": [ { "text": { "content": "{{ row.name }}" } } ] },
      "Email":  { "email": "{{ row.email }}" },
      "Status": { "select": { "name": "{{ row.status }}" } }
    }
  auth:
    type: bearer
    token_env: NOTION_TOKEN
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"notion"` | — | Required |
| `database_id` | string | — | Target Notion database id |
| `properties_template` | string \| null | null | Jinja2 template → JSON object of Notion page properties |
| `auth` | bearer | bearer | Bearer auth — set `token_env` to your integration token |
| `retry` | object \| null | null | Per-destination retry override |

## Authentication

Create an **internal integration** at
[notion.so/my-integrations](https://www.notion.so/my-integrations) and
copy its token. Then **share the target database with the integration**
(database → ⋯ → Connections → your integration) — without this, the API
returns a 404 even with a valid token.

```bash
export NOTION_TOKEN="secret_xxxxxxxxxxxxxxxx"
```

drt sends the `Notion-Version: 2022-06-28` header and `POST`s to
`https://api.notion.com/v1/pages`.

## Property template

`properties_template` renders to the `properties` object of a Notion
page create. Each key is a database column, and the value must match
that column's Notion property type. Common types:

| Notion type | Template shape |
|---|---|
| Title | `{ "title": [ { "text": { "content": "{{ row.x }}" } } ] }` |
| Rich text | `{ "rich_text": [ { "text": { "content": "{{ row.x }}" } } ] }` |
| Email | `{ "email": "{{ row.x }}" }` |
| Number | `{ "number": {{ row.x }} }` |
| Select | `{ "select": { "name": "{{ row.x }}" } }` |
| Checkbox | `{ "checkbox": {{ row.x }} }` |
| Date | `{ "date": { "start": "{{ row.x }}" } }` |

For `datetime` / `Decimal` / `UUID` values flowing into the template,
use the `tojson_safe` filter (v0.7.6+) to avoid serialization errors:
`"{{ row.created_at | tojson_safe }}"`.

## Notes

- (core) — no extra install required.
- Append-only — each row creates a new page. This destination does not
  update or dedupe existing pages.
- Every database referenced must be shared with the integration, or the
  API rejects the request.
- Per-row failures (property type mismatch, missing share) are recorded
  in `result.row_errors`; `on_error: skip` continues the batch.
- The exact property JSON is type-sensitive — preview with `--dry-run`
  and validate against one row before a full run.

## References

- [Notion API — create a page](https://developers.notion.com/reference/post-page)
- [Page property values](https://developers.notion.com/reference/page-property-values)
