# Jira Destination

> Create or update Jira issues from source rows via the Jira Cloud REST
> API v3. Create-by-default; updates when the row carries a known issue
> id.

## YAML Example

```yaml
destination:
  type: jira
  base_url_env: JIRA_BASE_URL          # https://myorg.atlassian.net
  email_env: JIRA_EMAIL
  token_env: JIRA_API_TOKEN
  project_key: "OPS"
  issue_type: "Task"
  summary_template: "Sync failure: {{ row.sync_name }}"
  description_template: "Run {{ row.run_id }} failed with: {{ row.error }}"
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"jira"` | — | Required |
| `base_url_env` | string | — | Env var → site URL (e.g. `https://myorg.atlassian.net`) |
| `email_env` | string | — | Env var → Jira account email |
| `token_env` | string | — | Env var → Jira API token |
| `project_key` | string | — | Target project key; supports Jinja2 (e.g. `{{ row.team }}`) |
| `issue_type` | string | `"Task"` | Issue type; supports Jinja2 |
| `summary_template` | string | — | Jinja2 template → issue summary |
| `description_template` | string | — | Jinja2 template → issue description |
| `issue_id_field` | string | `"issue_id"` | Row key that, when present, switches to update mode |
| `retry` | object \| null | null | Per-destination retry override |

## Authentication

Jira Cloud uses HTTP Basic auth with your account **email** as the
username and an **API token** as the password. Create a token at
[id.atlassian.com → API tokens](https://id.atlassian.com/manage-profile/security/api-tokens):

```bash
export JIRA_BASE_URL="https://myorg.atlassian.net"
export JIRA_EMAIL="you@example.com"
export JIRA_API_TOKEN="your-api-token"
```

## Create vs. update

- **Create (default):** if the source row has no `issue_id_field` value,
  drt `POST`s to `/rest/api/3/issue` to create a new issue.
- **Update:** if the row carries a value at `issue_id_field` (default
  `issue_id`), drt `PUT`s to `/rest/api/3/issue/<id>` to update that
  issue instead.

This lets a single sync both open new issues and keep existing ones in
step — include the Jira issue id in your source model for rows that map
to already-created issues.

## Common Patterns

**Route to a project per row:**
```yaml
project_key: "{{ row.team_project_key }}"
```

**Bug type with a templated summary:**
```yaml
issue_type: "Bug"
summary_template: "[{{ row.severity }}] {{ row.title }}"
```

## Notes

- (core) — no extra install required.
- `description_template` renders into the issue description. For Jira
  Cloud's Atlassian Document Format (ADF), supply the ADF JSON in the
  template if you need rich formatting; plain text is accepted as-is.
- The API token's user must have create/edit permission on the target
  project.
- Per-row failures (missing required field, permission denied) land in
  `result.row_errors`; `on_error: skip` continues the batch.

## References

- [Jira Cloud REST API v3 — issues](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issues/)
- [Manage API tokens](https://support.atlassian.com/atlassian-account/docs/manage-api-tokens-for-your-atlassian-account/)
