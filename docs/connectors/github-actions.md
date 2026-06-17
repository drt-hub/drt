# GitHub Actions Destination

> Trigger a GitHub Actions workflow (`workflow_dispatch`) per row. Core connector — no extra install.

## YAML Example

```yaml
destination:
  type: github_actions
  owner: my-org
  repo: my-app
  workflow_id: deploy.yml          # filename or numeric workflow ID
  ref: main                        # branch/tag to run on
  inputs_template: '{"environment": "{{ row.env }}", "version": "{{ row.version }}"}'
  auth:
    type: bearer
    token_env: GITHUB_TOKEN
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"github_actions"` | — | Required |
| `owner` | string | — | Repository owner (org or user). **Required** |
| `repo` | string | — | Repository name. **Required** |
| `workflow_id` | string | — | Workflow filename (e.g. `deploy.yml`) or numeric ID. **Required** |
| `ref` | string | `"main"` | Branch or tag the workflow runs on. |
| `inputs_template` | string \| null | null | Jinja2 template rendering a JSON object of `workflow_dispatch` inputs. |
| `auth` | Bearer | bearer | Token auth — set `token_env` to an env var holding a PAT / fine-grained token. |
| `retry` | RetryConfig \| null | null | Per-destination override of `sync.retry`. |

## Authentication

The workflow must declare a `workflow_dispatch` trigger. The token needs `actions: write` on the repo:

```bash
export GITHUB_TOKEN="ghp_..."   # or a fine-grained token with Actions: read/write
```

```yaml
auth:
  type: bearer
  token_env: GITHUB_TOKEN
```

## Notes

- Core connector — no `pip install` extras needed.
- One workflow-dispatch call per row; `inputs_template` keys must match the workflow's declared `inputs:`.
- Useful for activation patterns where a warehouse row should kick off a deploy / backfill / notification pipeline.
