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
| `retry_creates` | bool | `false` | `true` restores blind replay of an ambiguous run create (duplicate risk); see [Retry safety](#retry-safety) |
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

## Rate limiting

**Vendor limit:** 1,000 `workflow_dispatch` calls per hour per repository. **drt caps this destination at 5 req/s** — deliberately conservative, since one row equals one workflow run.

The limiter is shared per **token**, not per repository: GitHub's REST quota is per authenticated user/app across *all* repositories, so two syncs dispatching workflows into different repos under one token still share one budget.

Override to go **lower** than the cap:

```yaml
destination:
  type: github_actions
  owner: myorg
  repo: myrepo
  rate_limit:
    requests_per_second: 1
```

`destination.rate_limit` beats `sync.rate_limit`, which beats the default of 10/s. When two syncs share a token but request different rates, the lowest wins for both.

## Retry safety

A [`workflow_dispatch`](https://docs.github.com/en/rest/actions/workflows#create-a-workflow-dispatch-event) request carries no idempotency key, so it is a **blind create**. After a dropped response or a
retryable 5xx, drt returns a delivery-unknown `RowError` after one request: assume
a workflow run may already exist, check GitHub Actions before replaying, and do not auto-resend.
HTTP 429 and connection failures proven to happen before the request was sent
remain retryable.

`retry_creates: true` restores the legacy second attempt after an ambiguous
write and logs a duplicate-risk warning. See [retry safety](../guides/retry-safety.md).
Vendor contract rechecked 2026-11-25.

## Notes

- Core connector — no `pip install` extras needed.
- One workflow-dispatch call per row; `inputs_template` keys must match the workflow's declared `inputs:`.
- Useful for activation patterns where a warehouse row should kick off a deploy / backfill / notification pipeline.
- A successful dispatch confirms that GitHub accepted the request, not that the
  workflow finished. For incremental syncs, use a
  [reconciliation sync](../guides/reconciliation-syncs.md) when missed
  downstream work must be recovered.
