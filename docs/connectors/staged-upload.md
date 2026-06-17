# Staged Upload Destination

> Generic async bulk-API destination: stage a file, trigger a job, optionally poll for completion. Core connector — no extra install.

This is a building block for vendor bulk/batch APIs that follow the **upload → trigger → poll** shape (many marketing / CRM / ad platforms). The whole batch is serialised once and pushed through the three phases.

## YAML Example

```yaml
destination:
  type: staged_upload
  format: csv                    # "csv" (default) | "json" | "jsonl"
  stage:
    url: https://api.example.com/v1/uploads
    method: POST
    auth: { type: bearer, token_env: VENDOR_TOKEN }
    response_extract: { upload_id: "$.id", upload_url: "$.url" }
  trigger:
    url: https://api.example.com/v1/imports
    method: POST
    auth: { type: bearer, token_env: VENDOR_TOKEN }
    body_template: '{"upload_id": "{{ upload_id }}"}'
    response_extract: { job_id: "$.job_id" }
  poll:
    url: https://api.example.com/v1/imports/{{ job_id }}
    status_field: status
    success_values: [SUCCEEDED, COMPLETED]
    failure_values: [FAILED, ERROR]
    interval_seconds: 30
    timeout_seconds: 3600
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"staged_upload"` | — | Required |
| `format` | `"csv"` \| `"json"` \| `"jsonl"` | `"csv"` | Serialisation of the staged batch. |
| `stage` | phase | — | Phase 1 — upload the file. **Required** |
| `trigger` | phase | — | Phase 2 — kick off the import job. **Required** |
| `poll` | poll \| null | null | Phase 3 — poll the job to completion (optional; omit for fire-and-forget). |

**Phase (`stage` / `trigger`)**: `url`, `method` (`POST` default), `headers`, `auth`, `body_template`, `response_extract` (a `{name: JSONPath}` map that pulls values out of the response for later phases — e.g. an upload ID or signed URL).

**Poll (`poll`)**: `url`, `method` (`GET` default), `headers`, `auth`, `status_field` (default `status`), `success_values` (default `[SUCCEEDED, COMPLETED]`), `failure_values` (default `[FAILED, ERROR]`), `interval_seconds` (default 30), `timeout_seconds` (default 3600).

## How it works

1. **Stage** — the batch is serialised (`format`) and POSTed; `response_extract` captures values (e.g. `upload_id`).
2. **Trigger** — `body_template` (with the staged values in scope) starts the import; `response_extract` captures the `job_id`.
3. **Poll** — if configured, drt polls `url` every `interval_seconds` until `status_field` hits a `success`/`failure` value or `timeout_seconds` elapses.

`auth:` blocks accept the same shapes as the [REST API destination](rest-api.md).

## Notes

- Core connector — no `pip install` extras needed.
- Empty batches short-circuit — no upload / trigger / poll is performed when the source produced no rows.
- Purpose-built siblings on top of the same idea: [Salesforce Bulk API 2.0](../../drt/destinations/salesforce_bulk.py).
