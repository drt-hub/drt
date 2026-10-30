# Pipedrive Destination

> Create or update Pipedrive persons from warehouse rows. Pipedrive has no native upsert, so drt searches for an existing person first and then updates it or creates a new one.

## Installation

No extra package is needed:

```
pip install drt-core
```

## YAML Example

```yaml
destination:
  type: pipedrive
  api_token_env: PIPEDRIVE_API_TOKEN
  object: person
  match_field: email
sync:
  mode: full
  match_policy: upsert
  field_mappings:
    customer_tier: 9d5bd2d0f3a3c5ee1b2c3d4e5f60718293a4b5c6   # Pipedrive custom-field key
```

Set `PIPEDRIVE_API_TOKEN` to your Pipedrive API token. In this example the model query returns the columns `name`, `email` and `customer_tier`.

## Configuration

| Field          | Type           | Default                            | Description                                                                                  |
|----------------|----------------|------------------------------------|----------------------------------------------------------------------------------------------|
| `type`         | `"pipedrive"`  | —                                  | Required                                                                                     |
| `api_token`    | string \| null | null                               | API token. Prefer `api_token_env` so no secret sits in YAML                                  |
| `api_token_env`| string \| null | `PIPEDRIVE_API_TOKEN`              | Name of the environment variable that holds the API token                                    |
| `base_url`     | string \| null | `https://api.pipedrive.com/api/v2` | Override the API base URL (must start with `http://` or `https://`)                          |
| `object`       | `"person"`     | `person`                           | Pipedrive object to write. Only `person` is supported for now                                |
| `match_field`  | string         | `email`                            | Column used to find an existing person: `email`, `name` or `phone`                           |
| `retry`        | object \| null | null                               | Retry settings; overrides `sync.retry` as a whole when set                                   |
| `rate_limit`   | object \| null | null                               | Request pacing; overrides `sync.rate_limit` as a whole when set                              |

At least one of `api_token` and `api_token_env` is required.

## Authentication

The token is sent in the `x-api-token` request header and never in the URL, so it cannot end up in request logs. A Pipedrive API token belongs to one user in one company, and requests can do whatever that user is allowed to do. OAuth is not supported.

## How matching works

For every row, drt reads the value of the `match_field` column and searches Pipedrive for an exact match (`GET /persons/search` with `exact_match=true`; Pipedrive's exact match is not case sensitive):

| Search result    | `match_policy: upsert` (default) | `update_only`                   | `create_only`                   |
|------------------|----------------------------------|---------------------------------|---------------------------------|
| No person found  | Create a new person (`POST`)     | Skip the row                    | Create a new person (`POST`)    |
| One person found | Update that person (`PATCH`)     | Update that person (`PATCH`)    | Skip the row                    |
| Several found    | Row error, nothing is written    | Row error, nothing is written   | Row error, nothing is written   |

drt never guesses which of several matching people to update. Skipped rows are counted as `skipped`, and the ones skipped because of the policy are also counted in `skipped_no_match`. A row with an empty `match_field` value is a row error, and so is a search response that drt cannot read: it does not write in that case, because it could create a duplicate.

Within one batch, match values are remembered (case-insensitively). A second row with the same key updates the person the first row just created, or found, without searching again. If Pipedrive ever answers a create without returning the new person's id, a later row with the same key becomes a row error rather than risking a duplicate.

The memory lasts for one batch (one `load()` call). Across batches a later row relies on Pipedrive's search to find a person created earlier in the run, and the search index has been reported to lag briefly after a create. Keep rows that share a key inside one batch, or deduplicate them in your model query.

## Mapping columns to Pipedrive fields

`sync.field_mappings` renames columns, and the rename runs before the destination sees the rows. After that, drt maps each column like this:

| Column                | Becomes                                                                              |
|-----------------------|--------------------------------------------------------------------------------------|
| `name`                | `name` (required when a person is created)                                           |
| `owner_id`, `org_id`, `visible_to`, `label_ids` | The Pipedrive field of the same name                       |
| `email` / `emails`    | `emails: [{value, primary}]`. Accepts one value, a list of values, or Pipedrive objects. The first entry is marked primary if none is |
| `phone` / `phones`    | `phones: [{value, primary}]`, same rules                                             |
| **any other column**  | `custom_fields[<column name>]`                                                       |

**Custom fields:** Pipedrive stores custom fields under randomly generated 40-character keys. drt does not look these up for you, so `field_mappings` must rename every custom-field column to Pipedrive's key (see the example above; the keys are listed by Pipedrive's Person fields API). An unknown key is **not dropped**: it is sent as is, Pipedrive rejects it, and the API's message becomes a row error for that record.

A `NULL` in a custom-field column is sent as `null`, which Pipedrive treats as "clear this field". Filter or `COALESCE` such values in your model if you do not want existing values cleared.

When a person is updated, the `match_field` column itself is left out of the update because it already matches. An update with nothing else to change is counted as `skipped`.

## Sync modes

`full`, `incremental` and `upsert` all run the search-then-write path above for each row. `replace` and `mirror` are rejected by the engine before anything is extracted, because Pipedrive cannot do those operations. `sync.write_policy: fill_empty` is not supported either and is rejected the same way. Pipedrive cannot be read back by drt, so `drt plan` reports "plan unavailable" for this destination.

## Errors and retries

- Each failed row becomes a row error with the HTTP status and the first 500 characters of Pipedrive's response, and `sync.on_error: skip` (default) continues with the next row. With `on_error: fail`, drt stops at the first failed row and the remaining rows are not attempted.
- Transient errors (HTTP 429 and 5xx) are retried with exponential backoff by drt's shared retry layer; configure it with `retry`. A 429 caused by an exhausted daily budget keeps failing until the budget resets.
- A missing token raises an error for the whole batch. An empty batch returns immediately without contacting Pipedrive.

## Rate limits and per-row cost

Pipedrive charges each API call against a daily token budget that depends on your plan and number of seats, and it also limits bursts per token. According to Pipedrive's Persons API reference, a person search costs **20** tokens and a create or update costs **5**, so each row costs about **25** tokens (1,000 rows are roughly 25,000 tokens). Rows skipped by `match_policy` still cost the 20-token search, and a repeated key within a batch costs only one search.

Use `rate_limit.requests_per_second` (on the destination or in `sync`) to stay below your plan's burst limit:

```yaml
destination:
  type: pipedrive
  api_token_env: PIPEDRIVE_API_TOKEN
  rate_limit:
    requests_per_second: 5
```

## References

- [Pipedrive Persons API](https://developers.pipedrive.com/docs/api/v1/Persons)
- [Pipedrive authentication](https://pipedrive.readme.io/docs/core-api-concepts-authentication)
- [Pipedrive rate limiting](https://pipedrive.readme.io/docs/core-api-concepts-rate-limiting)
- [Pipedrive: guide for optimizing API usage](https://pipedrive.readme.io/docs/guide-for-optimizing-api-usage)