# Retry safety for creates and messages

An HTTP failure does not always mean the destination rejected a write. The
destination can commit an email, SMS, message, object, or event and then lose
the response. Retrying that request is safe only when the operation has a
stable identity or drt can reconcile the first attempt.

drt uses three write classifications:

- **Blind:** the destination exposes no proven replay key or exact lookup. After
  a response-read failure or retryable 5xx, drt assumes the write may have been
  delivered and returns a delivery-unknown `RowError`. It does not auto-resend.
- **Keyed:** every attempt carries the same non-empty vendor-supported identity.
  The destination contract deduplicates the retry, so normal retry policy applies.
- **Reconciled:** after an ambiguous create, drt searches with the same exact
  natural key. One match is accepted, multiple matches are refused, and a miss
  fails closed because a new write may not be visible in search yet.

HTTP 429 and connection-establishment failures proven to happen before request
bytes were sent remain retryable in every class. Read, write, and response
transport failures are delivery-unknown for blind and reconciled creates.

## Destinations covered so far

| Destination / operation | Classification | Replay or reconciliation identity | Scope and duplicate risk |
|---|---|---|---|
| Generic REST record `POST`, no native key | Blind | None | A retry can repeat any target side effect. |
| Generic REST record `POST` with a non-empty `native_idempotency_key` | Keyed only when the operator confirms the target honors the configured header | Rendered row + sync template, reused across attempts and runs | The key protects only for the target API's documented retention/scope. drt cannot verify a generic endpoint's promise. |
| Generic REST batch `POST` | Blind | The generated `request_id` is stable only inside one immediate retry loop and proves no target behavior | A retry can repeat the whole chunk. |
| SendGrid Mail Send | Blind | None. [`batch_id` controls scheduled-send grouping/cancellation](https://www.twilio.com/docs/sendgrid/for-developers/sending-email/scheduling-parameters); [`custom_args` are carried as event metadata](https://www.twilio.com/docs/sendgrid/for-developers/sending-email/unique-arguments), not dedup keys. | The recipient can receive the email twice. |
| Twilio classic Messages create | Blind | The [Messages create contract](https://www.twilio.com/docs/messaging/api/message-resource) exposes no client replay key. `I-Twilio-Idempotency-Token` documents Twilio-to-application webhook retries, not this outbound Messages request. | The recipient can receive and the account can be charged for two SMS messages. |
| Slack Incoming Webhook | Blind | The [Incoming Webhook contract](https://docs.slack.dev/messaging/sending-messages-using-incoming-webhooks/) exposes no client key or returned message timestamp. | A channel receives duplicate messages. |
| Discord Execute Webhook | Blind | The [Execute Webhook parameters](https://docs.discord.com/developers/resources/webhook#execute-webhook) expose no idempotency field. `wait` requests confirmation; it does not deduplicate. | A channel receives duplicate messages or embeds. |
| Microsoft Teams Incoming Webhook / workflow URL | Blind | The [Teams incoming-webhook contract](https://learn.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook) exposes no client dedup key. | A channel receives duplicate cards or messages; a workflow can trigger twice. |
| Pipedrive person create | Reconciled | Exact configured `match_field` search | Protects the immediate attempt when one match is visible. A stale miss fails closed; later runs still need the warehouse ledger or source discipline. |
| Intercom contact create | Reconciled when non-empty `external_id` and/or email exists; otherwise blind | Exact contact search | Same immediate protection and search-lag boundary; duplicate contacts are the risk. |
| HubSpot CRM object create | Reconciled when configured `id_property` is non-empty; otherwise blind | Exact equality search on `id_property` | Same immediate protection and search-lag boundary; duplicate CRM objects are the risk. |
| Klaviyo profile upsert | Keyed | Native email-keyed profile-import operation | Normal retries address one logical profile. |
| Klaviyo `create_only` profile and list membership | Reconciled | Exact email/profile membership lookup | A miss fails closed; duplicate profiles or relationship writes are the risk. |
| Klaviyo event | Keyed | Required stable `unique_id_field` with the same profile and metric | Normal retries address one logical event within Klaviyo's documented dedup scope. |

Vendor claims and endpoints above were rechecked on 2026-11-23. An absent field
or a similarly named feature on another endpoint is not enough to promote a
blind write to keyed.

## Every registered destination

This index lists **every** destination type drt registers (a test fails when a new
connector is missing here). "Status" says whether the write has been classified
under [ADR 0023](../adr/0023-non-idempotent-create-retries.md). Where it has not,
drt still retries an ambiguous write as it always did, so a lost response can
duplicate the object or message; the planned ADR step is noted.

| Type | Write kind | What drt retries | Status |
|---|---|---|---|
| `airtable` | Record create / upsert | One HTTP attempt loop around each request | Not yet classified (ADR 0023 item 3) |
| `amplitude` | Event ingestion | One HTTP attempt loop around each request | Not yet classified; stable-key verification planned (ADR 0023 item 3) |
| `azure_blob` | Object upload | Nothing: no drt retry; the SDK's own retries are not verified here | Documented only (ADR 0023 item 4) |
| `bigquery` | Warehouse write | Nothing: drt does not wrap warehouse writes in retries; replay safety follows `sync.mode` and `upsert_key` | Documented only; see the connector page |
| `clickhouse` | Warehouse insert | One retry with the same `insert_deduplication_token`; best effort, only effective when the table's deduplication window is enabled | Documented (ADR 0023 item 5) |
| `databricks` | Warehouse write | Nothing: as for `bigquery` | Documented only |
| `discord` | Webhook message | Blind (see above) | Classified |
| `elasticsearch` | Document index | One HTTP attempt loop around each request | Not yet classified; auto-generated IDs are blind (ADR 0023 item 3) |
| `email_smtp` | Email send | Nothing: drt does not retry SMTP sends | Documented only (ADR 0023 item 4) |
| `file` | Local file write | Nothing | Not applicable: no remote side effect |
| `gcs` | Object upload | Nothing: no drt retry; the SDK's own retries are not verified here | Documented only (ADR 0023 item 4) |
| `github_actions` | Workflow dispatch | One HTTP attempt loop around the dispatch | Not yet classified; a replay can start a second run (ADR 0023 item 1) |
| `google_ads` | Conversion / offline upload | One HTTP attempt loop around each request | Not yet classified (ADR 0023 item 3) |
| `google_sheets` | Sheet append | Nothing: drt does not retry the append | Documented only (ADR 0023 item 4) |
| `hubspot` | CRM object create / update | Reconciled when `id_property` is set (see above) | Classified |
| `intercom` | Contact create / update | Reconciled when a key exists (see above) | Classified |
| `jira` | Issue create | One HTTP attempt loop around each request | Not yet classified; a replay can create a second issue (ADR 0023 item 1) |
| `klaviyo` | Profile / list / event | Keyed or reconciled (see above) | Classified |
| `linear` | Issue create | One HTTP attempt loop around each request | Not yet classified; a replay can create a second issue (ADR 0023 item 1) |
| `meta_conversions` | Conversion event | One HTTP attempt loop around each request | Not yet classified; keyed-path verification planned (ADR 0023 item 3) |
| `mixpanel` | Event / profile | One HTTP attempt loop around each request | Not yet classified; keyed-path verification planned (ADR 0023 item 3) |
| `mysql` | Warehouse write | Nothing: as for `bigquery` | Documented only |
| `notion` | Page create | One HTTP attempt loop around each request | Not yet classified; a replay can create a second page (ADR 0023 item 1) |
| `parquet` | Local file write | Nothing | Not applicable: no remote side effect |
| `pipedrive` | Person create / update | Reconciled (see above) | Classified |
| `postgres` | Warehouse write | Nothing: as for `bigquery` | Documented only |
| `rest_api` | Generic HTTP write | Blind, or keyed with a configured native key (see above) | Classified |
| `s3` | Object upload | Nothing: no drt retry; the SDK's own retries are not verified here | Documented only (ADR 0023 item 4) |
| `salesforce_bulk` | Bulk job create / upload | Only the job-status **poll** (a read) is retried; job creation and upload are not | Documented (ADR 0023 item 4) |
| `sendgrid` | Email send | Blind (see above) | Classified |
| `slack` | Webhook message | Blind (see above) | Classified |
| `snowflake` | Warehouse write | Nothing: as for `bigquery` | Documented only |
| `sqs` | Queue message | Only transiently failed batch entries are resent; FIFO/standard identity is not yet verified here | Not yet classified (ADR 0023 item 3) |
| `staged_upload` | Stage + trigger + poll | Only the optional status **poll** (a read) is retried; staging and trigger are not | Documented (ADR 0023 item 4) |
| `teams` | Webhook message | Blind (see above) | Classified |
| `twilio` | SMS / message | Blind (see above) | Classified |
| `zendesk` | Ticket create | One HTTP attempt loop around each request | Not yet classified; a replay can create a second ticket (ADR 0023 item 2) |

## Opting back into blind replay

These destinations default to:

```yaml
destination:
  retry_creates: false
```

Set `retry_creates: true` only when availability is worth the duplicate-message
or duplicate-side-effect risk. It restores the legacy second attempt after an
ambiguous write and logs a warning when that escape hatch is actually used. It
does not change 429, proven pre-send connection, safe read, addressed update, or
keyed-write retries.

## Recovering a delivery-unknown row

Assume the first request may have succeeded. Inspect the destination using safe
business context, timestamps, recipients, or the configured natural key. Remove
or merge duplicates if necessary, then decide whether to replay the DLQ row. Do
not automatically call `drt retry` merely because drt did not receive success.

The optional warehouse idempotency ledger is useful across later runs and DLQ
replays, but it cannot settle this immediate ambiguity: drt marks the ledger only
after a positively confirmed destination result. Claiming before send would
trade duplicates for silent message loss.

