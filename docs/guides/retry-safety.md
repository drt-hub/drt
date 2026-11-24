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

