# ADR 0023 — Retrying non-idempotent creates

- **Status:** Accepted 2026-11-20; rollout step 5's unkeyed half is implemented
  for Airtable append, Elasticsearch/OpenSearch auto-id bulk requests, and Google
  Ads uploads without order IDs (2026-11-28). Their keyed request variants retain
  normal replay; the remaining step 5 event/queue destinations are still pending.
- **Issue:** [#1307](https://github.com/drt-hub/drt/issues/1307)
- **Relates to:** [ADR 0005](0005-state-location-and-write-grants.md) (durable
  state and the warehouse idempotency ledger),
  [ADR 0007](0007-protocol-stability-policy.md) (the frozen `Destination`
  Protocol), [#897](https://github.com/drt-hub/drt/issues/897) (destination-native
  idempotency keys), and [#1099](https://github.com/drt-hub/drt/issues/1099)
  (warehouse-backed delivery ledger).

## Context

### The ambiguous-success window

`with_retry()` knows whether an HTTP attempt raised, not whether the remote side
committed. By default it retries `httpx.HTTPStatusError` for 429, 500, 502, 503
and 504, every `httpx.TransportError`, and anything an additive `retry_on`
predicate approves. It honors `Retry-After` and makes up to three attempts
(`drt/destinations/retry.py:98-158`, `drt/config/base.py:536-541`).
`resolve_retry()` selects `destination.retry` over `sync.retry`; otherwise the
sync default applies (`drt/destinations/retry.py:22-32`). The retry helper has no
HTTP method, endpoint, idempotency key or operation kind in its contract. A
callable that performs `POST /persons` is therefore indistinguishable from one
that performs a safe status read.

That is wrong for a blind create. The server can commit a person, issue, page,
message, email, workflow or event and then lose the response. A reverse proxy can
also return a retryable 5xx after its upstream committed. Re-running the same
callable creates the side effect a second time. The first response never reaches
the connector, so a destination cache populated *after* a successful return does
not close the window. Pipedrive demonstrates the exact shape: it searches before
the create and remembers a returned id for the rest of the sync, but `api.create()`
itself is inside `_request()`'s retry loop and the cache assignment happens only
after it returns (`drt/destinations/pipedrive.py:138-183`,
`drt/destinations/pipedrive.py:189-198`,
`drt/destinations/pipedrive.py:278-304`). A committed create followed by a lost
response reaches attempt two before either the cache or another engine boundary
can help.

The inverse failure matters too. Disabling every POST retry would also disable
safe reads expressed as POST (HubSpot batch read and Intercom search), keyed
upserts, deduplicated event ingestion, and POST-shaped APIs whose operation is
idempotent even though the verb is not. The decision must classify the operation,
not mechanically ban one verb.

### Audit method and notation

This audit inspected every `drt/destinations/*.py` HTTP/SDK write call, the shared
retry and auth modules, generic REST, the ledger, and DLQ replay. The table is
grounded in the checked-in implementation, not an assumption that all vendor APIs
behave alike. `R` below means the shared retry set: configured status codes
(defaults 429/500/502/503/504) plus all `httpx.TransportError`s. `R+` means a
connector adds a predicate. “Unverified” means the repository does not establish a
vendor idempotency guarantee strongly enough for this ADR; it is deliberately not
an assertion that the vendor lacks one.

The existing mitigations column uses these names:

- **ledger #1099** — the optional Postgres warehouse delivery ledger;
- **match policy** — `update_only` / `create_only` behavior;
- **search-then-create** — a natural-key lookup before a create;
- **native key** — a stable value sent to a vendor-supported dedup surface.

### Destination audit

| Destination | Operation | HTTP verb / endpoint | Does retry wrap it? | Idempotency or natural key | What a duplicate looks like | Upsert-by-key or blind create | Existing mitigations |
|---|---|---|---|---|---|---|---|
| Generic REST | Record or batch request; pagination helper assumes a read but reuses the configured method | Configured `GET/POST/PUT/PATCH/DELETE` and URL | **Yes, R**, for record and batch writes and paginated requests (`drt/destinations/rest_api.py:150-188`, `drt/destinations/rest_api.py:243-280`, `drt/destinations/rest_api.py:419-434`) | Operator can render `native_idempotency_key` into a configurable header. Whether the target honors it is **unverified** (`drt/config/destinations_saas.py:71-97`). Batch mode generates a fresh request id per chunk/run, stable only inside that immediate retry loop (`drt/destinations/rest_api.py:117-148`). | Whatever the target creates or triggers is repeated; a batch can repeat every row. A mutating method used for pagination would repeat there too. | Unknown by construction; default method is blind `POST`. | Native header; ledger #1099 for keyed non-staged records. |
| HubSpot | CRM object create-or-update | `POST /crm/v3/objects/{object_type}`, then `PATCH .../{id}?idProperty=...` on 409 | **Yes, R** around the combined POST/409/PATCH operation (`drt/destinations/hubspot.py:100-163`, `drt/destinations/hubspot.py:326-373`) | Natural `id_property` is carried in the payload; vendor idempotency-key support is **unverified**. | A second contact/deal/company if the chosen property is not uniquely enforced; otherwise the retry becomes an update. | Upsert-by-key in drt. | 409-to-PATCH recovery; match policy; fill-empty batch read; ledger #1099 when a key resolves. |
| Pipedrive | Person create after exact search | `GET /persons/search`, then `POST /persons`; updates use `PATCH /persons/{id}` | **Yes, R** around every `_request`, including create (`drt/destinations/pipedrive.py:138-186`) | Exact `match_field` search is a natural reconciliation key; native idempotency-key support is **unverified**. | Two persons with the same email/name/phone. | Search-then-create, but the create attempt itself is blind. | Search-then-create; match policy; per-sync cache only after response; ledger #1099 if explicitly keyed. |
| Salesforce Bulk 2.0 | Create ingest job, upload CSV, close job; row operation may be insert/update/upsert/delete | `POST /jobs/ingest`, `PUT /jobs/ingest/{id}/batches`, `PATCH /jobs/ingest/{id}` | **No** for auth, job creation, upload and close. Only status `GET` uses R (`drt/destinations/salesforce_bulk.py:70-87`, `drt/destinations/salesforce_bulk.py:116-183`). | Upsert rows carry `externalIdFieldName`; job-create idempotency is **unverified** (`drt/config/destinations_saas.py:1148-1161`). | A second ingest job; `insert` may create duplicate objects, while keyed upsert may rewrite the same ones. | Configurable; job creation is blind, row upsert can be natural-keyed. | No immediate create retry today; staged destinations are excluded from ledger #1099 and DLQ ledger handling. |
| Intercom | Contact create, conflict-to-update; search for `update_only` | `POST /contacts`, conflict then `PUT /contacts/{id}`; search is `POST /contacts/search` | **Yes, R** around the whole branch (`drt/destinations/intercom.py:173-207`); the search call is reached through that outer retry | Payload `id`, `external_id` or email can locate a contact; 409 can return an existing id. Native key support is **unverified**. | A second contact if the committed create is not surfaced as a recoverable conflict. | Upsert-like conflict recovery; blind create before conflict is known. | 409-to-PUT; match policy; search for update-only; ledger #1099 when explicitly keyed. |
| Notion | Create database page | `POST /v1/pages` | **Yes, R** (`drt/destinations/notion.py:70-120`) | **Unverified**. No key or search is wired in the repository. | Two pages for one source row. | Blind create. | Ledger #1099 if the sync supplies a stable key; otherwise none. |
| Linear | Create issue GraphQL mutation | `POST /graphql` with `issueCreate` | **Yes, R** (`drt/destinations/linear.py:43-51`, `drt/destinations/linear.py:109-123`) | **Unverified**. No key or lookup is wired. | Two issues. | Blind create. | Ledger #1099 if explicitly keyed. |
| Jira | Create issue or update a supplied issue id | `POST /rest/api/3/issue`; `PUT /rest/api/3/issue/{id}` | **Yes, R** for both (`drt/destinations/jira.py:31-94`) | Supplied `issue_id_field` is a natural update key; create idempotency is **unverified**. | Two issues when the source row has no issue id. | Blind create or id-addressed update. | Source-provided issue id; ledger #1099 if explicitly keyed. |
| SendGrid | Send transactional email | `POST /v3/mail/send` | **Yes, R** (`drt/destinations/sendgrid.py:47-48`, `drt/destinations/sendgrid.py:108-118`) | **Unverified**. No key is wired. | Recipient receives the email twice. | Blind side effect. | Ledger #1099 if explicitly keyed. |
| Twilio | Send SMS | `POST /Accounts/{sid}/Messages.json` | **Yes, R** (`drt/destinations/twilio.py:77-109`) | Classic Messages endpoint key support is **unverified** for this ADR; no key is wired. | Recipient receives two SMS messages and may be charged twice. | Blind side effect. | Ledger #1099 if explicitly keyed. |
| Zendesk | User or organization create-or-update | `POST /users/create_or_update_many.json`; `POST /organizations/create_or_update.json` | **Yes, R** (`drt/destinations/zendesk.py:139-155`, `drt/destinations/zendesk.py:188-214`) | Payload may carry `id`/`external_id`; semantics without one and vendor key support are **unverified** (`drt/destinations/zendesk.py:264-278`). | A second user/organization when no stable identity is honored. | Natural-key upsert when an id is present; otherwise potentially blind. | Endpoint-level create-or-update; configurable id field; ledger #1099. |
| Klaviyo profiles | Profile create, conflict-to-update, optional list membership | `POST /profiles`, `PATCH /profiles/{id}`, `POST /lists/{id}/relationships/profiles` | **Yes, R** for all three (`drt/destinations/klaviyo.py:241-298`) | Email produces a 409 carrying the existing profile id in the implemented contract; separate vendor idempotency-key support is **unverified**. | A duplicate profile if conflict identity fails; repeated list membership should be treated as **unverified**. | Upsert-by-email in drt. | 409-to-PATCH; relationship uses returned profile id; ledger #1099. |
| Klaviyo events | Ingest event | `POST /events` | **Yes, R** (`drt/destinations/klaviyo.py:151-221`) | Config requires a stable `unique_id_field` for events (`drt/config/destinations_saas.py:540-551`). Exact vendor dedup scope/retention is **unverified** here. | Event counted twice or automation triggered twice if vendor dedup does not apply. | Keyed event. | Required unique id; optional `backfill` suppresses live flows; ledger #1099 can add cross-run defense. |
| Airtable | Append records or perform upsert | Append `POST /v0/{base}/{table}`; upsert `PATCH` with `performUpsert.fieldsToMergeOn` | **Yes, R** (`drt/destinations/airtable.py:68-94`) | Configured `primary_key` is a natural merge key for PATCH; append key support is **unverified**. | Duplicate Airtable rows, potentially the whole ten-row chunk. | Blind append or upsert-by-key. | `primary_key`/performUpsert; ledger #1099. |
| Mixpanel | Profile set or event import | `POST /engage`; `POST /import` | **Yes, R** (`drt/destinations/mixpanel.py`) | Profiles carry `$distinct_id`; events receive `$insert_id`, with fallback hashes excluding configured run metadata. Mixpanel deduplicates on event name, `distinct_id`, `time`, and `$insert_id`; drt classifies only a usable final tuple with a non-future integer timestamp. | Repeated profile set or double-counted event if the tuple is not honored or Mixpanel rewrites a component. | Idempotent profile set; event is keyed only when the final tuple is proven. | Natural profile id; full event tuple; ledger #1099. |
| Amplitude | Identify user or ingest events | `POST /identify`; `POST /2/httpapi` | **Yes, R** (`drt/destinations/amplitude.py`) | Identify carries user/device id; events get a source, existing, or fallback `insert_id`. Fallback hashes exclude configured run metadata. Vendor dedup retention is **unverified**. | Repeated identify mutation or double-counted event if the key is not honored. | Identify unclassified; event keyed. | Natural user/device id; stable insert id; ledger #1099. |
| Meta Conversions | Upload conversion events | `POST /{version}/{pixel_id}/events` | **Superseded by the 2026-11-28 amendment below.** The connector still retries R+ exactly as before, including HTTP 400 only when the response says `is_transient: true` (`drt/destinations/meta_conversions.py`). | `event_id` + `event_name` is documented for Pixel-to-server deduplication, not two server submissions. | Conversion may be counted twice after an ambiguous response. | **Unclassified; previous keyed inventory claim superseded.** | Keep previous retry behavior pending proof; ledger #1099 remains defense in depth. |
| Google Ads | Upload click conversions | `POST /customers/{id}:uploadClickConversions` | **Yes, R** (`drt/destinations/google_ads.py:270-285`) | Optional `native_idempotency_key` is rendered as `orderId`; repository code recognizes `ORDER_ID_ALREADY_IN_USE` as successful dedup (`drt/destinations/google_ads.py:135-157`, `drt/destinations/google_ads.py:239-248`, `drt/destinations/google_ads.py:332-358`). | Conversion imported twice when no order id is supplied. | Keyed only when configured; otherwise blind event upload. | Native order id; duplicate response treated as success; ledger #1099. |
| Slack | Incoming webhook message | `POST` configured webhook URL | **Yes, R** (`drt/destinations/slack.py:64-90`) | **Unverified**. No key is wired. | Duplicate channel message. | Blind side effect. | Ledger #1099 if explicitly keyed. |
| Discord | Execute webhook | `POST` configured webhook URL | **Yes, R** (`drt/destinations/discord.py:62-88`) | **Unverified**. No key is wired. | Duplicate channel message/embed. | Blind side effect. | Ledger #1099 if explicitly keyed. |
| Teams | Incoming webhook message/card | `POST` configured webhook URL | **Yes, R** (`drt/destinations/teams.py:65-102`) | **Unverified**. No key is wired. | Duplicate channel message/card. | Blind side effect. | Ledger #1099 if explicitly keyed. |
| GitHub Actions | Dispatch workflow | `POST /repos/{owner}/{repo}/actions/workflows/{id}/dispatches` | **Yes, R** (`drt/destinations/github_actions.py:71-74`, `drt/destinations/github_actions.py:111-122`) | **Unverified**. No key or returned run id is wired. | Two workflow runs with the same inputs. | Blind side effect. | Ledger #1099 if explicitly keyed. |
| Elasticsearch / OpenSearch | Bulk index or create documents | `POST /_bulk` | **Yes, R** for the whole batch (`drt/destinations/elasticsearch.py:72-94`) | `_id` is natural when `id_field` is set; without it the cluster generates ids. The repository documents `index` as replace/upsert and `create` as insert-only (`drt/destinations/elasticsearch.py:9-20`, `drt/destinations/elasticsearch.py:175-193`). | Auto-id mode creates a second document; a whole batch can repeat. | Upsert/create-by-id when configured; blind with auto ids. | `id_field`; per-item 409/error parsing; ledger #1099. |
| Amazon SQS | Send message batch | SDK `SendMessageBatch` | Only transient per-entry failures are replayed through `retry_on`; `httpx` handling does not cover boto exceptions (`drt/destinations/sqs.py:207-272`) | FIFO can carry `MessageDeduplicationId`; standard queues have none in this implementation (`drt/destinations/sqs.py:73-86`, `drt/destinations/sqs.py:175-205`). Vendor dedup window is **unverified** here. | A standard-queue message is delivered twice; FIFO can duplicate if no stable dedup id applies. | Blind standard send or keyed FIFO send. | Successful entries removed before retry; FIFO dedup id and group ordering; ledger #1099. |
| Staged upload | Upload file, trigger async job, poll | Arbitrary configured method/URL for stage and trigger; usually POST/PUT; GET/HEAD poll | **No** retry around stage or trigger. Only GET/HEAD polling uses R; mutating poll methods are deliberately not retried (`drt/destinations/staged_upload.py:123-165`, `drt/destinations/staged_upload.py:213-250`, `drt/destinations/staged_upload.py:285-316`) | Vendor/job-specific; **unverified**. No native key surface is wired. | Duplicate uploaded artifact and/or duplicate async job on an outer rerun or DLQ replay. | Unknown/configurable. | No immediate create retry today; ledger #1099 explicitly excludes staged destinations. |
| Google Sheets | Append or overwrite values | SDK `values.append`; clear/update for overwrite | No drt retry (`drt/destinations/google_sheets.py:124-146`) | Sheet/range is natural for overwrite; append has no key in this implementation. SDK retry behavior is **unverified**. | Repeated appended rows; overwrite replays the same range. | Blind append or addressed overwrite. | Per-sync header/write-state cache; no ledger-specific protection beyond outer engine applicability. |
| GCS / S3 / Azure Blob | Upload export object | Storage SDK upload / PUT | No `with_retry` in drt; SDK retry behavior is **unverified** (`drt/destinations/gcs.py:99-127`, `drt/destinations/s3.py:112-139`, `drt/destinations/azure_blob.py:112-136`) | Object key is natural. A fixed key overwrites; the default timestamped key changes across runs. | A second export object after an ambiguous failure and rerun. | Key-addressed within one call; a new run normally chooses a new key. | Fixed `key_template` overwrites; otherwise none. |
| SMTP email | Send email | SMTP `send_message`, not HTTP | No drt retry (`drt/destinations/email_smtp.py:66-103`) | SMTP protocol key support is **unverified**; no key is wired. | Recipient receives the email twice after an outer replay. | Blind side effect. | Ledger #1099 can filter before `load()` if explicitly keyed. |

Two adjacent cases are intentionally not treated as blind HTTP creates. OAuth2
client-credentials token exchange is an unwrapped POST, but it creates no
destination business object and caches only the access token
(`drt/destinations/auth.py:82-124`). ClickHouse has its own relevant precedent:
an ambiguous HTTP insert is retried with the same server dedup token, explicitly
documented as best-effort because table settings determine whether it works
(`drt/destinations/clickhouse.py:1-22`,
`drt/destinations/clickhouse.py:306-355`). The SQL destination audit otherwise
found no plain create-style HTTP call routed through `with_retry`.

### Why the ledger and DLQ do not close the immediate retry gap

The warehouse ledger is deliberately check-then-send-then-mark. The engine filters
known keys before `destination.load()`, then marks only positively attributable
successes after `load()` returns (`drt/engine/sync.py:1191-1219`,
`drt/engine/sync.py:1250-1279`). That order prevents a failed send from being
permanently claimed, but it also means the ledger cannot see a remote commit whose
response was lost. `with_retry()` can create the duplicate *inside the same
`load()` call*, before the engine has any success to mark. If all attempts fail
after at least one remote commit, the row remains unmarked and may enter the DLQ.

The ledger is opt-in, available only with a Postgres warehouse state backend and
`state.idempotency: true`; local, GCS, S3 and the other warehouse dialects receive
no ledger (`drt/state/idempotency.py:12-30`, `drt/state/factory.py:19-34`,
`drt/state/factory.py:141-179`). Its key comes from `sync.idempotency_key`, falling
back to a destination `upsert_key`; template failure silently disables it for that
row (`drt/state/idempotency.py:45-82`). It is therefore defense in depth across
runs, not proof that a particular request is safe to replay.

`drt retry` reuses the current destination and calls the same `load()` path in
chunks, so its inner HTTP behavior is identical (`drt/cli/commands/retry.py:286-299`).
It consults the ledger before replay, but a hit is left queued because the key says
some version was delivered, not that the queued payload was; a miss is resent
(`drt/cli/commands/retry.py:211-237`). Staged destinations are excluded from the
ledger (`drt/cli/commands/retry.py:193-207`). Confirmed retry successes are marked
per chunk (`drt/cli/commands/retry.py:444-464`), and DLQ removal is reconciled after
processing, with an unavoidable hard-kill gap (`drt/cli/commands/retry.py:301-329`).
A destination's per-sync cache is reset after replay as well
(`drt/cli/commands/retry.py:39-86`). Consequently neither the cache, ledger nor
DLQ can make an ambiguous blind create safe; each acts outside the dangerous
request/response boundary.

## Decision

Adopt a layered policy. Replay safety is a property of one concrete operation,
not of a destination class or HTTP verb alone.

### 1. Retry only operations proven safe to replay

Every HTTP retry call site classifies the request as one of:

- **Idempotent** — GET/HEAD/OPTIONS, a PUT/DELETE whose target resource is fixed,
  or an API-specific operation proven to converge on a natural key.
- **Keyed** — the exact same non-empty native idempotency/dedup key is computed
  before attempt one and reused byte-for-byte for every attempt.
- **Reconciliable create** — a blind create has a stable match key and a
  destination-specific read can determine whether the first attempt committed.
- **Blind create** — no proof or reconciliation exists.

`with_retry()` remains a small transport loop, but gains request-safety input (or a
new internal wrapper supplies it). It retries R/R+ only for idempotent or keyed
operations. A blind create may retry a proven pre-send connect-establishment
failure, but never an error after request bytes may have left the process. In
particular, `ReadTimeout`, a dropped response, `RemoteProtocolError`, and a
retryable HTTP response are ambiguous for a create. A connector may not label an
operation keyed merely because the payload contains an identifier: the vendor
must use it for deduplication or address the same resource.

The key is computed outside the retry closure. Generic REST record mode already
does that. Its batch mode sends the same generated request id on every attempt of one
chunk, but nothing proves an arbitrary target honours that header, and the value
changes on every run, so generic REST batch mode **stays classified blind** unless the
operator asserts that the configured endpoint deduplicates on it (an explicit
configuration contract); a stable value is necessary but not sufficient. Google Ads with `orderId`, Klaviyo
`unique_id`, Mixpanel/Amplitude insert ids, FIFO SQS dedup ids and Elasticsearch
`_id` each receive explicit connector tests before being classified safe. Meta's
`event_id` is not evidence for server-to-server retry deduplication (see the amendment). A vendor
claim that is still “unverified” in the audit defaults to blind.

This is internal behavior. It does not add a method to the frozen `Destination`
Protocol; ADR 0007's optional-capability escape hatch is unnecessary unless a
future engine-level caller, rather than the connector itself, must discover retry
safety.

### 2. Reconcile before retrying a create

For a reconciliable create, an ambiguous first attempt changes the next action:

1. Re-search by the same stable match key used before the create.
2. If exactly one object exists, treat the create as delivered and, where the
   destination's normal semantics require it, apply an id-addressed update.
3. If multiple objects exist, fail with an ambiguity error and their safe ids;
   never choose one silently.
4. If none exists, do **not** immediately send a second create. Search indexes can
   lag writes. Perform a small bounded destination-specific reconciliation poll
   only where the vendor contract supports it; otherwise fail as “delivery
   unknown” and require operator review.

Pipedrive is the first implementation: its exact match key and refusal on multiple
matches already exist, so the new work moves reconciliation *inside* the retry
decision rather than trusting the post-response cache. Intercom's conflict id,
HubSpot's `idProperty`, and Klaviyo's duplicate profile id are second-wave
candidates. Search-then-create reduces ordinary duplicates but is not itself
idempotency; it becomes a retry mitigation only when the ambiguous attempt is
followed by reconciliation rather than another create.

### 3. Make blind-create replay an explicit per-destination opt-in

Create-capable destination configs gain a direct field, separate from the shared
backoff policy:

```yaml
destination:
  type: notion
  retry:
    max_attempts: 3
    initial_backoff: 1
  retry_creates: false  # default for blind creates
```

`retry_creates` is direct destination policy, not a member of `sync.retry`. A
sync-wide `true` would accidentally authorize duplicate-producing creates for
every destination selected by that sync policy; the permission must sit beside
the endpoint whose semantics the operator reviewed.

The resolved behavior is:

| Operation classification | `retry_creates` omitted / false | `retry_creates: true` |
|---|---|---|
| Idempotent or keyed | Retry normally; field is irrelevant | Retry normally |
| Reconciliable create | Reconcile after ambiguity; never resend blind | Reconcile first; blind resend only after reconciliation exhausts, with a warning |
| Blind create | Do not retry after a possibly-sent attempt | Preserve legacy replay and emit a duplicate-risk warning |

`true` is an escape hatch for operators who prefer availability over duplicate
safety, not a claim of exactly-once delivery. The warning names the destination,
operation and attempt count but never logs payloads, secrets or idempotency keys.
`max_attempts: 1` remains a coarser existing way to disable every retry; it is not
a substitute for keeping safe reads and keyed writes retryable.

The warehouse ledger remains supported and recommended for cross-run/DLQ defense,
but it does not change the request classification. `match_policy: update_only`
avoids a create by definition and remains retryable when its update is id-addressed;
`create_only` still needs the blind-create policy because it narrows *what* to do,
not whether a repeated create is safe.

### 4. Roll out by duplicate cost and current exposure

1. **Human-visible and externally triggering blind writes:** generic REST POST
   without a native key; SendGrid, Twilio, Slack, Discord and Teams; Notion,
   Linear and Jira create; GitHub Actions dispatch. These have the clearest
   immediate harm and no repository-wired reconciliation.
2. **CRM/search creates:** Pipedrive first, then Intercom, HubSpot, Klaviyo profile
   and Zendesk. Preserve retries only after natural-key conflict/reconciliation
   tests prove the committed-first-attempt case.
3. **Append, bulk and event APIs:** Airtable append, Elasticsearch auto-id,
   Google Ads without order id, then the keyed Klaviyo/Mixpanel/Amplitude and FIFO
   SQS paths. The former Meta keyed rollout reference is **superseded by the
   2026-11-28 amendment**; Meta remains unclassified with unchanged retries. Keyed
   paths keep retries only after their stable-key tests pass; unkeyed variants
   default false.
4. **Currently non-retried or SDK-owned writes:** Salesforce job creation, staged
   upload stage/trigger, Google Sheets append, SMTP and object-store uploads. Add
   explicit classification and documentation so a later retry refactor cannot
   make them unsafe accidentally; do not invent new retries in this ADR's work.
5. **ClickHouse precedent:** retain its tokenized behavior and fault-injection
   coverage, but make its best-effort server-setting dependency visible in the
   same retry-safety documentation.

Within a destination, keyed/upsert mode lands before the blind mode's default is
changed. This prevents a coarse connector-wide switch from degrading operations
already proven safe.

### 5. Test the commit-then-drop failure, not only pre-commit failures

Unit mocks that raise before recording a side effect cannot reproduce #1307. Add a
shared fault-injection HTTP harness with two modes:

- commit the payload to an in-memory vendor state, then close the connection
  before any response bytes reach the client;
- commit upstream, then have a proxy return a configured retryable 5xx.

For every classification, assert remote state as well as `SyncResult`:

- blind default: one remote object, one delivery-unknown failure, zero second
  create attempts;
- explicit `retry_creates: true`: legacy second attempt occurs and the warning is
  emitted;
- keyed request: all attempts reuse the exact key and remote state contains one
  logical object/event;
- reconciliable request: the second action is a search, not a create, and an
  eventual-consistency miss fails closed;
- idempotent update/read: ordinary R statuses, transport errors and `Retry-After`
  behavior remain unchanged;
- batch request: no successful row is silently credited when the response is
  ambiguous;
- DLQ replay: a ledger miss does not bypass create safety, a ledger hit stays
  queued as today, and staged exclusions remain unchanged.

Keep the existing retry unit matrix for exhaustion, backoff and additive
`retry_on`; add operation-safety cases rather than replacing it. Connector tests
must cover both `on_error: fail` and `skip`, `match_policy`, native-key template
failure, missing/empty keys, and mixed safe/unsafe modes in the same destination.

### 6. Document uncertainty and operator recovery

Each HTTP/event connector page gains a “Retry safety” row stating the operation
classification, exact natural/native key, whether it protects only immediate
attempts or also later runs, and the duplicate side effect. Claims not backed by a
vendor contract and a connector test remain “unverified.” Generic REST documents
that setting an `Idempotency-Key` header is useful only if the target API honors
it. The migration guide explains `retry_creates`, delivery-unknown failures,
ledger limitations, DLQ behavior and how to search/reconcile before manual replay.

Release notes must call out the default change and list affected destinations.
Generated config/schema references include the new field. Observability adds a
bounded warning/counter for suppressed blind retries and reconciliation outcomes,
without changing `SyncResult` or CLI JSON until a separately versioned additive
schema is designed.

## Out of scope

- Exactly-once delivery across arbitrary external APIs. Without a vendor key or a
  transaction spanning drt and the destination, that guarantee is impossible.
- Claim-before-send in the warehouse ledger. It trades duplicates for silent loss
  when a send never commits.
- A generic search language for arbitrary REST APIs. Reconciliation remains
  destination-specific until a concrete common contract emerges.
- Retrofitting retries into Salesforce stage writes, staged-upload trigger calls,
  SMTP, Google Sheets or storage SDKs. This ADR prevents unsafe expansion; it does
  not add availability behavior to currently single-attempt paths.
- Changing source retries or SQL transaction retries. They have different commit
  and checkpoint boundaries.
- Persisting vendor object ids in the DLQ or ledger. That may improve later
  reconciliation, but requires its own retention and sensitive-metadata design.

## Alternatives considered

### Retry every configured transient failure as today

This maximizes apparent availability but treats “client did not receive success”
as “server did not commit.” That premise is false for a lost response or proxy 5xx
and is the data-integrity bug this ADR addresses.

### Never retry POST

This is simple but wrong in both directions. POST is used for safe searches,
natural-key upserts and event APIs with stable dedup ids, while PUT is only safe
when it addresses the same resource. Verb is a useful default signal, not the
complete operation contract.

### Rely only on the warehouse ledger

The ledger marks after `load()` returns, so it is necessarily too late for two
attempts inside one `with_retry()` call. It is also opt-in, Postgres-only today,
best-effort on template failure, and excluded for staged destinations. It remains
valuable defense in depth but cannot be the primary fix.

### Mark the ledger before sending

Claim-before-send would prevent duplicate attempts, but a crash, timeout before
connect, auth rejection or permanent 4xx would leave the record permanently
“delivered” even though the vendor never saw it. The current check-send-mark order
is correct (`drt/state/idempotency.py:21-30`); ambiguity must be handled at the
request boundary.

### Search before every create, then retry the create on a miss

The initial search is useful for ordinary upsert semantics, but a second search
immediately after a lost response can miss a newly committed object in an
eventually consistent index. Treating that miss as proof of absence recreates the
bug with an extra round trip. Reconciliation must be bounded and fail closed.

### Let each connector invent its own retry loop

Destination-specific knowledge is necessary for keys and reconciliation, but the
safe/ambiguous decision, warning behavior and fault-injection contract must be
shared. Otherwise fixes drift: one connector suppresses transport retries, another
still retries 503, and a third regenerates its key between attempts.

## Consequences

- A lost response from a blind create becomes an explicit delivery-unknown failure
  instead of an automatic duplicate risk.
- Some transient blind-create failures that previously appeared successful after a
  retry will now require reconciliation or operator action. Availability decreases
  where the API offers no safe replay mechanism; data integrity improves.
- Safe reads, addressed updates, keyed upserts and verified event dedup retain
  their current retry/backoff behavior.
- Destinations that mix modes need per-operation classification. A single class may
  safely retry one branch and suppress another.
- Reconciliation adds read traffic and latency after ambiguous failures, and
  eventual consistency means some runs fail even when the first create succeeded.
- `retry_creates: true` preserves legacy behavior but makes duplicate risk an
  explicit, reviewable choice and observable warning.
- The ledger and native keys remain complementary: native keys protect the
  request boundary; the ledger reduces later reruns/DLQ replays after a confirmed
  success.
- Vendor guarantees can change. Connector tests and docs must pin the API surface
  used, while unverified claims default to unsafe.
- The audit exposes unrelated asymmetries—staged writes are single-attempt and SQS
  transport exceptions are not handled by `httpx` retry—but this ADR does not
  silently change them.

## Compatibility

Adding `retry_creates` is an additive YAML field and changing the internal retry
helper does not alter the frozen `Destination.load(records, config, sync_options)`
signature. ADR 0007 therefore does not require a new Protocol or a major version
for the API shape.

Changing the omitted value from today's effective “retry every wrapped create” to
`false` **is nevertheless a breaking behavioral default** for existing blind-create
destinations. Operators who depend on the old availability behavior must add
`retry_creates: true` to preserve it. Under `VERSIONING.md`, that need to change
YAML to retain behavior belongs in a MAJOR release; calling it a Protocol-safe
internal refactor would not make it semver-safe.

The recommended compatibility sequence is:

1. In the current 1.x line, add classification, reconciliation, the field, warnings
   and `retry_creates: false` as an explicit opt-in safety setting, while retaining
   the legacy default for already-supported blind creates.
2. Mark the legacy omitted behavior deprecated in validation and release notes for
   at least two minor releases.
3. In v2.0, resolve an omitted value to false for blind creates. Keyed/idempotent
   requests remain behavior-compatible and need no migration.

If the maintainer instead treats duplicate creation as a correctness exception
urgent enough for a 1.x default flip, the release must say plainly that it is a
deliberate breaking behavior change, supply `retry_creates: true` as the migration,
and not cite ADR 0007 as permission. This ADR recommends honoring the published
semver policy while shipping the opt-in protection and reconciliation work
immediately.

Third-party destinations remain source-compatible. They do not become safe merely
because drt-core adds a classifier: plugin authors must adopt the internal helper
or document their own retry semantics. No method is added to `Destination`.

## Implementation plan

| PR | Slice | Main acceptance criteria |
|---:|---|---|
| 1 | Safety model and fault harness | Add internal operation classifications and a commit-then-drop/proxy-5xx harness; preserve byte-for-behavior retry results until call sites opt in. Test stable key reuse, pre-send versus ambiguous transport failures, R/R+ and `Retry-After`. |
| 2 | Config, validation and observability | Add direct per-destination `retry_creates`, schema generation, validation/deprecation warning and duplicate-risk/suppressed-retry warning. No frozen Protocol change. |
| 3 | Highest-risk blind writes | Classify generic REST, webhooks, SendGrid, Twilio, Notion, Linear, Jira create and GitHub dispatch. Explicit false suppresses ambiguous retries; true preserves legacy. Fault-injection tests assert remote side-effect counts. |
| 4 | CRM reconciliation | Implement Pipedrive re-search first, then Intercom/HubSpot/Klaviyo/Zendesk conflict or natural-key reconciliation. Test committed create + dropped response, zero/one/multiple matches and eventual-consistency miss. |
| 5 | Append, event, bulk and queue matrix | Split Airtable append/upsert, Elasticsearch auto-id/id, Google Ads keyed/unkeyed, Klaviyo event, Meta (**keyed rollout superseded by the 2026-11-28 amendment; leave unclassified**), Mixpanel, Amplitude and SQS standard/FIFO. A path is classified keyed only with a non-empty stable key and a vendor-contract test fixture. |
| 6 | Ledger, DLQ and staged regression coverage | Prove the ledger cannot authorize an unsafe resend, preserve DLQ hit/miss behavior and staged exclusions, and add no-retry regression tests for Salesforce job creation and staged-upload stage/trigger. Document Google Sheets/SMTP/blob outer-replay risk. |
| 7 | Documentation and 1.x deprecation | Add connector retry-safety tables, migration/recovery guide, examples for native keys and `retry_creates`, generated schemas and release notes. Validation warns when omitted on a blind-create destination during the deprecation window. |
| 8 | v2 default flip | After the deprecation floor, make omitted `retry_creates` false only for blind creates; keep safe/keyed retries unchanged. Run the fault harness across every classified destination family and publish the breaking-change migration. |

## Decisions (owner-approved 2026-11-20)

The maintainer accepted the recommended answer to every question below. They are
normative for the implementation PRs.

1. **When should the omitted default flip?** Recommendation: v2.0 after the
   two-minor deprecation floor, because preserving legacy behavior otherwise
   requires a YAML change and `VERSIONING.md` calls that breaking.
2. **Where should the escape hatch live?** Recommendation: direct
   `destination.retry_creates`, not `sync.retry`, because permission to duplicate
   is endpoint-specific and must not fan out through a global retry policy.
3. **May a blind create retry a proven connect-establishment failure?**
   Recommendation: yes only when the client proves no request bytes were sent;
   classify every read/write/response error as ambiguous because commit cannot be
   excluded.
4. **What should Pipedrive do when reconciliation finds no person?**
   Recommendation: fail delivery-unknown after a bounded re-search, never issue a
   second POST by default, because search-index lag makes absence non-authoritative.
5. **What evidence promotes an “unverified” vendor key to keyed-safe?**
   Recommendation: current vendor documentation for the exact endpoint plus a
   commit-then-drop connector test; field names or repository comments alone are
   insufficient for a data-integrity guarantee.
6. **Should the warehouse ledger become mandatory for blind destinations?**
   Recommendation: no; keep it opt-in defense in depth because it cannot protect
   the immediate retry window and is unavailable on most state backends.
7. **Is generic REST batch mode safe with its random per-chunk request id?**
   Recommendation: no — keep it blind by default. The id is only stable across the
   attempts of one chunk and no vendor behaviour is proven; classify it keyed only
   when the operator asserts the endpoint honours the header, and require an
   operator-supplied stable batch key before claiming DLQ/cross-run safety, because
   the current value changes on every run.
8. **What gates each destination rollout?** Recommendation: do not classify a
   create/upsert/event path safe until the shared fault harness proves one logical
   remote side effect after both a dropped response and a post-commit proxy 5xx;
   ordinary mocked 5xx tests do not exercise the bug.

## Amendment 2026-11-28 (Meta Conversions)

The inventory and rollout references above are marked superseded because review found
that Meta documents `event_id` + `event_name` deduplication between the
browser Pixel and the server API, **not between two server submissions**. A retry after
a lost response is therefore not proven safe, and per decision 8 the path is **not
classified**: it keeps its previous retry behavior (transient errors and 5xx are
retried) until server-to-server deduplication is confirmed, at which point it can be
classified keyed, or made blind with `retry_creates` if it is not. The connector guide,
API reference and retry-safety index state the same.
