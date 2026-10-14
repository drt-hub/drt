# Amazon SQS Destination

> Send warehouse records as JSON messages to an existing Amazon SQS queue.

## Installation

```bash
pip install 'drt-core[sqs]'
```

The SQS extra installs `boto3>=1.35.69`. boto3 is imported only when sending
a non-empty batch; the base drt installation does not require it.

## YAML Example — standard queue

```yaml
destination:
  type: sqs
  queue_url_env: SQS_QUEUE_URL
  region: us-east-1
sync:
  mode: full
```

Set `SQS_QUEUE_URL` to the URL of your existing standard queue.
Each extracted record becomes one JSON message body.

## YAML Example — FIFO queue

```yaml
destination:
  type: sqs
  queue_url_env: SQS_FIFO_QUEUE_URL
  region: us-east-1
  message_group_id_field: customer_id
  deduplication_id_field: event_id
sync:
  mode: full
```

Set `SQS_FIFO_QUEUE_URL` to a queue URL ending in `.fifo`. Source records
must contain `customer_id` and `event_id` in this example. Their values
become `MessageGroupId` and `MessageDeduplicationId` respectively.

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"sqs"` | — | Required |
| `queue_url_env` | string | — | Required reference to the queue URL, resolved through drt's environment/secret resolution |
| `region` | string \| null | null | AWS region; uses boto3's configured default when omitted |
| `message_group_id_field` | string \| null | null | Source field supplying the group ID; required for FIFO, rejected for standard queues |
| `deduplication_id_field` | string \| null | null | Source field supplying the deduplication ID; optional for FIFO, rejected for standard queues |
| `retry` | object \| null | null | Partial-entry retry settings; overrides `sync.retry` as a whole when set |
| `rate_limit` | object \| null | null | Request pacing settings; overrides `sync.rate_limit` as a whole when set |

FIFO detection uses the **resolved queue URL's `.fifo` suffix**, rather than
the presence of a group field. Validation happens at runtime after URL
resolution and before AWS client creation. A missing group field for FIFO,
or either FIFO-only field on a standard queue, raises a clear configuration
error. Empty batches return immediately without resolving the URL or
importing boto3.

Configured ID fields accept strings, numbers (including native Decimal
values), and UUIDs. They are rendered as non-empty strings of at most 128
characters containing only ASCII letters, digits, and punctuation; whitespace,
control characters, and non-ASCII characters are rejected locally as row errors.
Missing or invalid values are row errors. If
`deduplication_id_field` is omitted for FIFO, the queue
must have **content-based deduplication enabled**; drt leaves the
deduplication ID unset so SQS can derive it from the message body.

## Authentication

SQS uses boto3's standard credential chain, including AWS environment
variables, shared credential/config files, and workload IAM roles.
Credentials are not placed in sync YAML. Configure a default region through
AWS configuration or set `region` in the destination. The role or user
must have `sqs:SendMessage` permission on the target queue.

## Sync modes and delivery

Delivery is append-only. Use `sync.mode: full` to append the complete query
result, or `incremental` with a cursor to append newly selected rows.
`upsert` also appends messages; SQS does not update an existing message.
drt has no separate `append` mode. `replace` and `mirror` are rejected by
the engine's destination capability check before extraction or delivery.

Messages are sent in requests of at most 10 entries and at most 1 MiB of
UTF-8 message bodies. A single message exceeding 1 MiB becomes a row error.
Each message body is the complete JSON record; values such as dates are
converted to strings when they are not directly JSON-serializable.

Delivery is **at least once**. A hard failure part-way through a run does
not roll back messages already delivered by earlier batches. Rerunning
the sync can deliver those messages again, so consumers should be
**idempotent**: processing the same event more than once should have the
same effect as processing it once. For example, record processed event IDs
and ignore duplicates.

FIFO deduplication can suppress duplicates within SQS's deduplication window
when the same deduplication ID or message body is reused.

## FIFO ordering

Within a sync run, drt preserves the extracted record order for each
`MessageGroupId`, including during partial-failure retries. A request contains
at most one entry from each group. The next message in a group is sent only
after its predecessor is confirmed successful; different groups can share a
batch. A queue using a single group therefore sends one message per request.

A sender fault, exhausted retry, or invalid record with an identifiable
group blocks that group for the rest of the run. With `sync.on_error: skip`,
its later records become RowErrors without being sent, and are captured in
source order when the DLQ is enabled; independent groups continue. With
`fail`, loading stops and unattempted input rows count as skipped. Blocking
persists across engine batches and resets at the end of the run, including
after a raised exception.

This guarantee follows extraction order. Use an explicit `ORDER BY` in your
model query when chronological order matters, and avoid overlapping producers
or sync runs publishing the same group. It does not establish ordering across
separate runs or arbitrary manual replays; recover failed and blocked records
in their original per-group order. Direct callers reusing a destination across
logical runs should call `reset_write_state(config, sync_options)` between runs.

## Partial failures and retries

An HTTP 200 response can contain both successful and failed entries.
drt checks each entry separately:

- Successful entries are counted once and are never included in a partial-failure retry.
- `SenderFault=true` becomes a row error immediately, without retry.
- `SenderFault=false` retries only the failed entries using `destination.retry` (or `sync.retry` when omitted) for bounded attempts and exponential backoff.
- Entries still failing after the final attempt become row errors with their original record indices.

Use `sync.on_error: skip` to continue after row errors. With `fail`, drt
stops further sends and reports the accumulated counts and row errors to
the engine, which persists configured dead letters and fails the sync.
Rows not sent because loading stopped are counted as skipped. The current
request's outcomes and partial retries are accounted for before stopping;
invalid records detected during message construction stop loading immediately.
Request-level exceptions propagate when
the SDK cannot complete the request; boto3 retains its normal SDK retry
behavior. A response that does not account for every requested entry
aborts the sync rather than crediting unconfirmed messages as successes.
`destination.rate_limit` (or `sync.rate_limit` when omitted) paces requests,
including partial retries. Both retry and rate-limit precedence are
destination > sync > default; an override replaces the entire settings object.
For example:

```yaml
destination:
  type: sqs
  queue_url_env: SQS_QUEUE_URL
  retry:
    max_attempts: 4
    initial_backoff: 1.0
  rate_limit:
    requests_per_second: 20
    burst: 5
```

Workers sending to the same resolved queue URL share a rate limiter, even
when they use different environment variable names. Different queue URLs
have independent limiters; the URL includes the queue's account and regional
endpoint. When workers specify different limits for the same queue, the
shared limiter adopts the stricter settings.

## References

- [Amazon SQS SendMessageBatch API](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html)
- [FIFO group and deduplication IDs](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatchRequestEntry.html)
- [boto3 credential resolution](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html)
