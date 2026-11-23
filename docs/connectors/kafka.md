# Apache Kafka Destination

> Publish each warehouse record as a JSON message to an Apache Kafka topic.

## Installation

```bash
pip install 'drt-core[kafka]'
```

The optional extra installs `confluent-kafka>=2.16.1`, backed by librdkafka.
The SDK is imported only for non-empty batches. The
[2.16.1 release files](https://pypi.org/project/confluent-kafka/2.16.1/#files)
include CPython 3.10–3.13 wheels for common Linux, macOS and Windows platforms;
check the wheel tags for your OS, architecture and minimum OS/glibc version.
A platform without a matching wheel needs a source build and librdkafka.

## YAML example

```yaml
name: customer_events
model: ref('customer_events')
destination:
  type: kafka
  bootstrap_servers_env: KAFKA_BOOTSTRAP_SERVERS
  topic: customer_events
  key_field: customer_id
  delivery_timeout_s: 30
  enable_idempotence: true
  headers:
    source: drt
  auth:
    type: sasl_scram
    username_env: KAFKA_USERNAME
    password_env: KAFKA_PASSWORD
  rate_limit:
    requests_per_second: 100
sync:
  mode: incremental
  cursor_field: updated_at
  batch_size: 500
  on_error: fail
  dlq:
    enabled: true
```

Set `KAFKA_BOOTSTRAP_SERVERS` to a comma-separated broker list, for example
`broker-a.example.com:9093,broker-b.example.com:9093`, and supply the referenced
credentials. Provision the topic and producer permissions on your cluster.
Any `*_env` reference uses drt's normal environment/secret resolution, including
supported secret-provider URIs; do not put credential values in YAML.

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"kafka"` | — | Required |
| `bootstrap_servers_env` | string | — | Required reference resolving to the comma-separated broker list |
| `topic` | string | — | Required destination topic |
| `key_field` | string or null | null | Record field supplying the message key; optional |
| `value_format` | `"json"` | `"json"` | JSON is the only supported value format |
| `headers` | object | `{}` | Static string-to-string Kafka message headers |
| `auth` | object | — | Required; `auth.type` must be explicit |
| `delivery_timeout_s` | number | `30` | Positive, finite batch-flush wait in seconds; also sets librdkafka `message.timeout.ms` |
| `enable_idempotence` | boolean | `true` | Enable producer-session retry deduplication; explicit opt-out for clusters that cannot support it |
| `retry` | object or null | null | Local queue-full retries; replaces `sync.retry` when set |
| `rate_limit` | object or null | null | Message pacing; replaces `sync.rate_limit` when set |

`delivery_timeout_s` accepts 0.001–2,147,483.647 seconds and is converted to
integer milliseconds for librdkafka. It bounds one flush for the whole batch,
not one separate wait per record. Enqueueing, rate-limit waits and local retry
backoff can take additional time; it is not a deadline for the whole sync.
Empty batches return before resolving secrets, importing the SDK or connecting.

## Authentication

| `auth.type` | Transport | Required references |
|---|---|---|
| `none` | `PLAINTEXT` | None; explicit unauthenticated, unencrypted transport |
| `ssl` | `SSL` | None for system-trust TLS; client certificate/key references are optional |
| `sasl_plain` | `SASL_SSL` | `username_env`, `password_env` |
| `sasl_scram` | `SASL_SSL` | `username_env`, `password_env` |

SASL always uses TLS; `SASL_PLAINTEXT` is not supported. SCRAM defaults to
`SCRAM-SHA-512`; set `auth.scram_mechanism: SCRAM-SHA-256` to select SHA-256.
Both SASL modes can use the platform's default CA trust without custom files.

For `ssl` or either SASL mode, optionally add:

```yaml
auth:
  type: sasl_scram
  username_env: KAFKA_USERNAME
  password_env: KAFKA_PASSWORD
  scram_mechanism: SCRAM-SHA-512
  ca_env: KAFKA_CA_PATH
  cert_env: KAFKA_CLIENT_CERT_PATH
  key_env: KAFKA_CLIENT_KEY_PATH
```

These certificate references resolve to local PEM **file paths**, not PEM
contents. `ca_env` is independent; `cert_env` and `key_env` must be supplied
together. Credentials are rejected for `none`/`ssl`, and TLS file references
are rejected for `none`. Declared references must resolve to nonblank values.

## Message keys and values

Each input row becomes one UTF-8 JSON value containing the complete record.
Serialization uses `json.dumps(..., default=str)`: dates, Decimal values, UUIDs
and other values without a native JSON representation become strings. Schema
Registry and Avro are outside this version's scope.

When `key_field` is set, the value must be a string, integer, float, Decimal or
UUID, encoded as UTF-8 `str(value)`. Missing, null, empty-string, boolean or non-scalar keys become
individual row errors; drt does not silently replace them with null keys.
Without `key_field`, messages have no key and use the producer's partitioner.
A key controls partitioning; it is not an application-level idempotency key.

## Modes and guarantees

Delivery is append-only. `sync.mode: full` appends the query result;
`incremental` appends the selected cursor window. `upsert` also appends and does
not update an existing Kafka record. `replace` and `mirror` are rejected before
extraction by the destination capability check. Kafka does not support
`drt plan`/`drt apply` readback; see the [plan coverage matrix](../guides/plan-coverage.md).

A producer is reused across batches within a sync. Each `load()` waits for its
own batch's delivery reports: success counts, DLQ attribution and ledger
receipts reflect confirmed delivery, not acceptance into a local queue.
Finalization/reset are safety drain/close operations, not deferred success
accounting; unexpected outstanding messages are purged and make cleanup fail.
The client is detached even if cleanup fails, so repeated cleanup cannot reuse it.

Producer idempotence is enabled by default with `acks=all`. It deduplicates
librdkafka's retries within the producer session. Setting
`enable_idempotence: false` retains `acks=all` and limits in-flight requests to
one to preserve retry order. Ordering is per partition within a run, not
across partitions or separate runs. Use an explicit SQL `ORDER BY` when source
order matters, and avoid overlapping runs when their relative order matters.
Failed records can leave gaps; later DLQ replay does not restore original order.

Delivery across reruns and replays is **at least once**, with no cross-run
exactly-once guarantee. Messages already written cannot be rolled back if the
sync later fails. Consumers should deduplicate using a stable event ID or
reconcile downstream. See [idempotency keys (#897)](https://github.com/drt-hub/drt/issues/897)
and the [warehouse-backed ledger (#1099)](https://github.com/drt-hub/drt/issues/1099).
Compaction retains the latest value per key eventually; consumers can still
observe duplicate events before compaction.

## Errors, timeouts and retries

- Invalid keys and JSON serialization failures (including excessive nesting) are row errors.
  Error previews bound both container traversal and output size.
- Queue-full `BufferError` is a local, pre-send condition. drt polls and retries
  with `destination.retry`, then `sync.retry`; exhausted attempts become a row error.
- Other immediate `produce()` exceptions, including auth/config errors, abort
  the sync under both `on_error: skip` and `fail`.
- Fatal producer errors and authentication/configuration errors from delivery
  reports or the global error callback also abort under either error policy.
  Transient global events are left to librdkafka's recovery.
- librdkafka owns broker/network retries. Other failed delivery reports become row
  errors and are not resubmitted by drt in the same run; confirmed messages are
  never included in local retries.
- When the batch flush times out, unconfirmed records become individual timeout
  row errors. drt purges queued/in-flight producer work and serves purge callbacks
  before returning, so finalization or a later batch cannot resume those local
  sends/retries. Purge does not undo a broker write already accepted: a timeout
  or an in-flight purge can still mean "possibly delivered", and replay can duplicate it.
- Exceptional/interrupted loads purge unconfirmed messages before discarding
  their row correlation, so a later reset cannot flush those messages to Kafka.

With `on_error: skip`, loading continues after row errors. With `fail`, drt
stops enqueueing when an error is observed, resolves the already-enqueued batch
and reports its outcomes before the engine stops the sync. Rows never enqueued
are counted as skipped. Enable `sync.dlq` to retain row errors for investigation
and replay; see [the DLQ guide](../guides/dead-letter-queue.md).

## Rate limiting

One limiter acquisition is charged per message, outside the local queue-full
retry loop. Although the setting is named `requests_per_second`, its unit here
is **messages per second**; librdkafka controls broker-request batching.
Workers targeting the same normalized broker list plus topic share a limiter.
Broker lists are trimmed, deduplicated and sorted; different env-var aliases
resolving to the same list share the same identity. Different topics have
independent limiters. The backend receives a SHA-256 digest of that identity,
without plaintext broker/topic values; destination labels use the safe `kafka` label.
Destination retry/rate-limit settings override sync
settings as complete objects, and shared limiter settings use the stricter limit.

## References

- [Confluent Python producer API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#producer)
- [librdkafka producer configuration](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html)
- [Purge semantics and delivery ambiguity](https://docs.confluent.io/platform/current/clients/librdkafka/html/classRdKafka_1_1Producer.html)
