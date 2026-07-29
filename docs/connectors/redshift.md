# Redshift Source

> Read from an Amazon Redshift cluster as a drt sync **source**. Redshift speaks
> the PostgreSQL wire protocol, with an explicit `schema`.

## Profile (`~/.drt/profiles.yml`)

```yaml
redshift_prod:
  type: redshift
  host: my-cluster.xxx.us-east-1.redshift.amazonaws.com
  port: 5439
  dbname: analytics
  user: analyst
  password_env: REDSHIFT_PASSWORD
  schema: public
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"redshift"` | — | Required |
| `host` | string | — | Cluster endpoint |
| `port` | int | `5439` | Redshift default port |
| `dbname` | string | — | Database name |
| `user` | string | — | Username |
| `password_env` | string \| null | null | Env var holding the password (preferred) |
| `password` | string \| null | null | Inline password (not recommended) |
| `schema` | string | `public` | Default schema for unqualified table names |
| `fetch_size` | int | `10000` | Rows per server round trip when streaming ([#765](https://github.com/drt-hub/drt/issues/765)) |

## Notes

- Requires the extra: `pip install drt-core[redshift]`.
- Prefer `password_env` over an inline `password` — it keeps secrets out of the
  profile file.
- Each sync's `model` SQL runs against the cluster; qualify tables as
  `schema.table` or rely on the profile's `schema`.

## Retry on transient extract failures ([#766](https://github.com/drt-hub/drt/issues/766))

Opening the connection and running the query are retried automatically (3 attempts,
exponential backoff from 1s, capped at 60s). No configuration — it is always on.

**Retried** (psycopg2, the same classification as the [Postgres source](postgres.md), since
Redshift speaks the Postgres wire protocol through the same driver):

- `OperationalError` — connection refused, a cluster failover, a **paused serverless workgroup
  resuming**, or the WLM dropping an idle session.
- `InterfaceError` — the driver's own connection object went bad.

**Not retried:** `ProgrammingError` (bad SQL, missing relation, denied privilege), `DataError`,
`IntegrityError` — retrying only delays an error you have to fix anyway. **Authentication
failures are also never retried**, even though psycopg2 files them *under* `OperationalError`
(`InvalidPassword`, `InvalidAuthorizationSpecification`, SQLSTATE class `28`): three rapid
attempts with a bad credential can trip an account lockout and turn a config typo into an outage.

⚠️ **Scope: connection + query execution + fetching the result set only.** A failure *after the
first row has been yielded* is not retried and fails the sync — those rows are already loaded
into the destination and cannot be un-sent. See
[API_REFERENCE](../llm/API_REFERENCE.md#source-side-retry-766) for the full rationale.

## Streaming extraction ([#765](https://github.com/drt-hub/drt/issues/765))

Rows stream through a **server-side (named) cursor** in `fetch_size` batches instead of being
buffered whole, so peak memory tracks the batch rather than the result set — the difference
between a few MB and a few GB on a wide unload, and the difference between finishing and an OOM
kill on a 7 GB CI runner.

```yaml
redshift_prod:
  type: redshift
  host: my-cluster.xxx.us-east-1.redshift.amazonaws.com
  dbname: analytics
  user: analyst
  password_env: REDSHIFT_PASSWORD
  schema: analytics
  fetch_size: 10000
```

Memory scales with `fetch_size x row width`, so lower it for very wide rows rather than for large
tables.

⚠️ **The connection is held for the whole load** — a server-side cursor dies with its session. On
Redshift specifically, watch WLM queue timeouts and any idle-session limits on long syncs: a slow
destination now keeps the Redshift session open for the duration, where previously the extract
finished and disconnected first.

## References

- [Amazon Redshift documentation](https://docs.aws.amazon.com/redshift/)
