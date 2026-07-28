# SQL Server Source

> Read from a Microsoft SQL Server database as a drt sync **source** (via
> `pymssql`).

## Profile (`~/.drt/profiles.yml`)

```yaml
mssql_prod:
  type: sqlserver
  host: sql.example.com
  port: 1433
  database: analytics
  user: drt_reader
  password_env: MSSQL_PASSWORD
  schema: dbo
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"sqlserver"` | — | Required |
| `host` | string | — | Server hostname |
| `port` | int | `1433` | SQL Server default port |
| `database` | string | — | Database name |
| `user` | string | — | Username |
| `password_env` | string \| null | null | Env var holding the password (preferred) |
| `password` | string \| null | null | Inline password (not recommended) |
| `schema` | string | `dbo` | Default schema for unqualified table names |

## Notes

- Requires the extra: `pip install drt-core[sqlserver]` (pulls in `pymssql`).
- Prefer `password_env` over an inline `password`.
- Each sync's `model` SQL runs against the database; qualify tables as
  `schema.table` or rely on the profile's `schema`.

## Retry on transient extract failures ([#766](https://github.com/drt-hub/drt/issues/766))

Opening the connection and running the query are retried automatically (3 attempts,
exponential backoff from 1s, capped at 60s). No configuration — it is always on.

**Retried** (pymssql):

- `OperationalError` — connection refused, the server restarting, or an **Azure SQL failover**
  moving the database between replicas.
- `InterfaceError` — the driver's connection object went bad.

**Not retried:** `ProgrammingError` (bad SQL, missing table), `DataError`, `IntegrityError`,
`NotSupportedError`. pymssql follows PEP 249 exactly, so these are *siblings* of
`OperationalError` under `DatabaseError` — matching the two specific classes rather than the
base is what keeps a SQL typo from being retried three times.

⚠️ **Scope: connection + query execution + fetching the result set only.** A failure *after the
first row has been yielded* is not retried and fails the sync — those rows are already loaded
into the destination and cannot be un-sent. See
[API_REFERENCE](../llm/API_REFERENCE.md#source-side-retry-766) for the full rationale.

## References

- [SQL Server documentation](https://learn.microsoft.com/en-us/sql/)
- [pymssql documentation](https://www.pymssql.org/)
