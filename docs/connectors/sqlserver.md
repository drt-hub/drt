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

## References

- [SQL Server documentation](https://learn.microsoft.com/en-us/sql/)
- [pymssql documentation](https://www.pymssql.org/)
