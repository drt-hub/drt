# PostgreSQL Destination

> Upsert records into PostgreSQL tables using `INSERT ... ON CONFLICT DO UPDATE`.

## YAML Example

```yaml
destination:
  type: postgres
  host_env: PG_HOST
  port: 5432
  dbname_env: PG_DATABASE
  user_env: PG_USER
  password_env: PG_PASSWORD
  table: public.user_scores
  upsert_key: [user_id]
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"postgres"` | — | Required |
| `connection_string_env` | string \| null | null | Env var with full connection string (takes precedence) |
| `host` / `host_env` | string | — | Hostname (direct or env var) |
| `port` | int | `5432` | Port number |
| `dbname` / `dbname_env` | string | — | Database name |
| `user` / `user_env` | string | — | Username |
| `password` / `password_env` | string | — | Password |
| `table` | string | — | Target table (e.g., `public.users`) |
| `upsert_key` | list[str] | — | Columns for `ON CONFLICT` (must be unique constraint) |
| `ssl` | SslConfig \| null | null | SSL/TLS config |
| `lookups` | dict \| null | null | FK resolution via destination DB query |

## Authentication

**Option 1: Individual fields (recommended)**
```yaml
host_env: PG_HOST
dbname_env: PG_DATABASE
user_env: PG_USER
password_env: PG_PASSWORD
```

**Option 2: Connection string**
```yaml
connection_string_env: DATABASE_URL
# e.g. postgresql://user:pass@host:5432/dbname
```

**SSL:**
```yaml
ssl:
  enabled: true
  ca_env: PG_SSL_CA      # path to CA cert
  cert_env: PG_SSL_CERT   # path to client cert
  key_env: PG_SSL_KEY     # path to client key
```

## Common Patterns

**Upsert by email:**
```yaml
table: public.contacts
upsert_key: [email]
```

**Replace mode (TRUNCATE + INSERT):**
```yaml
sync:
  mode: replace
```

**FK resolution with destination_lookup:**
```yaml
lookups:
  department_id:
    table: departments
    match: { name: department_name }
    select: id
    on_miss: skip
```

## Notes

- Requires `pip install drt-core[postgres]` (uses `psycopg2`)
- `upsert_key` columns must have a UNIQUE constraint on the target table
- `drt test` validators (row_count, not_null, freshness, unique, accepted_values) work with PostgreSQL
- `--dry-run` shows row count diff for `mode: replace`