# Apache Iceberg (source)

Read [Apache Iceberg](https://iceberg.apache.org/) tables through a catalog (REST, SQL, Hive, or local) as a drt source.

```bash
pip install drt-core[iceberg]
```

drt loads the table via [`pyiceberg`](https://py.iceberg.apache.org/) into Arrow, registers it in an in-memory DuckDB, and runs your model SQL against it.

## Profile (`~/.drt/profiles.yml`)

```yaml
iceberg_prod:
  type: iceberg
  table: analytics.users                   # namespace.table
  catalog_uri: https://my-catalog/api      # REST catalog endpoint (omit for sql/local via properties)
  warehouse: s3://my-bucket/warehouse
  catalog_name: prod                        # optional; default "default"
  properties:                              # optional extra pyiceberg catalog properties
    s3.access-key-id_ENV: AWS_KEY           # a key ending in _ENV is read from that env var
```

| Field | Required | Notes |
|---|---|---|
| `type` | ✅ | `iceberg` |
| `table` | ✅ | Fully-qualified `namespace.table`. Registered in DuckDB under the table part. |
| `catalog_uri` | — | REST catalog URI (mapped to pyiceberg's `uri`). Omit for a SQL/Hive/local catalog configured via `properties`. |
| `warehouse` | — | Warehouse root (e.g. `s3://…`). |
| `catalog_name` | — | pyiceberg catalog name. Default `default`. |
| `properties` | — | Extra catalog properties (auth, catalog impl, etc.). Any key ending in `_ENV` is resolved from the named environment variable. |

See the [pyiceberg catalog docs](https://py.iceberg.apache.org/configuration/) for the full property set (REST / SQL / Hive / Glue catalogs, FileIO auth).

## Use it in a model

```sql
-- models/active_users.sql
SELECT id, email, updated_at
FROM users
WHERE updated_at > '{{ cursor }}'
```

```yaml
# syncs/users_to_hubspot.yml
source:
  profile: iceberg_prod
  model: active_users
```

## Notes

- The current snapshot is scanned into memory, then filtered by your SQL in DuckDB — prefer a narrow `SELECT` + incremental cursor for very large tables.
- Reading is one-directional (source only); drt does not write Iceberg tables.
