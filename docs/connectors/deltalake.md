# Delta Lake (source)

Read [Delta Lake](https://delta.io/) tables from a local path, S3, or GCS as a drt source — activate lakehouse data without a warehouse in between.

```bash
pip install drt-core[deltalake]
```

drt loads the Delta table with the [`deltalake`](https://pypi.org/project/deltalake/) (delta-rs) bindings into Arrow, registers it in an in-memory DuckDB, and runs your model SQL against it — so column selection and incremental cursor filters work like any other source.

## Profile (`~/.drt/profiles.yml`)

```yaml
lakehouse:
  type: deltalake
  location: s3://my-bucket/delta/users     # local path, s3://…, or gs://…
  table: users                             # SQL name to query it as (default: last path segment)
  storage_options:                         # optional cloud auth (delta-rs storage options)
    AWS_ACCESS_KEY_ID_ENV: AWS_KEY          # a key ending in _ENV is read from that env var
    AWS_SECRET_ACCESS_KEY_ENV: AWS_SECRET
```

| Field | Required | Notes |
|---|---|---|
| `type` | ✅ | `deltalake` |
| `location` | ✅ | Delta table root — local path, `s3://bucket/table`, or `gs://bucket/table` |
| `table` | — | SQL name the table is registered under. Defaults to the last path segment of `location`. |
| `storage_options` | — | Passed to delta-rs for cloud auth. Any key ending in `_ENV` is resolved from the named environment variable (e.g. `AWS_ACCESS_KEY_ID_ENV: AWS_KEY` → `AWS_ACCESS_KEY_ID` = `$AWS_KEY`). |

See the [delta-rs storage docs](https://delta-io.github.io/delta-rs/usage/loading-table/) for the full set of S3 / GCS / Azure options.

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
  profile: lakehouse
  model: active_users
```

## Notes

- The whole table is read into memory, then filtered by your SQL in DuckDB — fine for moderate tables; for very large tables prefer a narrow `SELECT` + an incremental cursor.
- Reading is one-directional (source only); drt does not write Delta tables.
