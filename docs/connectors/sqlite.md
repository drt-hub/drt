# SQLite Source

> Read tables/views from a local SQLite database file (or an in-memory DB) as a
> drt sync **source**. Bundled in drt-core — no extra install.

## Profile (`~/.drt/profiles.yml`)

```yaml
warehouse:
  type: sqlite
  database: app.db   # file path, or ":memory:"
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"sqlite"` | — | Required |
| `database` | string | `:memory:` | Path to the `.db` file, or `:memory:` for an ephemeral in-process DB |

## Notes

- (core) — uses Python's built-in `sqlite3`; nothing extra to install.
- Each sync's `model` SQL is executed against this database; reference any table
  or view it contains.
- Handy for local development and small operational databases you want to
  activate out to a SaaS destination.

## References

- [SQLite documentation](https://www.sqlite.org/docs.html)

## Streaming extraction ([#765](https://github.com/drt-hub/drt/issues/765))

Rows are read in `fetch_size` batches rather than materialised whole, so peak memory tracks the batch
instead of the result set: **+110.6 MB → +4.4 MB** on 300k rows of ~200B.

"It's a local file" does not make buffering free: the cost this removes is holding every row as a Python
object, which a local file incurs just as readily as a remote warehouse.

```yaml
  fetch_size: 10000   # rows per batch (default: 10000)
```

Memory scales with `fetch_size x row width`, not row count — lower it for very wide rows, not for big tables.
