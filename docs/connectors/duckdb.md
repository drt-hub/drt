# DuckDB Source

> Read tables/views from a local DuckDB database file (or an in-memory DB) as a
> drt sync **source**. Bundled in drt-core — no extra install.

## Profile (`~/.drt/profiles.yml`)

```yaml
warehouse:
  type: duckdb
  database: warehouse.duckdb   # file path, or ":memory:"
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"duckdb"` | — | Required |
| `database` | string | `:memory:` | Path to the `.duckdb` file, or `:memory:` for an ephemeral in-process DB |

## Notes

- (core) — DuckDB ships with drt-core; nothing extra to install.
- Each sync's `model` SQL is executed against this database; reference any table
  or view it contains.
- `drt init --template duckdb_to_rest` scaffolds a runnable DuckDB → REST project.
- `:memory:` only holds data created in the same process, so it's for tests/demos
  rather than persisted pipelines.

## References

- [DuckDB documentation](https://duckdb.org/docs/)

## Streaming extraction ([#765](https://github.com/drt-hub/drt/issues/765))

Rows are read in `fetch_size` batches rather than materialised whole, so peak memory tracks the batch
instead of the result set: **+150.9 MB → +42.2 MB** on 300k rows of ~200B.

"It's a local file" does not make buffering free: the cost this removes is holding every row as a Python
object, which a local file incurs just as readily as a remote warehouse.

DuckDB uses an explicit `fetchmany` loop rather than iterating, because its result object has no
`__iter__`.

```yaml
  fetch_size: 10000   # rows per batch (default: 10000)
```

Memory scales with `fetch_size x row width`, not row count — lower it for very wide rows, not for big tables.
