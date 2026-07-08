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
