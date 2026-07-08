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
