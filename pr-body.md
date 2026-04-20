## Problem

Syncing records that contain `dict` values (e.g. a BigQuery JSON column read as a Python dict, written to a Postgres JSONB column) crashes with:

```
ProgrammingError: can't adapt type 'dict'
```

psycopg2 does not ship a default adapter for `dict`, so bare dict values cannot be bound directly.

## Fix

Added a `_serialize_value()` helper that wraps `dict` values with `psycopg2.extras.Json` before passing them to `cursor.execute()`. Non-dict values pass through unchanged. The helper is used in both `_load_replace()` and `_load_upsert()`.

This is the same approach taken for the MySQL destination in #311 / v0.5.1.

## Testing

- Added unit test verifying that dict record values are wrapped with the `Json` adapter
- All 18 existing + new tests pass

Fixes #315
