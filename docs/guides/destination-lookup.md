# Destination Lookup — Resolve Foreign Keys During Sync

When syncing related tables via reverse ETL, child tables often have FK constraints referencing the parent table's auto-increment ID. The source warehouse doesn't know these IDs, so the FK cannot be resolved from the source alone.

**`destination_lookup`** solves this by querying the destination database once per sync to build an in-memory mapping, then enriching each source row with the resolved FK value.

## Quick Start

```yaml
name: sync_child_table
model: |
  SELECT user_id, candidate_interview_id, company_id
  FROM `project.dataset.reverse_etl__profile_source_sessions`

destination:
  type: mysql
  host_env: MYSQL_HOST
  dbname_env: MYSQL_DB
  user_env: MYSQL_USER
  password_env: MYSQL_PASSWORD
  table: profile_source_sessions
  upsert_key: [interviewer_profile_id, candidate_interview_id]

  lookups:
    interviewer_profile_id:       # column to populate in destination
      table: interviewer_profiles # destination DB table to query
      match: { user_id: user_id } # { destination_col: source_col }
      select: id                  # value to use from the destination table
```

## How It Works

1. **Before** loading rows, drt executes one SELECT per lookup on the destination DB:
   ```sql
   SELECT user_id, id FROM interviewer_profiles
   ```
2. An in-memory mapping is built: `{ user_id_value: id_value, ... }`
3. For each source row, the FK column is resolved using the mapping
4. The enriched row is then loaded to the destination as usual

## Configuration Reference

```yaml
lookups:
  <target_column>:          # column name to populate in the destination
    table: <string>         # destination DB table to look up
    match:                  # mapping: { destination_column: source_column }
      <dest_col>: <src_col>
    select: <string>        # column to fetch from the lookup table
    on_miss: skip           # what to do when no match is found
```

### `on_miss` Options

| Value  | Behavior |
|--------|----------|
| `skip` | (default) Skip the row and log a warning |
| `fail` | Treat as an error (respects `sync.on_error`) |
| `null` | Set the target column to NULL |

## Multiple Lookups

You can define multiple lookups in a single sync. Each lookup queries a different table and populates a different column:

```yaml
lookups:
  customer_id:
    table: customers
    match: { email: customer_email }
    select: id
  product_id:
    table: products
    match: { sku: product_sku }
    select: id
    on_miss: "null"
```

## Composite Match Keys

When the lookup requires matching on multiple columns:

```yaml
lookups:
  profile_id:
    table: profiles
    match:
      company_id: company_id
      user_id: user_id
    select: id
```

This generates: `SELECT company_id, user_id, id FROM profiles` and builds a composite key mapping.

## Supported Destinations

`lookups` is supported on all database destinations:

- **MySQL** (`type: mysql`)
- **PostgreSQL** (`type: postgres`)
- **ClickHouse** (`type: clickhouse`)

## Use Case: Parent-Child Table Sync

Parent table `interviewer_profiles` synced first:
```
BQ (user_id=1) --> CloudSQL (id=5, user_id=1)  # id is auto-increment
```

Child table `profile_source_sessions` synced second with lookup:
```
BQ (user_id=1, candidate_interview_id=64)
  --> lookup: interviewer_profiles WHERE user_id=1 --> id=5
  --> CloudSQL (interviewer_profile_id=5, candidate_interview_id=64)
```

## Performance

- Lookup query runs **once per sync** (not per row) — minimal overhead
- Mapping is built **in memory** (suitable for typical reverse ETL volumes)
- Multiple lookups each execute one additional query
