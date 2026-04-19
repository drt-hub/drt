# Feature: --dry-run Row Count Diff for Replace Mode (#339)

## Implementation Summary

This feature adds a safety check for `sync.mode: replace` during dry-run execution. When users run `drt run --dry-run` with replace mode, the tool now displays a before/after row count comparison for SQL destinations (PostgreSQL, MySQL, ClickHouse).

### Problem

Replace mode TRUNCATEs the entire destination table before inserting rows. If a source query unexpectedly returns 0 rows, the entire table gets wiped with no warning.

### Solution

Show a row count diff in dry-run output:

```
Dry run summary:
  Source: bigquery (project.dataset.sessions)
  Destination: mysql (profile_source_sessions)
  Rows to sync: 1,234
  Sync mode: replace
  ⚠ replace mode will TRUNCATE the destination table before inserting rows
  Current destination rows: 1,180 → New: 1,234 (+54)
```

This gives operators confidence before committing to the sync.

## Files Changed

### 1. **drt/destinations/sql_utils.py** (NEW)

- Centralized utility for SQL destination row count operations
- `get_row_count_for_destination()` function dispatches to the appropriate destination class method
- Supports PostgreSQL, MySQL, and ClickHouse
- Returns `None` for non-SQL destinations (REST API, Slack, etc.)

### 2. **drt/destinations/postgres.py**

- Added `get_row_count(config)` method
- Executes `SELECT COUNT(*) FROM table` to retrieve current row count
- Handles connection pooling and cleanup properly

### 3. **drt/destinations/mysql.py**

- Added `get_row_count(config)` method
- Executes `SELECT COUNT(*) FROM \`table\`` with backtick quoting
- Uses consistent connection pattern with PostgreSQL

### 4. **drt/destinations/clickhouse.py**

- Added `get_row_count(config)` method
- Executes `SELECT COUNT(*) FROM table`
- Parses `QueryResult.result_rows` to extract count

### 5. **drt/cli/output.py**

- Updated `print_dry_run_summary()` signature to accept optional `destination` parameter
- Added `_print_row_count_diff()` helper function for replace mode row count display
- Shows row count with color-coded diff:
  - Green for positive diff (more rows being added)
  - Red for negative diff (more rows being replaced)
  - Dim for zero diff
- Gracefully handles connection errors with informative fallback message

### 6. **drt/cli/main.py**

- Updated call to `print_dry_run_summary()` to pass `dest` (destination instance)
- Enables row count diff display for replace mode syncs

### 7. **tests/unit/test_dry_run_row_count.py** (NEW)

- Comprehensive test suite with 9 test cases:
  - Positive diff detection (+54 rows)
  - Negative diff detection (-1500 rows)
  - Zero diff handling (no change)
  - Connection error handling
  - Integration with `print_dry_run_summary()`
  - Behavior with/without destination parameter
  - Dangerous scenario detection (1180 → 0)
  - PostgreSQL destination support
  - Full output validation

## Design Decisions

### 1. **SQL-Only Feature**

- Only applies to SQL destinations (PostgreSQL, MySQL, ClickHouse)
- File-based and API destinations don't support row count queries
- Gracefully skips for non-SQL destinations

### 2. **Opt-In Via --dry-run**

- Row count query only runs during dry-run
- No additional queries in production runs (zero performance impact)
- Uses existing `dry_run` flag to control behavior

### 3. **Error Handling**

- Connection errors don't block the sync
- Displays helpful message: `(Could not retrieve current row count: ConnectionError)`
- Allows operators to proceed with confidence or troubleshoot connection issues

### 4. **Color Coding**

- Red: Negative diff (dangerous — data being deleted)
- Green: Positive diff (safe — more data added)
- Dim: Zero diff (no change to row count)
- Yellow warning: Replace mode truncate warning (always shown)

## Performance Impact

- **Dry-run mode:** +1 additional COUNT(\*) query per SQL destination (negligible)
- **Production runs:** Zero impact (feature only active in --dry-run)
- **Connection overhead:** Minimal; reuses existing connection patterns

## Testing

All 9 new tests passing:

```
test_print_row_count_diff_positive_diff PASSED
test_print_row_count_diff_negative_diff PASSED
test_print_row_count_diff_zero_diff PASSED
test_print_row_count_diff_handles_connection_error PASSED
test_print_dry_run_summary_includes_row_count_for_replace_mode PASSED
test_print_dry_run_summary_no_row_count_without_destination PASSED
test_print_dry_run_summary_replace_mode_zero_source_rows PASSED
test_get_row_count_for_postgres_destination PASSED
test_print_dry_run_summary_full_output PASSED
```

Existing CLI tests continue to pass (4/4 tests in test_cli_list_connectors.py).

## Example Usage

```bash
# Dry run with replace mode shows row count diff
$ drt run --dry-run --select my_sync

→ my_sync (dry-run)
Dry run summary:
  Source: bigquery (project.dataset.sessions)
  Destination: mysql (profile_source_sessions)
  Rows to sync: 1,234
  Sync mode: replace
  ⚠ replace mode will TRUNCATE the destination table before inserting rows
  Current destination rows: 1,180 → New: 1,234 (+54)

# Dangerous scenario: 0 rows from source
$ drt run --dry-run --select empty_sync

→ empty_sync (dry-run)
Dry run summary:
  Source: bigquery (project.dataset.empty_table)
  Destination: mysql (profile_archive)
  Rows to sync: 0
  Sync mode: replace
  ⚠ replace mode will TRUNCATE the destination table before inserting rows
  Current destination rows: 5,000 → New: 0 (-5000)  # RED WARNING!
```

## Future Enhancements

1. Extend to non-SQL destinations (Parquet file row count via pandas)
2. Add optional `--confirm` flag to require explicit approval for replace mode with negative diffs
3. Add row count diff to JSON output for machine-readable parsing
4. Cache row counts across parallel syncs to reduce redundant queries
