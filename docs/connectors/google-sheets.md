# Google Sheets Destination

> Write records to a Google Sheets spreadsheet (overwrite or append).

## YAML Example

```yaml
destination:
  type: google_sheets
  spreadsheet_id: "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms"
  sheet: "Sheet1"
  mode: overwrite
  credentials_env: GOOGLE_SHEETS_CREDENTIALS
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"google_sheets"` | — | Required |
| `spreadsheet_id` | string | — | Google Sheets ID (from the URL) |
| `sheet` | string | `"Sheet1"` | Sheet tab name |
| `mode` | `overwrite\|append` | `overwrite` | Write mode |
| `credentials_path` | string \| null | null | Path to service account JSON keyfile |
| `credentials_env` | string \| null | null | Env var containing keyfile path |

## Authentication

1. Create a [Service Account](https://console.cloud.google.com/iam-admin/serviceaccounts) in Google Cloud
2. Download the JSON keyfile
3. Share the spreadsheet with the service account email
4. Set the env var:

```bash
export GOOGLE_SHEETS_CREDENTIALS="/path/to/service-account.json"
```

## Common Patterns

**Overwrite (replace all data each run):**
```yaml
mode: overwrite
# clears the sheet and writes fresh data with headers
```

**Append (add new rows):**
```yaml
mode: append
# adds rows after existing data, no header row added
```

**Incremental sync to a specific tab:**
```yaml
sheet: "Daily Report"
mode: append
sync:
  mode: incremental
  cursor_field: created_at
```

## Notes

- Requires `pip install drt-core[sheets]` (uses `gspread` + `google-auth`)
- `overwrite` clears the sheet first, then writes headers + data
- `append` adds rows without clearing — combine with `mode: incremental` to avoid duplicates
- Google Sheets API has a quota of 300 requests/minute per project
- `spreadsheet_id` is the long string in the spreadsheet URL: `https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit`