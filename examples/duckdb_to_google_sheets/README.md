# DuckDB → Google Sheets

Export query results from a local DuckDB database to a Google Sheets spreadsheet.

## Prerequisites

- Python 3.10+
- A Google Cloud service account with Sheets API access
- A Google Sheets spreadsheet shared with the service account email

## Setup

1. Install drt with Sheets support:

```bash
pip install drt-core[sheets]
```

2. Create sample data:

```bash
python -c "
import duckdb
c = duckdb.connect('warehouse.duckdb')
c.execute('''CREATE TABLE IF NOT EXISTS users AS SELECT * FROM (VALUES
  (1, 'Alice', 'alice@example.com', 'Engineering'),
  (2, 'Bob',   'bob@example.com',   'Sales'),
  (3, 'Carol', 'carol@example.com', 'Marketing')
) t(id, name, email, department)''')
c.close()
"
```

3. Set up your profile:

```bash
mkdir -p ~/.drt
cat > ~/.drt/profiles.yml << 'EOF'
local:
  type: duckdb
  database: ./warehouse.duckdb
  dataset: main
EOF
```

4. Update `syncs/export_to_sheets.yml`:
   - Replace `spreadsheet_id` with your Google Sheets ID
   - Replace `credentials_path` with your service account key path
   - Update `sheet` name if not "Sheet1"

## Run

```bash
drt run --dry-run   # preview
drt run             # write to Google Sheets
drt status          # check result
```

## Notes

- `mode: overwrite` clears the sheet before writing (header + data)
- `mode: append` adds rows without clearing
- The service account email must have Editor access to the spreadsheet
