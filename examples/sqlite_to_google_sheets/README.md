# SQLite → Google Sheets

Export SQLite query results directly into Google Sheets using `drt`.

## Example Output

This will write the `users` table to your Google Sheet:

| id | name  | email                                         | department  |
| -- | ----- | --------------------------------------------- | ----------- |
| 1  | Alice | [alice@example.com](mailto:alice@example.com) | Engineering |
| 2  | Bob   | [bob@example.com](mailto:bob@example.com)     | Sales       |
| 3  | Carol | [carol@example.com](mailto:carol@example.com) | Marketing   |

---

## Prerequisites

* Python 3.10+
* A Google Cloud service account with Sheets API enabled
* A Google Sheets spreadsheet shared with the service account email

---

## Google Sheets Setup

1. Create a service account in Google Cloud
2. Enable the Google Sheets API
3. Download the JSON key file
4. Share your target Google Sheet with the service account email (Editor access)

---

## Setup

### 1. Install `drt` with Sheets support

```bash
pip install drt-core[sheets]
```

### 2. Create sample SQLite data

```bash
python -c "
import sqlite3

conn = sqlite3.connect('database.db')
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS users (
  id INTEGER,
  name TEXT,
  email TEXT,
  department TEXT
)
''')

cursor.executemany('INSERT INTO users VALUES (?, ?, ?, ?)', [
  (1, 'Alice', 'alice@example.com', 'Engineering'),
  (2, 'Bob',   'bob@example.com',   'Sales'),
  (3, 'Carol', 'carol@example.com', 'Marketing')
])

conn.commit()
conn.close()
"
```

### 3. Set up your profile

```bash
mkdir -p ~/.drt
cat > ~/.drt/profiles.yml << 'EOF'
local:
  type: sqlite
  database: ./database.db
EOF
```

* `database: ./database.db` is relative to where you run `drt`
* Use an absolute path if running from a different directory
---

## Run

```bash
drt run --dry-run   # preview
drt run             # write to Google Sheets
drt status          # check result
```

---

## Notes

* `mode: overwrite` clears the sheet before writing (header + data)
* `mode: append` adds rows without clearing

---


```
