# Quickstart: DuckDB → httpbin.org

Send 3 rows from a local DuckDB database to `https://httpbin.org/post` in under 5 minutes.
No cloud credentials required.

---

## Prerequisites

- Python 3.10+
- [uv](https://github.com/astral-sh/uv) (recommended) or pip
- [DuckDB CLI](https://duckdb.org/docs/installation/) (for seeding sample data)

---

## 1. Install drt

```bash
pip install drt-core[duckdb]
# or with uv:
uv pip install drt-core[duckdb]
```

---

## 2. Set up a profile

Create `~/.drt/profiles.yml` with the following content:

```yaml
local:
  type: duckdb
  database: ./warehouse.duckdb
```

drt will look for this profile when it reads `drt_project.yml` (which sets `profile: local`).

---

## 3. Seed DuckDB with sample data

From the `examples/quickstart/` directory, run:

```bash
duckdb ./warehouse.duckdb < seed.sql
```

This creates a `users` table with 3 rows (Alice, Bob, Carol).

---

## 4. Validate the configuration

```bash
drt validate
```

Expected output:

```
Config OK — 1 sync(s) found: post_users
```

---

## 5. Dry-run (no HTTP calls)

```bash
drt run --dry-run
```

drt will read the rows from DuckDB and print what it would POST, without sending any requests.

---

## 6. Run the sync

```bash
drt run
```

drt will POST each row individually to `https://httpbin.org/post`. httpbin echoes
the request back as JSON, so you'll see output similar to:

```json
{
  "json": {
    "id": 1,
    "name": "Alice",
    "email": "alice@example.com"
  },
  "url": "https://httpbin.org/post"
}
```

After all 3 records are processed:

```
Synced 3 record(s) via post_users  [OK]
```

---

## What's next?

- Swap `httpbin.org` for a real API endpoint in `syncs/post_users.yml`
- Change the profile to point at a BigQuery dataset (`type: bigquery`)
- Add more syncs under `syncs/` for additional tables or destinations
