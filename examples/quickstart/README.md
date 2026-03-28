# Quickstart: DuckDB → httpbin.org

Send 3 rows from a local DuckDB database to `https://httpbin.org/post` in under 5 minutes.
No cloud credentials required.

---

## Prerequisites

- Python 3.10+
- [uv](https://github.com/astral-sh/uv) (recommended) or pip

---

## 1. Install drt

```bash
pip install drt-core[duckdb]
# or with uv:
uv pip install drt-core[duckdb]
```

---

## 2. Set up a profile

Create `~/.drt/profiles.yml`:

```yaml
local:
  type: duckdb
  database: ./warehouse.duckdb
```

---

## 3. Seed DuckDB with sample data

```bash
python seed.py
# Seeded 3 rows into users table.
```

> **Alternative:** if you have the DuckDB CLI installed: `duckdb ./warehouse.duckdb < seed.sql`

---

## 4. Validate

```bash
drt validate
# ✓ post_users
```

---

## 5. Dry-run

```bash
drt run --dry-run
# → post_users
#   ✓ 3 synced (dry-run)
```

---

## 6. Run

```bash
drt run
# → post_users
#   ✓ 3 synced
```

httpbin echoes the payload back — each POST returns JSON like:

```json
{ "json": { "id": 1, "name": "Alice", "email": "alice@example.com" } }
```

---

## What's next?

- Replace `https://httpbin.org/post` with a real API endpoint
- Swap the profile for BigQuery (`type: bigquery`)
- Add more syncs under `syncs/`
