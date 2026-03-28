"""Seed sample data into DuckDB. Run with: python seed.py"""

import duckdb

con = duckdb.connect("./warehouse.duckdb")
con.execute("""
CREATE TABLE IF NOT EXISTS users AS
SELECT * FROM (VALUES
  (1, 'Alice', 'alice@example.com'),
  (2, 'Bob',   'bob@example.com'),
  (3, 'Carol', 'carol@example.com')
) t(id, name, email)
""")
print("Seeded 3 rows into users table.")
con.close()
