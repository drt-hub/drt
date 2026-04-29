# Initial database for GitHub Codespaces

import duckdb
from pathlib import Path

db_path = Path(__file__).resolve().parent.parent / "data" / "warehouse.duckdb"

conn = duckdb.connect(db_path)
conn.execute("""
CREATE TABLE IF NOT EXISTS users AS
SELECT 1 AS id, 'Alice' AS name, 'alice@example.com' AS email
UNION
SELECT 2, 'Bob', 'bob@example.com'
UNION
SELECT 3, 'John', 'john@example.com'
""")
conn.close()
