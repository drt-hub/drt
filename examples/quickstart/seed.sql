-- Run this to create sample data in DuckDB
-- Usage: duckdb ./warehouse.duckdb < seed.sql

CREATE TABLE IF NOT EXISTS users AS
SELECT * FROM (VALUES
  (1, 'Alice', 'alice@example.com'),
  (2, 'Bob',   'bob@example.com'),
  (3, 'Carol', 'carol@example.com')
) t(id, name, email);
