#!/usr/bin/env bash
set -e

mkdir -p data

export DRT_DB_PATH=$(pwd)/data/warehouse.duckdb

python scripts/init_db.py

echo "✓ duckdb_to_rest ready"
