#!/usr/bin/env bash
set -e

mkdir -p data output

export DRT_DB_PATH=$(pwd)/data/warehouse.duckdb
export OUTPUT_PATH=$(pwd)/output/users.csv

python scripts/init_db.py

echo "✓ duckdb_to_file ready"
