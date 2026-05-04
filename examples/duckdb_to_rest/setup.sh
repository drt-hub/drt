#!/usr/bin/env bash
set -e

mkdir -p ~/.drt data

cp -f profiles.yml.example ~/.drt/profiles.yml

python scripts/init_db.py

echo "✓ duckdb_to_rest ready. Try: drt run --select post_api"
