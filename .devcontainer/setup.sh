#!/usr/bin/env bash
set -e

pip install -e ".[duckdb]"

cd examples/duckdb_to_file
bash setup.sh

echo "✓ Playground ready (duckdb_to_file)"
