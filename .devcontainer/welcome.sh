#!/usr/bin/env bash

cat << 'EOF'
✓ Playground ready!

Try:
  cd examples/duckdb_to_file
  drt run --select export_csv

Or:
  cd examples/duckdb_to_rest
  bash setup.sh
  drt run --select post_api
EOF
