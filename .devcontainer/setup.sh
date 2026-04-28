#!/usr/bin/env bash
set -e

pip install -e ".[duckdb]"

mkdir -p ~/.drt examples/duckdb_to_file/data examples/duckdb_to_file/output

cat > ~/.drt/profiles.yml << PROFILE_EOF
local:
  type: duckdb
  database: /codespaces/drt/examples/duckdb_to_file/data/warehouse.duckdb
PROFILE_EOF

python examples/duckdb_to_file/scripts/init_db.py

echo
echo "✓ Playground ready. Try: cd examples/duckdb_to_file && drt run --select export_csv"
echo
echo "Or try: cd examples/duckdb_to_rest && bash setup.sh && drt run --select post_api"
