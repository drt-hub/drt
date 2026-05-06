#!/usr/bin/env bash
set -e

EXAMPLE_DIR="$(cd "$(dirname "$0")" && pwd)"
mkdir -p ~/.drt "$EXAMPLE_DIR/data"

# Append a distinct profile entry so we don't clobber profiles written by
# .devcontainer/setup.sh or other examples. Skip if rest_local already exists.
if ! grep -q "^rest_local:" ~/.drt/profiles.yml 2>/dev/null; then
  cat >> ~/.drt/profiles.yml <<PROFILE_EOF
rest_local:
  type: duckdb
  database: $EXAMPLE_DIR/data/warehouse.duckdb
PROFILE_EOF
fi

python "$EXAMPLE_DIR/scripts/init_db.py"

echo "✓ duckdb_to_rest ready. Try: drt run --select post_api"
