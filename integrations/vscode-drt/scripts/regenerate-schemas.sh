#!/usr/bin/env bash
# Regenerate the bundled JSON Schemas from the installed drt-core version.
#
# Requires drt-core importable in the current environment:
#   pip install drt-core
set -euo pipefail

cd "$(dirname "$0")/.."

python - <<'PY'
from pathlib import Path
import importlib.metadata as md
from drt.config.schema import write_schemas

written = write_schemas(Path("schemas"))
ver = md.version("drt-core")
print(f"Regenerated {len(written)} schema(s) from drt-core {ver}:")
for p in written:
    print(f"  - {p}")
print("\nRemember to bump the extension version and note the drt-core version in CHANGELOG.md.")
PY
