#!/usr/bin/env bash
# Regenerate the bundled JSON Schemas from the installed drt-core version.
#
# Requires drt-core installed with the `dev` extra, from a repo checkout —
# not a plain `pip install drt-core` — so the exact pydantic version pinned
# there (#1070) is what generates the schema, matching what CI's
# vscode-schema-drift job installs. A different pydantic version can emit a
# different JSON Schema for the destination union's callable discriminator.
#   pip install -e ".[dev]"
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
