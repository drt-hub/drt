#!/usr/bin/env python3
"""Sync version from pyproject.toml to all plugin JSON files."""

import json
import re
import sys
from pathlib import Path


def get_version_from_pyproject() -> str:
    text = Path("pyproject.toml").read_text()
    match = re.search(r'^version = "(.+)"', text, re.MULTILINE)
    if not match:
        print("ERROR: Could not find version in pyproject.toml")
        sys.exit(1)
    return match.group(1)


def update_json(path: str, version: str, key_path: list[str]) -> None:
    p = Path(path)
    data = json.loads(p.read_text())
    obj = data
    for key in key_path[:-1]:
        obj = obj[key]
    obj[key_path[-1]] = version
    p.write_text(json.dumps(data, indent=2) + "\n")


def main() -> None:
    version = sys.argv[1] if len(sys.argv) > 1 else get_version_from_pyproject()
    print(f"Syncing version {version}")

    update_json(".claude-plugin/plugin.json", version, ["version"])
    update_json("skills/drt/.claude-plugin/plugin.json", version, ["version"])
    update_json(".claude-plugin/marketplace.json", version, ["plugins", 0, "version"])

    print(f"✓ Version {version} synced to all plugin files")


if __name__ == "__main__":
    main()
