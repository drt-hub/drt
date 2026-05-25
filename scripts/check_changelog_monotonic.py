#!/usr/bin/env python3
"""Check that every drt-core ``v*`` git tag has a ``## [X.Y.Z]`` section in CHANGELOG.md.

Motivation
----------

PR #492 (orphan shadow cleanup, merged 2026-05-24) silently removed the
``## [0.7.4]`` section from CHANGELOG.md as part of a merge-conflict
resolution. The wheel published to PyPI as ``drt-core==0.7.4`` had been
live since 2026-05-23, but main's documentation no longer recorded it.
The regression wasn't caught until the v0.7.5 release pre-check 36 hours
later (PR #567 then restored the lost section).

Released-version sections are part of drt's public history. Once a
version ships to PyPI, its CHANGELOG entry should be append-only on
main — never silently removed by a subsequent PR. This script catches
that class of accident at PR-time.

Scope
-----

- **drt-core tags only** (``v0.7.4`` style, second-level headings
  ``## [0.7.4]``). The ``dagster-drt-v*`` tags use a separate H3
  format (``### [0.2.0] - ... (dagster-drt)``); not validated here.
- **Missing sections fail.** Edits to existing released-version
  sections (typo fixes, link repairs) are allowed.

Usage
-----

    python scripts/check_changelog_monotonic.py            # check current repo
    python scripts/check_changelog_monotonic.py --help     # usage

Exits non-zero with an actionable message when any tag is missing.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

_HEADING_RE = re.compile(r"^## \[(\d+\.\d+\.\d+(?:[.\-][a-z0-9]+)?)\]", re.MULTILINE)
_TAG_RE = re.compile(r"^v(\d+\.\d+\.\d+(?:[.\-][a-z0-9]+)?)$")

# Pre-existing CHANGELOG gaps known to predate this guard.
#
# These versions were tagged and published to PyPI but never had a
# ``## [X.Y.Z]`` section on main's CHANGELOG.md (the gap pre-existed
# when this check landed). Listed here so the guard remains useful for
# catching NEW regressions while not blocking on historical omissions.
#
# Remove an entry the moment the corresponding section is restored —
# the script then enforces it for future PRs automatically.
ALLOWLISTED_MISSING_VERSIONS: frozenset[str] = frozenset({
    "0.5.2",  # tagged 2026-04-14 and on PyPI, never had a main CHANGELOG entry
    "0.5.3",  # tagged 2026-04-15 and on PyPI, never had a main CHANGELOG entry
})


def parse_versions_from_changelog(content: str) -> set[str]:
    """Return the set of ``X.Y.Z`` versions that have a ``## [X.Y.Z]`` heading."""
    return set(_HEADING_RE.findall(content))


def parse_versions_from_tags(tags_text: str) -> set[str]:
    """Return the set of ``X.Y.Z`` versions parsed from ``git tag`` output.

    Only drt-core tags (``vX.Y.Z`` form) are returned. ``dagster-drt-v*`` tags
    are filtered out — they belong to a separately-versioned package whose
    CHANGELOG entries use an H3 format this script does not validate.
    """
    versions: set[str] = set()
    for line in tags_text.splitlines():
        match = _TAG_RE.match(line.strip())
        if match:
            versions.add(match.group(1))
    return versions


def check(changelog_text: str, tags_text: str) -> tuple[bool, str]:
    """Pure-function check: returns ``(ok, message)``."""
    changelog_versions = parse_versions_from_changelog(changelog_text)
    tag_versions = parse_versions_from_tags(tags_text)

    if not tag_versions:
        return True, "No drt-core v* tags found — nothing to check."

    missing = tag_versions - changelog_versions - ALLOWLISTED_MISSING_VERSIONS
    if missing:
        sorted_missing = sorted(missing, key=_version_sort_key)
        lines = [
            f"CHANGELOG.md is missing sections for {len(missing)} tagged version(s):",
            "",
            *(f"  - {v}  (tag: v{v})" for v in sorted_missing),
            "",
            "Each git tag v* must have a ## [X.Y.Z] section in CHANGELOG.md.",
            "If a section was lost during a merge, restore it from the tag:",
            "",
            f"  git show v{sorted_missing[0]}:CHANGELOG.md | "
            f"sed -n '/^## \\[{sorted_missing[0]}\\]/,/^## \\[/p'",
            "",
            "See PR #567 (the v0.7.4 restore) for an example.",
        ]
        return False, "\n".join(lines)

    return True, f"OK: all {len(tag_versions)} drt-core tagged version(s) covered in CHANGELOG.md."


def _version_sort_key(v: str) -> tuple[int, ...]:
    """Sort 0.7.10 after 0.7.9 (numeric, not lexical)."""
    parts = re.split(r"[.\-]", v)
    return tuple(int(p) if p.isdigit() else -1 for p in parts)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--changelog",
        type=Path,
        default=Path("CHANGELOG.md"),
        help="Path to CHANGELOG.md (default: CHANGELOG.md in cwd).",
    )
    args = parser.parse_args(argv)

    if not args.changelog.exists():
        print(f"ERROR: {args.changelog} not found", file=sys.stderr)
        return 1

    try:
        tags_text = subprocess.check_output(
            ["git", "tag", "-l", "v*"], text=True
        )
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        print(f"ERROR: failed to list git tags: {exc}", file=sys.stderr)
        return 1

    ok, message = check(args.changelog.read_text(), tags_text)
    stream = sys.stdout if ok else sys.stderr
    print(message, file=stream)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
