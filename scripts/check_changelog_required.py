#!/usr/bin/env python3
"""Verify that a code change ships with a CHANGELOG.md entry.

Usage:
    python scripts/check_changelog_required.py [BASE_REF] [HEAD_REF]

Defaults: BASE_REF=origin/main, HEAD_REF=HEAD.

Exit codes:
    0  No code change in the diff, OR CHANGELOG.md was updated.
    1  Code change present but CHANGELOG.md is untouched.

The CI workflow wraps this in `|| echo "::warning::..."` so it surfaces
as a warning rather than a blocking failure (false positives — pure
test-only PRs, dep bumps, i18n markers — are common enough that
hard-failing would create more friction than it removes). The script
itself returns a real exit code so the workflow can swap to hard-fail
later if the noise/signal ratio tips.

Sibling of ``check_changelog_monotonic.py`` (released-version coverage
guard) — see CONTRIBUTING.md "CI guard" section.
"""

from __future__ import annotations

import subprocess
import sys

# Paths that count as "code change" for the purposes of this check.
# Test changes alone don't require a CHANGELOG entry (test-only PRs are
# common cleanup), but production code or packaging changes do.
CODE_PATHS: tuple[str, ...] = ("drt/", "pyproject.toml")
CHANGELOG_FILE = "CHANGELOG.md"


def _git_diff_files(base: str, head: str, *paths: str) -> list[str]:
    """Return the list of files changed in ``paths`` between base..head."""
    result = subprocess.run(
        ["git", "diff", "--name-only", f"{base}..{head}", "--", *paths],
        capture_output=True,
        text=True,
        check=True,
    )
    return [line for line in result.stdout.splitlines() if line.strip()]


def check(base: str, head: str) -> tuple[bool, str]:
    """Return ``(ok, message)`` for the given ref pair."""
    code_changed = _git_diff_files(base, head, *CODE_PATHS)
    if not code_changed:
        return (True, f"OK: no code changes in {base}..{head} (skipping CHANGELOG check).")

    changelog_changed = _git_diff_files(base, head, CHANGELOG_FILE)
    if changelog_changed:
        return (
            True,
            f"OK: CHANGELOG.md updated alongside {len(code_changed)} code file(s).",
        )

    return (
        False,
        (
            f"WARN: {len(code_changed)} code file(s) changed but CHANGELOG.md is "
            "untouched. Add an entry under [Unreleased] for user-facing changes, "
            "internal refactors, or bug fixes — see CONTRIBUTING.md. "
            "Bypass with `[skip changelog]` in the PR description for pure "
            "CI/dep bumps or i18n marker updates."
        ),
    )


def main() -> int:
    base = sys.argv[1] if len(sys.argv) > 1 else "origin/main"
    head = sys.argv[2] if len(sys.argv) > 2 else "HEAD"

    ok, message = check(base, head)
    print(message)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
