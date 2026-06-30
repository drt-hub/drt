#!/usr/bin/env bash
# check-i18n-sync.sh — Detect stale i18n translations.
#
# Scans for translated files in two layouts:
#   1. Root-level suffix form:  *.{lang}.md           (e.g. README.ja.md)
#   2. docs/ subdirectory form: docs/**/{lang}/*.md   (e.g. docs/ja/configuration.md)
# and checks whether the English base file has been updated since the
# translation was last synced.
#
# Each translation file must contain a marker in its first 5 lines:
#   <!-- i18n-sync: base=README.md, hash=<commit-hash> -->
# The base is a repo-root-relative path. For docs/ translations it may point
# at any English source — e.g. base=docs/llm/API_REFERENCE.md.
#
# The script compares the recorded hash against the latest commit that
# touched the base file.  If they differ, a warning is printed.
#
# Exit codes:
#   0 — all translations are up to date (or no translations found)
#   1 — one or more translations are stale (warning only)

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

stale=0
checked=0

# Collect translation files from both layouts:
#   1. root-level    *.{lang}.md          (e.g. README.ja.md)
#   2. docs/ subdir  docs/**/{lang}/*.md  (e.g. docs/ja/configuration.md)
translations=()
for f in *.??.md; do
    if [ -f "$f" ]; then translations+=("$f"); fi
done
while IFS= read -r f; do
    if [ -n "$f" ]; then translations+=("$f"); fi
done < <(find docs -type f -path '*/[a-z][a-z]/*.md' 2>/dev/null | sort)

if [ "${#translations[@]}" -eq 0 ]; then
    echo "No translation files found."
    exit 0
fi

for translated in "${translations[@]}"; do
    [ -f "$translated" ] || continue

    # Extract the i18n-sync marker from the first 5 lines.
    marker=$(head -5 "$translated" | grep -o '<!-- i18n-sync: base=\([^,]*\), hash=\([a-f0-9]*\) -->' || true)
    if [ -z "$marker" ]; then
        echo "SKIP  $translated  (no i18n-sync marker)"
        continue
    fi

    # Parse base file and recorded hash.
    base=$(echo "$marker" | sed 's/.*base=\([^,]*\),.*/\1/')
    recorded_hash=$(echo "$marker" | sed 's/.*hash=\([a-f0-9]*\).*/\1/')

    if [ ! -f "$base" ]; then
        echo "WARN  $translated  base file '$base' not found"
        stale=1
        continue
    fi

    # Get the latest commit hash that touched the base file.
    latest_hash=$(git log -1 --format="%H" -- "$base" 2>/dev/null || echo "")
    if [ -z "$latest_hash" ]; then
        echo "SKIP  $translated  (cannot determine git history for '$base')"
        continue
    fi

    checked=$((checked + 1))

    if [ "$recorded_hash" = "$latest_hash" ]; then
        echo "OK    $translated  (synced with $base)"
    else
        echo "STALE $translated  (base=$base updated: ${latest_hash:0:7}, recorded: ${recorded_hash:0:7})"
        stale=1
    fi
done

echo ""
echo "Checked $checked translation(s)."

if [ "$stale" -ne 0 ]; then
    echo ""
    echo "Some translations are stale. To fix:"
    echo "  1. Update the translation content to match the base file"
    echo "  2. Update the hash in the i18n-sync marker:"
    echo "     git log -1 --format='%H' -- <base-file>"
    exit 1
fi
