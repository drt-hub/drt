#!/usr/bin/env bash
# check_drift.sh — Detect docs / skill / MCP drift against the connector registry.
#
# Motivation: connectors and MCP tools ship in code PRs, but the
# surrounding surfaces (docs/connectors/*.md, the /drt-create-sync and
# /drt-init skills, the README tables, the MCP server's docstring and
# drt_list_connectors inventory) are updated by hand and regularly lag
# behind. Real drift observed before this script existed:
#   - /drt-debug skill went 3 months without learning about `drt doctor`
#   - the MCP server went 4 months behind the CLI (no drt_doctor, no
#     --diff parity) and its docstring was missing drt_get_history
#   - drt_list_connectors' hardcoded inventory silently lagged the
#     registry by several connectors
#
# Checks (state-based — audits current reality, not a PR diff, so it
# also catches drift that accumulated before the check existed):
#   1. Every registered destination has docs/connectors/<name>.md
#   2. Every registered destination is mentioned in the /drt-create-sync skill
#   3. Every registered destination appears in README.md's Destinations table
#   4. Every registered source appears in README.md's Sources table
#   5. Every registered source is mentioned in the /drt-init skill
#   6. Every MCP tool is listed in drt/mcp/server.py's module docstring
#   7. Every MCP tool appears in README.md's MCP tools table
#   8. Every registered connector appears in the drt_list_connectors inventory
#
# Baseline: scripts/drift_baseline.txt holds known-accepted gaps, one
# `check_id:item` per line (e.g. `dest-doc:discord`). Baselined items
# are reported separately and do NOT fail the run — the file is a
# ratchet to burn down over time, not a permanent allowlist.
#
# Exit codes:
#   0 — no new drift (baselined gaps may still exist)
#   1 — new drift found (not in baseline)
#   2 — script error
#
# Usage: bash scripts/check_drift.sh   (or: make check-drift)

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

REGISTRY="drt/connectors/registry.py"
MCP_SERVER="drt/mcp/server.py"
CREATE_SYNC_SKILL="skills/drt/skills/drt-create-sync/SKILL.md"
INIT_SKILL="skills/drt/skills/drt-init/SKILL.md"
BASELINE="scripts/drift_baseline.txt"

for f in "$REGISTRY" "$MCP_SERVER" "$CREATE_SYNC_SKILL" "$INIT_SKILL" README.md; do
    if [ ! -f "$f" ]; then
        echo "ERROR: expected file not found: $f" >&2
        exit 2
    fi
done

new_drift=0
baselined=0

# is_baselined <check_id> <item> — 0 if the gap is recorded in the baseline.
is_baselined() {
    [ -f "$BASELINE" ] && grep -qx "$1:$2" "$BASELINE"
}

# report <check_id> <item> <message>
report() {
    local check_id="$1" item="$2" message="$3"
    if is_baselined "$check_id" "$item"; then
        echo "KNOWN $check_id:$item  $message (baselined)"
        baselined=$((baselined + 1))
    else
        echo "DRIFT $check_id:$item  $message"
        new_drift=$((new_drift + 1))
    fi
}

# fuzzy_term <type_key> — normalised search term: underscores → spaces.
# Display surfaces (skills, README tables) use human names like
# "GitHub Actions" / "Email SMTP"; matching the space-joined type key
# case-insensitively covers almost all of them. Exceptions live in
# display_alias below.
fuzzy_term() {
    echo "$1" | tr '_' ' '
}

# display_alias <type_key> — overrides for connectors whose display
# name shares no token with the type key.
display_alias() {
    case "$1" in
        file)   echo "CSV" ;;       # README/skill row is "CSV / JSON / JSONL file"
        *)      echo "" ;;
    esac
}

# mentions <type_key> <file> — 0 if the connector is mentioned in file.
mentions() {
    local key="$1" file="$2"
    local term alias
    term="$(fuzzy_term "$key")"
    alias="$(display_alias "$key")"
    grep -qi -- "$term" "$file" && return 0
    [ -n "$alias" ] && grep -qi -- "$alias" "$file" && return 0
    return 1
}

destinations=$(grep -oE 'register_destination\("[a-z_0-9]+"' "$REGISTRY" | grep -oE '"[a-z_0-9]+"' | tr -d '"' | sort)
sources=$(grep -oE 'register_source\("[a-z_0-9]+"' "$REGISTRY" | grep -oE '"[a-z_0-9]+"' | tr -d '"' | sort)
mcp_tools=$(grep -oE '    def (drt_[a-z_]+)\(' "$MCP_SERVER" | grep -oE 'drt_[a-z_]+' | sort)

echo "== drift check: $(echo "$destinations" | wc -l | tr -d ' ') destinations, $(echo "$sources" | wc -l | tr -d ' ') sources, $(echo "$mcp_tools" | wc -l | tr -d ' ') MCP tools =="
echo

# ---------------------------------------------------------------------------
# Check 1: destination → docs/connectors/<name>.md  (underscores → hyphens)
# ---------------------------------------------------------------------------
for dest in $destinations; do
    doc_name="${dest//_/-}"
    if [ ! -f "docs/connectors/${doc_name}.md" ]; then
        report "dest-doc" "$dest" "docs/connectors/${doc_name}.md missing"
    fi
done

# ---------------------------------------------------------------------------
# Check 2: destination → /drt-create-sync skill mention
# ---------------------------------------------------------------------------
for dest in $destinations; do
    if ! mentions "$dest" "$CREATE_SYNC_SKILL"; then
        report "dest-skill" "$dest" "not mentioned in /drt-create-sync skill"
    fi
done

# ---------------------------------------------------------------------------
# Check 3: destination → README.md Destinations table
# ---------------------------------------------------------------------------
readme_dest_section=$(awk '/^### Destinations/,/^## /' README.md)
for dest in $destinations; do
    term="$(fuzzy_term "$dest")"
    alias="$(display_alias "$dest")"
    if ! echo "$readme_dest_section" | grep -qi -- "$term"; then
        if [ -z "$alias" ] || ! echo "$readme_dest_section" | grep -qi -- "$alias"; then
            report "dest-readme" "$dest" "no row in README.md Destinations table"
        fi
    fi
done

# ---------------------------------------------------------------------------
# Check 4: source → README.md Sources table
# ---------------------------------------------------------------------------
readme_src_section=$(awk '/^### Sources/,/^### Destinations/' README.md)
for src in $sources; do
    term="$(fuzzy_term "$src")"
    if ! echo "$readme_src_section" | grep -qi -- "$term"; then
        report "src-readme" "$src" "no row in README.md Sources table"
    fi
done

# ---------------------------------------------------------------------------
# Check 5: source → /drt-init skill mention
# ---------------------------------------------------------------------------
for src in $sources; do
    if ! mentions "$src" "$INIT_SKILL"; then
        report "src-skill" "$src" "not mentioned in /drt-init skill"
    fi
done

# ---------------------------------------------------------------------------
# Check 6: MCP tool → module docstring (first 30 lines of server.py)
# ---------------------------------------------------------------------------
docstring=$(head -30 "$MCP_SERVER")
for tool in $mcp_tools; do
    if ! echo "$docstring" | grep -q "$tool"; then
        report "mcp-docstring" "$tool" "not listed in drt/mcp/server.py module docstring"
    fi
done

# ---------------------------------------------------------------------------
# Check 7: MCP tool → README.md MCP tools table
# ---------------------------------------------------------------------------
for tool in $mcp_tools; do
    if ! grep -q "\`$tool\`" README.md; then
        report "mcp-readme" "$tool" "not in README.md MCP tools table"
    fi
done

# ---------------------------------------------------------------------------
# Check 8: registered connector → drt_list_connectors inventory
# The sources and destinations blocks are matched separately so a type
# key present in one list can't mask its absence from the other (e.g.
# "snowflake" in sources must not satisfy the destinations check).
# ---------------------------------------------------------------------------
list_connectors_fn=$(awk '/def drt_list_connectors/,/^    # ----/' "$MCP_SERVER")
inventory_src_block=$(echo "$list_connectors_fn" | awk '/"sources": \[/,/"destinations": \[/')
inventory_dest_block=$(echo "$list_connectors_fn" | awk '/"destinations": \[/,0')
for dest in $destinations; do
    if ! echo "$inventory_dest_block" | grep -q "\"type\": \"$dest\""; then
        report "mcp-inventory-dest" "$dest" "not in drt_list_connectors destinations inventory"
    fi
done
for src in $sources; do
    if ! echo "$inventory_src_block" | grep -q "\"type\": \"$src\""; then
        report "mcp-inventory-src" "$src" "not in drt_list_connectors sources inventory"
    fi
done

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo
echo "== summary: $new_drift new drift item(s), $baselined baselined gap(s) =="
if [ "$new_drift" -gt 0 ]; then
    echo
    echo "New drift found. Fix the surfaces above, or — only when a gap is"
    echo "intentional — record it in $BASELINE as 'check_id:item'."
    exit 1
fi
echo "No new drift. (Baselined gaps are tracked in $BASELINE — burn them down when possible.)"
exit 0
