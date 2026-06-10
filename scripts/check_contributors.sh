#!/usr/bin/env bash
#
# check_contributors.sh — audit .all-contributorsrc against current reality.
#
# Three checks:
#   1. URL drift — profile URL in .all-contributorsrc differs from the one
#      rendered in README.md (typically because the README was hand-edited
#      without syncing the source-of-truth). Surfaces the failure mode that
#      caused PR #616 → #617 / #620.
#   2. Missing entry — a contributor with at least one merged PR is absent
#      from .all-contributorsrc.
#   3. Triage eligibility — a contributor with 5+ merged PRs, active in the
#      last 30 days, who is NOT yet a Triage Collaborator per GOVERNANCE.md
#      criteria. Surfaces forgotten elevations.
#
# Exit codes:
#   0 — no issues, .all-contributorsrc is in sync
#   1 — one or more issues found; details on stdout
#   2 — script error (gh / jq missing, unauthenticated, etc.)
#
# Local usage:
#   make check-contributors
#   ./scripts/check_contributors.sh
#
# CI usage (.github/workflows/contributors-audit.yml runs this weekly).

set -euo pipefail

REPO="drt-hub/drt"
ALL_CONTRIBUTORS_FILE=".all-contributorsrc"
README_FILE="README.md"
ACTIVE_WINDOW_DAYS=30
TRIAGE_THRESHOLD=5

# --- prerequisites -----------------------------------------------------------

require() {
    if ! command -v "$1" > /dev/null 2>&1; then
        echo "error: '$1' is required but not installed" >&2
        exit 2
    fi
}

require gh
require jq

if ! gh auth status > /dev/null 2>&1; then
    echo "error: gh CLI not authenticated (run 'gh auth login' or set GITHUB_TOKEN)" >&2
    exit 2
fi

if [[ ! -f "$ALL_CONTRIBUTORS_FILE" ]]; then
    echo "error: $ALL_CONTRIBUTORS_FILE not found (run from repo root)" >&2
    exit 2
fi

# --- helpers -----------------------------------------------------------------

header() {
    echo
    echo "=== $1 ==="
}

ISSUES=0

flag_issue() {
    ISSUES=$((ISSUES + 1))
}

# --- data: parse .all-contributorsrc ----------------------------------------

contributors_json=$(jq -c '.contributors' "$ALL_CONTRIBUTORS_FILE")
logins=$(echo "$contributors_json" | jq -r '.[].login')

# --- Check 1: URL drift ------------------------------------------------------

header "Check 1: profile URL drift (.all-contributorsrc vs README.md)"

drift_count=0
while IFS=$'\t' read -r login profile; do
    # Find the contributor's row in README.md by matching the avatar+login
    # signature and pull the outer <a href="..."> of the SAME <td>.
    readme_url=$(grep -oE "<a href=\"[^\"]+\"><img src=\"https://avatars.githubusercontent.com/[^\"]+\"[^>]*alt=\"[^\"]*\"/><br /><sub><b>[^<]+</b></sub></a><br /><a href=\"https://github.com/${REPO}/[^\"]*author=${login}\"" "$README_FILE" | head -1 | sed -E 's|^<a href="([^"]+)".*|\1|' || true)

    if [[ -z "$readme_url" ]]; then
        echo "  ?  @${login}: not found in $README_FILE (handled by Check 2 below if applicable)"
        continue
    fi

    if [[ "$readme_url" != "$profile" ]]; then
        echo "  ✗ @${login}: drift"
        echo "      .all-contributorsrc: $profile"
        echo "      $README_FILE:        $readme_url"
        drift_count=$((drift_count + 1))
        flag_issue
    fi
done < <(echo "$contributors_json" | jq -r '.[] | [.login, .profile] | @tsv')

if [[ "$drift_count" -eq 0 ]]; then
    echo "  ✓ no profile URL drift between $README_FILE and $ALL_CONTRIBUTORS_FILE"
fi

# --- Check 2: missing entries (merged-PR author not in .all-contributorsrc) --

header "Check 2: contributors with merged PRs missing from $ALL_CONTRIBUTORS_FILE"

# Filter out bot accounts (allcontributors, dependabot, etc.) — they don't
# need entries in .all-contributorsrc since their contributions are automated.
merged_authors=$(
    gh search prs --repo="$REPO" --merged --json author -L 200 \
        -q '.[] | .author.login // empty' \
        | sort -u \
        | grep -vE '\[bot\]$|^dependabot$|^renovate$|^github-actions$' \
        || true
)

missing=()
while read -r author; do
    [[ -z "$author" ]] && continue
    if ! echo "$logins" | grep -qix -- "$author"; then
        missing+=("$author")
    fi
done <<< "$merged_authors"

if [[ "${#missing[@]}" -eq 0 ]]; then
    echo "  ✓ every merged-PR author has an entry in $ALL_CONTRIBUTORS_FILE"
else
    echo "  ✗ missing entries (these accounts have merged PRs but no entry):"
    for m in "${missing[@]}"; do
        pr_count=$(gh search prs --repo="$REPO" --author="$m" --merged --json number -L 100 -q "length")
        echo "      @${m}: $pr_count merged PRs"
        flag_issue
    done
fi

# --- Check 3: Triage Collaborator eligibility -------------------------------

header "Check 3: GOVERNANCE.md Triage Collaborator eligibility (${TRIAGE_THRESHOLD}+ merged PRs, active last ${ACTIVE_WINDOW_DAYS} days)"

triage_logins=$(
    gh api "repos/${REPO}/collaborators?affiliation=all&per_page=100" \
        --jq '.[] | select(.permissions.triage == true) | .login' \
        | sort -u
)

all_recent_prs=$(
    gh search prs --repo="$REPO" --merged --json author,closedAt -L 200
)

cutoff=$(python3 -c "
import datetime
print((datetime.datetime.now(datetime.timezone.utc)
       - datetime.timedelta(days=${ACTIVE_WINDOW_DAYS})).date().isoformat())
")

eligible_count=0
while IFS=$'\t' read -r author count latest; do
    [[ -z "$author" ]] && continue
    [[ "$count" -lt "$TRIAGE_THRESHOLD" ]] && continue
    [[ "$latest" < "$cutoff" ]] && continue

    if echo "$triage_logins" | grep -qix -- "$author"; then
        continue
    fi

    echo "  ✗ @${author}: $count merged PRs, last activity $latest — NOT yet Triage Collaborator"
    echo "      GOVERNANCE.md path: open a Discussion proposing the invitation"
    eligible_count=$((eligible_count + 1))
    flag_issue
done < <(
    echo "$all_recent_prs" \
        | jq -r 'group_by(.author.login)
                   | .[]
                   | select(.[0].author.login != null)
                   | [.[0].author.login,
                      length,
                      (max_by(.closedAt).closedAt[0:10])]
                   | @tsv'
)

if [[ "$eligible_count" -eq 0 ]]; then
    echo "  ✓ no overdue Triage Collaborator invitations (everyone meeting the bar is already invited)"
fi

# --- summary -----------------------------------------------------------------

echo
if [[ "$ISSUES" -eq 0 ]]; then
    echo "✓ All checks passed — $ALL_CONTRIBUTORS_FILE is in sync."
    exit 0
else
    echo "✗ $ISSUES issue(s) found. See above for details."
    echo
    echo "Remediation:"
    echo "  - URL drift   → edit $ALL_CONTRIBUTORS_FILE to match $README_FILE (the README is hand-fixed more often)"
    echo "  - missing     → @all-contributors add @user for code (in any PR comment) OR edit $ALL_CONTRIBUTORS_FILE directly"
    echo "  - Triage      → open a Discussion proposing the invitation per GOVERNANCE.md"
    exit 1
fi
