Review an open pull request for drt.

## Input

The user will provide a PR number or URL.

## Steps

1. **Fetch PR details**: `gh pr view <number>` and `gh pr diff <number>`

2. **CI status**: `gh pr checks <number>` — report pass/fail

3. **Code review** — check for:
   - Correct use of `Destination` / `Source` Protocol (config type is `DestinationConfig` / `ProfileConfig`, with `assert isinstance()`)
   - No new `type: ignore` (only allowed for external library issues)
   - Tests included for new functionality
   - `ruff` and `mypy` compliance

4. **Documentation check** — if user-facing changes:
   - CHANGELOG.md entry
   - README.md connectors/roadmap updated
   - docs/llm/API_REFERENCE.md updated
   - Skills updated if new destinations

5. **Author check** — `gh api users/<login>`:
   - Account age and activity
   - Previous contributions to drt or other OSS
   - Signs of spam/bot behavior (e.g. multiple "assign me" comments across repos)

6. **Report** — summarize with severity levels:
   - 🔴 Must fix before merge
   - 🟡 Should fix but not blocking
   - 🟢 Looks good

7. **Suggest response** — draft a review comment for the PR author