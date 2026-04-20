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
   - Missing trailing newlines
   - Import ordering (ruff I001)
   - Type signature updates when adding new connectors (e.g. `_get_source` in `cli/main.py`, `ProfileConfig` union)
   - Lazy imports for optional dependencies (psycopg2, pymysql, etc.) — no top-level imports of extras

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
   - 🔴 Must fix before merge (no tests, security issues, broken protocol)
   - 🟡 Should fix but not blocking (style, missing docs)
   - 🟢 Looks good
   - For minor issues (trailing newlines, import order, whitespace): mark as 🟢 — maintainer will fix post-merge

7. **Suggest response** — draft a review comment following the tone & workflow guidelines below

---

## Review Tone & Workflow Guidelines

drt is an early-stage OSS project focused on growing its contributor community. The review process should reflect that.

### Principles

- **Lower the merge barrier** — Don't block PRs on minor style issues. Merge and fix small things yourself.
- **Be warm and welcoming** — Every contributor is a potential long-term community member.
- **Acknowledge good work first** — Lead with what's good before asking for changes.

### Approve (merge-ready PRs)

Use this pattern:

```
🎉 [Specific praise for what's good about the PR]

I'll clean up a few minor things after merge ([list items]). No action needed from your side.

Thanks for the contribution! If you enjoy using drt, a ⭐ would be appreciated 🙏
```

- Merge with `gh pr merge --squash --admin`
- Fix lint, trailing newlines, import ordering yourself in a follow-up commit
- Always run `ruff check --fix`, `ruff format`, `mypy`, and tests before pushing fixes

### Request Changes (PRs that need work)

Use this pattern:

```
Thanks for [acknowledge the effort]! 🙌

A couple of things to address before merging:

1. **[Issue]** — [Clear, actionable instruction with reference to existing code as example]
2. **[Issue]** — ...

[Positive note about what's already good] — happy to merge once addressed!
```

Only request changes for:
- Missing tests for new functionality
- Broken CI / merge conflicts
- Security concerns
- Protocol violations

Do NOT request changes for:
- Trailing newlines, whitespace, import ordering (fix yourself)
- Style preferences (unittest vs pytest, etc.)
- Missing docs (can be added later)

### Contributor Engagement (adapt by context)

**First-time contributor:**
```
Welcome to drt! 🎉 [praise]

Thanks for the contribution! If you enjoy using drt, a ⭐ on the repo would mean a lot.
Happy to see more PRs from you!
```

**Returning contributor:**
```
Great to see you back! 🙌 [praise]

If you're looking for more to work on, check out our good first issues:
https://github.com/drt-hub/drt/issues?q=is%3Aopen+label%3A%22good+first+issue%22
```

**Major feature PR (new connector, integration, etc.):**
```
🎉 [praise] This is a great addition to drt!

Would you be interested in helping maintain this connector going forward?
We'd love to have you as a regular contributor.
```

Always include at least one of:
- ⭐ Star request
- Link to good first issues
- Invitation to contribute more / maintain the feature

### Post-Merge Cleanup Checklist

After merging a PR, always:

1. `git pull origin main`
2. Fix trailing newlines, import ordering, trailing whitespace
3. Run `ruff check --fix && ruff format` on changed files
4. Run `mypy drt` — check for type signature gaps (e.g. new profiles not added to `_get_source`)
5. Run `pytest` on affected test files
6. Run `make check-i18n` — if stale, sync Japanese translations and update the hash marker
7. Commit with `chore: clean up [feature] (#PR follow-up)`
8. Push and verify CI passes