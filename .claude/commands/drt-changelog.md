Generate a draft `[Unreleased]` CHANGELOG entry from merged PRs since the last release tag, formatted to match `CHANGELOG.md`'s existing style.

Use this when:
- Preparing a release and want a draft starting point
- After several PRs have merged and `[Unreleased]` has fallen behind
- You want to audit what's queued for the next release without reading every PR

## When NOT to use this
- A single PR just merged → update `[Unreleased]` inline in that PR instead
- You already wrote the entry by hand → no need to regenerate
- Looking at *shipped* releases → read `CHANGELOG.md` or `gh release list`

## Procedure

### 1. Find the most recent release tag

```bash
git fetch --tags --quiet
LAST_TAG=$(git describe --tags --abbrev=0 --match 'v*')
echo "Last release: $LAST_TAG"
```

If no `v*` tag exists, fall back to the first commit on `main` and inform the user this is a first-release draft.

### 2. List merged PRs since that tag

```bash
git log "$LAST_TAG"..HEAD --merges --pretty=format:'%h %s' --reverse
```

For squash-merged repos (drt uses squash-merge), use this instead — squash commits aren't merge commits:

```bash
git log "$LAST_TAG"..HEAD --pretty=format:'%h|%s' --reverse
```

Filter out the release prep commit itself (typically `chore: release prep — drt-core vX.Y.Z`).

### 3. Parse conventional commit prefixes

For each commit, extract the conventional commit prefix:
- `feat(...)` or `feat: ...` → **Added**
- `fix(...)` or `fix: ...` → **Fixed**
- `refactor(...)` or `perf(...)` → **Changed**
- `docs(...)` → **Documentation** (only include if reader-facing)
- `chore(...)`, `test(...)`, `ci(...)` → skip unless reader-facing
- `BREAKING CHANGE:` in body, or `!` after type (e.g. `feat!:`) → **Breaking changes** (top of the entry)

Extract the PR number from the commit subject (squash-merge format ends with `(#NNN)`).

### 4. Format to match CHANGELOG.md style

Read the most recent shipped release section in `CHANGELOG.md` to match the exact bullet style — drt uses bold-prefixed bullets followed by an em-dash and a sentence, with `(#NNN, PR #MMM)` citations:

```markdown
## [Unreleased]

### Added

- **Short feature name** (#NNN, PR #MMM): One-sentence description of what changed and why a user would care. Contributed by @login when applicable.

### Fixed

- **Bug headline** (#NNN, PR #MMM): What was broken, what's fixed now.

### Changed

- **Refactor or perf headline** (#NNN, PR #MMM): What's different from the user's perspective (or "internal-only" if it isn't).

### Breaking changes

- **What broke** (#NNN, PR #MMM): What users must do to migrate.
```

Skip empty sections — don't emit `### Added` if there are no new features.

### 5. Enrich with PR context (best-effort)

For each PR number, fetch the PR title and first paragraph of the body to write a useful one-sentence summary:

```bash
gh pr view <NNN> --json title,body,author --jq '{title, author: .author.login, body: (.body | split("\n\n")[0])}'
```

Use the PR body to write the user-facing sentence — commit subjects are often too terse. Include `Contributed by @<login>` when the author is not a maintainer.

### 6. Present the draft

Output the formatted block ready to paste into `CHANGELOG.md` directly above `## [Unreleased]`'s existing entries (or replace `[Unreleased]` if it's empty/stale). Flag anything ambiguous:

- Commits without a conventional prefix → list them under "Uncategorized" for human review
- PRs whose body doesn't clearly state user impact → mark with `[needs review]`

Do **not** write to `CHANGELOG.md` automatically — the maintainer should paste it in so they can edit phrasing and merge with any handwritten entries already there.

## Style invariants

- Bullets start with a bolded headline, then a colon, then a one-sentence description
- Cite both the issue number and the PR number when available: `(#NNN, PR #MMM)`
- Cite the contributor (`Contributed by @login`) for non-maintainer PRs — drt's CHANGELOG always does this
- Use past tense ("Added", "Fixed") in section headings; present tense in bullets
- One sentence per bullet — readers skim, paragraphs go in PR descriptions
- Don't repeat what's obvious from the headline ("Fixed a bug where..." is noise; "Snowflake destination dropped rows when..." is signal)
