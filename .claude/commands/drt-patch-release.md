Cut a drt-core **patch release** (e.g. v0.5.1) for a bug fix on top of the latest minor release tag, without pulling in unreleased features from `main`.

Use this when an already-shipped minor release has a bug that users hit in production and you cannot wait for the next minor to ship the fix. For minor/major releases (v0.X.0), use `drt-release-check.md` instead.

## When NOT to use this
- The fix can wait for the next minor → just merge to `main` normally
- `main` has no in-flight features → you can release directly from `main` without cherry-pick
- The change adds a feature, breaks API, or expands surface area → not a patch by semver; bump minor instead

## Procedure

### 1. Land the fix on `main` first
- Branch `fix/<short-name>` from `main`, implement, add tests
- `make lint && make test` must pass
- Open PR titled `fix(<area>): <summary>`. Body must mention the patch release plan ("Also shipped in vX.Y.Z")
- Update `CHANGELOG.md` in two places on this PR:
  - `[Unreleased] ### Fixed` — entry noting the fix and "Also shipped in [X.Y.Z](#xyz---YYYY-MM-DD)"
  - Insert a full `## [X.Y.Z] - YYYY-MM-DD` section above `[X.Y.0]` so `main` already documents the patch release. This avoids needing a follow-up PR after the patch ships.
- Wait for CI green, then squash-merge

### 2. Create the release branch from the previous tag
```bash
git fetch --tags
git checkout -b release/X.Y.Z vX.Y.0
```
- Branch name: `release/X.Y.Z` (e.g. `release/0.5.1`). This is reusable if a future X.Y.Z+1 is needed.

### 3. Cherry-pick the fix commit
- Use the **squash-merge SHA on `main`** (single commit), not the original PR branch SHAs
- `git cherry-pick <sha>`
- Resolve conflicts if any (rare for small fixes)

### 4. Bump version + CHANGELOG on the release branch
- `pyproject.toml`: bump `version = "X.Y.Z"`
- `CHANGELOG.md`: add `## [X.Y.Z] - YYYY-MM-DD` with `### Fixed` section just above `## [X.Y.0]`
- `make lint && make test` — confirm the older tree still passes (test count will be lower than `main`; that's expected)
- Commit: `chore: release prep — drt-core vX.Y.Z`
- `git push -u origin release/X.Y.Z`

### 5. Verify before tagging (last chance to abort)
```bash
git log --oneline vX.Y.0..release/X.Y.Z       # should be 2 commits: fix + release prep
git diff --stat vX.Y.0..release/X.Y.Z         # should touch only fix files + pyproject.toml + CHANGELOG.md
```
- Confirm zero `main`-only commits leaked in
- Re-run `make test` on the release branch

### 6. Tag and push (triggers PyPI publish — irreversible)
```bash
git tag -a vX.Y.Z -m "drt-core vX.Y.Z — patch release for #<issue>"
git push origin vX.Y.Z
```
- `.github/workflows/publish-drt-core.yml` triggers on `v*` tag push and publishes to PyPI via Trusted Publishing
- **PyPI does not allow re-publishing the same version**. If publish fails, you must bump to X.Y.Z+1, not retry the same tag

### 7. Verify publish + create GitHub Release
```bash
gh run watch <run-id> --exit-status
curl -s https://pypi.org/pypi/drt-core/json | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['info']['version'])"
# should print X.Y.Z

gh release create vX.Y.Z --latest --title "vX.Y.Z" --notes "..."
gh release list --limit 3   # confirm vX.Y.Z shows Latest
```
- Release notes: copy the `[X.Y.Z]` CHANGELOG section, add install snippet and `Full Changelog` compare link
- `--latest` is important: `dagster-drt-v*` releases use date-based auto-detect and can steal the Latest flag

### 8. Cleanup
- `git branch -d fix/<short-name>` (already deleted on remote by squash-merge)
- Keep `release/X.Y.Z` branch on remote — useful if a X.Y.Z+1 patch is ever needed
- Update local: `git checkout main && git pull`

## Gotchas (battle-tested)

- **Branch protection on main**: requires reviewer + signed commits + status checks. With `enforce_admins: false`, admin can bypass via `gh pr merge --admin`. Reserve for solo OSS where reviewer is yourself.
- **Signed commits**: if `required_signatures: enabled`, unsigned commits cannot be merged via normal flow. Either configure GPG/SSH signing or use `--admin`.
- **CHANGELOG drift**: if you only add `[X.Y.Z]` to the release branch and forget `main`, the next minor release notes will silently omit the patch. Always update both in step 1.
- **Cherry-pick wrong SHA**: if you cherry-pick the unmerged PR branch SHA instead of the squash-merge SHA on `main`, you get the same content but a different parent. Cherry-pick from `main` after merge.
- **PyPI publish failed previously?** Check `gh run list --workflow=publish-drt-core.yml` for the previous version's run. A failed publish for X.Y.0 might mean it was published manually — confirm PyPI state before assuming the workflow is reliable.
- **dagster-drt is independent**: it has its own tag prefix `dagster-drt-v*` and its own publish workflow. Patching dagster-drt does not require a drt-core patch release and vice versa.
- **No need to backport docs/README changes**: patch releases ship a code fix only. README, roadmap, connectors table updates belong on `main` for the next minor.

## Files touched in a typical patch release

On `main` (via fix PR):
- `<the fix>` (e.g. `drt/destinations/<x>.py`)
- `tests/unit/test_<x>.py`
- `CHANGELOG.md` (both `[Unreleased]` and new `[X.Y.Z]` section)

On `release/X.Y.Z` (cherry-pick + release prep):
- Same fix files (cherry-picked)
- `pyproject.toml` (version bump)
- `CHANGELOG.md` (just the new `[X.Y.Z]` section)