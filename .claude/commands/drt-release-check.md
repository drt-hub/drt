Check that all documentation and version references are consistent before a drt release, then cut it. Battle-tested against v0.7.9–v0.7.11.

## Phase 0 — Completeness sweep (do this FIRST)

1. **Enumerate every merged PR since the last tag** — the `[Unreleased]` block is regularly incomplete:
   ```bash
   git log v{PREV}..origin/main --oneline
   ```
   Build a matrix: every `(#NNN)` PR number must appear in the new CHANGELOG section. Entries that cite the *issue* number should gain the PR link too. Conventional exemptions: changelog-meta PRs (edits to CHANGELOG itself) and ROADMAP-only planning PRs.

2. **Periphery sweep for new config fields / auth options** — the drift audit does not cover everything. Hand-kept surfaces that silently lag:
   - `drt/cli/commands/profile.py` `_ADD_FIELD_SPECS` (interactive `drt profile add` prompts)
   - `docs/llm/CONTEXT.md` connector support matrix
   - `docs/llm/API_REFERENCE.md` profile / sync examples
   - (`drt/cli/_connector_detail.py` is pydantic-model-derived — auto-correct, no action needed)

   Quick check: `grep -rn "<new_field>" skills/ .claude/ docs/llm/ drt/cli/`
   (v0.7.11 example: #745 added `private_key_env` to configs/docs/connectors/vscode-schema but missed exactly these three — caught by this grep, fixed in #748.)

3. **Automated gates**: `python scripts/check_changelog_monotonic.py` · `bash scripts/check_drift.sh` · `make test && make lint`

## Phase 1 — Version + docs consistency

4. **Version bump — 4 manifests + 1 label must agree**:
   - `pyproject.toml` (project.version)
   - `.claude-plugin/marketplace.json`
   - `.claude-plugin/plugin.json`
   - `skills/drt/.claude-plugin/plugin.json`
   - `docs/llm/CONTEXT.md` "Current version" line

5. **CHANGELOG** — cut `[Unreleased]` as `## [X.Y.Z] - YYYY-MM-DD`; leave an empty `## [Unreleased]` header above it.

6. **ROADMAP.md** — add the shipped section: heading `## vX.Y.Z — <theme> ✅ Shipped YYYY-MM-DD`, a "Released as **vX.Y.Z** … See [CHANGELOG.md](CHANGELOG.md#xyz---yyyy-mm-dd) and the GitHub Release" line, one narrative paragraph, then `---`. ⚠️ Insert position: the recent-release cluster is **newest-first** — insert immediately BEFORE the previous release's section, not at the chronological end of the file.

7. **CLAUDE.md** — new Current Status bullet at the top (dense, issue-linked, ends "No breaking changes — drop-in upgrade from vPREV").

8. **README.md** — roadmap table ✅ for the new version; connectors table lists new destinations/sources; Quickstart current.

9. **SECURITY.md** — current version in Supported Versions.

10. **Skills** — `.claude/commands/drt-create-sync.md` + `skills/drt/skills/drt-create-sync/SKILL.md` list all destinations; `drt-init` (both copies) lists all `init_wizard.py` source types.

11. **Connector wiring** — dispatch is the centralized registry (`drt/connectors/registry.py`); the CLI list + install extras derive from `drt/config/connectors.py` (`connector_inventory()` SSoT), guarded by registry-parity tests and `check_drift.sh` Check 8. (`_get_source` / `_get_destination` in `drt/cli/main.py` are legacy re-export wrappers only — don't audit them by hand.)

12. **MCP Server** — `drt_list_connectors` derives from the SSoT (auto). Check that new CLI capabilities have MCP parity or a tracked follow-up issue (the #718 pattern).

13. **dagster-drt dependency** — `integrations/dagster-drt/pyproject.toml` `drt-core>=` floor still valid; if drt-core broke API, dagster-drt tests must still pass.

14. **GitHub Topics** — new connectors as topics (`gh api repos/drt-hub/drt/topics`, 20 max — swap out an old one if full).

15. **GitHub Milestone** — issues closed or moved; no open PRs blocking.

## Phase 2 — Ship (release-PR + tag flow, proven on v0.7.10 / v0.7.11)

16. **Release PR**: branch `chore/release-vX.Y.Z` carrying all of the above (work in a `git worktree` if another session shares the checkout). Wait for CI green.

17. **Pre-merge re-sweep** — ⚠️ parallel sessions / contributors may merge while the PR is open:
    ```bash
    git fetch && git log origin/chore/release-vX.Y.Z..origin/main --oneline
    ```
    If main moved: rebase, sweep the new PRs into the CHANGELOG section (repeat until quiet), then merge — admin, with explicit owner approval.

18. **Tag** (irreversible — PyPI refuses the same version twice; a failed publish means bump to X.Y.Z+1, never retag):
    ```bash
    git tag -a vX.Y.Z <merge-sha> -m "drt-core vX.Y.Z — <one-line theme + key issue refs>"
    git push origin vX.Y.Z
    ```
    `publish-drt-core.yml` re-verifies (lint/type/tests against the tag) then publishes via Trusted Publishing.

19. **Verify publish**: watch the run to completion, then
    ```bash
    curl -s https://pypi.org/pypi/drt-core/json | python3 -c "import json,sys; print(json.load(sys.stdin)['info']['version'])"
    ```
    must print X.Y.Z.

20. **GitHub Release — EDIT, don't create**: the publish workflow's SBOM step (`softprops/action-gh-release`) already auto-created a bare release with the SBOM attached, so `gh release create` fails `422 tag_name already exists`. Instead:
    ```bash
    gh release edit vX.Y.Z --title "vX.Y.Z" --notes-file notes.md
    ```
    Notes format (match v0.7.10 / v0.7.11): bold one-line headline → `## Highlights` with emoji `###` sections (YAML snippets for config-visible features) → `### 🛠️ Also in this release` → "No breaking changes — drop-in upgrade from vPREV." → `## Install / upgrade` → `**Full Changelog**` compare link. Confirm the SBOM asset survived the edit.

21. **dagster-drt release** (only if its version bumped): `gh release create dagster-drt-vX.Y.Z --latest=false` — MUST pass `--latest=false` or date-based auto-detect steals Latest from drt-core.

22. **Post-release**: `gh release list --limit 5` (drt-core shows Latest) · notify-drt-web fires automatically on release-published (expect a sync PR in drt-web) · sync local main, delete release branches / worktrees.

Report any inconsistencies found and fix them before tagging.
