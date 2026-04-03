Check that all documentation and version references are consistent before a drt release.

## Steps

1. **Version consistency** — verify the same version string in:
   - `pyproject.toml` (project.version)
   - `.claude-plugin/marketplace.json`
   - `.claude-plugin/plugin.json`
   - `skills/drt/.claude-plugin/plugin.json`

2. **CHANGELOG** — verify there is an entry for the current version with today's date

3. **README.md** — verify:
   - Roadmap table: current version has ✅
   - Connectors table: new destinations/sources are listed with correct status
   - Quickstart section is up to date

4. **README.ja.md** — verify:
   - Connectors table matches README.md (same sources, destinations, versions)
   - Roadmap table matches README.md
   - If out of sync, update or note what needs translation

5. **CLAUDE.md** — verify:
   - Current Status reflects the latest version
   - Sources/Destinations lists are complete
   - Roadmap Reference is current

6. **SECURITY.md** — verify current version is in Supported Versions

7. **docs/llm/CONTEXT.md** — verify:
   - Current version is correct
   - Destinations table includes all destinations
   - Sources table includes all sources

8. **docs/llm/API_REFERENCE.md** — verify:
   - All source types have profile config examples
   - All destination types have config examples
   - All destination types have complete sync examples

9. **Skills** — verify:
   - `.claude/commands/drt-create-sync.md` lists all destinations
   - `skills/drt/skills/drt-create-sync/SKILL.md` lists all destinations
   - `.claude/commands/drt-init.md` lists all source types supported by init_wizard.py
   - `skills/drt/skills/drt-init/SKILL.md` lists all source types supported by init_wizard.py

10. **dagster-drt dependency** — verify:
    - `integrations/dagster-drt/pyproject.toml` has `drt-core>=` matching or exceeding the version being released
    - If drt-core has breaking changes, dagster-drt tests still pass

11. **CLI wiring** — verify all connectors are wired:
    - `_get_source()` in `drt/cli/main.py` handles every source type in `ProfileConfig`
    - `_get_destination()` in `drt/cli/main.py` handles every destination type in `DestinationConfig`
    - `init_wizard.py` source type choices include all implemented sources
    - Return type annotations match the implementations

12. **MCP Server** — verify:
    - `drt/mcp/server.py` tool descriptions are accurate
    - All MCP tools listed in README.md match actual implementations

13. **GitHub Topics** — verify:
    - New connectors (sources/destinations) are added as topics: `gh api repos/drt-hub/drt/topics`
    - Max 20 topics — remove outdated ones if needed before adding
    - Add with: `gh repo edit drt-hub/drt --add-topic <name>`

14. **CI** — verify all tests pass: `make test && make lint`

15. **GitHub Milestone** — verify:
    - All milestone issues are closed or moved
    - No open PRs blocking the release

16. **GitHub Release — drt-core** — create (or verify exists):
    - `gh release create v{VERSION}` with title `v{VERSION}`
    - Release notes: "What's New" sections matching CHANGELOG entry
    - Mark as `--latest` for the primary release
    - Include `Full Changelog` compare link

17. **GitHub Release — dagster-drt** (if version bumped) — create (or verify exists):
    - `gh release create dagster-drt-v{VERSION} --latest=false` with title `dagster-drt v{VERSION}`
    - Release notes: features, requirements, PyPI link
    - **MUST pass `--latest=false`** (gh defaults to auto-detect by date, which will steal Latest from drt-core)

18. **Verify Latest flag** — after all releases are created:
    - `gh release list --limit 5` — confirm drt-core `v{VERSION}` shows `Latest`, dagster-drt does not
    - If wrong: `gh release edit dagster-drt-v{VERSION} --latest=false && gh release edit v{VERSION} --latest`

Report any inconsistencies found and suggest fixes.