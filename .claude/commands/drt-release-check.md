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

4. **CLAUDE.md** — verify:
   - Current Status reflects the latest version
   - Sources/Destinations lists are complete
   - Roadmap Reference is current

5. **SECURITY.md** — verify current version is in Supported Versions

6. **docs/llm/CONTEXT.md** — verify:
   - Current version is correct
   - Destinations table includes all destinations

7. **docs/llm/API_REFERENCE.md** — verify:
   - All destination types have config examples
   - All destination types have complete examples

8. **Skills** — verify:
   - `.claude/commands/drt-create-sync.md` lists all destinations
   - `skills/drt/skills/drt-create-sync/SKILL.md` lists all destinations

9. **CI** — verify all tests pass: `make test && make lint`

10. **GitHub** — verify:
    - All milestone issues are closed or moved
    - No open PRs blocking the release

Report any inconsistencies found and suggest fixes.