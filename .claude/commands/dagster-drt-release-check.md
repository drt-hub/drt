Check that all documentation and version references are consistent before a dagster-drt release.

## Steps

1. **Version consistency** — verify the version string in:
   - `integrations/dagster-drt/pyproject.toml` (project.version)

2. **Dependencies** — verify:
   - `drt-core` minimum version in `integrations/dagster-drt/pyproject.toml` matches or exceeds the latest published drt-core version
   - `dagster` minimum version is reasonable

3. **CHANGELOG** — verify there is an entry for the current dagster-drt version with today's date in `integrations/dagster-drt/CHANGELOG.md` (if exists) or in the main `CHANGELOG.md`

4. **README** — verify `integrations/dagster-drt/README.md`:
   - PyPI badge version matches pyproject.toml
   - All public API is documented: `drt_assets()`, `DagsterDrtTranslator`, `DrtConfig`
   - Usage examples are current and working
   - Install instructions use correct package name

5. **Exports** — verify `integrations/dagster-drt/dagster_drt/__init__.py`:
   - All public classes/functions are exported in `__all__`
   - Exports match what README documents

6. **Tests** — verify all dagster-drt tests pass:
   ```bash
   cd integrations/dagster-drt && pip install -e "../../[dev]" -e "." && pytest tests/ -v
   ```

7. **Publish workflow** — verify `.github/workflows/publish-dagster-drt.yml`:
   - Tag pattern matches release convention (`dagster-drt-v*`)
   - Build directory is correct (`integrations/dagster-drt`)
   - PyPI Trusted Publishing is configured (check GitHub repo Settings > Environments > pypi)

8. **Main project references** — verify:
   - Main `README.md` mentions dagster-drt with correct install instructions
   - `CLAUDE.md` lists dagster-drt in integrations
   - `docs/llm/CONTEXT.md` mentions dagster-drt

Report any inconsistencies found and suggest fixes.