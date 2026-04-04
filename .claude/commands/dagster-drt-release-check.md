Check that all documentation and version references are consistent before a dagster-drt release.

## Steps

1. **Version consistency** — verify the version string in:
   - `integrations/dagster-drt/pyproject.toml` (project.version)

2. **Dependencies** — verify:
   - `drt-core` minimum version in `integrations/dagster-drt/pyproject.toml` matches or exceeds the latest published drt-core version
   - `dagster` minimum version is reasonable

3. **CHANGELOG** — verify there is an entry for the current dagster-drt version with today's date in `integrations/dagster-drt/CHANGELOG.md` (if exists) or in the main `CHANGELOG.md`

4. **README (integration)** — verify `integrations/dagster-drt/README.md`:
   - PyPI badge version matches pyproject.toml
   - All public API is documented:
     - `@drt_assets` decorator (new, recommended)
     - `build_drt_asset_specs()` (spec-only generation)
     - `DagsterDrtResource` (execution resource)
     - `DagsterDrtTranslator` / `DrtTranslatorData` (customization)
     - `DrtConfig` (run configuration)
     - `drt_assets_legacy()` (deprecated, if still exported)
   - Pipes usage example is included (using `build_drt_asset_specs` + `@multi_asset`)
   - Migration guide from v0.1 to v0.2 is present (if breaking changes)
   - Install instructions use correct package name

5. **Exports** — verify `integrations/dagster-drt/dagster_drt/__init__.py`:
   - All public classes/functions are exported in `__all__`
   - Exports match what README documents
   - Expected exports: `drt_assets`, `drt_assets_legacy`, `DrtConfig`, `DagsterDrtResource`, `DagsterDrtTranslator`, `DrtTranslatorData`, `build_drt_asset_specs`

6. **Tests** — verify all dagster-drt tests pass:
   ```bash
   cd integrations/dagster-drt && pip install -e "../../[dev]" -e "." && pytest tests/ -v
   ```

7. **Publish workflow** — verify `.github/workflows/publish-dagster-drt.yml`:
   - Tag pattern matches release convention (`dagster-drt-v*`)
   - Build directory is correct (`integrations/dagster-drt`)
   - PyPI Trusted Publishing is configured (check GitHub repo Settings > Environments > pypi)

8. **Main project references** — verify these files have up-to-date dagster-drt content:
   - `README.md` — dagster-drt section uses new API (`@drt_assets` decorator pattern)
   - `README.ja.md` — Japanese translation matches README.md dagster-drt section
   - `CLAUDE.md` — lists dagster-drt in integrations
   - `docs/llm/CONTEXT.md` — dagster-drt section uses new API
   - `docs/guides/using-with-dbt.md` — dagster-drt link is valid
   - `SECURITY.md` — supported versions table includes current dagster-drt version

9. **i18n sync** — run `make check-i18n` and verify README.ja.md is in sync

Report any inconsistencies found and suggest fixes.