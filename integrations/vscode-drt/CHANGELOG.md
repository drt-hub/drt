# Changelog

All notable changes to the **drt — Reverse ETL** VS Code extension are documented
here. This extension versions independently of `drt-core`; each release notes the
drt-core version its bundled schemas were generated from.

## [0.1.7] - Unreleased

- Bundled JSON Schemas regenerated from drt-core: `syncs/*.yml` now validates the
  `alerts.on_degraded` block (degraded-sync alert conditions, drt-hub/drt#784).

## [0.1.6] - Unreleased

- Bundled JSON Schemas regenerated from drt-core: `drt_project.yml` now validates
  the `vars:` block (project vars, drt-hub/drt#783).
- Bundled JSON Schemas regenerated from drt-core: `syncs/*.yml` now validates the
  `sync.match_policy` field (`upsert` | `update_only` | `create_only`,
  drt-hub/drt#757).

## [0.1.0] - Unreleased

Initial release.

- YAML validation, autocomplete, and hover for `drt_project.yml` and `syncs/*.yml`
  via a `yamlValidation` contribution to `redhat.vscode-yaml`.
- Bundled JSON Schemas generated from drt-core **0.7.9**
  (`drt.config.schema.generate_sync_schema()` / `generate_project_schema()`).
- No runtime code — declarative schema association only.
