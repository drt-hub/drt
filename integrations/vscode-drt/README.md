# drt — Reverse ETL (VS Code)

YAML validation, autocomplete, and hover docs for [drt](https://github.com/drt-hub/drt) sync files, powered by drt's own config schema.

As you edit `syncs/*.yml` and `drt_project.yml`, you get:

- **Validation** — unknown fields, wrong types, and invalid `destination.type` / `source.type` values are flagged inline, before you run `drt validate`.
- **Autocomplete** — field names and enum values (all 30+ destination types, sync modes, `on_error` policies, auth types, …) are suggested as you type.
- **Hover docs** — descriptions for config blocks and fields (improves as field descriptions land upstream — see *Schema source* below).

## How it works

This extension is intentionally thin: it contributes drt's JSON Schema to the
[YAML Language Server](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml)
(`redhat.vscode-yaml`, installed automatically as a dependency) via the
`yamlValidation` contribution point. The YAML extension does the validation,
completion, and hover work; this extension just tells it which schema applies to
which files. There is no runtime code to activate.

The bundled schemas are generated directly from drt's Pydantic config models
(`drt.config.schema.generate_sync_schema()` / `generate_project_schema()` — the
same schema behind `drt validate --emit-schema` and the MCP `drt_get_schema`
tool), so there is no hand-maintained copy to drift from the source of truth.

## File associations

| Pattern | Schema |
| --- | --- |
| `drt_project.yml` / `.yaml` | `drt_project.schema.json` |
| `syncs/*.yml` / `.yaml` | `sync.schema.json` |

If your sync files live elsewhere, or you want to scope the schema to a single
project, add a `yaml.schemas` entry to your workspace `settings.json`:

```jsonc
{
  "yaml.schemas": {
    "./schemas/sync.schema.json": "config/syncs/*.yml"
  }
}
```

## Schema source & versioning

The bundled schemas are a snapshot generated from a specific drt-core version
(see `CHANGELOG.md`). drt adds connectors and fields most releases, so the
snapshot is regenerated and republished on drt releases.

To regenerate against the drt-core installed in your environment:

```bash
# from integrations/vscode-drt/
./scripts/regenerate-schemas.sh
```

A future enhancement (tracked in drt-hub/drt#293) is an opt-in setting that
regenerates the schema from the workspace's installed drt version on activation,
so power users always match their exact install.

## Requirements

- [`redhat.vscode-yaml`](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml) — installed automatically as an extension dependency.

## Known limitations

- **Hover text is only as rich as the schema.** drt's config models don't yet
  carry per-field descriptions, so block-level docstrings show on hover but
  individual fields are sparse. Adding `Field(description=...)` upstream improves
  hover here, in `drt validate`, in the MCP schema, and in the `docs/llm/`
  reference simultaneously.
- **`syncs/*.yml` matching is name-based**, so a non-drt project with a `syncs/`
  folder of YAML could pick up the schema. Disable per-workspace via
  `yaml.schemas` if needed.

## Contributing

This extension lives in the drt monorepo at `integrations/vscode-drt/`. Issues
and PRs go to [drt-hub/drt](https://github.com/drt-hub/drt). Licensed Apache-2.0.
