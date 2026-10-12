# Using drt with dbt and Apache Ossie

drt can read your dbt project's `target/manifest.json` to resolve `ref()` model references to fully-qualified table names.

It can also scaffold sync files from a dbt manifest or an [Apache
Ossie](https://ossie.apache.org/) semantic model. Scaffolding writes reviewable
YAML; it does not add a runtime dependency on either tool.

## Scaffold from a dbt manifest

Use the existing interactive manifest flow to list dbt models and select the
ones to activate:

```bash
drt init --from-dbt target/manifest.json
```

drt writes `syncs/sync_<model>.yml` files with REST API destination
placeholders. Existing files are skipped rather than overwritten.

## Scaffold from an Apache Ossie model

The Ossie spike reads the small activation-relevant subset of a YAML or JSON
model directly; `apache-ossie` is not required. By default it generates every
dataset with a `primary_key` and prints why it skips a dataset without one:

```bash
drt init --from-ossie semantic_model.yaml
```

Select datasets by repeating `--dataset`, and choose the initial destination,
field-expression dialect, and output directory explicitly:

```bash
drt init --from-ossie semantic_model.yaml \
  --dataset customer \
  --dataset account \
  --destination postgres \
  --dialect SNOWFLAKE \
  --output-dir syncs/
```

Supported scaffold destinations are `postgres` (the default) and `hubspot`.
The generic `rest_api` destination is deliberately unavailable because a
plain per-record POST cannot promise keyed upsert or replay safety. PostgreSQL
keeps composite primary keys as `upsert_key`; HubSpot requires one primary-key
column for `id_property`. Each sync selects the dataset fields in the requested
dialect, falling back field-by-field to `ANSI_SQL`. If neither expression
exists, generation stops before writing any files and names the dataset,
field, and missing dialect.

Generated files contain `sync.mode: upsert`, provenance, and TODOs for source
credentials, the destination table or HubSpot object, and destination property
mappings.
Ossie metrics are listed but deliberately not selected: the model has no
activation entity grain from which drt could safely infer a `GROUP BY`.

The reader tolerates unknown keys and warns on missing or unrecognised
versions, because `0.2.0.dev0` is explicitly unstable. It still rejects a
document without a usable model/dataset/field shape and refuses input larger
than 5 MB. Field aliases must be safe lowercase, non-reserved SQL identifiers.
Table sources may have at most three safe, non-reserved identifier segments; a
single `SELECT`/`WITH` source is wrapped as a subquery. The SQL safety check is
deliberately conservative rather than dialect-aware: it requires balanced
single, double, or backtick quotes with doubled-quote escaping, and rejects
backslashes, dollar signs, prefixed string literals, control characters,
separators, and comment markers when their interpretation could be ambiguous.
One trailing query semicolon is allowed. See the [spike
findings](../research/ossie-activation-spike.md) for the mapping gaps and
recommendation.

## Setup

No extra installation needed. Just run `dbt run` before `drt run` so that `target/manifest.json` exists.

## How it works

When drt encounters `ref('my_model')` in a sync definition, it:

1. Checks `syncs/models/my_model.sql` for a custom query
2. Reads `target/manifest.json` for dbt's fully-qualified relation name
3. Falls back to `SELECT * FROM <dataset>.my_model`

## Example workflow

```bash
# 1. Run dbt to build models
dbt run

# 2. Run drt to activate data
drt run
```

## Pipeline automation

Use Dagster, Airflow, or a simple script to chain them:

```bash
dbt run && drt run
```

Or with [dagster-drt](../integrations/dagster-drt/):

```python
from dagster import Definitions
from dagster_dbt import dbt_assets
from dagster_drt import drt_assets

defs = Definitions(
    assets=[*dbt_assets, *drt_assets("path/to/project")],
)
```

## Publish drt syncs in dbt lineage

Generate dbt exposures for every drt sync whose `model` is an exact `ref(...)`:

```bash
mkdir -p models/exposures
drt docs generate --format dbt-exposures \
  > models/exposures/drt_exposures.yml
```

Each exposure depends on the referenced dbt model and records the drt sync,
destination type, and sync mode under `meta.drt`. Its URL is computed
deterministically for the matching HTML sync page under `--output` (default:
`target/docs`) and is relative to the `target/` root served by `dbt docs serve`,
so the default URL starts with `docs/sync/` whether or not HTML docs have been
generated yet. Raw-SQL models are omitted and named in YAML comments because
drt deliberately does not guess lineage by parsing SQL. A `ref(...)` with a
matching `syncs/models/<name>.sql` override is omitted too: that local SQL takes
precedence at runtime, so publishing the dbt ref would claim false lineage.
If `--output` points outside `target/`, the HTML page is not reachable through
`dbt docs serve`, so the exporter omits the URL. The command only writes to
stdout; you choose and commit the destination file yourself.

The output is sorted by sync name and byte-identical for an unchanged project,
so it can be regenerated and checked for drift in CI. See [CI/CD
Integration](ci-cd-integration.md#fail-on-dbt-exposure-drift).
