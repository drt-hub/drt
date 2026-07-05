# DWH smoke harness (#674, part of #654)

Real-warehouse end-to-end checks for the cloud destinations (Snowflake,
Databricks, BigQuery). They drive the same pipeline as the rest of the
integration suite — seeded DuckDB `users` → engine → **real** warehouse table →
read back → verify — to catch dialect-specific behaviour that the
mock-injected unit suites can't.

## Why this is a safe no-op until secrets exist

Each test is gated twice:

1. **Driver gate** — `pytest.importorskip(...)` skips the module unless the
   warehouse extra is installed (`drt-core[snowflake|databricks|bigquery]`).
2. **Credential gate** — `require_env(...)` skips unless the `DRT_SMOKE_*`
   env vars are set.

So a plain `pytest` run, every fork, and the upstream repo *before secrets are
added* are all green without ever opening a connection. The `dwh-smoke`
workflow injects the secrets from the repo so the checks run for real only on
`drt-hub/drt`.

## Running locally

```bash
pip install -e ".[dev,duckdb,snowflake]"     # or databricks / bigquery
export DRT_SMOKE_SNOWFLAKE_ACCOUNT=...        # see the secret list below
# ...export the rest...
pytest -m dwh_smoke tests/integration/dwh/test_snowflake_smoke.py -v
```

## Required repo secrets (maintainer-owned)

Add these under **Settings → Secrets and variables → Actions**. Per the split,
the cloud accounts and the "verified ✓" sign-off are the maintainer's
(#671 / #672 / #673). Until a warehouse's secrets exist, its job no-ops.

**Snowflake** (#671)
| Secret | Notes |
| --- | --- |
| `SMOKE_SNOWFLAKE_ACCOUNT` | account identifier |
| `SMOKE_SNOWFLAKE_USER` | |
| `SMOKE_SNOWFLAKE_PASSWORD` | |
| `SMOKE_SNOWFLAKE_DATABASE` | a throwaway DB the role can create/drop tables in |
| `SMOKE_SNOWFLAKE_SCHEMA` | |
| `SMOKE_SNOWFLAKE_WAREHOUSE` | |

Snowflake prerequisites (#671):

- A **throwaway database + schema** the role can `CREATE`/`DROP` tables in, plus a
  running **virtual warehouse** for compute. Scope the grants to the throwaway
  schema, not the whole account — the swap leg builds and drops a
  `<table>__drt_swap` shadow, and the complex-type leg creates ARRAY / OBJECT /
  VARIANT tables.
- Least-privilege grants for the role: `USAGE` on the warehouse + database +
  schema, and `CREATE TABLE` on the schema (`ALTER TABLE ... SWAP WITH` needs
  ownership/`OWNERSHIP`-equivalent on both names, which the creating role holds
  for tables it created).
- The Snowflake leg drives four paths against the throwaway schema — `mode: insert`,
  `replace_strategy: swap` (`ALTER TABLE ... SWAP WITH` #434), complex-type
  `PARSE_JSON` serialization (VARIANT / OBJECT / ARRAY #317 Layer 3 / #653), and
  `test_connection` — and drops everything it creates (target + `__drt_swap`
  shadow) in `finally`.

**Databricks** (#672)
| Secret | Notes |
| --- | --- |
| `SMOKE_DATABRICKS_HOST` | workspace hostname, e.g. `dbc-….cloud.databricks.com` |
| `SMOKE_DATABRICKS_HTTP_PATH` | SQL warehouse HTTP path |
| `SMOKE_DATABRICKS_TOKEN` | PAT (`dapi…`) |
| `SMOKE_DATABRICKS_CATALOG` | |
| `SMOKE_DATABRICKS_SCHEMA` | |

Databricks prerequisites (#672):

- A **Unity Catalog** catalog + schema the token principal can write to (Hive
  Metastore works too — set the catalog to `hive_metastore`). All tables are
  created **Delta** (`USING DELTA`): the `replace_strategy: swap` leg relies on
  Delta `INSERT OVERWRITE` snapshot-isolation atomicity.
- A running **SQL warehouse**; its HTTP path is `SMOKE_DATABRICKS_HTTP_PATH`.
  The complex-type leg uses a `VARIANT` column, so the warehouse must be on a
  channel that supports VARIANT (current serverless/pro warehouses do).
- Least-privilege grants for the token principal: `USE CATALOG` + `USE SCHEMA`,
  plus `CREATE TABLE` / `MODIFY` on the smoke schema — the swap leg builds and
  drops a `<table>__drt_swap` shadow, and the complex-type leg creates ARRAY /
  STRUCT / VARIANT tables. Scope the grants to the throwaway schema, not the
  whole catalog.
- The `insert` + `replace_strategy: swap` + complex-type (`from_json` /
  `parse_json`) + `test_connection` legs all run against the same throwaway
  schema and drop everything they create in `finally`.

**BigQuery** (#673)
| Secret | Notes |
| --- | --- |
| `SMOKE_BIGQUERY_PROJECT` | |
| `SMOKE_BIGQUERY_DATASET` | a throwaway dataset |
| `SMOKE_BIGQUERY_KEYFILE_JSON` | full service-account JSON; the workflow writes it to a temp file |

BigQuery prerequisites (#673):

- A **billing-enabled** project (BigQuery jobs require an active billing account,
  even for tiny throwaway tables).
- **Service-account keyfile** auth. The BigQuery leg drives two write paths — the
  streaming `insert` path and the temp-table `MERGE` path (`<table>_drt_tmp` →
  `MERGE` → drop, #645) — both against a throwaway dataset.
- Least-privilege roles for the service account: `roles/bigquery.jobUser` on the
  project (to run load/query/MERGE jobs) **plus** dataset-scoped
  `roles/bigquery.dataEditor` on the smoke dataset (to create/insert/drop the
  target + `_drt_tmp` tables). Avoid granting project-wide `dataEditor`.
- Caveat: if the org enforces the `iam.disableServiceAccountKeyCreation`
  constraint, a keyfile can't be minted for the SA — provision the key in a
  project/folder where that policy is not enforced, or use an exempted SA.

## Adding a warehouse leg

`test_snowflake_smoke.py` is the reference shape. To add another, copy it and
swap: the `importorskip` driver, the destination config, and the
read-back/cleanup connection. The source side (`seed_duckdb_users`) and the
3-row assertion stay identical.
