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

**Databricks** (#672)
| Secret | Notes |
| --- | --- |
| `SMOKE_DATABRICKS_HOST` | workspace hostname, e.g. `dbc-….cloud.databricks.com` |
| `SMOKE_DATABRICKS_HTTP_PATH` | SQL warehouse HTTP path |
| `SMOKE_DATABRICKS_TOKEN` | PAT (`dapi…`) |
| `SMOKE_DATABRICKS_CATALOG` | |
| `SMOKE_DATABRICKS_SCHEMA` | |

**BigQuery** (#673)
| Secret | Notes |
| --- | --- |
| `SMOKE_BIGQUERY_PROJECT` | |
| `SMOKE_BIGQUERY_DATASET` | a throwaway dataset |
| `SMOKE_BIGQUERY_KEYFILE_JSON` | full service-account JSON; the workflow writes it to a temp file |

## Adding a warehouse leg

`test_snowflake_smoke.py` is the reference shape. To add another, copy it and
swap: the `importorskip` driver, the destination config, and the
read-back/cleanup connection. The source side (`seed_duckdb_users`) and the
3-row assertion stay identical.
