# DWH smoke — account provisioning

Reproducible setup for the throwaway warehouse accounts the [DWH smoke
harness](../README.md) runs against (epic #654 · legs #671 / #672 / #673).

These scripts create only the **empty vessel** — an isolated database/dataset/
catalog, a least-privilege role/SA, cost guardrails. The smoke tests create,
populate, verify, and drop their own throwaway tables (unique names, dropped in
a `finally`), so **no data needs seeding**.

Nothing here is secret except the passwords/keys you fill in. The credentials
themselves live only in **GitHub Actions secrets** (and a shared vault) — never
in git.

## The loop (per warehouse)

1. **Sign up** for the warehouse (use the project's org email, not a personal
   account, so the account is an org asset).
2. **Provision** — run the script below. It sets up the empty vessel + an
   `AUTO_SUSPEND` / budget guardrail (a running warehouse is the classic cost
   bomb).
3. **Collect** the values listed at the bottom of each script.
4. **Register** them as repo secrets (`Settings → Secrets and variables →
   Actions`, or `gh secret set SMOKE_… --repo drt-hub/drt`).
5. **Run** the smoke: Actions → `dwh-smoke` → *Run workflow* (`workflow_dispatch`).
   The job for that warehouse flips from no-op to a real run and goes green.
   It also runs nightly (03:00 UTC).

Until a warehouse's secrets exist, its `dwh-smoke` job **no-ops green** — so
adding accounts one at a time never breaks CI.

## Snowflake — `snowflake.sql`

Card-free 30-day trial. Run [`snowflake.sql`](./snowflake.sql) as
`ACCOUNTADMIN` in a Worksheet, filling in a strong password. Account identifier
is under the account menu (bottom-left) → *Copy account identifier*.
Secrets: `SMOKE_SNOWFLAKE_{ACCOUNT,USER,PASSWORD,DATABASE,SCHEMA,WAREHOUSE}`.

## BigQuery — `bigquery.sh`

Needs a **billing-enabled** project (a payment card — even the free tier
requires billing enabled). ⚠️ A project created *after* billing signup is
**not auto-linked** to the billing account — link it first or every write
fails with `billingNotEnabled` / "not allowed in the free tier":

```bash
gcloud billing accounts list
gcloud billing projects link <PROJECT> --billing-account=<ACCOUNT_ID>
``` Effectively $0/month for KB-scale smoke tables.
Run [`bigquery.sh`](./bigquery.sh) with `gcloud` authenticated; load the
generated keyfile's contents into `SMOKE_BIGQUERY_KEYFILE_JSON`, then delete
the local keyfile. Secrets: `SMOKE_BIGQUERY_{PROJECT,DATASET,KEYFILE_JSON}`.

## Databricks — `databricks.sql` + manual steps

The most expensive leg; consider deferring. [`databricks.sql`](./databricks.sql)
covers the catalog/schema/grants. The rest is UI/API:

- **Workspace**: a Databricks workspace on a cloud (trial or paid). Host =
  `dbc-xxxx.cloud.databricks.com`.
- **SQL warehouse**: create a (2X-)Small warehouse with the **minimum auto-stop**
  (cost guardrail). Its *Connection details* give the HTTP path.
- **PAT**: Settings → Developer → Access tokens → generate (`dapi…`).

Secrets: `SMOKE_DATABRICKS_{HOST,HTTP_PATH,TOKEN,CATALOG,SCHEMA}`.

## Cost & ownership

Steady-state (correctly configured): Snowflake ~$1–3, BigQuery ~$0, Databricks
~$8–15 per month. The dominant risk across all three is **leaving compute
running** — always set auto-suspend/auto-stop + a budget/credit hard cap first.
Account ownership, card strategy, and the CI-vs-contributor split are maintainer
decisions kept in private strategy notes.
