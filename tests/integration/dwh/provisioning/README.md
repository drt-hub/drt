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

Card-free 30-day trial (converts to pay-as-you-go on a card afterwards — the
resource monitor in the script is the hard cap either way). Run
[`snowflake.sql`](./snowflake.sql) as `ACCOUNTADMIN` in a Worksheet (**Run All**,
not "run current statement" — partial runs leave the user with no roles),
filling in a strong password. Account identifier is under the account menu
(bottom-left) → *Copy account identifier*. Then convert the user to
`TYPE = SERVICE` + RSA key pair (the script's last section) — new accounts
enforce MFA on password sign-ins, so key-pair is the programmatic path (#737).
Secrets: `SMOKE_SNOWFLAKE_{ACCOUNT,USER,PRIVATE_KEY,DATABASE,SCHEMA,WAREHOUSE}`.

## BigQuery — `bigquery.sh`

Needs a **billing-enabled** project (a payment card — even the free tier
requires billing enabled). ⚠️ A project created *after* billing signup is
**not auto-linked** to the billing account — link it first or every write
fails with `billingNotEnabled` / "not allowed in the free tier":

```bash
gcloud billing accounts list
gcloud billing projects link <PROJECT> --billing-account=<ACCOUNT_ID>
```

Effectively $0/month for KB-scale smoke tables.
Run [`bigquery.sh`](./bigquery.sh) with `gcloud` authenticated; load the
generated keyfile's contents into `SMOKE_BIGQUERY_KEYFILE_JSON`, then delete
the local keyfile. Secrets: `SMOKE_BIGQUERY_{PROJECT,DATASET,KEYFILE_JSON}`.

## Databricks — `databricks.sql` + manual steps

**Free Edition** — structurally $0, no card, and includes a serverless SQL
warehouse, PATs, and Unity Catalog (everything the smoke needs).
[`databricks.sql`](./databricks.sql) covers the catalog/schema/grants. The
rest is UI/API:

- **Workspace**: sign up for Databricks Free Edition. Host =
  `dbc-xxxx.cloud.databricks.com`.
- **SQL warehouse**: the built-in serverless starter warehouse works as-is.
  Its *Connection details* give the HTTP path.
- **PAT**: Settings → Developer → Access tokens → generate (`dapi…`).
  ⚠️ PATs default to a **90-day lifetime** — calendar the rotation.

Secrets: `SMOKE_DATABRICKS_{HOST,HTTP_PATH,TOKEN,CATALOG,SCHEMA}`.

## Cost & ownership

Steady-state (correctly configured): Snowflake ~$1–3 (resource monitor hard-caps
at 5 credits/month), BigQuery ~$0 (budget-alerted), Databricks $0 (Free Edition —
no billing account exists to bill). On Snowflake/BigQuery the dominant risk is
**leaving compute running** — always set auto-suspend + a budget/credit hard cap
first. Account ownership, card strategy, and the CI-vs-contributor split are
maintainer decisions kept in private strategy notes.
