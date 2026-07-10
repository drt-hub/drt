-- Databricks provisioning for the DWH smoke harness (#654 / #672).
--
-- The catalog + schema + grants are SQL (run in a Databricks SQL editor on the
-- SQL warehouse). The warehouse, the PAT, and the workspace itself are UI/API
-- steps — see the "Databricks — manual (UI/API) steps" section in
-- provisioning/README.md.
--
-- Cost note: the smoke account runs on Databricks Free Edition — structurally
-- $0 (no billing account exists). On a paid workspace, Serverless SQL bills
-- DBUs + cloud compute and a warehouse left running is a cost bomb like
-- Snowflake's — keep AUTO-STOP at its minimum there.
--
-- The smoke tests create/populate/drop their own throwaway tables (insert /
-- replace INSERT OVERWRITE / STRUCT-ARRAY-MAP + VARIANT complex types), so no
-- data needs seeding — only the empty catalog + schema + write grants.

-- ── 1. Throwaway catalog + schema (Unity Catalog three-part name) ──────────
CREATE CATALOG IF NOT EXISTS drt_smoke;
CREATE SCHEMA  IF NOT EXISTS drt_smoke.smoke;

-- ── 2. Least-privilege grants to the smoke principal ───────────────────────
-- Replace <SMOKE_PRINCIPAL> with the user/service principal whose PAT you use.
-- USE + CREATE TABLE + MODIFY cover insert / INSERT OVERWRITE (replace-swap) /
-- from_json complex-type writes; the tests drop what they create.
GRANT USE CATALOG      ON CATALOG drt_smoke        TO `<SMOKE_PRINCIPAL>`;
GRANT USE SCHEMA       ON SCHEMA  drt_smoke.smoke  TO `<SMOKE_PRINCIPAL>`;
GRANT CREATE TABLE     ON SCHEMA  drt_smoke.smoke  TO `<SMOKE_PRINCIPAL>`;
GRANT MODIFY           ON SCHEMA  drt_smoke.smoke  TO `<SMOKE_PRINCIPAL>`;

-- ── Secret mapping (register as SMOKE_DATABRICKS_* repo secrets) ────────────
--   SMOKE_DATABRICKS_HOST      = <workspace host, e.g. dbc-xxxx.cloud.databricks.com>
--   SMOKE_DATABRICKS_HTTP_PATH = <SQL warehouse HTTP path, from its Connection details>
--   SMOKE_DATABRICKS_TOKEN     = <PAT, dapi...>
--   SMOKE_DATABRICKS_CATALOG   = drt_smoke
--   SMOKE_DATABRICKS_SCHEMA    = smoke
