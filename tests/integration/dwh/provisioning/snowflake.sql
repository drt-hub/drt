-- Snowflake provisioning for the DWH smoke harness (#654 / #671).
--
-- Creates a throwaway database + schema, an XS warehouse with aggressive
-- auto-suspend, a least-privilege role, a programmatic user, and a resource
-- monitor that hard-caps monthly credits. Idempotent — safe to re-run.
--
-- Run once, as ACCOUNTADMIN, in a Worksheet on a fresh trial account.
-- Then collect the six SMOKE_SNOWFLAKE_* values (see provisioning/README.md)
-- and register them as repo secrets.
--
-- Nothing here is secret except the password you fill in below; this file is
-- committed as reproducible infra (the smoke tests create/populate/drop their
-- own throwaway tables, so no data needs seeding).

-- ── 1. Throwaway database + schema (the "empty vessel") ────────────────────
CREATE DATABASE IF NOT EXISTS DRT_SMOKE;
CREATE SCHEMA   IF NOT EXISTS DRT_SMOKE.PUBLIC;

-- ── 2. XS warehouse — AUTO_SUSPEND is the #1 cost guardrail ─────────────────
-- A warehouse left running is the classic Snowflake cost bomb. 60s suspend +
-- per-second billing means a nightly smoke run costs fractions of a credit.
CREATE WAREHOUSE IF NOT EXISTS DRT_SMOKE_WH
  WAREHOUSE_SIZE      = 'XSMALL'
  AUTO_SUSPEND        = 60          -- seconds
  AUTO_RESUME         = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- ── 3. Resource monitor — hard cap so "some freedom" can't run away ────────
CREATE RESOURCE MONITOR IF NOT EXISTS DRT_SMOKE_MON
  WITH CREDIT_QUOTA   = 5           -- credits/month; smoke uses a tiny fraction
       FREQUENCY      = MONTHLY
       START_TIMESTAMP = IMMEDIATELY
       TRIGGERS ON 100 PERCENT DO SUSPEND;
ALTER WAREHOUSE DRT_SMOKE_WH SET RESOURCE_MONITOR = DRT_SMOKE_MON;

-- ── 4. Least-privilege role ────────────────────────────────────────────────
-- CREATE TABLE on the schema is enough: the smoke tests create their own
-- throwaway tables (insert / replace-swap / VARIANT-OBJECT-ARRAY) and drop
-- them in a finally block. Table ownership (implied by CREATE) covers the
-- ALTER TABLE ... SWAP WITH the replace-swap test needs.
CREATE ROLE IF NOT EXISTS DRT_SMOKE_ROLE;
GRANT USAGE        ON WAREHOUSE DRT_SMOKE_WH     TO ROLE DRT_SMOKE_ROLE;
GRANT USAGE        ON DATABASE  DRT_SMOKE        TO ROLE DRT_SMOKE_ROLE;
GRANT USAGE        ON SCHEMA    DRT_SMOKE.PUBLIC TO ROLE DRT_SMOKE_ROLE;
GRANT CREATE TABLE ON SCHEMA    DRT_SMOKE.PUBLIC TO ROLE DRT_SMOKE_ROLE;

-- ── 5. Programmatic user (fill in a strong password) ───────────────────────
CREATE USER IF NOT EXISTS DRT_SMOKE_USER
  PASSWORD             = '<SET-A-STRONG-PASSWORD>'
  DEFAULT_ROLE         = DRT_SMOKE_ROLE
  DEFAULT_WAREHOUSE    = DRT_SMOKE_WH
  MUST_CHANGE_PASSWORD = FALSE;
GRANT ROLE DRT_SMOKE_ROLE TO USER DRT_SMOKE_USER;

-- ── Secret mapping (register these; values → SMOKE_SNOWFLAKE_* repo secrets)─
--   SMOKE_SNOWFLAKE_ACCOUNT   = <account identifier, e.g. orgname-accountname>
--   SMOKE_SNOWFLAKE_USER      = DRT_SMOKE_USER
--   SMOKE_SNOWFLAKE_PASSWORD  = <the password above>
--   SMOKE_SNOWFLAKE_DATABASE  = DRT_SMOKE
--   SMOKE_SNOWFLAKE_SCHEMA    = PUBLIC
--   SMOKE_SNOWFLAKE_WAREHOUSE = DRT_SMOKE_WH
