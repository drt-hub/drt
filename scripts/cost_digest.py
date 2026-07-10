"""Daily DWH cost digest → Discord (#710, Layer 2b).

Reads month-to-date usage from each smoke warehouse's own usage views and
posts a one-line digest to a private ops Discord channel. This is the
*visibility* layer of the cost-monitoring design — the *prevention* layer
(hard caps: Snowflake resource monitor, GCP budget, Databricks Free Edition)
lives in the accounts themselves and works even if nobody reads the digest.

Identities: a dedicated read-only "cost watcher" per warehouse
(``DRT_COST_USER`` / ``drt-cost-sa``) — never the smoke user, whose
privileges stay least-possible. Databricks runs on Free Edition (no billing
account exists), so its line is a constant $0.

Env (all optional — a missing leg reports "n/a" and a failing leg becomes an
inline ⚠️ note, so a warehouse hiccup never "fails red"; the smoke workflow is
the health signal, this is a bookkeeping note. The one exception: a failing
*webhook delivery* raises and fails the run — if the delivery channel itself
is broken, nobody would see the digest, so that must be loud):

- ``DRT_COST_SNOWFLAKE_ACCOUNT`` / ``_USER`` / ``_WAREHOUSE``, plus
  ``_PRIVATE_KEY`` (PEM, key-pair auth for a ``TYPE = SERVICE`` user —
  preferred; new Snowflake accounts enforce MFA on password users) or
  ``_PASSWORD`` (legacy fallback)
- ``DRT_COST_BIGQUERY_PROJECT`` (+ ``GOOGLE_APPLICATION_CREDENTIALS``)
- ``DRT_COST_DISCORD_WEBHOOK``
- ``DRT_COST_SF_CREDIT_BUDGET`` (default 5, mirrors the resource monitor)
- ``DRT_COST_BQ_USD_BUDGET`` (default 7 ≈ the ¥1,000 GCP budget)
"""

from __future__ import annotations

import datetime as dt
import json
import os
import sys
import urllib.request

# Approximate unit prices for the digest line only — the authoritative
# numbers are each cloud's bill. Standard edition AWS ap-northeast-1 is
# ~$2.85/credit; BigQuery on-demand US is $6.25/TiB scanned.
SNOWFLAKE_USD_PER_CREDIT = 2.85
BIGQUERY_USD_PER_TIB = 6.25


def month_start_utc() -> dt.datetime:
    now = dt.datetime.now(dt.timezone.utc)
    return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def snowflake_mtd_credits() -> float | None:
    """MTD credits across all warehouses (ACCOUNT_USAGE lags ~1-3h; fine daily)."""
    account = os.environ.get("DRT_COST_SNOWFLAKE_ACCOUNT")
    user = os.environ.get("DRT_COST_SNOWFLAKE_USER")
    warehouse = os.environ.get("DRT_COST_SNOWFLAKE_WAREHOUSE")
    private_key_pem = os.environ.get("DRT_COST_SNOWFLAKE_PRIVATE_KEY")
    password = os.environ.get("DRT_COST_SNOWFLAKE_PASSWORD")
    if not all([account, user, warehouse]) or not (private_key_pem or password):
        return None
    import snowflake.connector

    auth: dict[str, object] = {}
    if private_key_pem:
        # Key-pair auth (TYPE = SERVICE user) — the connector wants DER bytes.
        from cryptography.hazmat.primitives import serialization

        key = serialization.load_pem_private_key(
            private_key_pem.encode(), password=None
        )
        auth["private_key"] = key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    else:
        auth["password"] = password

    conn = snowflake.connector.connect(
        account=account, user=user, warehouse=warehouse, **auth
    )
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COALESCE(SUM(credits_used), 0) "
            "FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY "
            "WHERE start_time >= DATE_TRUNC('month', CURRENT_TIMESTAMP())"
        )
        row = cur.fetchone()
        return float(row[0]) if row else 0.0
    finally:
        conn.close()


def bigquery_mtd_tib() -> float | None:
    """MTD TiB billed across the project (INFORMATION_SCHEMA.JOBS_BY_PROJECT)."""
    project = os.environ.get("DRT_COST_BIGQUERY_PROJECT")
    if not project or not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        return None
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    job = client.query(
        "SELECT COALESCE(SUM(total_bytes_billed), 0) "
        f"FROM `{project}.region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT "
        "WHERE creation_time >= TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MONTH)"
    )
    value = list(job.result())[0][0]
    return float(value or 0) / (1024**4)


def build_digest() -> str:
    month = month_start_utc().strftime("%Y-%m")
    sf_budget = float(os.environ.get("DRT_COST_SF_CREDIT_BUDGET", "5"))
    bq_budget = float(os.environ.get("DRT_COST_BQ_USD_BUDGET", "7"))

    parts: list[str] = []
    over_budget = False

    try:
        credits = snowflake_mtd_credits()
    except Exception as e:  # noqa: BLE001 — a broken leg must not kill the digest
        print(f"[debug] snowflake leg failed: {e}", file=sys.stderr)
        parts.append(f"Snowflake ⚠️ query failed ({type(e).__name__})")
        credits = None
    else:
        if credits is None:
            parts.append("Snowflake n/a")
        else:
            usd = credits * SNOWFLAKE_USD_PER_CREDIT
            flag = ""
            if credits > sf_budget:
                flag, over_budget = " 🚨over cap", True
            parts.append(f"Snowflake {credits:.2f}cr (~${usd:.2f}){flag}")

    try:
        tib = bigquery_mtd_tib()
    except Exception as e:  # noqa: BLE001
        print(f"[debug] bigquery leg failed: {e}", file=sys.stderr)
        parts.append(f"BigQuery ⚠️ query failed ({type(e).__name__})")
        tib = None
    else:
        if tib is None:
            parts.append("BigQuery n/a")
        else:
            usd = tib * BIGQUERY_USD_PER_TIB
            flag = ""
            if usd > bq_budget:
                flag, over_budget = " 🚨over budget", True
            parts.append(f"BigQuery {tib:.4f}TiB (~${usd:.2f}){flag}")

    parts.append("Databricks $0 (Free Edition)")

    status = "🚨 CHECK SPEND" if over_budget else "under budget ✅"
    return f"📊 drt DWH cost digest (MTD {month}): " + " · ".join(parts) + f" — {status}"


def post_to_discord(message: str) -> bool:
    webhook = os.environ.get("DRT_COST_DISCORD_WEBHOOK")
    if not webhook:
        return False
    body = json.dumps({"content": message}).encode()
    req = urllib.request.Request(
        webhook,
        data=body,
        headers={
            "Content-Type": "application/json",
            # Discord's edge (Cloudflare) rejects the default Python-urllib
            # user agent with 403 — any explicit UA passes.
            "User-Agent": "drt-cost-digest (github.com/drt-hub/drt)",
        },
    )
    urllib.request.urlopen(req, timeout=30)  # noqa: S310 — webhook URL from secrets
    return True


def main() -> int:
    message = build_digest()
    print(message)
    if post_to_discord(message):
        print("posted to Discord.")
    else:
        print("DRT_COST_DISCORD_WEBHOOK not set — printed only.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
