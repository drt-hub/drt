"""Deterministic local data for the review-before-you-write demo.

The warehouse is a DuckDB file. The CRM is the local Postgres container from
``docker-compose.yml``. There are no cloud credentials or external requests.
"""

from __future__ import annotations

import argparse
import os
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any

RNG_SEED = 20261013
WAREHOUSE_ROW_COUNT = 1_500
CRM_ONLY_ROW_COUNT = 12
REP_POPULATED_COUNT = 1_050
REP_DIFFERENT_COUNT = 300
DANGEROUS_SUBSET_COUNT = 25

INDUSTRIES = (
    "Business Services",
    "Consumer",
    "Financial Services",
    "Healthcare",
    "Manufacturing",
    "Media",
    "Retail",
    "Software",
)
EMPLOYEE_BANDS = ("1-10", "11-50", "51-200", "201-1000", "1001+")


@dataclass(frozen=True)
class DemoData:
    warehouse: list[dict[str, Any]]
    crm: list[dict[str, Any]]


def build_dataset() -> DemoData:
    """Build the same rows every time from one fixed RNG seed."""
    rng = random.Random(RNG_SEED)
    warehouse: list[dict[str, Any]] = []
    for company_id in range(1, WAREHOUSE_ROW_COUNT + 1):
        warehouse.append(
            {
                "company_id": company_id,
                "domain": f"company-{company_id:04d}.example",
                "name": f"Company {company_id:04d}",
                "inferred_industry": rng.choice(INDUSTRIES),
                "employee_band": rng.choice(EMPLOYEE_BANDS),
            }
        )

    shuffled_ids = list(range(1, WAREHOUSE_ROW_COUNT + 1))
    rng.shuffle(shuffled_ids)
    populated_ids = set(shuffled_ids[:REP_POPULATED_COUNT])
    different_ids = set(shuffled_ids[:REP_DIFFERENT_COUNT])
    empty_ids = shuffled_ids[REP_POPULATED_COUNT:]
    null_ids = set(empty_ids[:150])
    empty_string_ids = set(empty_ids[150:300])

    crm: list[dict[str, Any]] = []
    for row in warehouse:
        company_id = int(row["company_id"])
        inferred = str(row["inferred_industry"])
        if company_id in different_ids:
            industry = INDUSTRIES[(INDUSTRIES.index(inferred) + 1) % len(INDUSTRIES)]
        elif company_id in populated_ids:
            industry = inferred
        elif company_id in null_ids:
            industry = None
        elif company_id in empty_string_ids:
            industry = ""
        else:
            industry = "   "
        crm.append(
            {
                "company_id": company_id,
                "domain": row["domain"],
                "name": row["name"],
                "industry": industry,
            }
        )

    for offset in range(1, CRM_ONLY_ROW_COUNT + 1):
        company_id = WAREHOUSE_ROW_COUNT + offset
        crm.append(
            {
                "company_id": company_id,
                "domain": f"crm-only-{offset:02d}.example",
                "name": f"CRM-only Company {offset:02d}",
                "industry": "Rep-created",
            }
        )
    return DemoData(warehouse=warehouse, crm=crm)


def _is_empty(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip(" ") == "")


def expected_counts(data: DemoData | None = None) -> dict[str, int]:
    """Derive the numbers quoted by the README, video, and tests."""
    dataset = data or build_dataset()
    warehouse_by_id = {row["company_id"]: row for row in dataset.warehouse}
    matching_crm = [row for row in dataset.crm if row["company_id"] in warehouse_by_id]
    return {
        "warehouse_rows": len(dataset.warehouse),
        "crm_rows": len(dataset.crm),
        "rows_to_fill": sum(_is_empty(row["industry"]) for row in matching_crm),
        "values_kept": sum(
            not _is_empty(row["industry"])
            and row["industry"] != warehouse_by_id[row["company_id"]]["inferred_industry"]
            for row in matching_crm
        ),
        "rows_not_in_warehouse": sum(
            row["company_id"] not in warehouse_by_id for row in dataset.crm
        ),
        "dangerous_deletes": len(dataset.crm) - DANGEROUS_SUBSET_COUNT,
    }


def format_counts(counts: dict[str, int]) -> str:
    return "\n".join(
        (
            f"Warehouse rows: {counts['warehouse_rows']}",
            f"CRM rows: {counts['crm_rows']}",
            f"Rows to fill: {counts['rows_to_fill']} (150 NULL, 150 empty, 150 spaces-only)",
            f"Different rep-entered values kept: {counts['values_kept']}",
            f"CRM-only rows not in warehouse: {counts['rows_not_in_warehouse']}",
            f"Dangerous mirror would delete: {counts['dangerous_deletes']}",
        )
    )


def seed_duckdb(path: Path, data: DemoData | None = None) -> dict[str, int]:
    import duckdb

    dataset = data or build_dataset()
    connection = duckdb.connect(str(path))
    try:
        connection.execute("DROP TABLE IF EXISTS enriched_companies")
        connection.execute(
            """
            CREATE TABLE enriched_companies (
                company_id BIGINT PRIMARY KEY,
                domain TEXT NOT NULL,
                name TEXT NOT NULL,
                inferred_industry TEXT NOT NULL,
                employee_band TEXT NOT NULL
            )
            """
        )
        connection.executemany(
            "INSERT INTO enriched_companies VALUES (?, ?, ?, ?, ?)",
            [
                (
                    row["company_id"],
                    row["domain"],
                    row["name"],
                    row["inferred_industry"],
                    row["employee_band"],
                )
                for row in dataset.warehouse
            ],
        )
    finally:
        connection.close()
    return expected_counts(dataset)


def seed_postgres(dsn: str, data: DemoData | None = None) -> dict[str, int]:
    import psycopg2

    dataset = data or build_dataset()
    connection = psycopg2.connect(dsn)
    try:
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS companies")
            # The enrichment sync selects only company_id + industry. PostgreSQL
            # validates the proposed INSERT row before ON CONFLICT can update the
            # existing row, so every unselected column must be nullable or have a
            # default when a partial-column upsert uses match_policy: upsert. The
            # native update_only UPDATE form would not hit this INSERT-time check.
            cursor.execute(
                """
                CREATE TABLE companies (
                    company_id BIGINT PRIMARY KEY,
                    domain TEXT,
                    name TEXT,
                    industry TEXT
                )
                """
            )
            cursor.executemany(
                "INSERT INTO companies VALUES (%s, %s, %s, %s)",
                [
                    (row["company_id"], row["domain"], row["name"], row["industry"])
                    for row in dataset.crm
                ],
            )
        connection.commit()
    finally:
        connection.close()
    return expected_counts(dataset)


def verify_postgres_after_apply(dsn: str, data: DemoData | None = None) -> dict[str, int]:
    import psycopg2

    dataset = data or build_dataset()
    source = {row["company_id"]: row for row in dataset.warehouse}
    before = {row["company_id"]: row for row in dataset.crm}
    connection = psycopg2.connect(dsn)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT company_id, industry FROM companies ORDER BY company_id")
            rows = cursor.fetchall()
    finally:
        connection.close()

    assert len(rows) == len(dataset.crm), "the apply created or deleted CRM rows"
    for company_id, industry in rows:
        original = before[company_id]["industry"]
        if company_id not in source:
            assert industry == original, "a CRM-only row changed"
        elif _is_empty(original):
            assert industry == source[company_id]["inferred_industry"], (
                "an empty value was not filled"
            )
        else:
            assert industry == original, "a rep-entered value was overwritten"
    return expected_counts(dataset)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", type=Path, default=Path("warehouse.duckdb"))
    parser.add_argument("--postgres-dsn", default=os.environ.get("REVIEW_DEMO_PG_DSN"))
    parser.add_argument("--duckdb-only", action="store_true")
    parser.add_argument("--verify", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if args.verify:
        if not args.postgres_dsn:
            raise SystemExit("REVIEW_DEMO_PG_DSN or --postgres-dsn is required")
        counts = verify_postgres_after_apply(args.postgres_dsn)
        print(
            f"Verified: {counts['rows_to_fill']} blanks filled; "
            f"{counts['values_kept']} conflicting rep values and "
            f"{counts['rows_not_in_warehouse']} CRM-only rows untouched."
        )
        return

    data = build_dataset()
    counts = seed_duckdb(args.duckdb, data)
    if not args.duckdb_only:
        if not args.postgres_dsn:
            raise SystemExit("REVIEW_DEMO_PG_DSN or --postgres-dsn is required")
        seed_postgres(args.postgres_dsn, data)
    print(format_counts(counts))


if __name__ == "__main__":
    main()
