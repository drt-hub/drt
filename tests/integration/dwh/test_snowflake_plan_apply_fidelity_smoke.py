"""Live Snowflake plan → apply fidelity (#1223, 3b); see ``_plan_apply_fidelity``.

Runs only with the existing ``DRT_SMOKE_SNOWFLAKE_*`` credentials, so forks skip.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from ._plan_apply_fidelity import Warehouse, check_fidelity, source  # noqa: F401
from .conftest import require_env

pytestmark = pytest.mark.dwh_smoke


@contextmanager
def _snowflake() -> Iterator[Warehouse]:
    snowflake_connector = pytest.importorskip("snowflake.connector")
    account, user = "DRT_SMOKE_SNOWFLAKE_ACCOUNT", "DRT_SMOKE_SNOWFLAKE_USER"
    key, password = "DRT_SMOKE_SNOWFLAKE_PRIVATE_KEY", "DRT_SMOKE_SNOWFLAKE_PASSWORD"
    if not os.environ.get(key) and not os.environ.get(password):
        pytest.skip("Snowflake smoke auth not set")
    env = require_env(
        account,
        user,
        "DRT_SMOKE_SNOWFLAKE_DATABASE",
        "DRT_SMOKE_SNOWFLAKE_SCHEMA",
        "DRT_SMOKE_SNOWFLAKE_WAREHOUSE",
    )
    auth: dict[str, Any] = {}
    if os.environ.get(key):
        from drt.config.credentials import load_snowflake_private_key

        auth["private_key"] = load_snowflake_private_key(os.environ[key])
    else:
        auth["password"] = os.environ[password]
    conn = snowflake_connector.connect(
        account=env[account],
        user=env[user],
        warehouse=env["DRT_SMOKE_SNOWFLAKE_WAREHOUSE"],
        database=env["DRT_SMOKE_SNOWFLAKE_DATABASE"],
        schema=env["DRT_SMOKE_SNOWFLAKE_SCHEMA"],
        **auth,
    )
    prefix = f"{env['DRT_SMOKE_SNOWFLAKE_DATABASE']}.{env['DRT_SMOKE_SNOWFLAKE_SCHEMA']}"

    def run(sql: str) -> list[Any]:
        with conn.cursor() as cur:
            cur.execute(sql)
            return list(cur.fetchall()) if cur.description else []

    def create(table: str, rows: list[tuple[int, str]]) -> None:
        run(f"CREATE OR REPLACE TABLE {prefix}.{table} (id INTEGER, name VARCHAR)")
        values = ", ".join(f"({i}, '{n}')" for i, n in rows)
        run(f"INSERT INTO {prefix}.{table} VALUES {values}")

    def read(table: str) -> list[tuple[int, str]]:
        return [tuple(r) for r in run(f"SELECT id, name FROM {prefix}.{table} ORDER BY id")]  # type: ignore[misc]

    def destination(table: str) -> dict[str, Any]:
        creds = {"private_key_env": key} if os.environ.get(key) else {"password_env": password}
        return {
            "type": "snowflake",
            "account_env": account,
            "user_env": user,
            **creds,
            "database": env["DRT_SMOKE_SNOWFLAKE_DATABASE"],
            "schema": env["DRT_SMOKE_SNOWFLAKE_SCHEMA"],
            "warehouse": env["DRT_SMOKE_SNOWFLAKE_WAREHOUSE"],
            "table": table,
            "mode": "merge",
            "upsert_key": ["id"],
        }

    try:
        yield Warehouse(
            "snowflake",
            create,
            read,
            lambda table: run(f"DROP TABLE IF EXISTS {prefix}.{table}") and None,
            destination,
        )
    finally:
        conn.close()


def test_the_planned_change_set_is_what_apply_does(source: Path) -> None:  # noqa: F811
    with _snowflake() as wh:
        check_fidelity(source, wh)
