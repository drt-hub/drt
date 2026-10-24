"""Live Databricks plan → apply fidelity (#1223, 3b); see ``_plan_apply_fidelity``.

Runs only with the existing ``DRT_SMOKE_DATABRICKS_*`` credentials, so forks skip.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from ._plan_apply_fidelity import Warehouse, check_fidelity, source  # noqa: F401
from .conftest import require_env

pytestmark = pytest.mark.dwh_smoke


@contextmanager
def _databricks() -> Iterator[Warehouse]:
    dbsql = pytest.importorskip("databricks.sql")
    host, http_path, token = (
        "DRT_SMOKE_DATABRICKS_HOST",
        "DRT_SMOKE_DATABRICKS_HTTP_PATH",
        "DRT_SMOKE_DATABRICKS_TOKEN",
    )
    env = require_env(
        host, http_path, token, "DRT_SMOKE_DATABRICKS_CATALOG", "DRT_SMOKE_DATABRICKS_SCHEMA"
    )
    conn = dbsql.connect(
        server_hostname=env[host], http_path=env[http_path], access_token=env[token]
    )
    prefix = f"{env['DRT_SMOKE_DATABRICKS_CATALOG']}.{env['DRT_SMOKE_DATABRICKS_SCHEMA']}"

    def run(sql: str) -> list[Any]:
        with conn.cursor() as cur:
            cur.execute(sql)
            return list(cur.fetchall()) if cur.description else []

    def create(table: str, rows: list[tuple[int, str]]) -> None:
        run(f"CREATE OR REPLACE TABLE {prefix}.{table} (id INT, name STRING)")
        values = ", ".join(f"({i}, '{n}')" for i, n in rows)
        run(f"INSERT INTO {prefix}.{table} VALUES {values}")

    def read(table: str) -> list[tuple[int, str]]:
        return [tuple(r) for r in run(f"SELECT id, name FROM {prefix}.{table} ORDER BY id")]  # type: ignore[misc]

    def destination(table: str) -> dict[str, Any]:
        return {
            "type": "databricks",
            "host_env": host,
            "http_path_env": http_path,
            "token_env": token,
            "catalog": env["DRT_SMOKE_DATABRICKS_CATALOG"],
            "schema": env["DRT_SMOKE_DATABRICKS_SCHEMA"],
            "table": table,
            "mode": "merge",
            "upsert_key": ["id"],
        }

    try:
        yield Warehouse(
            "databricks",
            create,
            read,
            lambda table: run(f"DROP TABLE IF EXISTS {prefix}.{table}") and None,
            destination,
        )
    finally:
        conn.close()


def test_the_planned_change_set_is_what_apply_does(source: Path) -> None:  # noqa: F811
    with _databricks() as wh:
        check_fidelity(source, wh)
