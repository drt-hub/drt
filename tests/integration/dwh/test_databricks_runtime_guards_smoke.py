"""Live Databricks proof for run-time delete guards (#1218 part 2).

Delta has no multi-statement rollback, so the guard counts first and re-checks
the DELETE/MERGE's own result row. A mock cannot show that the result row is
readable (a pass would otherwise raise ``GuardOutcomeCommitted``) or that the
count predicate matches what is actually deleted. Runs only with the existing
``DRT_SMOKE_DATABRICKS_*`` credentials.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest

from drt.config.models import DatabricksDestinationConfig, SyncOptions
from drt.destinations.databricks import DatabricksDestination
from drt.engine.guards import GuardTripped

from .conftest import require_env, unique_table

pytestmark = pytest.mark.dwh_smoke

dbsql = pytest.importorskip("databricks.sql")

HOST_ENV = "DRT_SMOKE_DATABRICKS_HOST"
HTTP_PATH_ENV = "DRT_SMOKE_DATABRICKS_HTTP_PATH"
TOKEN_ENV = "DRT_SMOKE_DATABRICKS_TOKEN"


class _Env:
    def __init__(self, creds: dict[str, str], conn: Any) -> None:
        self.catalog = creds["DRT_SMOKE_DATABRICKS_CATALOG"]
        self.schema = creds["DRT_SMOKE_DATABRICKS_SCHEMA"]
        self.conn = conn

    def config(self, table: str, **extra: Any) -> DatabricksDestinationConfig:
        return DatabricksDestinationConfig(
            **{
                "type": "databricks",
                "host_env": HOST_ENV,
                "http_path_env": HTTP_PATH_ENV,
                "token_env": TOKEN_ENV,
                "catalog": self.catalog,
                "schema": self.schema,
                "table": table,
                "mode": "merge",
                "upsert_key": ["id"],
                **extra,
            }
        )

    def fq(self, table: str) -> str:
        return f"{self.catalog}.{self.schema}.{table}"

    def run(self, sql: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(sql)

    def rows(self, table: str, columns: str = "id") -> list[tuple[Any, ...]]:
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT {columns} FROM {self.fq(table)} ORDER BY {columns}")
            return [tuple(r) for r in cur.fetchall()]

    def seed(self, table: str, ids: range) -> None:
        self.run(f"CREATE OR REPLACE TABLE {self.fq(table)} (id INT, v STRING)")
        values = ", ".join(f"({i}, 'v{i}')" for i in ids)
        self.run(f"INSERT INTO {self.fq(table)} VALUES {values}")

    def drop(self, *tables: str) -> None:
        for table in tables:
            self.run(f"DROP TABLE IF EXISTS {self.fq(table)}")


@contextmanager
def _env() -> Iterator[_Env]:
    creds = require_env(
        HOST_ENV,
        HTTP_PATH_ENV,
        TOKEN_ENV,
        "DRT_SMOKE_DATABRICKS_CATALOG",
        "DRT_SMOKE_DATABRICKS_SCHEMA",
    )
    conn = dbsql.connect(
        server_hostname=creds[HOST_ENV],
        http_path=creds[HTTP_PATH_ENV],
        access_token=creds[TOKEN_ENV],
    )
    try:
        yield _Env(creds, conn)
    finally:
        conn.close()


def test_destination_mirror_counts_first_then_deletes_and_verifies_the_result_row() -> None:
    with _env() as env:
        table = unique_table("drt_guard_dest")
        try:
            env.seed(table, range(1, 6))
            config = env.config(table)
            destination = DatabricksDestination()

            destination._mirror_keys = [(1,), (2,)]
            with pytest.raises(GuardTripped) as raised:
                destination.finalize_sync(
                    config,
                    SyncOptions(mode="mirror", guards={"max_deletes": 2}),  # type: ignore[arg-type]
                )
            assert raised.value.trips[0].observed == 3
            assert env.rows(table) == [(i,) for i in range(1, 6)], "a refusal must delete nothing"

            destination._mirror_keys = [(1,), (2,)]
            with pytest.raises(GuardTripped):
                destination.finalize_sync(
                    config,
                    SyncOptions(mode="mirror", guards={"max_delete_pct": 59}),  # type: ignore[arg-type]
                )
            assert env.rows(table) == [(i,) for i in range(1, 6)]

            # A pass proves the DELETE result row is readable: otherwise
            # GuardOutcomeCommitted would be raised here.
            destination._mirror_keys = [(1,), (2,)]
            destination.finalize_sync(
                config,
                SyncOptions(mode="mirror", guards={"max_delete_pct": 60}),  # type: ignore[arg-type]
            )
            assert env.rows(table) == [(1,), (2,)]

            env.seed(table, range(1, 6))
            destination._mirror_keys = [(1,), (2,)]
            forced = SyncOptions(mode="mirror", guards={"max_deletes": 0})  # type: ignore[arg-type]
            forced._force_guards = True
            destination.finalize_sync(config, forced)
            assert env.rows(table) == [(1,), (2,)]
            assert [(t.guard, t.observed) for t in forced._forced_guard_trips] == [
                ("max_deletes", 3)
            ]
        finally:
            env.drop(table)


def test_composite_key_scoped_mirror_uses_merge_and_counts_the_same_rows() -> None:
    with _env() as env:
        table = unique_table("drt_guard_comp")
        try:
            env.run(f"CREATE OR REPLACE TABLE {env.fq(table)} (tenant STRING, id INT, v STRING)")
            env.run(
                f"INSERT INTO {env.fq(table)} VALUES "
                "('a', 1, 'x'), ('a', 2, 'x'), ('a', 3, 'x'), ('b', 1, 'x'), ('b', 2, 'x')"
            )
            config = env.config(table, upsert_key=["tenant", "id"])
            destination = DatabricksDestination()

            def options(**limits: Any) -> SyncOptions:
                return SyncOptions(
                    mode="mirror",
                    mirror={"scope": ["tenant"]},  # type: ignore[arg-type]
                    guards=limits,  # type: ignore[arg-type]
                )

            # Scope 'a' observed with only key 1: rows (a,2), (a,3) are stale; 'b' untouched.
            destination._mirror_keys = [("a", 1)]
            destination._mirror_scopes = {("a",)}
            with pytest.raises(GuardTripped) as raised:
                destination.finalize_sync(config, options(max_deletes=1))
            assert raised.value.trips[0].observed == 2
            assert len(env.rows(table, "tenant, id")) == 5

            destination._mirror_keys = [("a", 1)]
            destination._mirror_scopes = {("a",)}
            destination.finalize_sync(config, options(max_deletes=2, max_delete_pct=100))
            assert env.rows(table, "tenant, id") == [("a", 1), ("b", 1), ("b", 2)]
        finally:
            env.drop(table)


def test_replace_swap_refusal_keeps_live_table_and_force_overwrites() -> None:
    with _env() as env:
        table = unique_table("drt_guard_swap")
        shadow = f"{table}__drt_swap"
        try:
            env.seed(table, range(1, 6))
            env.seed(shadow, range(10, 11))
            config = env.config(table)
            destination = DatabricksDestination()
            destination._swap_shadow_created = True
            destination._swap_table = env.fq(table)
            options = SyncOptions(
                mode="replace",
                replace_strategy="swap",
                guards={"max_deletes": 2},  # type: ignore[arg-type]
            )
            with pytest.raises(GuardTripped):
                destination.finalize_sync(config, options)
            assert env.rows(table) == [(i,) for i in range(1, 6)]
            with env.conn.cursor() as cur:
                cur.execute(f"SHOW TABLES IN {env.catalog}.{env.schema} LIKE '{shadow}'")
                assert cur.fetchall() == [], "a refused swap drops its shadow"

            env.seed(shadow, range(10, 11))
            destination._swap_shadow_created = True
            destination._swap_table = env.fq(table)
            options._force_guards = True
            destination.finalize_sync(config, options)
            assert env.rows(table) == [(10,)]
        finally:
            env.drop(table, shadow)


def test_tracked_mirror_refusal_keeps_rows_and_state_then_force_deletes() -> None:
    with _env() as env:
        table = unique_table("drt_guard_trk")
        sync_name = f"guard_smoke_{table}"
        try:
            env.seed(table, range(1, 6))
            config = env.config(table)
            destination = DatabricksDestination()

            def options(**limits: Any) -> SyncOptions:
                opts = SyncOptions(
                    mode="mirror",
                    mirror={"strategy": "tracked"},  # type: ignore[arg-type]
                    guards=limits or None,  # type: ignore[arg-type]
                )
                opts._sync_name = sync_name
                return opts

            destination._mirror_keys = [(i,) for i in range(1, 6)]
            destination.finalize_sync(config, options())  # baseline: records 5 tracked keys
            assert env.rows(table) == [(i,) for i in range(1, 6)]

            destination._mirror_keys = [(1,), (2,)]
            with pytest.raises(GuardTripped):
                destination.finalize_sync(config, options(max_deletes=2))
            assert env.rows(table) == [(i,) for i in range(1, 6)]

            destination._mirror_keys = [(1,), (2,)]
            forced = options(max_deletes=2, max_delete_pct=100)
            forced._force_guards = True
            destination.finalize_sync(config, forced)
            assert env.rows(table) == [(1,), (2,)]
            assert [t.guard for t in forced._forced_guard_trips] == ["max_deletes"]
        finally:
            env.drop(table)
            env.run(
                f"DELETE FROM {env.catalog}.{env.schema}._drt_synced_keys "
                f"WHERE sync_name = '{sync_name}'"
            )
