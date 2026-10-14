"""Live Snowflake proof for run-time delete guards (#1218 part 2).

Snowflake autocommits, so only an explicit BEGIN lets a refused DELETE roll
back. Mocks cannot prove that: these tests assert the rows are *still there*
after a refusal, and gone after a pass or an explicit force. Runs only with the
existing ``DRT_SMOKE_SNOWFLAKE_*`` credentials.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest

from drt.config.models import SnowflakeDestinationConfig, SyncOptions
from drt.destinations.snowflake import SnowflakeDestination
from drt.engine.guards import GuardTripped

from .conftest import require_env, unique_table

pytestmark = pytest.mark.dwh_smoke

snowflake_connector = pytest.importorskip("snowflake.connector")

ACCOUNT_ENV = "DRT_SMOKE_SNOWFLAKE_ACCOUNT"
USER_ENV = "DRT_SMOKE_SNOWFLAKE_USER"
PASSWORD_ENV = "DRT_SMOKE_SNOWFLAKE_PASSWORD"
KEY_ENV = "DRT_SMOKE_SNOWFLAKE_PRIVATE_KEY"


def _require_creds() -> dict[str, str]:
    if not os.environ.get(KEY_ENV) and not os.environ.get(PASSWORD_ENV):
        pytest.skip(
            "Snowflake smoke auth not set: need DRT_SMOKE_SNOWFLAKE_PRIVATE_KEY "
            "(preferred) or DRT_SMOKE_SNOWFLAKE_PASSWORD."
        )
    return require_env(
        ACCOUNT_ENV,
        USER_ENV,
        "DRT_SMOKE_SNOWFLAKE_DATABASE",
        "DRT_SMOKE_SNOWFLAKE_SCHEMA",
        "DRT_SMOKE_SNOWFLAKE_WAREHOUSE",
    )


def _connect(creds: dict[str, str]) -> Any:
    auth: dict[str, object] = {}
    pem = os.environ.get(KEY_ENV)
    if pem:
        from drt.config.credentials import load_snowflake_private_key

        auth["private_key"] = load_snowflake_private_key(pem)
    else:
        auth["password"] = os.environ[PASSWORD_ENV]
    return snowflake_connector.connect(
        account=creds[ACCOUNT_ENV],
        user=creds[USER_ENV],
        warehouse=creds["DRT_SMOKE_SNOWFLAKE_WAREHOUSE"],
        database=creds["DRT_SMOKE_SNOWFLAKE_DATABASE"],
        schema=creds["DRT_SMOKE_SNOWFLAKE_SCHEMA"],
        **auth,
    )


class _Env:
    def __init__(self, creds: dict[str, str], conn: Any) -> None:
        self.creds = creds
        self.conn = conn
        self.prefix = (
            f"{creds['DRT_SMOKE_SNOWFLAKE_DATABASE']}.{creds['DRT_SMOKE_SNOWFLAKE_SCHEMA']}"
        )

    def config(self, table: str) -> SnowflakeDestinationConfig:
        auth = (
            {"private_key_env": KEY_ENV}
            if os.environ.get(KEY_ENV)
            else {"password_env": PASSWORD_ENV}
        )
        return SnowflakeDestinationConfig(
            **{
                "type": "snowflake",
                "account_env": ACCOUNT_ENV,
                "user_env": USER_ENV,
                **auth,
                "database": self.creds["DRT_SMOKE_SNOWFLAKE_DATABASE"],
                "schema": self.creds["DRT_SMOKE_SNOWFLAKE_SCHEMA"],
                "table": table,
                "warehouse": self.creds["DRT_SMOKE_SNOWFLAKE_WAREHOUSE"],
                "mode": "merge",
                "upsert_key": ["id"],
            }
        )

    def fq(self, table: str) -> str:
        return f"{self.prefix}.{table}"

    def run(self, sql: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(sql)

    def ids(self, table: str) -> list[int]:
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT id FROM {self.fq(table)} ORDER BY id")
            return [r[0] for r in cur.fetchall()]

    def seed(self, table: str, ids: range) -> None:
        self.run(f"CREATE OR REPLACE TABLE {self.fq(table)} (id INTEGER, v VARCHAR)")
        values = ", ".join(f"({i}, 'v{i}')" for i in ids)
        self.run(f"INSERT INTO {self.fq(table)} VALUES {values}")


@contextmanager
def _env() -> Iterator[_Env]:
    creds = _require_creds()
    conn = _connect(creds)
    try:
        yield _Env(creds, conn)
    finally:
        conn.close()


def _drop(env: _Env, *tables: str) -> None:
    for table in tables:
        env.run(f"DROP TABLE IF EXISTS {env.fq(table)}")


def test_destination_mirror_refusal_leaves_rows_then_pass_and_force_delete() -> None:
    with _env() as env:
        table = unique_table("DRT_GUARD_DEST")
        try:
            env.seed(table, range(1, 6))
            config = env.config(table)

            destination = SnowflakeDestination()
            destination._mirror_keys = [(1,), (2,)]
            with pytest.raises(GuardTripped) as raised:
                destination.finalize_sync(
                    config,
                    SyncOptions(mode="mirror", guards={"max_deletes": 2}),  # type: ignore[arg-type]
                )
            assert raised.value.trips[0].observed == 3
            assert env.ids(table) == [1, 2, 3, 4, 5], "refusal must roll the DELETE back"

            # A percentage limit: 3 of 5 = 60% allowed at 60, refused at 59.
            destination._mirror_keys = [(1,), (2,)]
            with pytest.raises(GuardTripped):
                destination.finalize_sync(
                    config,
                    SyncOptions(mode="mirror", guards={"max_delete_pct": 59}),  # type: ignore[arg-type]
                )
            assert env.ids(table) == [1, 2, 3, 4, 5]

            destination._mirror_keys = [(1,), (2,)]
            destination.finalize_sync(
                config,
                SyncOptions(mode="mirror", guards={"max_delete_pct": 60}),  # type: ignore[arg-type]
            )
            assert env.ids(table) == [1, 2], "a pass must COMMIT the DELETE"

            env.seed(table, range(1, 6))
            destination._mirror_keys = [(1,), (2,)]
            forced = SyncOptions(mode="mirror", guards={"max_deletes": 0})  # type: ignore[arg-type]
            forced._force_guards = True
            destination.finalize_sync(config, forced)
            assert env.ids(table) == [1, 2]
            assert [t.guard for t in forced._forced_guard_trips] == ["max_deletes"]
        finally:
            _drop(env, table)


def test_replace_swap_refusal_keeps_live_table_and_force_swaps() -> None:
    with _env() as env:
        table = unique_table("DRT_GUARD_SWAP")
        shadow = f"{table}__drt_swap"
        try:
            env.seed(table, range(1, 6))
            env.seed(shadow, range(10, 11))
            config = env.config(table)

            destination = SnowflakeDestination()
            destination._swap_shadow_created = True
            destination._swap_table = env.fq(table)
            options = SyncOptions(
                mode="replace",
                replace_strategy="swap",
                guards={"max_deletes": 2},  # type: ignore[arg-type]
            )
            with pytest.raises(GuardTripped):
                destination.finalize_sync(config, options)
            assert env.ids(table) == [1, 2, 3, 4, 5], "a refused swap must not swap"
            with env.conn.cursor() as cur:
                cur.execute(f"SHOW TABLES LIKE '{shadow}' IN SCHEMA {env.prefix}")
                assert cur.fetchall() == [], "a refused swap drops its shadow"

            env.seed(shadow, range(10, 11))
            destination._swap_shadow_created = True
            destination._swap_table = env.fq(table)
            options._force_guards = True
            destination.finalize_sync(config, options)
            assert env.ids(table) == [10]
        finally:
            _drop(env, table, shadow)


def test_tracked_mirror_refusal_keeps_rows_and_state_then_force_deletes() -> None:
    with _env() as env:
        table = unique_table("DRT_GUARD_TRK")
        sync_name = f"guard_smoke_{table.lower()}"
        try:
            env.seed(table, range(1, 6))
            config = env.config(table)
            destination = SnowflakeDestination()

            def options(**guards: Any) -> SyncOptions:
                opts = SyncOptions(
                    mode="mirror",
                    mirror={"strategy": "tracked"},  # type: ignore[arg-type]
                    guards=guards or None,  # type: ignore[arg-type]
                )
                opts._sync_name = sync_name
                return opts

            # Run 1 records all five keys as tracked (baseline; nothing to delete).
            destination._mirror_keys = [(i,) for i in range(1, 6)]
            destination.finalize_sync(config, options())
            assert env.ids(table) == [1, 2, 3, 4, 5]

            # Run 2 observes only {1, 2}: three deletes, refused at max_deletes=2.
            destination._mirror_keys = [(1,), (2,)]
            with pytest.raises(GuardTripped):
                destination.finalize_sync(config, options(max_deletes=2))
            assert env.ids(table) == [1, 2, 3, 4, 5], "refusal must keep the rows"

            # State was not rewritten by the refused run: the same three are still stale.
            destination._mirror_keys = [(1,), (2,)]
            forced = options(max_deletes=2)
            forced._force_guards = True
            destination.finalize_sync(config, forced)
            assert env.ids(table) == [1, 2]
            assert [t.guard for t in forced._forced_guard_trips] == ["max_deletes"]
        finally:
            _drop(env, table)
            env.run(f"DELETE FROM {env.prefix}._drt_synced_keys WHERE sync_name = '{sync_name}'")
