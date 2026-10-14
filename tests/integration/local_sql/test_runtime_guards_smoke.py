"""Live PostgreSQL/MySQL proof for run-time change guards (#1218 part 2).

Unit tests pin SQL shape. These tests prove the actual databases leave target
data intact on refusal and perform the destructive finalize only when within
the configured limit or explicitly forced. Skips without Docker.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from drt.config.credentials import MySQLProfile, PostgresProfile
from drt.config.models import (
    MySQLDestinationConfig,
    PostgresDestinationConfig,
    SyncConfig,
    SyncOptions,
)
from drt.destinations.mysql import MySQLDestination
from drt.destinations.postgres import PostgresDestination
from drt.engine.guards import GuardTripped
from drt.engine.observer import StatePersistingObserver
from drt.engine.sync import run_sync
from drt.state.history import HistoryManager
from drt.state.manager import StateManager, SyncState
from drt.state.watermark import LocalWatermarkStorage

from .conftest import require_docker

pytestmark = pytest.mark.local_sql_smoke

psycopg2 = pytest.importorskip("psycopg2")
pymysql = pytest.importorskip("pymysql")
testcontainers_postgres = pytest.importorskip("testcontainers.postgres")
testcontainers_mysql = pytest.importorskip("testcontainers.mysql")


@pytest.fixture(scope="module")
def pg() -> Iterator[PostgresProfile]:
    require_docker()
    with testcontainers_postgres.PostgresContainer(
        "postgres:16-alpine",
        username="admin",
        password="adminpass",
        dbname="testdb",
        driver=None,
    ) as postgres:
        yield PostgresProfile(
            type="postgres",
            host=postgres.get_container_host_ip(),
            port=int(postgres.get_exposed_port(5432)),
            dbname="testdb",
            user="admin",
            password="adminpass",
        )


def _pg_connect(pg: PostgresProfile) -> Any:
    return psycopg2.connect(
        host=pg.host,
        port=pg.port,
        dbname=pg.dbname,
        user=pg.user,
        password=pg.password,
    )


def _pg_config(pg: PostgresProfile, table: str) -> PostgresDestinationConfig:
    return PostgresDestinationConfig(
        type="postgres",
        host=pg.host,
        port=pg.port,
        dbname=pg.dbname,
        user=pg.user,
        password=pg.password,
        table=table,
        upsert_key=["id"],
    )


def _reset_pg(pg: PostgresProfile, table: str) -> None:
    with _pg_connect(pg) as conn, conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        cur.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT)")
        cur.execute(f"INSERT INTO {table} VALUES (1, 'one'), (2, 'two'), (3, 'three')")


def _pg_rows(pg: PostgresProfile, table: str) -> list[tuple[int, str]]:
    with _pg_connect(pg) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT id, name FROM {table} ORDER BY id")
        return list(cur.fetchall())


class _Rows:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def extract(self, *_args: Any, **_kwargs: Any) -> Iterator[dict[str, Any]]:
        yield from self.rows


def _sync(pg: PostgresProfile, table: str, guards: dict[str, Any]) -> SyncConfig:
    return SyncConfig.model_validate(
        {
            "name": f"runtime_{table}",
            "model": "SELECT 1",
            "destination": _pg_config(pg, table).model_dump(),
            "sync": {"mode": "mirror", "guards": guards},
        }
    )


def test_postgres_destination_guard_trip_force_and_within_limit_are_atomic(
    pg: PostgresProfile, tmp_path: Path
) -> None:
    table = "runtime_guard_destination"
    _reset_pg(pg, table)
    sync = _sync(pg, table, {"max_deletes": 1})
    state = StateManager(tmp_path)
    state.save_sync(
        SyncState(sync.name, "2026-01-01T00:00:00+00:00", 3, "success", last_cursor_value="old")
    )
    history = HistoryManager(tmp_path)
    watermarks = LocalWatermarkStorage(tmp_path)
    watermarks.save(sync.name, "old-watermark")

    with pytest.raises(GuardTripped):
        run_sync(
            sync,
            _Rows([{"id": 1, "name": "one"}]),
            PostgresDestination(),
            pg,
            tmp_path,
            state_manager=state,
            watermark_storage=watermarks,
            history_manager=history,
            observer=StatePersistingObserver(state, watermarks),
        )
    assert _pg_rows(pg, table) == [(1, "one"), (2, "two"), (3, "three")]
    assert state.get_last_sync(sync.name).last_cursor_value == "old"  # type: ignore[union-attr]
    assert watermarks.get(sync.name) == "old-watermark"
    assert history.read(sync.name)[0].guards["forced"] is False  # type: ignore[index,union-attr]

    result = run_sync(
        sync,
        _Rows([{"id": 1, "name": "one"}]),
        PostgresDestination(),
        pg,
        tmp_path,
        history_manager=history,
        force_guards=True,
    )
    assert _pg_rows(pg, table) == [(1, "one")]
    assert result.guards is not None and result.guards["forced"] is True
    assert history.read(sync.name)[0].guards == result.guards

    _reset_pg(pg, table)
    within = _sync(pg, table, {"max_deletes": 2, "max_delete_pct": 67})
    run_sync(
        within,
        _Rows([{"id": 1, "name": "one"}]),
        PostgresDestination(),
        pg,
        tmp_path,
    )
    assert _pg_rows(pg, table) == [(1, "one")]


def test_postgres_scoped_composite_destination_trip_is_atomic(
    pg: PostgresProfile, tmp_path: Path
) -> None:
    table = "runtime_guard_scoped_composite"
    with _pg_connect(pg) as conn, conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        cur.execute(
            f"CREATE TABLE {table} (tenant TEXT, id INTEGER, name TEXT, PRIMARY KEY (tenant, id))"
        )
        cur.execute(f"INSERT INTO {table} VALUES ('a', 1, 'a1'), ('a', 2, 'a2'), ('b', 1, 'b1')")

    config = _pg_config(pg, table).model_copy(update={"upsert_key": ["tenant", "id"]})
    sync = SyncConfig.model_validate(
        {
            "name": "runtime_scoped_composite",
            "model": "SELECT 1",
            "destination": config.model_dump(),
            "sync": {
                "mode": "mirror",
                "mirror": {"scope": ["tenant"]},
                "guards": {"max_deletes": 0},
            },
        }
    )
    state = StateManager(tmp_path)
    state.save_sync(SyncState(sync.name, "old", 3, "success", last_cursor_value="old"))
    watermarks = LocalWatermarkStorage(tmp_path)
    watermarks.save(sync.name, "old-watermark")

    with pytest.raises(GuardTripped):
        run_sync(
            sync,
            _Rows([{"tenant": "a", "id": 1, "name": "a1"}]),
            PostgresDestination(),
            pg,
            tmp_path,
            state_manager=state,
            watermark_storage=watermarks,
            observer=StatePersistingObserver(state, watermarks),
        )

    with _pg_connect(pg) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT tenant, id, name FROM {table} ORDER BY tenant, id")
        assert cur.fetchall() == [("a", 1, "a1"), ("a", 2, "a2"), ("b", 1, "b1")]
    assert state.get_last_sync(sync.name).last_cursor_value == "old"  # type: ignore[union-attr]
    assert watermarks.get(sync.name) == "old-watermark"


def _load_and_finalize(
    destination: PostgresDestination,
    config: PostgresDestinationConfig,
    options: SyncOptions,
    ids: list[int],
) -> None:
    if ids:
        destination.load([{"id": value, "name": str(value)} for value in ids], config, options)
    destination.finalize_sync(config, options)


def test_postgres_tracked_diff_and_replace_swap_guards(pg: PostgresProfile) -> None:
    tracked_table = "runtime_guard_tracked"
    _reset_pg(pg, tracked_table)
    config = _pg_config(pg, tracked_table)
    baseline = SyncOptions(mode="mirror", mirror={"strategy": "tracked"})
    baseline._sync_name = "tracked-smoke"
    _load_and_finalize(PostgresDestination(), config, baseline, [1, 2, 3])

    guarded = SyncOptions(
        mode="mirror",
        mirror={"strategy": "tracked"},
        guards={"max_deletes": 1},
    )
    guarded._sync_name = "tracked-smoke"
    with pytest.raises(GuardTripped):
        _load_and_finalize(PostgresDestination(), config, guarded, [1])
    assert len(_pg_rows(pg, tracked_table)) == 3

    guarded._force_guards = True
    _load_and_finalize(PostgresDestination(), config, guarded, [1])
    assert [row[0] for row in _pg_rows(pg, tracked_table)] == [1]

    diff_table = "runtime_guard_diff"
    _reset_pg(pg, diff_table)
    diff_options = SyncOptions(
        mode="mirror",
        incremental_strategy="diff",
        diff={},
        mirror={"strategy": "diff"},
        guards={"max_deletes": 1},
    )
    diff_options._diff_removed_keys = [{"id": 2}, {"id": 3}]
    with pytest.raises(GuardTripped):
        PostgresDestination().finalize_sync(_pg_config(pg, diff_table), diff_options)
    assert len(_pg_rows(pg, diff_table)) == 3
    diff_options._force_guards = True
    PostgresDestination().finalize_sync(_pg_config(pg, diff_table), diff_options)
    assert [row[0] for row in _pg_rows(pg, diff_table)] == [1]

    swap_table = "runtime_guard_swap"
    _reset_pg(pg, swap_table)
    swap_config = _pg_config(pg, swap_table)
    disjoint_options = SyncOptions(
        mode="replace",
        replace_strategy="swap",
        guards={"max_deletes": 0},
    )
    disjoint = PostgresDestination()
    disjoint.load(
        [{"id": value, "name": f"new-{value}"} for value in (10, 11, 12)],
        swap_config,
        disjoint_options,
    )
    with pytest.raises(GuardTripped):
        disjoint.finalize_sync(swap_config, disjoint_options)
    assert _pg_rows(pg, swap_table) == [(1, "one"), (2, "two"), (3, "three")]

    swap_options = SyncOptions(
        mode="replace",
        replace_strategy="swap",
        guards={"max_deletes": 1},
    )
    swap = PostgresDestination()
    swap.load([{"id": 1, "name": "one"}], swap_config, swap_options)
    with pytest.raises(GuardTripped):
        swap.finalize_sync(swap_config, swap_options)
    assert len(_pg_rows(pg, swap_table)) == 3
    with _pg_connect(pg) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('runtime_guard_swap__drt_swap')")
        assert cur.fetchone()[0] is None

    forced_swap = PostgresDestination()
    swap_options._force_guards = True
    forced_swap.load([{"id": 1, "name": "one"}], swap_config, swap_options)
    forced_swap.finalize_sync(swap_config, swap_options)
    assert _pg_rows(pg, swap_table) == [(1, "one")]


def test_mysql_destination_guard_refuses_then_forces_through_engine(tmp_path: Path) -> None:
    require_docker()
    with testcontainers_mysql.MySqlContainer("mysql:8.4") as mysql:
        config = MySQLDestinationConfig(
            type="mysql",
            host=mysql.get_container_host_ip(),
            port=int(mysql.get_exposed_port(3306)),
            dbname=mysql.dbname,
            user=mysql.username,
            password=mysql.password,
            table="runtime_guard",
            upsert_key=["id"],
        )

        profile = MySQLProfile(
            type="mysql",
            host=config.host,
            port=config.port,
            dbname=config.dbname,
            user=config.user,
            password=config.password,
        )

        # ``_connect`` is a classmethod. Accept the bound/config arguments just
        # like the other local_sql MySQL test doubles instead of accidentally
        # requiring a zero-argument call.
        def connect(*_args: Any, **_kwargs: Any) -> Any:
            return pymysql.connect(
                host=config.host,
                port=config.port,
                user=config.user,
                password=config.password,
                database=config.dbname,
            )

        with connect() as conn, conn.cursor() as cur:
            cur.execute("CREATE TABLE runtime_guard (id INTEGER PRIMARY KEY, name TEXT)")
            cur.execute("INSERT INTO runtime_guard VALUES (1, 'one'), (2, 'two'), (3, 'three')")
            conn.commit()

        sync = SyncConfig.model_validate(
            {
                "name": "runtime_mysql_engine",
                "model": "SELECT 1",
                "destination": config.model_dump(),
                "sync": {"mode": "mirror", "guards": {"max_deletes": 1}},
            }
        )
        state = StateManager(tmp_path)
        state.save_sync(SyncState(sync.name, "old", 3, "success", last_cursor_value="old"))
        watermarks = LocalWatermarkStorage(tmp_path)
        watermarks.save(sync.name, "old-watermark")
        history = HistoryManager(tmp_path)

        with patch.object(MySQLDestination, "_connect", side_effect=connect):
            with pytest.raises(GuardTripped):
                run_sync(
                    sync,
                    _Rows([{"id": 1, "name": "one"}]),
                    MySQLDestination(),
                    profile,
                    tmp_path,
                    state_manager=state,
                    watermark_storage=watermarks,
                    history_manager=history,
                    observer=StatePersistingObserver(state, watermarks),
                )
        with connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM runtime_guard")
            assert cur.fetchone()[0] == 3
        assert state.get_last_sync(sync.name).last_cursor_value == "old"  # type: ignore[union-attr]
        assert watermarks.get(sync.name) == "old-watermark"
        assert history.read(sync.name)[0].guards["forced"] is False  # type: ignore[index,union-attr]

        with patch.object(MySQLDestination, "_connect", side_effect=connect):
            result = run_sync(
                sync,
                _Rows([{"id": 1, "name": "one"}]),
                MySQLDestination(),
                profile,
                tmp_path,
                history_manager=history,
                force_guards=True,
            )
        with connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT id FROM runtime_guard ORDER BY id")
            assert cur.fetchall() == ((1,),)
        assert result.guards is not None and result.guards["forced"] is True
