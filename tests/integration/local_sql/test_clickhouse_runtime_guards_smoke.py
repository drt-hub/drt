"""Live ClickHouse proof for run-time delete guards (#1218 part 2).

The count() queries reuse the mutation's WHERE text, and the swap comparison is
a NULL-safe ``NOT IN (SELECT ...)``. Mocks cannot prove ClickHouse accepts that
SQL or that the rows survive a refusal. Skips without Docker or the driver.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

import pytest

from drt.config.models import ClickHouseDestinationConfig, SyncOptions
from drt.destinations.clickhouse import ClickHouseDestination
from drt.engine.guards import GuardTripped

from .conftest import require_docker

pytestmark = pytest.mark.local_sql_smoke

clickhouse_connect = pytest.importorskip("clickhouse_connect")
containers = pytest.importorskip("testcontainers.core.container")
waiting = pytest.importorskip("testcontainers.core.waiting_utils")


@pytest.fixture(scope="module")
def ch() -> Iterator[Any]:
    require_docker()
    container = (
        containers.DockerContainer("clickhouse/clickhouse-server:24.8-alpine")
        .with_exposed_ports(8123)
        .with_env("CLICKHOUSE_USER", "default")
        .with_env("CLICKHOUSE_PASSWORD", "chpass")
    )
    with container:
        port = int(container.get_exposed_port(8123))
        host = container.get_container_host_ip()
        deadline = time.monotonic() + 90
        client = None
        while time.monotonic() < deadline:
            try:
                client = clickhouse_connect.get_client(
                    host=host, port=port, username="default", password="chpass"
                )
                client.command("SELECT 1")
                break
            except Exception:  # noqa: BLE001 — still starting
                time.sleep(1)
        else:
            pytest.skip("ClickHouse container did not become ready")
        yield host, port, client


def _config(ch: Any, table: str, **extra: Any) -> ClickHouseDestinationConfig:
    host, port, _client = ch
    return ClickHouseDestinationConfig(
        type="clickhouse",
        host=host,
        port=port,
        user="default",
        password="chpass",
        database="default",
        table=table,
        upsert_key=["id"],
        **extra,
    )


def _ids(ch: Any, table: str) -> list[int]:
    return [r[0] for r in ch[2].query(f"SELECT id FROM {table} ORDER BY id").result_rows]


def _seed(ch: Any, table: str, ids: range) -> None:
    client = ch[2]
    client.command(f"DROP TABLE IF EXISTS {table}")
    client.command(f"CREATE TABLE {table} (id Int32, v String) ENGINE = MergeTree ORDER BY id")
    client.command(f"INSERT INTO {table} VALUES " + ", ".join(f"({i}, 'v{i}')" for i in ids))


def _mirror(**limits: Any) -> SyncOptions:
    return SyncOptions(mode="mirror", guards=limits)  # type: ignore[arg-type]


def test_destination_mirror_refusal_keeps_rows_then_pass_and_force_delete(ch: Any) -> None:
    table = "drt_guard_dest"
    _seed(ch, table, range(1, 6))
    config = _config(ch, table)
    destination = ClickHouseDestination()

    destination._mirror_keys = [(1,), (2,)]
    with pytest.raises(GuardTripped) as raised:
        destination.finalize_sync(config, _mirror(max_deletes=2))
    assert raised.value.trips[0].observed == 3
    assert _ids(ch, table) == [1, 2, 3, 4, 5], "a refusal must delete nothing"

    destination._mirror_keys = [(1,), (2,)]
    with pytest.raises(GuardTripped):
        destination.finalize_sync(config, _mirror(max_delete_pct=59))
    assert _ids(ch, table) == [1, 2, 3, 4, 5]

    destination._mirror_keys = [(1,), (2,)]
    destination.finalize_sync(config, _mirror(max_delete_pct=60))
    assert _ids(ch, table) == [1, 2]

    _seed(ch, table, range(1, 6))
    destination._mirror_keys = [(1,), (2,)]
    forced = _mirror(max_deletes=0)
    forced._force_guards = True
    destination.finalize_sync(config, forced)
    assert _ids(ch, table) == [1, 2]
    assert [t.guard for t in forced._forced_guard_trips] == ["max_deletes"]


def test_scoped_composite_count_matches_the_mutation(ch: Any) -> None:
    table = "drt_guard_comp"
    client = ch[2]
    client.command(f"DROP TABLE IF EXISTS {table}")
    client.command(
        f"CREATE TABLE {table} (tenant String, id Int32) ENGINE = MergeTree ORDER BY (tenant, id)"
    )
    client.command(f"INSERT INTO {table} VALUES ('a', 1), ('a', 2), ('a', 3), ('b', 1), ('b', 2)")
    config = _config(ch, table).model_copy(update={"upsert_key": ["tenant", "id"]})
    destination = ClickHouseDestination()

    def options(**limits: Any) -> SyncOptions:
        return SyncOptions(
            mode="mirror",
            mirror={"scope": ["tenant"]},  # type: ignore[arg-type]
            guards=limits,  # type: ignore[arg-type]
        )

    destination._mirror_keys = [("a", 1)]
    destination._mirror_scopes = {("a",)}
    with pytest.raises(GuardTripped) as raised:
        destination.finalize_sync(config, options(max_deletes=1))
    assert raised.value.trips[0].observed == 2

    destination._mirror_keys = [("a", 1)]
    destination._mirror_scopes = {("a",)}
    destination.finalize_sync(config, options(max_deletes=2, max_delete_pct=100))
    rows = client.query(f"SELECT tenant, id FROM {table} ORDER BY tenant, id").result_rows
    assert rows == [("a", 1), ("b", 1), ("b", 2)]


def test_replace_swap_refusal_keeps_live_table_and_force_exchanges(ch: Any) -> None:
    table = "drt_guard_swap"
    shadow = f"{table}__drt_swap"
    _seed(ch, table, range(1, 6))
    _seed(ch, shadow, range(10, 11))
    config = _config(ch, table)
    destination = ClickHouseDestination()
    destination._swap_shadow_created = True
    destination._swap_table = table
    options = SyncOptions(
        mode="replace",
        replace_strategy="swap",
        guards={"max_deletes": 2},  # type: ignore[arg-type]
    )
    with pytest.raises(GuardTripped):
        destination.finalize_sync(config, options)
    assert _ids(ch, table) == [1, 2, 3, 4, 5]
    assert ch[2].command(f"EXISTS TABLE {shadow}") == 0, "a refused swap drops its shadow"

    _seed(ch, shadow, range(10, 11))
    destination._swap_shadow_created = True
    destination._swap_table = table
    options._force_guards = True
    destination.finalize_sync(config, options)
    assert _ids(ch, table) == [10]


def test_tracked_mirror_refusal_keeps_rows_and_state_then_force_deletes(ch: Any) -> None:
    table = "drt_guard_trk"
    _seed(ch, table, range(1, 6))
    config = _config(ch, table)
    destination = ClickHouseDestination()

    def options(**limits: Any) -> SyncOptions:
        opts = SyncOptions(
            mode="mirror",
            mirror={"strategy": "tracked"},  # type: ignore[arg-type]
            guards=limits or None,  # type: ignore[arg-type]
        )
        opts._sync_name = "guard_smoke_trk"
        return opts

    destination._mirror_keys = [(i,) for i in range(1, 6)]
    destination.finalize_sync(config, options())  # baseline
    assert _ids(ch, table) == [1, 2, 3, 4, 5]

    destination._mirror_keys = [(1,), (2,)]
    with pytest.raises(GuardTripped):
        destination.finalize_sync(config, options(max_deletes=2))
    assert _ids(ch, table) == [1, 2, 3, 4, 5]

    destination._mirror_keys = [(1,), (2,)]
    forced = options(max_deletes=2, max_delete_pct=100)
    forced._force_guards = True
    destination.finalize_sync(config, forced)
    assert _ids(ch, table) == [1, 2]


def test_replace_swap_counts_a_null_key_even_when_the_shadow_holds_a_sentinel_lookalike(
    ch: Any,
) -> None:
    """A live NULL key is absent from the shadow even if the shadow holds the text a
    sentinel-based comparison would have mapped NULL to (Codex review, B4)."""
    table = "drt_guard_swap_null"
    shadow = f"{table}__drt_swap"
    client = ch[2]
    for name in (table, shadow):
        client.command(f"DROP TABLE IF EXISTS {name}")
        client.command(
            f"CREATE TABLE {name} (id Nullable(String)) ENGINE = MergeTree ORDER BY tuple() "
            "SETTINGS allow_nullable_key = 1"
        )
    client.command(f"INSERT INTO {table} VALUES (NULL)")
    client.command(f"INSERT INTO {shadow} VALUES ('__drt_null__')")
    destination = ClickHouseDestination()
    destination._swap_shadow_created = True
    destination._swap_table = table
    options = SyncOptions(
        mode="replace",
        replace_strategy="swap",
        guards={"max_deletes": 0},  # type: ignore[arg-type]
    )
    with pytest.raises(GuardTripped) as raised:
        destination.finalize_sync(_config(ch, table), options)
    assert raised.value.trips[0].observed == 1
    assert client.query(f"SELECT count() FROM {table}").result_rows == [(1,)]


def test_replace_swap_treats_a_null_key_present_in_both_tables_as_kept(ch: Any) -> None:
    """With ClickHouse's default transform_null_in=0 a bare NULL never matches under IN."""
    table = "drt_guard_swap_nullnull"
    shadow = f"{table}__drt_swap"
    client = ch[2]
    for name in (table, shadow):
        client.command(f"DROP TABLE IF EXISTS {name}")
        client.command(
            f"CREATE TABLE {name} (id Nullable(String)) ENGINE = MergeTree ORDER BY tuple() "
            "SETTINGS allow_nullable_key = 1"
        )
        client.command(f"INSERT INTO {name} VALUES (NULL), ('x')")
    destination = ClickHouseDestination()
    destination._swap_shadow_created = True
    destination._swap_table = table
    options = SyncOptions(
        mode="replace",
        replace_strategy="swap",
        guards={"max_deletes": 0},  # type: ignore[arg-type]
    )
    destination.finalize_sync(_config(ch, table), options)  # nothing removed -> no trip
