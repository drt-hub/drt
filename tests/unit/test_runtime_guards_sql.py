"""Run-time delete guards at the shared PostgreSQL/MySQL SQL finalize seam."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.models import MySQLDestinationConfig, PostgresDestinationConfig, SyncOptions
from drt.destinations.mysql import MySQLDestination
from drt.destinations.postgres import PostgresDestination
from drt.engine.guards import GuardTripped


def _mysql_config() -> MySQLDestinationConfig:
    return MySQLDestinationConfig(
        type="mysql",
        host="localhost",
        dbname="db",
        user="user",
        password="pass",
        table="scores",
        upsert_key=["id"],
    )


def _postgres_config() -> PostgresDestinationConfig:
    return PostgresDestinationConfig(
        type="postgres",
        host="localhost",
        dbname="db",
        user="user",
        password="pass",
        table="public.scores",
        upsert_key=["id"],
    )


def _conn(*counts: int) -> MagicMock:
    conn = MagicMock()
    cur = conn.cursor.return_value
    cur.fetchone.side_effect = [(count,) for count in counts]
    return conn


@pytest.mark.parametrize(
    ("destination", "config", "patch_target"),
    [
        (
            PostgresDestination(),
            _postgres_config(),
            "drt.destinations.postgres.PostgresDestination._connect",
        ),
        (MySQLDestination(), _mysql_config(), "drt.destinations.mysql.MySQLDestination._connect"),
    ],
)
def test_destination_guard_trips_before_delete(
    destination: Any, config: Any, patch_target: str
) -> None:
    destination._mirror_keys = [(1,), (2,)]
    options = SyncOptions(mode="mirror", guards={"max_deletes": 2})  # type: ignore[arg-type]
    conn = _conn(3, 5)

    with patch(patch_target, return_value=conn), pytest.raises(GuardTripped) as raised:
        destination.finalize_sync(config, options)

    assert raised.value.trips[0].observed == 3
    statements = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
    assert len(statements) == 2
    assert all("COUNT" in statement for statement in statements)
    assert not any("DELETE" in statement for statement in statements)
    conn.commit.assert_not_called()
    assert destination._mirror_keys is None


def test_destination_guard_force_records_and_deletes_with_exact_boundary() -> None:
    destination = MySQLDestination()
    destination._mirror_keys = [(1,), (2,)]
    config = _mysql_config()
    options = SyncOptions(mode="mirror", guards={"max_delete_pct": 60})  # type: ignore[arg-type]
    options._force_guards = True
    conn = _conn(3, 5)  # exactly 60% is allowed

    with patch.object(MySQLDestination, "_connect", return_value=conn):
        destination.finalize_sync(config, options)

    assert options._forced_guard_trips == []
    assert "DELETE" in conn.cursor.return_value.execute.call_args_list[-1].args[0]

    destination._mirror_keys = [(1,), (2,)]
    options = SyncOptions(mode="mirror", guards={"max_delete_pct": 59.99})  # type: ignore[arg-type]
    options._force_guards = True
    conn = _conn(3, 5)
    with patch.object(MySQLDestination, "_connect", return_value=conn):
        destination.finalize_sync(config, options)
    assert [trip.guard for trip in options._forced_guard_trips] == ["max_delete_pct"]
    assert "DELETE" in conn.cursor.return_value.execute.call_args_list[-1].args[0]


def test_destination_scope_is_identical_on_delete_count_and_baseline() -> None:
    destination = MySQLDestination()
    destination._mirror_keys = [("tenant-a", 1)]
    destination._mirror_scopes = {("tenant-a",)}
    config = _mysql_config().model_copy(update={"upsert_key": ["tenant", "id"]})
    options = SyncOptions(
        mode="mirror",
        mirror={"scope": ["tenant"]},
        guards={"max_deletes": 10},
    )
    conn = _conn(1, 2)

    with patch.object(MySQLDestination, "_connect", return_value=conn):
        destination.finalize_sync(config, options)

    calls = conn.cursor.return_value.execute.call_args_list
    assert "`tenant` IN (%s) AND" in calls[0].args[0]
    assert "`tenant` IN (%s)" in calls[1].args[0]
    assert calls[0].args[1][0] == calls[1].args[1][0] == "tenant-a"


def test_count_builders_cover_composite_scope_and_keys_for_both_dialects() -> None:
    mysql = MySQLDestination()
    statement, params = mysql._build_mirror_count(
        "db.scores",
        ["tenant", "id"],
        [("a", 1), ("a", 2)],
        ["region", "tenant"],
        [("eu", "a"), ("us", "a")],
        negate=False,
    )
    assert "(`region`, `tenant`) IN ((%s, %s), (%s, %s))" in statement
    assert "(`tenant`, `id`) IN ((%s, %s), (%s, %s))" in statement
    assert params == ["eu", "a", "us", "a", "a", 1, "a", 2]

    postgres = PostgresDestination()
    single_baseline, single_params = postgres._build_mirror_count(
        "public.scores",
        ["id"],
        [(1,)],
        ["tenant"],
        [("a",)],
        baseline=True,
    )
    composite, composite_params = postgres._build_mirror_count(
        "public.scores",
        ["tenant", "id"],
        [("a", 1), ("a", 2)],
        ["region", "tenant"],
        [("eu", "a")],
        negate=False,
    )
    assert single_params == (("a",),)
    assert composite_params == ((("eu", "a"),), (("a", 1), ("a", 2)))
    assert "SELECT COUNT" in str(single_baseline)
    assert "SELECT COUNT" in str(composite)
    assert "SELECT COUNT" in str(postgres._build_table_count("public.scores"))
    assert "DROP TABLE" in str(postgres._build_drop_table("public.scores__swap"))


def test_diff_guard_fails_closed_without_opening_connection_then_force_deletes() -> None:
    destination = MySQLDestination()
    options = SyncOptions(
        mode="mirror",
        incremental_strategy="diff",
        diff={},
        mirror={"strategy": "diff"},
        guards={"max_delete_pct": 1},
    )
    options._diff_removed_keys = [{"id": 1}]

    with (
        patch.object(MySQLDestination, "_connect") as connect,
        pytest.raises(GuardTripped, match="cannot be evaluated"),
    ):
        destination.finalize_sync(_mysql_config(), options)
    connect.assert_not_called()

    options._force_guards = True
    conn = _conn()
    with patch.object(MySQLDestination, "_connect", return_value=conn):
        destination.finalize_sync(_mysql_config(), options)
    assert options._forced_guard_trips[0].observed is None
    assert "DELETE" in conn.cursor.return_value.execute.call_args.args[0]


def test_diff_without_delete_guards_uses_the_original_delete_path() -> None:
    destination = MySQLDestination()
    options = SyncOptions(
        mode="mirror",
        incremental_strategy="diff",
        diff={},
        mirror={"strategy": "diff"},
    )
    options._diff_removed_keys = [{"id": 1}]
    conn = _conn()

    with patch.object(MySQLDestination, "_connect", return_value=conn):
        destination.finalize_sync(_mysql_config(), options)

    assert len(conn.cursor.return_value.execute.call_args_list) == 1
    assert "DELETE" in conn.cursor.return_value.execute.call_args.args[0]


def test_replace_swap_trip_drops_shadow_without_swapping_and_force_swaps() -> None:
    destination = MySQLDestination()
    destination._swap_shadow_created = True
    destination._swap_table = "scores"
    options = SyncOptions(mode="replace", replace_strategy="swap", guards={"max_deletes": 1})
    conn = _conn(5, 2)

    with patch.object(MySQLDestination, "_connect", return_value=conn), pytest.raises(GuardTripped):
        destination.finalize_sync(_mysql_config(), options)

    statements = [call.args[0] for call in conn.cursor.return_value.execute.call_args_list]
    assert statements == [
        "SELECT COUNT(*) FROM `scores`",
        "SELECT COUNT(*) FROM `scores__drt_swap`",
        "DROP TABLE IF EXISTS `scores__drt_swap`",
    ]
    assert not any("RENAME TABLE" in statement for statement in statements)

    destination._swap_shadow_created = True
    destination._swap_table = "scores"
    options._force_guards = True
    conn = _conn(5, 2)
    with patch.object(MySQLDestination, "_connect", return_value=conn):
        destination.finalize_sync(_mysql_config(), options)
    statements = [call.args[0] for call in conn.cursor.return_value.execute.call_args_list]
    assert any("RENAME TABLE" in statement for statement in statements)
    assert options._forced_guard_trips[0].guard == "max_deletes"


def test_replace_swap_without_guards_does_not_count() -> None:
    destination = MySQLDestination()
    destination._swap_shadow_created = True
    destination._swap_table = "scores"
    conn = _conn()

    with (
        patch.object(MySQLDestination, "_connect", return_value=conn),
        patch.object(MySQLDestination, "_complete_swap") as complete_swap,
    ):
        destination.finalize_sync(
            _mysql_config(), SyncOptions(mode="replace", replace_strategy="swap")
        )

    conn.cursor.return_value.execute.assert_not_called()
    complete_swap.assert_called_once()


def test_no_delete_guards_preserve_the_exact_statement_sequence() -> None:
    def statements(options: SyncOptions) -> list[tuple[Any, ...]]:
        destination = MySQLDestination()
        destination._mirror_keys = [(1,), (2,)]
        conn = _conn()
        with patch.object(MySQLDestination, "_connect", return_value=conn):
            destination.finalize_sync(_mysql_config(), options)
        return [call.args for call in conn.cursor.return_value.execute.call_args_list]

    original = statements(SyncOptions(mode="mirror"))
    plan_only = statements(
        SyncOptions(
            mode="mirror",
            guards={"max_creates": 0, "max_updates_pct": 0},
        )
    )

    assert original == plan_only
    assert len(original) == 1 and "DELETE" in original[0][0]


def test_tracked_guard_counts_staged_diff_and_refuses_state_rewrite() -> None:
    destination = MySQLDestination()
    destination._mirror_keys = [(1,), (2,)]
    options = SyncOptions(
        mode="mirror",
        mirror={"strategy": "tracked"},
        guards={"max_deletes": 1},
    )
    options._sync_name = "guarded"
    conn = MagicMock()
    cur = conn.cursor.return_value

    def fetchone() -> tuple[int] | None:
        statement = str(cur.execute.call_args.args[0])
        if "information_schema.tables" in statement:
            return (1,)
        if "LIMIT 1" in statement:
            return (1,)
        if "COUNT(*)" in statement and "NOT EXISTS" in statement:
            return (2,)
        if "COUNT(*)" in statement:
            return (4,)
        return None

    def fetchall() -> list[tuple[str, str]]:
        statement = str(cur.execute.call_args.args[0])
        if "SELECT s.key_hash" in statement:
            return [("h3", json.dumps([3])), ("h4", json.dumps([4]))]
        if "SELECT c.key_hash" in statement:
            return []
        return []

    cur.fetchone.side_effect = fetchone
    cur.fetchall.side_effect = fetchall
    with patch.object(MySQLDestination, "_connect", return_value=conn), pytest.raises(GuardTripped):
        destination.finalize_sync(_mysql_config(), options)

    statements = [str(call.args[0]) for call in cur.execute.call_args_list]
    assert any("COUNT(*)" in statement and "NOT EXISTS" in statement for statement in statements)
    assert not any(statement.startswith("DELETE FROM `scores`") for statement in statements)
    batch_statements = [str(call.args[0]) for call in cur.executemany.call_args_list]
    assert batch_statements == [
        "INSERT INTO __drt_mirror_diff_keys (key_hash, key_json) VALUES (%s, %s)"
    ]


def test_tracked_guard_scoped_counts_and_no_temp_fallback() -> None:
    from drt.destinations._mirror_state import key_hash, key_json

    destination = MySQLDestination()
    destination._mirror_keys = [(1, "a")]
    destination._mirror_scopes = {(1,)}
    options = SyncOptions(
        mode="mirror",
        mirror={"strategy": "tracked", "scope": ["parent_id"]},
        guards={"max_delete_pct": 60},
    )
    options._sync_name = "guarded-scoped"
    config = _mysql_config().model_copy(update={"upsert_key": ["parent_id", "id"]})
    conn = MagicMock()
    cur = conn.cursor.return_value
    previous = [
        (key_hash((1, "b")), key_json((1, "b")), key_json((1,))),
        (key_hash((2, "x")), key_json((2, "x")), key_json((2,))),
    ]

    def fetchone() -> tuple[int] | None:
        statement = str(cur.execute.call_args.args[0])
        if "information_schema.tables" in statement:
            return (1,)
        if "information_schema.columns" in statement:
            return (2,)
        if "LIMIT 1" in statement:
            return (1,)
        return None

    cur.fetchone.side_effect = fetchone
    cur.fetchall.side_effect = [previous, []]
    with (
        patch.object(MySQLDestination, "_connect", return_value=conn),
        patch.object(
            MySQLDestination,
            "_stage_mirror_keys",
            return_value=(False, "__drt_mirror_diff_keys"),
        ),
        pytest.raises(GuardTripped),
    ):
        destination.finalize_sync(config, options)

    target_deletes = [
        call
        for call in cur.execute.call_args_list
        if str(call.args[0]).startswith("DELETE FROM `scores`")
    ]
    assert target_deletes == []


def test_tracked_guard_unscoped_fallback_uses_materialized_baseline() -> None:
    from drt.destinations._mirror_state import key_hash, key_json

    destination = MySQLDestination()
    destination._mirror_keys = [(1,)]
    options = SyncOptions(
        mode="mirror",
        mirror={"strategy": "tracked"},
        guards={"max_delete_pct": 60},
    )
    options._sync_name = "guarded-unscoped"
    conn = MagicMock()
    cur = conn.cursor.return_value
    previous = [
        (key_hash((1,)), key_json((1,))),
        (key_hash((2,)), key_json((2,))),
    ]

    def fetchone() -> tuple[int] | None:
        statement = str(cur.execute.call_args.args[0])
        if "information_schema.tables" in statement:
            return (1,)
        if "LIMIT 1" in statement:
            return (1,)
        return None

    cur.fetchone.side_effect = fetchone
    cur.fetchall.side_effect = [previous, []]
    with (
        patch.object(MySQLDestination, "_connect", return_value=conn),
        patch.object(
            MySQLDestination,
            "_stage_mirror_keys",
            return_value=(False, "__drt_mirror_diff_keys"),
        ),
    ):
        destination.finalize_sync(_mysql_config(), options)

    assert any(
        str(call.args[0]).startswith("DELETE FROM `scores`") for call in cur.execute.call_args_list
    )


def test_tracked_guard_scoped_legacy_rows_fail_closed_on_percentage() -> None:
    from drt.destinations._mirror_state import key_hash, key_json

    destination = MySQLDestination()
    destination._mirror_keys = [(1, "a")]
    destination._mirror_scopes = {(1,)}
    options = SyncOptions(
        mode="mirror",
        mirror={"strategy": "tracked", "scope": ["parent_id"]},
        guards={"max_delete_pct": 60},
    )
    options._sync_name = "guarded-legacy-scope"
    config = _mysql_config().model_copy(update={"upsert_key": ["parent_id", "id"]})
    conn = MagicMock()
    cur = conn.cursor.return_value

    def fetchone() -> tuple[int] | None:
        statement = str(cur.execute.call_args.args[0])
        if "information_schema.tables" in statement:
            return (1,)
        if "information_schema.columns" in statement:
            return (2,)
        if "LIMIT 1" in statement:
            return (1,)
        if "COUNT(*)" in statement and "NOT EXISTS" in statement:
            return (1,)
        if "COUNT(*)" in statement:
            return (2,)
        return None

    def fetchall() -> list[tuple[Any, ...]]:
        statement = str(cur.execute.call_args.args[0])
        if "SELECT s.key_hash" in statement:
            return [(key_hash((1, "b")), key_json((1, "b")), None)]
        return []

    cur.fetchone.side_effect = fetchone
    cur.fetchall.side_effect = fetchall
    with (
        patch.object(MySQLDestination, "_connect", return_value=conn),
        pytest.raises(GuardTripped) as exc_info,
    ):
        destination.finalize_sync(config, options)

    assert exc_info.value.trips[0].observed is None
    assert not any(
        str(call.args[0]).startswith("DELETE FROM `scores`") for call in cur.execute.call_args_list
    )


def test_tracked_guard_scoped_staged_counts_use_scope_predicate() -> None:
    from drt.destinations._mirror_state import key_hash, key_json

    destination = MySQLDestination()
    destination._mirror_keys = [(1, "a")]
    destination._mirror_scopes = {(1,)}
    options = SyncOptions(
        mode="mirror",
        mirror={"strategy": "tracked", "scope": ["parent_id"]},
        guards={"max_delete_pct": 60},
    )
    options._sync_name = "guarded-scoped"
    config = _mysql_config().model_copy(update={"upsert_key": ["parent_id", "id"]})
    conn = MagicMock()
    cur = conn.cursor.return_value

    def fetchone() -> tuple[int] | None:
        statement = str(cur.execute.call_args.args[0])
        if "information_schema.tables" in statement:
            return (1,)
        if "information_schema.columns" in statement:
            return (2,)
        if "LIMIT 1" in statement:
            return (1,)
        if "COUNT(*)" in statement and "NOT EXISTS" in statement:
            return (1,)
        if "COUNT(*)" in statement:
            return (2,)
        return None

    def fetchall() -> list[tuple[Any, ...]]:
        statement = str(cur.execute.call_args.args[0])
        if "SELECT s.key_hash" in statement:
            return [(key_hash((1, "b")), key_json((1, "b")), key_json((1,)))]
        return []

    cur.fetchone.side_effect = fetchone
    cur.fetchall.side_effect = fetchall
    with patch.object(MySQLDestination, "_connect", return_value=conn):
        destination.finalize_sync(config, options)

    count_calls = [
        call
        for call in cur.execute.call_args_list
        if "COUNT(*)" in str(call.args[0]) and "_drt_synced_keys" in str(call.args[0])
    ]
    assert len(count_calls) == 2
    assert all("scope_key IN (%s)" in str(call.args[0]) for call in count_calls)


def test_base_guard_sql_hooks_are_explicit_dialect_seams() -> None:
    from drt.destinations.sql_base import BaseSqlDestination

    base = BaseSqlDestination()
    assert base._supports_runtime_delete_guards() is False
    with pytest.raises(NotImplementedError):
        base._build_mirror_count("t", ["id"], [(1,)])
    with pytest.raises(NotImplementedError):
        base._build_table_count("t")
    with pytest.raises(NotImplementedError):
        base._build_drop_table("t")
