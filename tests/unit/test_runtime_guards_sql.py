"""Run-time delete guards at the shared PostgreSQL/MySQL SQL finalize seam."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.models import MySQLDestinationConfig, PostgresDestinationConfig, SyncOptions
from drt.destinations.mysql import MySQLDestination
from drt.destinations.postgres import PostgresDestination
from drt.engine.guards import GuardOutcomeUncertain, GuardTripped


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


def _conn(*counts: int, rowcount: int = 0) -> MagicMock:
    conn = MagicMock()
    cur = conn.cursor.return_value
    remaining = iter(counts)

    def fetchone() -> tuple[Any, ...]:
        statement = str(cur.execute.call_args.args[0])
        if "SELECT engine FROM information_schema.tables" in statement:
            return ("InnoDB",)
        return (next(remaining),)

    cur.fetchone.side_effect = fetchone
    cur.rowcount = rowcount
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
def test_destination_guard_uses_actual_delete_count_then_rolls_back(
    destination: Any, config: Any, patch_target: str
) -> None:
    destination._mirror_keys = [(1,), (2,)]
    options = SyncOptions(mode="mirror", guards={"max_deletes": 2})  # type: ignore[arg-type]
    conn = _conn(5, rowcount=3)

    with patch(patch_target, return_value=conn), pytest.raises(GuardTripped) as raised:
        destination.finalize_sync(config, options)

    assert raised.value.trips[0].observed == 3
    statements = [
        str(call.args[0])
        for call in conn.cursor.return_value.execute.call_args_list
        if "SELECT engine FROM information_schema.tables" not in str(call.args[0])
    ]
    assert len(statements) == 2
    assert "COUNT" in statements[0]
    assert "DELETE" in statements[1]
    conn.commit.assert_not_called()
    conn.rollback.assert_called_once()
    assert destination._mirror_keys is None


def test_destination_guard_force_records_and_deletes_with_exact_boundary() -> None:
    destination = MySQLDestination()
    destination._mirror_keys = [(1,), (2,)]
    config = _mysql_config()
    options = SyncOptions(mode="mirror", guards={"max_delete_pct": 60})  # type: ignore[arg-type]
    options._force_guards = True
    conn = _conn(5, rowcount=3)  # exactly 60% is allowed

    with patch.object(MySQLDestination, "_connect", return_value=conn):
        destination.finalize_sync(config, options)

    assert options._forced_guard_trips == []
    assert "DELETE" in conn.cursor.return_value.execute.call_args_list[-1].args[0]

    destination._mirror_keys = [(1,), (2,)]
    options = SyncOptions(mode="mirror", guards={"max_delete_pct": 59.99})  # type: ignore[arg-type]
    options._force_guards = True
    conn = _conn(5, rowcount=3)
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
    conn = _conn(2, rowcount=1)

    with patch.object(MySQLDestination, "_connect", return_value=conn):
        destination.finalize_sync(config, options)

    calls = conn.cursor.return_value.execute.call_args_list[-2:]
    assert "`tenant` IN (%s)" in calls[0].args[0]
    assert "`tenant` IN (%s) AND" in calls[1].args[0]
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

    mysql_swap = mysql._build_swap_delete_count("scores", "scores__shadow", ["tenant", "id"])
    postgres_swap = str(
        postgres._build_swap_delete_count(
            "public.scores", "public.scores__shadow", ["tenant", "id"]
        )
    )
    assert "l.`tenant` <=> s.`tenant` AND l.`id` <=> s.`id`" in mysql_swap
    assert postgres_swap.count("IS NOT DISTINCT FROM") == 2

    mysql_single, mysql_single_params = mysql._build_mirror_count(
        "scores", ["id"], [(1,), (2,)], negate=False
    )
    postgres_single, postgres_single_params = postgres._build_mirror_count(
        "public.scores", ["id"], [(1,), (2,)], ["tenant"], [("a",)], negate=False
    )
    postgres_unscoped, postgres_unscoped_params = postgres._build_mirror_count(
        "public.scores", ["tenant", "id"], [("a", 1)], negate=True
    )
    assert "`id` IN (%s, %s)" in mysql_single and mysql_single_params == [1, 2]
    assert "IN" in str(postgres_single)
    assert postgres_single_params == (("a",), (1, 2))
    assert "NOT IN" in str(postgres_unscoped)
    assert postgres_unscoped_params == ((("a", 1),),)

    pg_cur = MagicMock()
    postgres._lock_swap_tables(pg_cur, "public.scores", "public.scores__shadow")
    pg_lock = str(pg_cur.execute.call_args.args[0])
    assert "LOCK TABLE" in pg_lock
    assert "scores" in pg_lock and "scores__shadow" in pg_lock

    broken_unlock = MagicMock()
    broken_unlock.cursor.return_value.execute.side_effect = RuntimeError("unlock failed")
    mysql._unlock_swap_tables(broken_unlock)  # best-effort and must not raise


def test_mysql_runtime_guards_refuse_nontransactional_target_before_delete() -> None:
    destination = MySQLDestination()
    destination._mirror_keys = [(1,)]
    options = SyncOptions(mode="mirror", guards={"max_deletes": 0})
    conn = MagicMock()
    cur = conn.cursor.return_value
    cur.fetchone.return_value = ("MyISAM",)

    with (
        patch.object(MySQLDestination, "_connect", return_value=conn),
        pytest.raises(RuntimeError, match="require InnoDB.*target table.*MyISAM"),
    ):
        destination.finalize_sync(_mysql_config(), options)

    statements = [str(call.args[0]) for call in cur.execute.call_args_list]
    assert not any(statement.startswith("DELETE") for statement in statements)
    conn.rollback.assert_not_called()


def test_mysql_runtime_guards_require_innodb_for_tracked_state_and_qualified_tables() -> None:
    destination = MySQLDestination()
    cur = MagicMock()
    cur.fetchone.side_effect = [("InnoDB",), ("MEMORY",)]

    with pytest.raises(RuntimeError, match="tracked-state table.*MEMORY"):
        destination._ensure_runtime_guard_transactionality(
            cur,
            [
                ("analytics.scores", "target table"),
                ("analytics._drt_synced_keys", "tracked-state table"),
            ],
        )

    assert [call.args[1] for call in cur.execute.call_args_list] == [
        ("analytics", "scores"),
        ("analytics", "_drt_synced_keys"),
    ]


def test_mysql_runtime_guard_engine_probe_fails_closed_when_unknown_or_unreadable() -> None:
    destination = MySQLDestination()
    cur = MagicMock()
    cur.fetchone.return_value = None
    with pytest.raises(RuntimeError, match="unknown/missing"):
        destination._ensure_runtime_guard_transactionality(cur, [("scores", "target table")])

    cur.execute.side_effect = PermissionError("information_schema denied")
    with pytest.raises(RuntimeError, match="could not verify.*rollback safety is unknown"):
        destination._ensure_runtime_guard_transactionality(cur, [("scores", "target table")])


def test_diff_guard_uses_actual_delete_count_and_rolls_back_then_force_deletes() -> None:
    destination = MySQLDestination()
    options = SyncOptions(
        mode="mirror",
        incremental_strategy="diff",
        diff={},
        mirror={"strategy": "diff"},
        guards={"max_delete_pct": 1},
    )
    options._diff_removed_keys = [{"id": 1}]

    conn = _conn(rowcount=1)
    with (
        patch.object(MySQLDestination, "_connect", return_value=conn),
        pytest.raises(GuardTripped, match="cannot be evaluated"),
    ):
        destination.finalize_sync(_mysql_config(), options)
    conn.rollback.assert_called_once()

    options._force_guards = True
    conn = _conn(rowcount=1)
    with patch.object(MySQLDestination, "_connect", return_value=conn):
        destination.finalize_sync(_mysql_config(), options)
    assert options._forced_guard_trips[0].observed is None
    assert "DELETE" in conn.cursor.return_value.execute.call_args.args[0]


def test_guard_rollback_failure_reports_an_uncertain_outcome_for_destination_and_diff() -> None:
    destination = MySQLDestination()
    destination._mirror_keys = [(1,)]
    options = SyncOptions(mode="mirror", guards={"max_deletes": 0})
    conn = _conn(1, rowcount=1)
    conn.rollback.side_effect = RuntimeError("rollback failed")
    conn.close.side_effect = RuntimeError("destination close failed")
    with (
        patch.object(MySQLDestination, "_connect", return_value=conn),
        pytest.raises(GuardOutcomeUncertain) as destination_trip,
    ):
        destination.finalize_sync(_mysql_config(), options)
    assert "rollback failed" in str(destination_trip.value)
    assert "outcome is uncertain" in str(destination_trip.value)
    assert "nothing was deleted" not in str(destination_trip.value).lower()
    assert "destination close failed" in destination_trip.value._drt_notes[0]

    diff = MySQLDestination()
    diff_options = SyncOptions(
        mode="mirror",
        incremental_strategy="diff",
        diff={},
        mirror={"strategy": "diff"},
        guards={"max_deletes": 0},
    )
    diff_options._diff_removed_keys = [{"id": 1}]
    conn = _conn(rowcount=1)
    conn.rollback.side_effect = RuntimeError("diff rollback failed")
    conn.close.side_effect = RuntimeError("diff close failed")
    with (
        patch.object(MySQLDestination, "_connect", return_value=conn),
        pytest.raises(GuardOutcomeUncertain) as diff_trip,
    ):
        diff.finalize_sync(_mysql_config(), diff_options)
    assert "diff rollback failed" in str(diff_trip.value)
    assert "outcome is uncertain" in str(diff_trip.value)
    assert "diff close failed" in diff_trip.value._drt_notes[0]


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
    conn = _conn(5, 3)

    with patch.object(MySQLDestination, "_connect", return_value=conn), pytest.raises(GuardTripped):
        destination.finalize_sync(_mysql_config(), options)

    statements = [call.args[0] for call in conn.cursor.return_value.execute.call_args_list]
    assert statements == [
        "SELECT engine FROM information_schema.tables "
        "WHERE table_schema = DATABASE() AND table_name = %s",
        "LOCK TABLES `scores` WRITE, `scores__drt_swap` WRITE",
        "SELECT COUNT(*) FROM `scores`",
        "SELECT COUNT(*) FROM `scores` AS l WHERE NOT EXISTS "
        "(SELECT 1 FROM `scores__drt_swap` AS s WHERE l.`id` <=> s.`id`)",
        "DROP TABLE IF EXISTS `scores__drt_swap`",
        "UNLOCK TABLES",
    ]
    assert not any("RENAME TABLE" in statement for statement in statements)

    destination._swap_shadow_created = True
    destination._swap_table = "scores"
    options._force_guards = True
    conn = _conn(5, 3)
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


def test_replace_swap_equal_cardinality_disjoint_keys_trips() -> None:
    destination = MySQLDestination()
    destination._swap_shadow_created = True
    destination._swap_table = "scores"
    options = SyncOptions(mode="replace", replace_strategy="swap", guards={"max_deletes": 0})
    # Five live rows and five shadow rows, but all five live keys are absent.
    conn = _conn(5, 5)

    with (
        patch.object(MySQLDestination, "_connect", return_value=conn),
        patch.object(MySQLDestination, "_complete_swap") as complete_swap,
        pytest.raises(GuardTripped) as raised,
    ):
        destination.finalize_sync(_mysql_config(), options)

    assert raised.value.trips[0].observed == 5
    complete_swap.assert_not_called()


def test_keyless_replace_swap_uses_documented_cardinality_fallback() -> None:
    destination = MySQLDestination()
    destination._swap_shadow_created = True
    destination._swap_table = "scores"
    config = _mysql_config().model_copy(update={"upsert_key": []})
    options = SyncOptions(mode="replace", replace_strategy="swap", guards={"max_deletes": 0})
    conn = _conn(5, 5)

    with (
        patch.object(MySQLDestination, "_connect", return_value=conn),
        patch.object(MySQLDestination, "_complete_swap") as complete_swap,
    ):
        destination.finalize_sync(config, options)

    statements = [str(call.args[0]) for call in conn.cursor.return_value.execute.call_args_list]
    assert "SELECT COUNT(*) FROM `scores__drt_swap`" in statements
    complete_swap.assert_called_once()


def test_swap_cleanup_failure_does_not_mask_unforced_guard_trip() -> None:
    destination = MySQLDestination()
    destination._swap_shadow_created = True
    destination._swap_table = "scores"
    options = SyncOptions(mode="replace", replace_strategy="swap", guards={"max_deletes": 0})
    conn = _conn(5, 2)
    cur = conn.cursor.return_value

    def execute(statement: Any, *_args: Any) -> None:
        if str(statement).startswith("DROP TABLE IF EXISTS"):
            raise RuntimeError("cleanup failed")

    cur.execute.side_effect = execute
    conn.rollback.side_effect = RuntimeError("cleanup rollback also failed")
    conn.close.side_effect = RuntimeError("swap close failed")
    with (
        patch.object(MySQLDestination, "_connect", return_value=conn),
        pytest.raises(GuardTripped) as raised,
    ):
        destination.finalize_sync(_mysql_config(), options)

    assert raised.value.trips[0].observed == 2
    assert any("cleanup failed" in note for note in raised.value._drt_notes)
    assert any("swap close failed" in note for note in raised.value._drt_notes)
    conn.rollback.assert_called_once()


def test_forced_swap_trip_survives_post_commit_drop_failure() -> None:
    destination = MySQLDestination()
    destination._swap_shadow_created = True
    destination._swap_table = "scores"
    options = SyncOptions(mode="replace", replace_strategy="swap", guards={"max_deletes": 0})
    options._force_guards = True
    conn = _conn(5, 2)
    cur = conn.cursor.return_value

    def execute(statement: Any, *_args: Any) -> None:
        if str(statement) == "DROP TABLE `scores__drt_old`":
            raise RuntimeError("old-table cleanup failed")

    cur.execute.side_effect = execute
    with (
        patch.object(MySQLDestination, "_connect", return_value=conn),
        pytest.raises(RuntimeError, match="old-table cleanup failed"),
    ):
        destination.finalize_sync(_mysql_config(), options)

    assert [trip.guard for trip in options._forced_guard_trips] == ["max_deletes"]
    assert any(str(call.args[0]).startswith("RENAME TABLE") for call in cur.execute.call_args_list)


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
    cur.rowcount = 2
    conn.rollback.side_effect = RuntimeError("tracked rollback failed")
    conn.close.side_effect = RuntimeError("tracked close failed")

    def fetchone() -> tuple[int] | None:
        statement = str(cur.execute.call_args.args[0])
        if "SELECT engine FROM information_schema.tables" in statement:
            return ("InnoDB",)  # type: ignore[return-value]
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
    with (
        patch.object(MySQLDestination, "_connect", return_value=conn),
        pytest.raises(GuardOutcomeUncertain) as raised,
    ):
        destination.finalize_sync(_mysql_config(), options)

    statements = [str(call.args[0]) for call in cur.execute.call_args_list]
    assert any(
        "COUNT(*)" in statement and "_drt_synced_keys" in statement for statement in statements
    )
    assert any(statement.startswith("DELETE FROM `scores`") for statement in statements)
    conn.rollback.assert_called_once()
    assert "tracked rollback failed" in str(raised.value)
    assert "tracked close failed" in raised.value._drt_notes[0]
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
    cur.rowcount = 1
    previous = [
        (key_hash((1, "b")), key_json((1, "b")), key_json((1,))),
        (key_hash((2, "x")), key_json((2, "x")), key_json((2,))),
    ]

    def fetchone() -> tuple[int] | None:
        statement = str(cur.execute.call_args.args[0])
        if "SELECT engine FROM information_schema.tables" in statement:
            return ("InnoDB",)  # type: ignore[return-value]
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
    assert len(target_deletes) == 1
    conn.rollback.assert_called_once()


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
    cur.rowcount = 1
    previous = [
        (key_hash((1,)), key_json((1,))),
        (key_hash((2,)), key_json((2,))),
    ]

    def fetchone() -> tuple[int] | None:
        statement = str(cur.execute.call_args.args[0])
        if "SELECT engine FROM information_schema.tables" in statement:
            return ("InnoDB",)  # type: ignore[return-value]
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
    cur.rowcount = 1

    def fetchone() -> tuple[int] | None:
        statement = str(cur.execute.call_args.args[0])
        if "SELECT engine FROM information_schema.tables" in statement:
            return ("InnoDB",)  # type: ignore[return-value]
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
    assert any(
        str(call.args[0]).startswith("DELETE FROM `scores`") for call in cur.execute.call_args_list
    )
    conn.rollback.assert_called_once()


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
    cur.rowcount = 1

    def fetchone() -> tuple[int] | None:
        statement = str(cur.execute.call_args.args[0])
        if "SELECT engine FROM information_schema.tables" in statement:
            return ("InnoDB",)  # type: ignore[return-value]
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
    assert len(count_calls) == 1
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
        base._build_swap_delete_count("t", "t_shadow", ["id"])
    with pytest.raises(NotImplementedError):
        base._build_drop_table("t")


@pytest.mark.parametrize("bad", [None, -1, "7", 7.5, True])
def test_unusable_rowcount_rolls_back_explicitly_and_never_reads_as_a_count(bad: Any) -> None:
    destination = MySQLDestination()
    destination._mirror_keys = [(1,)]
    options = SyncOptions(mode="mirror", guards={"max_deletes": 1000})  # type: ignore[arg-type]
    conn = _conn(5, rowcount=bad)  # type: ignore[arg-type]

    with (
        patch.object(MySQLDestination, "_connect", return_value=conn),
        pytest.raises(RuntimeError, match="could not determine"),
    ):
        destination.finalize_sync(_mysql_config(), options)

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()


def test_unusable_rowcount_with_failed_rollback_is_an_uncertain_outcome() -> None:
    destination = MySQLDestination()
    destination._mirror_keys = [(1,)]
    options = SyncOptions(mode="mirror", guards={"max_deletes": 1000})  # type: ignore[arg-type]
    conn = _conn(5, rowcount=None)  # type: ignore[arg-type]
    conn.rollback.side_effect = RuntimeError("rollback failed")

    with (
        patch.object(MySQLDestination, "_connect", return_value=conn),
        pytest.raises(GuardOutcomeUncertain) as raised,
    ):
        destination.finalize_sync(_mysql_config(), options)

    assert "rollback failed" in str(raised.value)


def test_interrupt_after_the_delete_rolls_back_before_propagating() -> None:
    destination = MySQLDestination()
    destination._mirror_keys = [(1,)]
    options = SyncOptions(mode="mirror", guards={"max_deletes": 1000})  # type: ignore[arg-type]
    conn = _conn(5, rowcount=1)
    with (
        patch.object(MySQLDestination, "_connect", return_value=conn),
        patch.object(MySQLDestination, "_affected_rows", side_effect=KeyboardInterrupt),
        pytest.raises(KeyboardInterrupt),
    ):
        destination.finalize_sync(_mysql_config(), options)

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()


def test_uncertain_outcome_names_the_original_non_guard_failure() -> None:
    destination = MySQLDestination()
    destination._mirror_keys = [(1,)]
    options = SyncOptions(mode="mirror", guards={"max_deletes": 1000})  # type: ignore[arg-type]
    conn = _conn(5, rowcount=None)  # type: ignore[arg-type]
    conn.rollback.side_effect = RuntimeError("rollback failed")
    with (
        patch.object(MySQLDestination, "_connect", return_value=conn),
        pytest.raises(GuardOutcomeUncertain) as raised,
    ):
        destination.finalize_sync(_mysql_config(), options)
    assert "could not determine how many rows" in str(raised.value)
    assert isinstance(raised.value.original_error, RuntimeError)


def test_interrupt_during_the_delete_itself_rolls_back_before_propagating() -> None:
    destination = MySQLDestination()
    destination._mirror_keys = [(1,)]
    options = SyncOptions(mode="mirror", guards={"max_deletes": 1000})  # type: ignore[arg-type]
    conn = _conn(5, rowcount=1)
    cur = conn.cursor.return_value
    original = cur.execute.side_effect

    def execute(statement: Any, *args: Any) -> Any:
        if str(statement).startswith("DELETE"):
            raise KeyboardInterrupt
        return original(statement, *args) if original else None

    cur.execute.side_effect = execute
    with (
        patch.object(MySQLDestination, "_connect", return_value=conn),
        pytest.raises(KeyboardInterrupt),
    ):
        destination.finalize_sync(_mysql_config(), options)

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()


def _interrupt_on_delete(conn: MagicMock) -> None:
    """Make the guarded ``DELETE`` raise ``KeyboardInterrupt`` after it was sent."""
    cur = conn.cursor.return_value
    original = cur.execute.side_effect

    def execute(statement: Any, *args: Any) -> Any:
        if str(statement).startswith("DELETE FROM"):
            raise KeyboardInterrupt
        return original(statement, *args) if original else None

    cur.execute.side_effect = execute


def test_interrupt_during_the_diff_mirror_delete_rolls_back_before_propagating() -> None:
    destination = MySQLDestination()
    options = SyncOptions(
        mode="mirror",
        incremental_strategy="diff",
        diff={},
        mirror={"strategy": "diff"},
        guards={"max_deletes": 1000},
    )
    options._diff_removed_keys = [{"id": 1}]
    conn = _conn(rowcount=1)
    _interrupt_on_delete(conn)

    with (
        patch.object(MySQLDestination, "_connect", return_value=conn),
        pytest.raises(KeyboardInterrupt),
    ):
        destination.finalize_sync(_mysql_config(), options)

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()


def test_interrupt_during_the_tracked_mirror_delete_rolls_back_before_propagating() -> None:
    destination = MySQLDestination()
    destination._mirror_keys = [(1,), (2,)]
    options = SyncOptions(
        mode="mirror",
        mirror={"strategy": "tracked"},
        guards={"max_deletes": 1000},
    )
    options._sync_name = "guarded"
    conn = MagicMock()
    cur = conn.cursor.return_value
    cur.rowcount = 2

    def fetchone() -> tuple[Any, ...] | None:
        statement = str(cur.execute.call_args.args[0])
        if "SELECT engine FROM information_schema.tables" in statement:
            return ("InnoDB",)
        if "information_schema.tables" in statement or "LIMIT 1" in statement:
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
        return []

    cur.fetchone.side_effect = fetchone
    cur.fetchall.side_effect = fetchall
    _interrupt_on_delete(conn)

    with (
        patch.object(MySQLDestination, "_connect", return_value=conn),
        pytest.raises(KeyboardInterrupt),
    ):
        destination.finalize_sync(_mysql_config(), options)

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
