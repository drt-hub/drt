"""Run-time delete guards on Snowflake (#1218 part 2): autocommit needs an explicit BEGIN."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.models import SnowflakeDestinationConfig, SyncOptions
from drt.destinations.snowflake import SnowflakeDestination
from drt.engine.guards import GuardTripped


def _config(**extra: Any) -> SnowflakeDestinationConfig:
    return SnowflakeDestinationConfig(
        type="snowflake",
        account_env="SF_ACCOUNT",
        user_env="SF_USER",
        password_env="SF_PASS",
        database="DB",
        schema="PUBLIC",
        table="SCORES",
        warehouse="WH",
        upsert_key=["id"],
        **extra,
    )


def _conn(*counts: int, rowcount: Any = 0) -> MagicMock:
    conn = MagicMock()
    cur = conn.cursor.return_value
    cur.__enter__.return_value = cur  # real cursors return themselves
    remaining = iter(counts)
    cur.fetchone.side_effect = lambda: (next(remaining),)
    cur.rowcount = rowcount
    return conn


def _statements(conn: MagicMock) -> list[str]:
    return [str(c.args[0]) for c in conn.cursor.return_value.execute.call_args_list]


def test_destination_guard_begins_txn_judges_rowcount_and_rolls_back() -> None:
    destination = SnowflakeDestination()
    destination._mirror_keys = [(1,), (2,)]
    options = SyncOptions(mode="mirror", guards={"max_deletes": 2})  # type: ignore[arg-type]
    conn = _conn(5, rowcount=3)

    with (
        patch.object(SnowflakeDestination, "_connect", return_value=conn),
        pytest.raises(GuardTripped) as raised,
    ):
        destination.finalize_sync(_config(), options)

    assert raised.value.trips[0].observed == 3
    statements = _statements(conn)
    assert [s.split(" ")[0] for s in statements] == ["SELECT", "BEGIN", "DELETE"]
    assert "COMMIT" not in statements
    conn.rollback.assert_called_once()


def test_destination_within_limits_commits_the_explicit_txn() -> None:
    destination = SnowflakeDestination()
    destination._mirror_keys = [(1,), (2,)]
    options = SyncOptions(mode="mirror", guards={"max_deletes": 5})  # type: ignore[arg-type]
    conn = _conn(5, rowcount=3)

    with patch.object(SnowflakeDestination, "_connect", return_value=conn):
        destination.finalize_sync(_config(), options)

    assert _statements(conn)[-3:] == ["BEGIN", _statements(conn)[-2], "COMMIT"]
    assert _statements(conn)[-2].startswith("DELETE FROM DB.PUBLIC.SCORES")
    conn.rollback.assert_not_called()


def test_forced_trip_still_commits_and_is_recorded() -> None:
    destination = SnowflakeDestination()
    destination._mirror_keys = [(1,)]
    options = SyncOptions(mode="mirror", guards={"max_deletes": 0})  # type: ignore[arg-type]
    options._force_guards = True
    conn = _conn(5, rowcount=3)

    with patch.object(SnowflakeDestination, "_connect", return_value=conn):
        destination.finalize_sync(_config(), options)

    assert _statements(conn)[-1] == "COMMIT"
    assert [t.guard for t in options._forced_guard_trips] == ["max_deletes"]


def test_no_guards_keeps_autocommit_statement_sequence() -> None:
    destination = SnowflakeDestination()
    destination._mirror_keys = [(1,)]
    conn = _conn(rowcount=3)

    with patch.object(SnowflakeDestination, "_connect", return_value=conn):
        destination.finalize_sync(_config(), SyncOptions(mode="mirror"))

    statements = _statements(conn)
    assert len(statements) == 1 and statements[0].startswith("DELETE FROM")


@pytest.mark.parametrize("unknown", [None, -1])
def test_unknown_rowcount_refuses_instead_of_reading_as_zero(unknown: Any) -> None:
    destination = SnowflakeDestination()
    destination._mirror_keys = [(1,)]
    options = SyncOptions(mode="mirror", guards={"max_deletes": 1000})  # type: ignore[arg-type]
    conn = _conn(5, rowcount=unknown)

    with (
        patch.object(SnowflakeDestination, "_connect", return_value=conn),
        pytest.raises(RuntimeError, match="could not determine"),
    ):
        destination.finalize_sync(_config(), options)

    assert "COMMIT" not in _statements(conn)


def test_count_builders_mirror_the_delete_predicate() -> None:
    d = SnowflakeDestination()
    stmt, params = d._build_mirror_count("T", ["id"], [(1,), (2,)])
    assert stmt == "SELECT COUNT(*) FROM T WHERE id NOT IN (%s, %s)"
    assert params == [1, 2]
    stmt, params = d._build_mirror_count("T", ["a", "b"], [(1, 2)], negate=False)
    assert stmt == "SELECT COUNT(*) FROM T WHERE (a, b) IN ((%s, %s))"
    assert params == [1, 2]
    stmt, params = d._build_mirror_count("T", ["id"], [(1,)], ["tenant"], [("x",), ("y",)])
    assert stmt == "SELECT COUNT(*) FROM T WHERE tenant IN (%s, %s) AND id NOT IN (%s)"
    assert params == ["x", "y", 1]
    stmt, params = d._build_mirror_count(
        "T", ["id"], [(1,)], ["t1", "t2"], [("x", 1)], baseline=True
    )
    assert stmt == "SELECT COUNT(*) FROM T WHERE (t1, t2) IN ((%s, %s))"
    assert params == ["x", 1]
    assert d._build_mirror_count("T", ["id"], [], baseline=True) == (
        "SELECT COUNT(*) FROM T",
        [],
    )
    assert d._build_table_count("T") == "SELECT COUNT(*) FROM T"
    assert d._build_drop_table("T") == "DROP TABLE IF EXISTS T"
    assert d._build_swap_delete_count("T", "S", ["a", "b"]) == (
        "SELECT COUNT(*) FROM T AS l WHERE NOT EXISTS "
        "(SELECT 1 FROM S AS s WHERE l.a IS NOT DISTINCT FROM s.a "
        "AND l.b IS NOT DISTINCT FROM s.b)"
    )


def test_every_hook_the_flag_activates_is_implemented() -> None:
    d = SnowflakeDestination()
    assert d._supports_runtime_delete_guards() is True
    d._build_mirror_count("T", ["id"], [(1,)])
    d._build_table_count("T")
    d._build_swap_delete_count("T", "S", ["id"])
    d._build_drop_table("T")
    d._lock_swap_tables(MagicMock(), "T", "S")


def test_replace_swap_trip_drops_shadow_resets_state_and_force_swaps() -> None:
    destination = SnowflakeDestination()
    destination._swap_shadow_created = True
    destination._swap_table = "DB.PUBLIC.SCORES"
    options = SyncOptions(mode="replace", replace_strategy="swap", guards={"max_deletes": 1})
    conn = _conn(5, 3)

    with (
        patch.object(SnowflakeDestination, "_connect", return_value=conn),
        pytest.raises(GuardTripped),
    ):
        destination.finalize_sync(_config(), options)

    statements = _statements(conn)
    assert statements[0] == "SELECT COUNT(*) FROM DB.PUBLIC.SCORES"
    assert statements[-1] == "DROP TABLE IF EXISTS DB.PUBLIC.SCORES__drt_swap"
    assert not any("SWAP WITH" in s for s in statements)
    assert destination._swap_shadow_created is False

    destination._swap_shadow_created = True
    destination._swap_table = "DB.PUBLIC.SCORES"
    options._force_guards = True
    conn = _conn(5, 3)
    with patch.object(SnowflakeDestination, "_connect", return_value=conn):
        destination.finalize_sync(_config(), options)
    assert any("SWAP WITH" in s for s in _statements(conn))
    assert options._forced_guard_trips[0].guard == "max_deletes"


def test_tracked_guard_keeps_ddl_outside_the_begin_window_and_skips_state_rewrite() -> None:
    destination = SnowflakeDestination()
    destination._mirror_keys = [(1,), (2,)]
    options = SyncOptions(mode="mirror", mirror={"strategy": "tracked"}, guards={"max_deletes": 1})  # type: ignore[arg-type]
    options._sync_name = "guarded"
    conn = MagicMock()
    cur = conn.cursor.return_value
    cur.__enter__.return_value = cur
    cur.rowcount = 2

    def fetchone() -> Any:
        statement = str(cur.execute.call_args.args[0])
        if "COUNT(*)" in statement:
            return (4,)
        return (1,)

    def fetchall() -> list[Any]:
        statement = str(cur.execute.call_args.args[0])
        if statement.startswith("SHOW TABLES"):
            return [("t",)]
        if "SELECT s.key_hash" in statement:
            return [("h3", json.dumps([3])), ("h4", json.dumps([4]))]
        return []

    cur.fetchone.side_effect = fetchone
    cur.fetchall.side_effect = fetchall

    with (
        patch.object(SnowflakeDestination, "_connect", return_value=conn),
        pytest.raises(GuardTripped),
    ):
        destination.finalize_sync(_config(), options)

    statements = _statements(conn)
    begin = statements.index("BEGIN")
    after = statements[begin + 1 :]
    assert after and after[0].startswith("DELETE FROM DB.PUBLIC.SCORES")
    # Judged immediately after the DELETE: no DDL, no state rewrite, no COMMIT.
    assert len(after) == 1
    conn.rollback.assert_called_once()
    assert cur.executemany.call_count == 1  # only the diff-key staging inserts


def test_refused_guarded_delete_clears_the_open_transaction_flag() -> None:
    destination = SnowflakeDestination()
    destination._mirror_keys = [(1,)]
    options = SyncOptions(mode="mirror", guards={"max_deletes": 0})  # type: ignore[arg-type]
    conn = _conn(5, rowcount=3)

    with (
        patch.object(SnowflakeDestination, "_connect", return_value=conn),
        pytest.raises(GuardTripped),
    ):
        destination.finalize_sync(_config(), options)

    assert destination._guard_txn_open is False
