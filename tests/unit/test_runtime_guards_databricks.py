"""Run-time delete guards on Databricks (#1218 part 2): Delta cannot roll back.

The decision is made on a ``COUNT(*)`` over the DELETE's own predicate, then
re-checked against the statement's result row. Nothing here can be proven by a
mock beyond SQL shape and ordering; the live behaviour is in
``tests/integration/dwh/test_databricks_runtime_guards_smoke.py``.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.models import DatabricksDestinationConfig, SyncOptions
from drt.destinations.databricks import DatabricksDestination
from drt.engine.guards import GuardOutcomeCommitted, GuardTripped

KEYS = "main.default.__drt_mirror_keys_scores"


def _config(**extra: Any) -> DatabricksDestinationConfig:
    return DatabricksDestinationConfig.model_validate(
        {
            "type": "databricks",
            "host_env": "DB_HOST",
            "http_path_env": "DB_HTTP_PATH",
            "token_env": "DB_TOKEN",
            "catalog": "main",
            "schema": "default",
            "table": "scores",
            "introspect_schema": False,
            "upsert_key": ["id"],
            **extra,
        }
    )


class _Cursor:
    """Scripted Delta cursor: COUNT(*) pops ``counts``; DML returns ``dml``."""

    def __init__(
        self,
        counts: list[int],
        dml: tuple[Any, ...] | None = (3,),
        columns: tuple[str, ...] = ("num_affected_rows",),
        fail_drop: bool = False,
    ) -> None:
        self.counts = list(counts)
        self.dml = dml
        self.columns = columns
        self.fail_drop = fail_drop
        self.statements: list[str] = []
        self.params: list[Any] = []
        self._row: tuple[Any, ...] | None = None
        self.description: Any = None
        self.state_rows: list[tuple[Any, ...]] = []

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False

    def execute(self, sql: str, params: Any = None) -> None:
        self.statements.append(sql)
        self.params.append(params)
        self.description = None
        self._row = None
        if (
            sql.startswith("SELECT COUNT(*)")
            and "information_schema" not in sql
            and "_drt_synced_keys" not in sql
        ):
            self._row = (self.counts.pop(0),)
        elif sql.startswith(("DELETE FROM", "MERGE INTO")):
            self._row = self.dml
            self.description = [(c,) for c in self.columns] if self.dml is not None else None
        elif sql.startswith("DROP TABLE") and self.fail_drop:
            raise RuntimeError("drop failed")

    def fetchone(self) -> Any:
        return self._row

    def fetchall(self) -> list[Any]:
        return []


def _conn(cur: _Cursor) -> MagicMock:
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


def _run(
    destination: DatabricksDestination, config: Any, options: SyncOptions, cur: _Cursor
) -> Any:
    with patch.object(DatabricksDestination, "_connect", return_value=_conn(cur)):
        return destination.finalize_sync(config, options)


def _guards(**limits: Any) -> SyncOptions:
    return SyncOptions(mode="mirror", guards=limits)  # type: ignore[arg-type]


def _dml(cur: _Cursor) -> list[str]:
    return [s for s in cur.statements if s.startswith(("DELETE FROM", "MERGE INTO"))]


def test_refusal_happens_before_the_delete_and_drops_the_scratch_table() -> None:
    destination = DatabricksDestination()
    destination._mirror_keys = [(1,), (2,)]
    cur = _Cursor([3])

    with pytest.raises(GuardTripped) as raised:
        _run(destination, _config(), _guards(max_deletes=2), cur)

    assert raised.value.trips[0].observed == 3
    assert not isinstance(raised.value, GuardOutcomeCommitted)
    assert _dml(cur) == []
    assert any(
        s.startswith("SELECT COUNT(*) FROM main.default.scores WHERE id NOT IN")
        for s in cur.statements
    )
    assert cur.statements[-1] == f"DROP TABLE IF EXISTS {KEYS}"


def test_pass_deletes_and_verifies_the_statement_result() -> None:
    destination = DatabricksDestination()
    destination._mirror_keys = [(1,), (2,)]
    cur = _Cursor([3, 5], dml=(3,))

    _run(destination, _config(), _guards(max_deletes=3, max_delete_pct=60), cur)

    count_sqls = [s for s in cur.statements if s.startswith("SELECT COUNT(*)")]
    assert count_sqls[-1] == "SELECT COUNT(*) FROM main.default.scores"  # the table baseline
    assert len(_dml(cur)) == 1


def test_late_larger_delete_is_reported_as_already_committed() -> None:
    destination = DatabricksDestination()
    destination._mirror_keys = [(1,)]
    cur = _Cursor([1], dml=(9,))

    with pytest.raises(GuardOutcomeCommitted) as raised:
        _run(destination, _config(), _guards(max_deletes=1), cur)

    message = str(raised.value)
    assert "already committed" in message and "9 row(s) were deleted" in message
    assert raised.value.trips[0].observed == 9
    assert cur.statements[-1] == f"DROP TABLE IF EXISTS {KEYS}"


def test_forced_trip_with_unreadable_result_keeps_the_forced_record() -> None:
    from drt.engine.guards import guards_outcome_of

    destination = DatabricksDestination()
    destination._mirror_keys = [(1,)]
    options = _guards(max_deletes=1)
    options._force_guards = True
    cur = _Cursor([5], dml=None)
    with pytest.raises(GuardOutcomeCommitted) as raised:
        _run(destination, _config(), options, cur)

    outcome = guards_outcome_of(raised.value)
    assert outcome["forced"] is True
    assert outcome["tripped"][0]["guard"] == "max_deletes"


def test_unreadable_result_row_refuses_to_claim_nothing_was_deleted() -> None:
    destination = DatabricksDestination()
    destination._mirror_keys = [(1,)]
    cur = _Cursor([1], dml=None)

    with pytest.raises(GuardOutcomeCommitted, match="could not be read back"):
        _run(destination, _config(), _guards(max_deletes=5), cur)


def test_forced_trip_deletes_and_records_once_even_when_the_late_count_is_larger() -> None:
    destination = DatabricksDestination()
    destination._mirror_keys = [(1,)]
    options = _guards(max_deletes=1)
    options._force_guards = True
    cur = _Cursor([2], dml=(4,))

    _run(destination, _config(), options, cur)

    assert [(t.guard, t.observed) for t in options._forced_guard_trips] == [("max_deletes", 4)]


def test_composite_key_scope_counts_with_exists_and_the_same_scope_predicate() -> None:
    destination = DatabricksDestination()
    destination._mirror_keys = [("a", 1)]
    destination._mirror_scopes = {("a",)}
    config = _config(upsert_key=["tenant", "id"])
    options = SyncOptions(
        mode="mirror",
        mirror={"scope": ["tenant"]},  # type: ignore[arg-type]
        guards={"max_deletes": 9, "max_delete_pct": 90},  # type: ignore[arg-type]
    )
    cur = _Cursor([1, 4], dml=(1,), columns=("num_affected_rows", "num_deleted_rows"))
    cur.dml = (7, 1)

    _run(destination, config, options, cur)

    count_sqls = [s for s in cur.statements if s.startswith("SELECT COUNT(*)")]
    assert count_sqls[0] == (
        "SELECT COUNT(*) FROM main.default.scores AS t WHERE t.tenant IN (?) AND "
        f"NOT EXISTS (SELECT 1 FROM {KEYS} AS s WHERE t.tenant = s.tenant AND t.id = s.id)"
    )
    assert count_sqls[1] == "SELECT COUNT(*) FROM main.default.scores WHERE tenant IN (?)"
    assert cur.params[cur.statements.index(count_sqls[1])] == ["a"]
    assert _dml(cur)[0].startswith("MERGE INTO")


def test_diff_mirror_percentage_fails_closed_without_a_baseline() -> None:
    destination = DatabricksDestination()
    options = SyncOptions(
        mode="mirror",
        incremental_strategy="diff",
        mirror={"strategy": "diff"},  # type: ignore[arg-type]
        guards={"max_delete_pct": 50},  # type: ignore[arg-type]
    )
    options._diff_removed_keys = [{"id": 1}]
    options._sync_name = "s"
    cur = _Cursor([1])

    with pytest.raises(GuardTripped):
        _run(destination, _config(), options, cur)

    assert _dml(cur) == []


class _TrackedCursor(_Cursor):
    """Adds the tracked-mirror state queries on top of the scripted cursor."""

    def __init__(self, stale: list[tuple[Any, ...]], state_count: int, **kw: Any) -> None:
        super().__init__(**kw)
        self.stale = stale
        self.state_count = state_count

    def fetchone(self) -> Any:
        last = self.statements[-1]
        if "information_schema.columns" in last:
            return (2,)
        if last.startswith("SELECT 1 FROM"):
            return (1,)
        if last.startswith("SELECT COUNT(*) FROM main.default._drt_synced_keys"):
            return (self.state_count,)
        return super().fetchone()

    def fetchall(self) -> list[Any]:
        last = self.statements[-1]
        if last.startswith("SHOW TABLES"):
            return [("t",)]
        if last.startswith("SELECT s.key_hash"):
            return self.stale
        return []


def _tracked_options(**extra: Any) -> SyncOptions:
    options = SyncOptions(
        mode="mirror",
        mirror={"strategy": "tracked", **extra},  # type: ignore[arg-type]
        guards={"max_delete_pct": 10},  # type: ignore[arg-type]
    )
    options._sync_name = "tracked_sync"
    return options


def _assert_state_untouched(cur: _Cursor) -> None:
    for statement in cur.statements:
        assert not statement.startswith(("INSERT INTO main.default._drt_synced_keys",))
        assert not statement.startswith("UPDATE main.default._drt_synced_keys")
        assert not statement.startswith("DELETE FROM main.default._drt_synced_keys")


def test_tracked_unscoped_baseline_comes_from_state_and_a_refusal_writes_no_state() -> None:
    destination = DatabricksDestination()
    destination._mirror_keys = [(1,)]
    stale = [("h2", json.dumps([2])), ("h3", json.dumps([3]))]
    cur = _TrackedCursor(stale, state_count=4, counts=[2])

    with pytest.raises(GuardTripped) as raised:
        _run(destination, _config(), _tracked_options(), cur)

    assert raised.value.trips[0].observed == 50.0  # 2 of the 4 tracked rows
    assert _dml(cur) == []
    _assert_state_untouched(cur)
    baseline_sqls = [s for s in cur.statements if "FROM main.default._drt_synced_keys s WHERE" in s]
    assert any(s.startswith("SELECT COUNT(*)") for s in baseline_sqls)
    assert any("__drt_mirror_diff_keys" in s and s.startswith("DROP TABLE") for s in cur.statements)


def test_tracked_scoped_baseline_uses_the_scope_predicate_and_survives_drop_failure() -> None:
    destination = DatabricksDestination()
    destination._mirror_keys = [("a", 1)]
    destination._mirror_scopes = {("a",)}
    config = _config(upsert_key=["tenant", "id"])
    stale = [("h2", json.dumps(["a", 2]), json.dumps(["a"]))]
    cur = _TrackedCursor(stale, state_count=4, counts=[1])
    options = _tracked_options(scope=["tenant"])
    original_execute = cur.execute

    def execute(sql: str, params: Any = None) -> None:
        original_execute(sql, params)
        if sql.startswith("DROP TABLE IF EXISTS main.default.__drt_mirror_diff_keys"):
            raise RuntimeError("diff drop failed")

    cur.execute = execute  # type: ignore[method-assign]

    with pytest.raises(GuardTripped):
        _run(destination, config, options, cur)

    baseline = next(
        i
        for i, s in enumerate(cur.statements)
        if s.startswith("SELECT COUNT(*) FROM main.default._drt_synced_keys")
    )
    assert "scope_key IN (?)" in cur.statements[baseline]
    assert cur.params[baseline][2:] == [json.dumps(["a"])]
    assert _dml(cur) == []


def test_tracked_scoped_with_legacy_rows_fails_percentage_closed() -> None:
    destination = DatabricksDestination()
    destination._mirror_keys = [("a", 1)]
    destination._mirror_scopes = {("a",)}
    config = _config(upsert_key=["tenant", "id"])
    stale = [
        ("h2", json.dumps(["a", 2]), json.dumps(["a"])),
        ("h3", json.dumps(["a", 3]), None),  # written before the scope columns existed
    ]
    cur = _TrackedCursor(stale, state_count=1000, counts=[2])

    with pytest.raises(GuardTripped):
        _run(destination, config, _tracked_options(scope=["tenant"]), cur)

    assert not any(
        s.startswith("SELECT COUNT(*) FROM main.default._drt_synced_keys") for s in cur.statements
    )


def test_refused_run_does_not_heal_legacy_scope_rows() -> None:
    destination = DatabricksDestination()
    destination._mirror_keys = [("a", 1)]
    destination._mirror_scopes = {("a",)}
    config = _config(upsert_key=["tenant", "id"])
    stale = [
        ("h2", json.dumps(["a", 2]), json.dumps(["a"])),  # doomed
        ("h3", json.dumps(["b", 3]), None),  # legacy row of an unobserved scope
    ]
    cur = _TrackedCursor(stale, state_count=1000, counts=[2])

    with pytest.raises(GuardTripped):
        _run(destination, config, _tracked_options(scope=["tenant"]), cur)

    _assert_state_untouched(cur)


def test_replace_swap_trip_drops_shadow_and_never_overwrites_then_force_overwrites() -> None:
    destination = DatabricksDestination()
    destination._swap_shadow_created = True
    destination._swap_table = "main.default.scores"
    options = SyncOptions(mode="replace", replace_strategy="swap", guards={"max_deletes": 1})  # type: ignore[arg-type]
    cur = _Cursor([5, 3])

    with pytest.raises(GuardTripped):
        _run(destination, _config(), options, cur)

    assert cur.statements[0] == "SELECT COUNT(*) FROM main.default.scores"
    assert "l.id <=> s.id" in cur.statements[1]
    assert not any(s.startswith("INSERT OVERWRITE") for s in cur.statements)
    assert cur.statements[-1] == "DROP TABLE IF EXISTS main.default.scores__drt_swap"

    destination._swap_shadow_created = True
    destination._swap_table = "main.default.scores"
    options._force_guards = True
    cur = _Cursor([5, 3])
    _run(destination, _config(), options, cur)
    assert any(s.startswith("INSERT OVERWRITE") for s in cur.statements)
    assert options._forced_guard_trips[0].guard == "max_deletes"


def test_no_guards_means_no_extra_statements() -> None:
    destination = DatabricksDestination()
    destination._mirror_keys = [(1,)]
    cur = _Cursor([], dml=(3,))

    _run(destination, _config(), SyncOptions(mode="mirror"), cur)

    assert not any(s.startswith("SELECT COUNT(*)") for s in cur.statements)
    assert len(_dml(cur)) == 1


def test_scratch_table_cleanup_failure_never_masks_the_refusal() -> None:
    destination = DatabricksDestination()
    destination._mirror_keys = [(1,)]
    cur = _Cursor([9], fail_drop=True)

    with pytest.raises(GuardTripped):
        _run(destination, _config(), _guards(max_deletes=1), cur)


@pytest.mark.parametrize(
    ("columns", "row", "expected"),
    [
        (("num_affected_rows",), (4,), 4),
        (("num_affected_rows", "num_deleted_rows"), (9, 2), 2),
        (("num_affected_rows",), (-1,), None),
        (("num_affected_rows",), (True,), None),
        (("other",), (1,), None),
    ],
)
def test_deleted_rows_reads_only_trustworthy_counts(
    columns: tuple[str, ...], row: tuple[Any, ...], expected: int | None
) -> None:
    cur = _Cursor([], dml=row, columns=columns)
    cur.execute("DELETE FROM t")
    assert DatabricksDestination._deleted_rows(cur) == expected


def test_deleted_rows_treats_a_broken_cursor_as_unverified() -> None:
    cur = MagicMock()
    cur.fetchone.side_effect = RuntimeError("closed")
    assert DatabricksDestination._deleted_rows(cur) is None
    cur = MagicMock()
    cur.fetchone.return_value = None
    assert DatabricksDestination._deleted_rows(cur) is None


@pytest.mark.parametrize("path", ["destination", "diff", "tracked"])
def test_connection_close_failure_never_masks_a_guard_refusal(path: str) -> None:
    destination = DatabricksDestination()
    if path == "destination":
        destination._mirror_keys = [(1,), (2,)]
        options = _guards(max_deletes=2)
        cur = _Cursor([3])
    elif path == "diff":
        options = SyncOptions(
            mode="mirror",
            incremental_strategy="diff",
            mirror={"strategy": "diff"},  # type: ignore[arg-type]
            guards={"max_deletes": 1},  # type: ignore[arg-type]
        )
        options._diff_removed_keys = [{"id": 1}, {"id": 2}]
        options._sync_name = "s"
        cur = _Cursor([2])
    else:
        destination._mirror_keys = [(1,)]
        options = _tracked_options()
        options.guards = _guards(max_deletes=1).guards
        cur = _TrackedCursor([("h2", json.dumps([2])), ("h3", json.dumps([3]))], 4, counts=[2])
    conn = _conn(cur)
    conn.close.side_effect = RuntimeError("close failed")

    with (
        patch.object(DatabricksDestination, "_connect", return_value=conn),
        pytest.raises(GuardTripped),
    ):
        destination.finalize_sync(_config(), options)

    conn.close.assert_called_once()


def test_connection_close_failure_still_surfaces_when_nothing_tripped() -> None:
    destination = DatabricksDestination()
    destination._mirror_keys = [(1,)]
    cur = _Cursor([1], dml=(1,))
    conn = _conn(cur)
    conn.close.side_effect = RuntimeError("close failed")

    with (
        patch.object(DatabricksDestination, "_connect", return_value=conn),
        pytest.raises(RuntimeError, match="close failed"),
    ):
        destination.finalize_sync(_config(), _guards(max_deletes=5))
