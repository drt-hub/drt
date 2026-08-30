"""Tests for the dialect-agnostic BaseSqlDestination helpers (#719)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError
from drt.destinations.sql_base import BaseSqlDestination


def _mirror(scope: list[str] | None = None, strategy: str = "destination") -> SimpleNamespace:
    return SimpleNamespace(
        mode="mirror", mirror=SimpleNamespace(scope=scope, strategy=strategy)
    )


def _cfg(upsert_key: list[str] | None = None) -> SimpleNamespace:
    return SimpleNamespace(upsert_key=upsert_key)


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------


def test_init_defaults() -> None:
    d = BaseSqlDestination()
    assert d._mirror_keys is None
    assert d._mirror_scopes is None
    assert d._schema_cache == {}
    assert d._swap_table is None
    assert d._replace_truncated is False
    assert d._swap_shadow_created is False


# ---------------------------------------------------------------------------
# _validate_mirror_scope (#687)
# ---------------------------------------------------------------------------


def test_validate_mirror_scope_raises_on_missing_column() -> None:
    d = BaseSqlDestination()
    with pytest.raises(ValueError, match="mirror.scope columns missing"):
        d._validate_mirror_scope([{"id": 1}], _cfg(), _mirror(scope=["parent_id"]))


def test_validate_mirror_scope_ok_when_present() -> None:
    d = BaseSqlDestination()
    d._validate_mirror_scope(
        [{"parent_id": 1, "id": 2}], _cfg(), _mirror(scope=["parent_id"])
    )


def test_validate_mirror_scope_noop_when_not_mirror() -> None:
    d = BaseSqlDestination()
    d._validate_mirror_scope([{"id": 1}], _cfg(), SimpleNamespace(mode="upsert", mirror=None))


def test_validate_mirror_scope_tracked_requires_scope_subset_of_upsert_key() -> None:
    """#694 — scope + strategy: tracked needs scope columns to also be
    upsert_key columns, so scope values can be derived from the tracked
    key rather than persisted separately."""
    d = BaseSqlDestination()
    with pytest.raises(ValueError, match="mirror.scope columns must be part of"):
        d._validate_mirror_scope(
            [{"parent_id": 1, "id": 2}],
            _cfg(upsert_key=["id"]),
            _mirror(scope=["parent_id"], strategy="tracked"),
        )


def test_validate_mirror_scope_tracked_accepts_scope_subset_of_upsert_key() -> None:
    d = BaseSqlDestination()
    d._validate_mirror_scope(
        [{"parent_id": 1, "id": 2}],
        _cfg(upsert_key=["parent_id", "id"]),
        _mirror(scope=["parent_id"], strategy="tracked"),
    )


def test_validate_mirror_scope_destination_strategy_allows_scope_outside_upsert_key() -> None:
    """The subset requirement is tracked-only — destination-strategy scope
    (#687, unchanged) never persists a state table, so there's nothing to
    derive scope from and the constraint doesn't apply."""
    d = BaseSqlDestination()
    d._validate_mirror_scope(
        [{"parent_id": 1, "id": 2}],
        _cfg(upsert_key=["id"]),
        _mirror(scope=["parent_id"], strategy="destination"),
    )


# ---------------------------------------------------------------------------
# _accumulate_mirror_state (#340 / #687)
# ---------------------------------------------------------------------------


def test_accumulate_requires_upsert_key() -> None:
    d = BaseSqlDestination()
    cfg = SimpleNamespace(upsert_key=[])
    with pytest.raises(ValueError, match="requires destination.upsert_key"):
        d._accumulate_mirror_state([{"id": 1}], SyncResult(), cfg, _mirror())


def test_accumulate_keys_skips_failed_rows() -> None:
    d = BaseSqlDestination()
    cfg = SimpleNamespace(upsert_key=["id"])
    result = SyncResult()
    result.row_errors.append(
        RowError(batch_index=1, record_preview="", http_status=None, error_message="x")
    )
    d._accumulate_mirror_state(
        [{"id": 10}, {"id": 20}, {"id": 30}], result, cfg, _mirror()
    )
    assert d._mirror_keys == [(10,), (30,)]  # index 1 (failed) skipped
    assert d._mirror_scopes is None


def test_accumulate_collects_distinct_scopes() -> None:
    d = BaseSqlDestination()
    cfg = SimpleNamespace(upsert_key=["id"])
    d._accumulate_mirror_state(
        [{"id": 1, "parent_id": "a"}, {"id": 2, "parent_id": "a"}],
        SyncResult(),
        cfg,
        _mirror(scope=["parent_id"]),
    )
    assert d._mirror_keys == [(1,), (2,)]
    assert d._mirror_scopes == {("a",)}


# ---------------------------------------------------------------------------
# dialect hooks (#719)
# ---------------------------------------------------------------------------


def test_dialect_hooks_are_declared() -> None:
    # The base defines the hook names the template methods depend on.
    for hook in (
        "_dialect_connect",
        "_qualify_ident",
        "_load_replace_swap",
        "_load_replace",
        "_load_upsert",
        "_build_mirror_delete",
    ):
        assert hasattr(BaseSqlDestination, hook), hook


def test_base_dialect_hooks_raise_not_implemented() -> None:
    # The base stubs are abstract by contract: a subclass MUST override them.
    # This locks that contract (and catches a future warehouse base that
    # forgets to implement a hook — the #720 direction).
    base = BaseSqlDestination()
    with pytest.raises(NotImplementedError):
        base._dialect_connect(object())
    with pytest.raises(NotImplementedError):
        base._qualify_ident("x")


def test_base_load_hooks_raise_not_implemented() -> None:
    # The three write-path hooks the pulled-up ``load`` dispatches to are
    # abstract by contract — each SQL dialect implements its own.
    base = BaseSqlDestination()
    with pytest.raises(NotImplementedError):
        base._load_replace_swap(None, None, [], [], "t", object(), object())
    with pytest.raises(NotImplementedError):
        base._load_replace(None, None, [], [], "t", object(), object())
    with pytest.raises(NotImplementedError):
        base._load_upsert(None, None, [], [], object(), object())


def test_base_finalize_hooks_raise_not_implemented() -> None:
    # The swap-rename + shadow/old naming hooks and the mirror-DELETE builder
    # (the phase-2b placeholder-expansion seam) are abstract by contract.
    base = BaseSqlDestination()
    with pytest.raises(NotImplementedError):
        base._rename_swap(None, None, "t", "s", "o")
    with pytest.raises(NotImplementedError):
        base._shadow_name("t")
    with pytest.raises(NotImplementedError):
        base._old_name("t")
    with pytest.raises(NotImplementedError):
        base._build_mirror_delete("t", ["id"], [(1,)])


def test_base_tracked_state_hooks_raise_not_implemented() -> None:
    # The tracked-mirror state-table hooks (#686 / #695) are abstract by
    # contract: identifier derivation, existence probe, DDL, and the
    # template→executable translation each carry a dialect (psycopg2
    # ``Composed`` vs plain ``str``).
    base = BaseSqlDestination()
    with pytest.raises(NotImplementedError):
        base._state_table_ident(object())
    with pytest.raises(NotImplementedError):
        base._state_table_exists(None, None, "_drt_synced_keys")
    with pytest.raises(NotImplementedError):
        base._create_state_table(None, "x")
    with pytest.raises(NotImplementedError):
        base._state_sql("SELECT 1 FROM {}", "x")


# ---------------------------------------------------------------------------
# load template (#719 phase 2a)
# ---------------------------------------------------------------------------


def _load_dest(events: list[str], mode: str, replace_strategy: str = "delete") -> Any:
    """A BaseSqlDestination subclass whose write hooks record which path ran."""

    class _Cur:
        pass

    class _Conn:
        def cursor(self) -> _Cur:
            return _Cur()

        def close(self) -> None:
            events.append("close")

    class _Dest(BaseSqlDestination):
        def _dialect_connect(
            self, config: Any, query_tags: dict[str, str] | None = None
        ) -> Any:
            events.append("connect")
            return _Conn()

        def _load_replace_swap(self, *a: Any, **k: Any) -> SyncResult:
            events.append("replace_swap")
            return SyncResult()

        def _load_replace(self, *a: Any, **k: Any) -> SyncResult:
            events.append("replace")
            return SyncResult()

        def _load_upsert(self, *a: Any, **k: Any) -> SyncResult:
            events.append("upsert")
            return SyncResult()

    return _Dest()


def _load_options(mode: str, replace_strategy: str = "delete") -> SimpleNamespace:
    return SimpleNamespace(
        mode=mode, replace_strategy=replace_strategy, mirror=None
    )


def test_load_empty_records_returns_early() -> None:
    events: list[str] = []
    d = _load_dest(events, "upsert")
    result = d.load([], SimpleNamespace(upsert_key=["id"]), _load_options("upsert"))
    assert isinstance(result, SyncResult)
    assert events == []  # never connected


def test_load_dispatches_replace_swap() -> None:
    events: list[str] = []
    d = _load_dest(events, "replace")
    d.load(
        [{"id": 1}],
        SimpleNamespace(upsert_key=["id"], table="t"),
        _load_options("replace", replace_strategy="swap"),
    )
    assert events == ["connect", "replace_swap", "close"]


def test_load_dispatches_replace_delete() -> None:
    events: list[str] = []
    d = _load_dest(events, "replace")
    d.load(
        [{"id": 1}],
        SimpleNamespace(upsert_key=["id"], table="t"),
        _load_options("replace", replace_strategy="delete"),
    )
    assert events == ["connect", "replace", "close"]


def test_load_dispatches_upsert_no_mirror_accumulate() -> None:
    events: list[str] = []
    d = _load_dest(events, "upsert")
    d.load(
        [{"id": 1}],
        SimpleNamespace(upsert_key=["id"], table="t"),
        _load_options("upsert"),
    )
    assert events == ["connect", "upsert", "close"]
    assert d._mirror_keys is None  # not accumulated when mode != mirror


def test_load_mirror_accumulates_state() -> None:
    events: list[str] = []
    d = _load_dest(events, "mirror")
    d.load(
        [{"id": 1}, {"id": 2}],
        SimpleNamespace(upsert_key=["id"], table="t"),
        _load_options("mirror"),
    )
    assert events == ["connect", "upsert", "close"]
    assert d._mirror_keys == [(1,), (2,)]  # accumulated for mirror


def test_load_closes_connection_on_error() -> None:
    events: list[str] = []

    class _Conn:
        def cursor(self) -> Any:
            raise RuntimeError("boom")

        def close(self) -> None:
            events.append("close")

    class _Dest(BaseSqlDestination):
        def _dialect_connect(
            self, config: Any, query_tags: dict[str, str] | None = None
        ) -> Any:
            return _Conn()

    d = _Dest()
    with pytest.raises(RuntimeError, match="boom"):
        d.load(
            [{"id": 1}],
            SimpleNamespace(upsert_key=["id"], table="t"),
            _load_options("upsert"),
        )
    assert events == ["close"]  # finally ran


# ---------------------------------------------------------------------------
# test_connection (#719)
# ---------------------------------------------------------------------------


def test_connection_runs_select_1_and_closes() -> None:
    events: list[str] = []

    class _Cur:
        def execute(self, sql: str) -> None:
            events.append(f"execute:{sql}")

    class _Conn:
        def cursor(self) -> _Cur:
            events.append("cursor")
            return _Cur()

        def close(self) -> None:
            events.append("close")

    class _Dest(BaseSqlDestination):
        def _dialect_connect(
            self, config: Any, query_tags: dict[str, str] | None = None
        ) -> Any:
            events.append("connect")
            return _Conn()

    d = _Dest()
    assert d.test_connection(object()) is None
    assert events == ["connect", "cursor", "execute:SELECT 1", "close"]


def test_connection_closes_even_when_execute_raises() -> None:
    events: list[str] = []

    class _Cur:
        def execute(self, sql: str) -> None:
            raise RuntimeError("boom")

    class _Conn:
        def cursor(self) -> _Cur:
            return _Cur()

        def close(self) -> None:
            events.append("close")

    class _Dest(BaseSqlDestination):
        def _dialect_connect(
            self, config: Any, query_tags: dict[str, str] | None = None
        ) -> Any:
            return _Conn()

    d = _Dest()
    with pytest.raises(RuntimeError, match="boom"):
        d.test_connection(object())
    assert events == ["close"]  # finally ran despite the error


# ---------------------------------------------------------------------------
# _record_row_error (#722 seam / #719)
# ---------------------------------------------------------------------------


def test_record_row_error_appends_truncated_preview() -> None:
    d = BaseSqlDestination()
    result = SyncResult()
    big = {"x": "y" * 500}
    d._record_row_error(result, 3, big, ValueError("boom"))
    assert result.failed == 1
    err = result.row_errors[0]
    assert err.batch_index == 3
    assert err.error_message == "boom"
    assert len(err.record_preview) <= 200
    assert err.http_status is None


# ---------------------------------------------------------------------------
# finalize_sync template (#719 phase 2a)
# ---------------------------------------------------------------------------


def _finalize_dest(events: list[str]) -> Any:
    """A BaseSqlDestination subclass recording the swap-finalize hook calls."""

    class _Cur:
        pass

    class _Conn:
        def cursor(self) -> _Cur:
            return _Cur()

        def close(self) -> None:
            events.append("close")

    class _Dest(BaseSqlDestination):
        def _dialect_connect(
            self, config: Any, query_tags: dict[str, str] | None = None
        ) -> Any:
            events.append("connect")
            return _Conn()

        def _shadow_name(self, table: str) -> str:
            return f"{table}__shadow"

        def _old_name(self, table: str) -> str:
            return f"{table}__old"

        def _rename_swap(
            self, conn: Any, cur: Any, table: str, shadow: str, old: str
        ) -> None:
            events.append(f"rename:{table}:{shadow}:{old}")

        def _finalize_mirror(self, config: Any, sync_options: Any) -> SyncResult:
            events.append("finalize_mirror")
            return SyncResult()

    return _Dest()


def test_finalize_sync_mirror_dispatches_and_resets_state() -> None:
    events: list[str] = []
    d = _finalize_dest(events)
    d._mirror_keys = [(1,)]
    d._mirror_scopes = {("a",)}
    result = d.finalize_sync(object(), SimpleNamespace(mode="mirror"))
    assert isinstance(result, SyncResult)
    assert events == ["finalize_mirror"]
    # mirror state reset regardless of result
    assert d._mirror_keys is None
    assert d._mirror_scopes is None


def test_finalize_sync_returns_none_when_no_swap() -> None:
    events: list[str] = []
    d = _finalize_dest(events)
    # not mirror, and no swap shadow created
    assert d.finalize_sync(object(), SimpleNamespace(mode="replace")) is None
    assert events == []


def test_finalize_sync_swap_delegates_rename_and_resets() -> None:
    events: list[str] = []
    d = _finalize_dest(events)
    d._swap_shadow_created = True
    d._swap_table = "public.scores"
    result = d.finalize_sync(object(), SimpleNamespace(mode="replace"))
    assert isinstance(result, SyncResult)
    assert events == [
        "connect",
        "rename:public.scores:public.scores__shadow:public.scores__old",
        "close",
    ]
    assert d._swap_shadow_created is False
    assert d._swap_table is None


# ---------------------------------------------------------------------------
# _finalize_mirror template (#719 phase 2b)
# ---------------------------------------------------------------------------


def _mirror_dest(events: list[str], tracked_result: Any = None) -> Any:
    """A BaseSqlDestination subclass recording the mirror-DELETE hook calls.

    ``_build_mirror_delete`` is the dialect seam (PG returns
    ``(Composed, tuple)``, MySQL ``(str, list)``); here it just echoes its
    arguments so the base control flow can be asserted dialect-free.
    """

    class _Cur:
        def execute(self, stmt: Any, params: Any = None) -> None:
            events.append(f"execute:{stmt}:{params}")

    class _Conn:
        def cursor(self) -> _Cur:
            return _Cur()

        def commit(self) -> None:
            events.append("commit")

        def close(self) -> None:
            events.append("close")

    class _Dest(BaseSqlDestination):
        def _dialect_connect(
            self, config: Any, query_tags: dict[str, str] | None = None
        ) -> Any:
            events.append("connect")
            return _Conn()

        def _build_mirror_delete(
            self,
            table: str,
            upsert_cols: list[str],
            keys: list[tuple[Any, ...]],
            scope_cols: list[str] | None = None,
            scopes: list[tuple[Any, ...]] | None = None,
            negate: bool = True,
        ) -> tuple[Any, Any]:
            return (
                f"DELETE {table} {upsert_cols} scope={scope_cols} negate={negate}",
                (sorted(keys), scopes),
            )

        def _finalize_mirror_tracked(
            self, config: Any, sync_options: Any
        ) -> SyncResult | None:
            events.append("tracked")
            return tracked_result

    return _Dest()


def test_finalize_mirror_returns_none_without_keys() -> None:
    events: list[str] = []
    d = _mirror_dest(events)
    cfg = SimpleNamespace(table="t", upsert_key=["id"])
    # never engaged (None) and engaged-but-empty both skip the DELETE
    assert d._finalize_mirror(cfg, _mirror()) is None
    d._mirror_keys = []
    assert d._finalize_mirror(cfg, _mirror()) is None
    assert events == []


def test_finalize_mirror_dispatches_tracked_strategy() -> None:
    events: list[str] = []
    sentinel = SyncResult()
    d = _mirror_dest(events, tracked_result=sentinel)
    d._mirror_keys = [(1,)]
    opts = SimpleNamespace(
        mode="mirror", mirror=SimpleNamespace(scope=None, strategy="tracked")
    )
    assert d._finalize_mirror(SimpleNamespace(table="t", upsert_key=["id"]), opts) is (
        sentinel
    )
    assert events == ["tracked"]


def test_finalize_mirror_dedupes_keys_and_executes_delete() -> None:
    events: list[str] = []
    d = _mirror_dest(events)
    d._mirror_keys = [(1,), (2,), (1,)]
    opts = SimpleNamespace(
        mode="mirror", mirror=SimpleNamespace(scope=None, strategy="destination")
    )
    result = d._finalize_mirror(SimpleNamespace(table="t", upsert_key=["id"]), opts)
    assert isinstance(result, SyncResult)
    assert events == [
        "connect",
        "execute:DELETE t ['id'] scope=None negate=True:([(1,), (2,)], None)",
        "commit",
        "close",
    ]


def test_finalize_mirror_passes_scope_cols_and_scopes() -> None:
    events: list[str] = []
    d = _mirror_dest(events)
    d._mirror_keys = [(1,)]
    d._mirror_scopes = {("a",)}
    opts = SimpleNamespace(
        mode="mirror",
        mirror=SimpleNamespace(scope=["tenant_id"], strategy="destination"),
    )
    d._finalize_mirror(SimpleNamespace(table="t", upsert_key=["id"]), opts)
    assert events == [
        "connect",
        "execute:DELETE t ['id'] scope=['tenant_id'] negate=True:([(1,)], [('a',)])",
        "commit",
        "close",
    ]


def test_finalize_mirror_no_mirror_options_means_no_scope() -> None:
    events: list[str] = []
    d = _mirror_dest(events)
    d._mirror_keys = [(1,)]
    opts = SimpleNamespace(mode="mirror", mirror=None)
    d._finalize_mirror(SimpleNamespace(table="t", upsert_key=["id"]), opts)
    assert "scope=None" in events[1]


def test_finalize_mirror_closes_connection_when_execute_raises() -> None:
    events: list[str] = []

    class _Cur:
        def execute(self, stmt: Any, params: Any = None) -> None:
            raise RuntimeError("boom")

    class _Conn:
        def cursor(self) -> _Cur:
            return _Cur()

        def commit(self) -> None:
            events.append("commit")

        def close(self) -> None:
            events.append("close")

    class _Dest(BaseSqlDestination):
        def _dialect_connect(
            self, config: Any, query_tags: dict[str, str] | None = None
        ) -> Any:
            return _Conn()

        def _build_mirror_delete(
            self,
            table: str,
            upsert_cols: list[str],
            keys: list[tuple[Any, ...]],
            scope_cols: list[str] | None = None,
            scopes: list[tuple[Any, ...]] | None = None,
            negate: bool = True,
        ) -> tuple[Any, Any]:
            return ("DELETE", ())

    d = _Dest()
    d._mirror_keys = [(1,)]
    with pytest.raises(RuntimeError):
        d._finalize_mirror(
            SimpleNamespace(table="t", upsert_key=["id"]),
            SimpleNamespace(mode="mirror", mirror=None),
        )
    assert events == ["close"]


# ---------------------------------------------------------------------------
# _finalize_mirror_tracked template (#719 phase 2b / #686)
# ---------------------------------------------------------------------------


def _tracked_dest(
    events: list[str],
    raw_diff: list[tuple[str, str]] | None = None,
    to_insert: list[tuple[str, str]] | None = None,
    previous: list[tuple[str, str]] | None = None,
    previous_exists: bool = True,
    exists: bool = True,
    staging_error: Exception | None = None,
    staging_insert_error: Exception | None = None,
) -> Any:
    """A BaseSqlDestination subclass recording the tracked-state hook calls.

    Every dialect seam (state identifier derivation, existence probe, DDL,
    and the ``template → executable`` translation that hides ``Composed`` vs
    ``str``) is stubbed to a plain string so the base control flow can be
    asserted dialect-free.

    #694 part 2: the base now issues three distinct read queries against the
    (fake) cursor — a baseline existence probe (``fetchone``, distinguished
    by ``LIMIT 1``), the SQL-side diff (``fetchall``, ``SELECT s.key_hash``
    prefix — state joined against the staged current keys), and the
    genuinely-new-keys-to-insert query (``fetchall``, ``SELECT c.key_hash``
    prefix — staged keys joined against state). ``raw_diff``/``to_insert``
    are what the *database* would have computed for each; the base's own
    Python-side diffing is gone, so these are supplied directly rather than
    derived from a full ``previous`` key set the way the old fixture did.
    """

    class _Cur:
        def __init__(self) -> None:
            self._last_sql = ""

        def execute(self, stmt: Any, params: Any = None) -> None:
            events.append(f"execute:{stmt}:{params}")
            self._last_sql = str(stmt)
            if self._last_sql.startswith("CREATE TEMPORARY TABLE") and staging_error:
                raise staging_error

        def executemany(self, stmt: Any, rows: Any) -> None:
            events.append(f"executemany:{stmt}:{rows}")
            if str(stmt).startswith("INSERT INTO __drt") and staging_insert_error:
                raise staging_insert_error

        def fetchone(self) -> Any:
            if "LIMIT 1" in self._last_sql:
                return (1,) if previous_exists else None
            return None

        def fetchall(self) -> list[tuple[str, str]]:
            if self._last_sql.startswith("SELECT s.key_hash"):
                return list(raw_diff or [])
            if self._last_sql.startswith("SELECT c.key_hash"):
                return list(to_insert or [])
            if self._last_sql.startswith("SELECT key_hash"):
                return list(previous or [])
            return []

    class _Conn:
        def cursor(self) -> _Cur:
            return _Cur()

        def commit(self) -> None:
            events.append("commit")

        def close(self) -> None:
            events.append("close")

    class _Dest(BaseSqlDestination):
        def _dialect_connect(
            self, config: Any, query_tags: dict[str, str] | None = None
        ) -> Any:
            events.append("connect")
            return _Conn()

        def _state_table_ident(self, config: Any) -> tuple[Any, Any, Any]:
            return ("STATE", "sch", "sch._drt_synced_keys")

        def _state_table_exists(self, cur: Any, scope: Any, raw: str) -> bool:
            events.append(f"exists?:{scope}:{raw}")
            return exists

        def _create_state_table(self, cur: Any, ident: Any) -> None:
            events.append(f"create:{ident}")

        def _state_sql(self, template: str, ident: Any) -> Any:
            return template.format(ident)

        def _build_mirror_delete(
            self,
            table: str,
            upsert_cols: list[str],
            keys: list[tuple[Any, ...]],
            scope_cols: list[str] | None = None,
            scopes: list[tuple[Any, ...]] | None = None,
            negate: bool = True,
        ) -> tuple[Any, Any]:
            return (
                f"DELETE {table} {upsert_cols} scope={scope_cols} negate={negate}",
                sorted(keys),
            )

    return _Dest()


def _tracked_opts() -> Any:
    opts = SimpleNamespace(
        mode="mirror", mirror=SimpleNamespace(scope=None, strategy="tracked")
    )
    opts._sync_name = "s1"
    return opts


def test_tracked_creates_state_table_only_when_absent() -> None:
    events: list[str] = []
    d = _tracked_dest(events, exists=False)
    d._mirror_keys = [(1,)]
    d._finalize_mirror_tracked(SimpleNamespace(table="t", upsert_key=["id"]), _tracked_opts())
    assert "exists?:sch:sch._drt_synced_keys" in events
    assert "create:STATE" in events

    events2: list[str] = []
    d2 = _tracked_dest(events2, exists=True)
    d2._mirror_keys = [(1,)]
    d2._finalize_mirror_tracked(
        SimpleNamespace(table="t", upsert_key=["id"]), _tracked_opts()
    )
    assert not any(e.startswith("create:") for e in events2)


def test_tracked_baseline_skips_delete_and_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from drt.destinations._mirror_state import key_hash, key_json

    events: list[str] = []
    d = _tracked_dest(
        events,
        previous_exists=False,
        to_insert=[(key_hash((1,)), key_json((1,)))],
    )
    d._mirror_keys = [(1,), (1,)]
    with caplog.at_level("WARNING"):
        result = d._finalize_mirror_tracked(
            SimpleNamespace(table="t", upsert_key=["id"]), _tracked_opts()
        )
    assert isinstance(result, SyncResult)
    assert any("baselin" in r.message.lower() for r in caplog.records)
    # no target DELETE was built; only the state rewrite touched the DB
    assert not any("DELETE t" in e for e in events)
    # dedupe: two identical keys collapse to one state row
    assert "executemany:INSERT INTO STATE" in " | ".join(events)
    assert events[-2:] == ["commit", "close"]


def test_tracked_deletes_previous_minus_current_via_build_hook() -> None:
    from drt.destinations._mirror_state import key_hash, key_json

    events: list[str] = []
    d = _tracked_dest(events, raw_diff=[(key_hash((2,)), key_json((2,)))])
    d._mirror_keys = [(1,)]
    d._finalize_mirror_tracked(
        SimpleNamespace(table="t", upsert_key=["id"]), _tracked_opts()
    )
    # the stale key (2,) is deleted through the shared builder with negate=False
    assert "execute:DELETE t ['id'] scope=None negate=False:[(2,)]" in events


def test_tracked_no_stale_keys_issues_no_target_delete() -> None:
    events: list[str] = []
    d = _tracked_dest(events, raw_diff=[], to_insert=[])
    d._mirror_keys = [(1,)]
    d._finalize_mirror_tracked(
        SimpleNamespace(table="t", upsert_key=["id"]), _tracked_opts()
    )
    assert not any("DELETE t" in e for e in events)


def test_tracked_stages_current_keys_before_diffing() -> None:
    """#694 part 2: current's keys are staged into a scratch table *before*
    the diff query runs, and the scratch table is dropped at the end — the
    diff (``previous - current``) and the new-keys query (``current -
    previous``) both join against it rather than reading the full state
    table into Python."""
    from drt.destinations._mirror_state import key_hash, key_json

    events: list[str] = []
    d = _tracked_dest(events)
    d._mirror_keys = [(1,), (2,)]
    d._finalize_mirror_tracked(
        SimpleNamespace(table="t", upsert_key=["id"]), _tracked_opts()
    )
    create_idx = next(
        i for i, e in enumerate(events) if e.startswith("execute:CREATE TEMPORARY TABLE")
    )
    diff_idx = next(i for i, e in enumerate(events) if e.startswith("execute:SELECT s.key_hash"))
    drop_idx = next(i for i, e in enumerate(events) if e.startswith("execute:DROP TABLE"))
    assert create_idx < diff_idx < drop_idx
    # both current keys' hash/json were inserted into the staging table
    stage_insert = next(e for e in events if e.startswith("executemany:INSERT INTO __drt"))
    for k in (1, 2):
        assert key_hash((k,)) in stage_insert and key_json((k,)) in stage_insert


def test_tracked_inserts_only_genuinely_new_keys() -> None:
    """A current key already tracked under the same hash never needs
    rewriting (hash is a pure function of key_json) — only rows the
    ``NOT EXISTS`` query actually returns as new get inserted."""
    from drt.destinations._mirror_state import key_hash, key_json

    events: list[str] = []
    d = _tracked_dest(events, raw_diff=[], to_insert=[(key_hash((2,)), key_json((2,)))])
    d._mirror_keys = [(1,), (2,)]
    d._finalize_mirror_tracked(
        SimpleNamespace(table="t", upsert_key=["id"]), _tracked_opts()
    )
    insert_calls = [e for e in events if e.startswith("executemany:INSERT INTO STATE")]
    assert len(insert_calls) == 1
    assert "'s1'" in insert_calls[0] and key_hash((2,)) in insert_calls[0]
    assert key_hash((1,)) not in insert_calls[0]


@pytest.mark.parametrize("failure_site", ["create", "insert"])
def test_tracked_falls_back_to_client_diff_when_staging_is_unavailable(
    failure_site: str,
) -> None:
    """The fallback computes both previous-current and current-previous.

    The savepoint rollback is part of the contract: without it Postgres would
    remain in ``InFailedSqlTransaction`` after a denied CREATE.
    """
    from drt.destinations._mirror_state import key_hash, key_json

    events: list[str] = []
    d = _tracked_dest(
        events,
        previous=[
            (key_hash((1,)), key_json((1,))),
            (key_hash((3,)), key_json((3,))),
        ],
        staging_error=(
            RuntimeError("temporary-table privilege denied")
            if failure_site == "create"
            else None
        ),
        staging_insert_error=(
            RuntimeError("temporary-table population failed")
            if failure_site == "insert"
            else None
        ),
    )
    d._mirror_keys = [(1,), (2,)]

    d._finalize_mirror_tracked(
        SimpleNamespace(table="t", upsert_key=["id"]), _tracked_opts()
    )

    assert any(e.startswith("execute:SAVEPOINT drt_diff_keys") for e in events)
    assert any(e.startswith("execute:ROLLBACK TO SAVEPOINT drt_diff_keys") for e in events)
    assert not any(e.startswith("execute:DROP TABLE") for e in events)
    assert not any(e.startswith("execute:SELECT c.key_hash") for e in events)
    assert "execute:DELETE t ['id'] scope=None negate=False:[(3,)]" in events
    insert_calls = [e for e in events if e.startswith("executemany:INSERT INTO STATE")]
    assert len(insert_calls) == 1
    assert key_hash((2,)) in insert_calls[0]
    assert key_hash((1,)) not in insert_calls[0]


def test_tracked_falls_back_to_table_name_when_sync_name_absent() -> None:
    from drt.destinations._mirror_state import key_hash, key_json

    events: list[str] = []
    d = _tracked_dest(
        events,
        previous_exists=False,
        to_insert=[(key_hash((1,)), key_json((1,)))],
    )
    d._mirror_keys = [(1,)]
    opts = SimpleNamespace(
        mode="mirror", mirror=SimpleNamespace(scope=None, strategy="tracked")
    )
    opts._sync_name = None
    d._finalize_mirror_tracked(SimpleNamespace(table="t", upsert_key=["id"]), opts)
    assert any("executemany:" in e and "'t'" in e for e in events)


def test_tracked_closes_connection_when_execute_raises() -> None:
    events: list[str] = []

    class _Cur:
        def execute(self, stmt: Any, params: Any = None) -> None:
            raise RuntimeError("boom")

    class _Conn:
        def cursor(self) -> _Cur:
            return _Cur()

        def close(self) -> None:
            events.append("close")

    class _Dest(BaseSqlDestination):
        def _dialect_connect(
            self, config: Any, query_tags: dict[str, str] | None = None
        ) -> Any:
            return _Conn()

        def _state_table_ident(self, config: Any) -> tuple[Any, Any, Any]:
            return ("STATE", None, "_drt_synced_keys")

        def _state_table_exists(self, cur: Any, scope: Any, raw: str) -> bool:
            return True

        def _state_sql(self, template: str, ident: Any) -> Any:
            return template.format(ident)

    d = _Dest()
    d._mirror_keys = [(1,)]
    with pytest.raises(RuntimeError):
        d._finalize_mirror_tracked(
            SimpleNamespace(table="t", upsert_key=["id"]), _tracked_opts()
        )
    assert events == ["close"]
