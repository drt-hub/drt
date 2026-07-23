"""Tests for the dialect-agnostic BaseSqlDestination helpers (#719)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError
from drt.destinations.sql_base import BaseSqlDestination


def _mirror(scope: list[str] | None = None) -> SimpleNamespace:
    return SimpleNamespace(mode="mirror", mirror=SimpleNamespace(scope=scope))


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
        d._validate_mirror_scope([{"id": 1}], _mirror(scope=["parent_id"]))


def test_validate_mirror_scope_ok_when_present() -> None:
    d = BaseSqlDestination()
    d._validate_mirror_scope([{"parent_id": 1, "id": 2}], _mirror(scope=["parent_id"]))


def test_validate_mirror_scope_noop_when_not_mirror() -> None:
    d = BaseSqlDestination()
    d._validate_mirror_scope([{"id": 1}], SimpleNamespace(mode="upsert", mirror=None))


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
        def _dialect_connect(self, config: Any) -> Any:
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
        def _dialect_connect(self, config: Any) -> Any:
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
        def _dialect_connect(self, config: Any) -> Any:
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
        def _dialect_connect(self, config: Any) -> Any:
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
