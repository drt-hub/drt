"""Tests for the shared SQL-destination helpers (#722)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from drt.destinations.sql_utils import (
    MIRROR_UPSERT_KEY_MSG,
    RowCountable,
    backtick_quote_ident,
    check_mirror_supported,
    check_scope_subset_of_upsert_key,
    get_row_count_for_destination,
    unsupported_tracked_scope_msg,
)


class _Counter:
    def __init__(self, n: int) -> None:
        self.n = n

    def get_row_count(self, config: object) -> int:
        return self.n


class _NotCountable:
    pass


# ---------------------------------------------------------------------------
# backtick_quote_ident — shared by MySQL + ClickHouse
# ---------------------------------------------------------------------------


def test_backtick_quote_unqualified() -> None:
    assert backtick_quote_ident("scores") == "`scores`"


def test_backtick_quote_qualified() -> None:
    assert backtick_quote_ident("mydb.scores") == "`mydb`.`scores`"


def test_backtick_quote_three_part() -> None:
    assert backtick_quote_ident("a.b.c") == "`a`.`b`.`c`"


# ---------------------------------------------------------------------------
# RowCountable — capability discovery
# ---------------------------------------------------------------------------


def test_rowcountable_isinstance() -> None:
    assert isinstance(_Counter(5), RowCountable)
    assert not isinstance(_NotCountable(), RowCountable)


def test_get_row_count_for_countable_destination() -> None:
    assert get_row_count_for_destination(_Counter(42), config=object()) == 42


def test_get_row_count_for_non_countable_returns_none() -> None:
    assert get_row_count_for_destination(_NotCountable(), config=object()) is None


# ---------------------------------------------------------------------------
# Mirror guard messages — centralized, tests assert exact wording
# ---------------------------------------------------------------------------


def test_upsert_key_message_is_stable() -> None:
    assert MIRROR_UPSERT_KEY_MSG == (
        "sync.mode: mirror requires destination.upsert_key "
        "(needed to identify which rows to DELETE)."
    )


def test_unsupported_tracked_scope_message_names_dialect() -> None:
    """#692 closed out tracked/scope support on every SQL destination that
    implements mirror mode (postgres, mysql, snowflake, clickhouse,
    databricks), so no live call site passes ``supports_tracked_scope=False``
    anymore. This exercises the message's string formatting directly with a
    placeholder for whatever future dialect might add mirror mode without
    tracked/scope from day one."""
    msg = unsupported_tracked_scope_msg("newdialect")
    assert msg == (
        "mirror.strategy: tracked / mirror.scope are not yet supported on newdialect "
        "(supported: postgres, mysql, snowflake, clickhouse, databricks)."
    )


# ---------------------------------------------------------------------------
# check_mirror_supported — shared mirror-capability guard
# ---------------------------------------------------------------------------


def _mirror_opts(strategy: str | None = None, scope: object = None) -> SimpleNamespace:
    return SimpleNamespace(mode="mirror", mirror=SimpleNamespace(strategy=strategy, scope=scope))


def test_check_mirror_supported_noop_for_non_mirror() -> None:
    # Non-mirror sync: no upsert_key needed, no raise.
    check_mirror_supported(
        SimpleNamespace(upsert_key=[]),
        SimpleNamespace(mode="upsert", mirror=None),
        "snowflake",
    )


def test_check_mirror_supported_requires_upsert_key() -> None:
    with pytest.raises(ValueError, match="requires destination.upsert_key"):
        check_mirror_supported(
            SimpleNamespace(upsert_key=[]),
            SimpleNamespace(mode="mirror", mirror=None),
            "snowflake",
        )


def test_check_mirror_supported_rejects_tracked_and_scope() -> None:
    """#692 closed out every real dialect (see the message test above), so
    no live call site passes ``supports_tracked_scope=False`` for
    tracked/scope anymore — this exercises the rejection branch directly
    with a placeholder dialect that never opts in."""
    cfg = SimpleNamespace(upsert_key=["id"])
    with pytest.raises(ValueError, match="not yet supported on newdialect"):
        check_mirror_supported(cfg, _mirror_opts(strategy="tracked"), "newdialect")
    with pytest.raises(ValueError, match="not yet supported on newdialect"):
        check_mirror_supported(cfg, _mirror_opts(scope=["parent_id"]), "newdialect")


def test_check_mirror_supported_ok_for_plain_mirror() -> None:
    check_mirror_supported(SimpleNamespace(upsert_key=["id"]), _mirror_opts(), "snowflake")


def test_check_mirror_supported_allows_tracked_and_scope_when_dialect_opts_in() -> None:
    """#692 — a dialect passing supports_tracked_scope=True (every real
    dialect implementing mirror mode, as of Databricks landing) is not
    rejected the way an unopted-in dialect still is."""
    cfg = SimpleNamespace(upsert_key=["id"])
    check_mirror_supported(
        cfg, _mirror_opts(strategy="tracked"), "snowflake", supports_tracked_scope=True
    )
    check_mirror_supported(
        cfg, _mirror_opts(scope=["parent_id"]), "snowflake", supports_tracked_scope=True
    )


def test_check_mirror_supported_still_enforces_scope_subset_when_supported() -> None:
    """Opting into tracked/scope support doesn't waive #694's scope ⊆
    upsert_key constraint for the tracked+scope combination."""
    cfg = SimpleNamespace(upsert_key=["id"])
    with pytest.raises(ValueError, match="mirror.scope columns must be part of"):
        check_mirror_supported(
            cfg,
            _mirror_opts(strategy="tracked", scope=["parent_id"]),
            "snowflake",
            supports_tracked_scope=True,
        )


# ---------------------------------------------------------------------------
# check_scope_subset_of_upsert_key (#694)
# ---------------------------------------------------------------------------


def test_check_scope_subset_of_upsert_key_noop_for_destination_strategy() -> None:
    """The subset constraint is tracked-only; destination-strategy scope
    (#687) has no state to derive from and isn't constrained by it."""
    check_scope_subset_of_upsert_key(
        SimpleNamespace(upsert_key=["id"]), _mirror_opts(scope=["parent_id"])
    )


def test_check_scope_subset_of_upsert_key_noop_when_no_scope() -> None:
    check_scope_subset_of_upsert_key(
        SimpleNamespace(upsert_key=["id"]), _mirror_opts(strategy="tracked")
    )


def test_check_scope_subset_of_upsert_key_accepts_subset() -> None:
    check_scope_subset_of_upsert_key(
        SimpleNamespace(upsert_key=["parent_id", "id"]),
        _mirror_opts(strategy="tracked", scope=["parent_id"]),
    )


def test_check_scope_subset_of_upsert_key_rejects_non_subset() -> None:
    with pytest.raises(ValueError, match="mirror.scope columns must be part of"):
        check_scope_subset_of_upsert_key(
            SimpleNamespace(upsert_key=["id"]),
            _mirror_opts(strategy="tracked", scope=["parent_id"]),
        )
