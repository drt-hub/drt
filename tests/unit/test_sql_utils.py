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
    """clickhouse (not snowflake, since #692 added Snowflake support) — a
    dialect this message still actually rejects."""
    msg = unsupported_tracked_scope_msg("clickhouse")
    assert msg == (
        "mirror.strategy: tracked / mirror.scope are not yet supported on clickhouse "
        "(supported: postgres, mysql, snowflake — see #692 follow-ups)."
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
    cfg = SimpleNamespace(upsert_key=["id"])
    with pytest.raises(ValueError, match="not yet supported on databricks"):
        check_mirror_supported(cfg, _mirror_opts(strategy="tracked"), "databricks")
    with pytest.raises(ValueError, match="not yet supported on clickhouse"):
        check_mirror_supported(cfg, _mirror_opts(scope=["parent_id"]), "clickhouse")


def test_check_mirror_supported_ok_for_plain_mirror() -> None:
    check_mirror_supported(SimpleNamespace(upsert_key=["id"]), _mirror_opts(), "snowflake")


def test_check_mirror_supported_allows_tracked_and_scope_when_dialect_opts_in() -> None:
    """#692 — a dialect passing supports_tracked_scope=True (Snowflake) is
    not rejected the way clickhouse/databricks still are."""
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
