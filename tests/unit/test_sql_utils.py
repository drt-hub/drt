"""Tests for the shared SQL-destination helpers (#722)."""

from __future__ import annotations

from drt.destinations.sql_utils import (
    MIRROR_UPSERT_KEY_MSG,
    RowCountable,
    backtick_quote_ident,
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
    msg = unsupported_tracked_scope_msg("snowflake")
    assert msg == (
        "mirror.strategy: tracked / mirror.scope are not yet supported on snowflake "
        "(supported: postgres, mysql — see #686 follow-ups)."
    )
