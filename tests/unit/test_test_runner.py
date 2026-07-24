"""Tests for drt test runner."""

from __future__ import annotations

import datetime

import pytest
from pydantic import ValidationError

from drt.config.models import (
    AcceptedValuesTest,
    FreshnessTest,
    NotNullTest,
    RowCountTest,
    SyncTest,
    UniqueTest,
)
from drt.engine.test_runner import build_failing_rows_query, build_test_query, render_query_test
from drt.engine.test_runner import test_display_name as get_test_display_name


def test_build_row_count_min() -> None:
    t = SyncTest(row_count=RowCountTest(min=1))
    query, check = build_test_query(t, "public.users")
    assert "COUNT(*)" in query
    assert check(5) is True
    assert check(0) is False


def test_build_row_count_max() -> None:
    t = SyncTest(row_count=RowCountTest(max=100))
    _, check = build_test_query(t, "public.users")
    assert check(50) is True
    assert check(101) is False


def test_build_row_count_min_max() -> None:
    t = SyncTest(row_count=RowCountTest(min=10, max=100))
    _, check = build_test_query(t, "public.users")
    assert check(50) is True
    assert check(5) is False
    assert check(101) is False


def test_build_not_null() -> None:
    t = SyncTest(not_null=NotNullTest(columns=["id", "name"]))
    query, check = build_test_query(t, "public.users")
    assert "id" in query
    assert "name" in query
    assert "NULL" in query.upper()
    assert check(0) is True
    assert check(3) is False


def test_build_unknown_test_raises() -> None:
    with pytest.raises(ValueError, match="Exactly one sync test must be configured"):
        SyncTest()


def test_safe_table_rejects_injection() -> None:
    t = SyncTest(row_count=RowCountTest(min=1))
    with pytest.raises(ValueError, match="Invalid character"):
        build_test_query(t, "users; DROP TABLE")


# ---------------------------------------------------------------------------
# query test type (#779)
# ---------------------------------------------------------------------------


def test_build_query_wraps_in_count() -> None:
    """The user's failing-rows query is wrapped in COUNT(*) so it reuses the
    existing single-int execute_test_query path unchanged (#779)."""
    t = SyncTest(query="SELECT * FROM {{ table }} WHERE total < 0")
    query, check = build_test_query(t, "public.orders")
    assert query == (
        "SELECT COUNT(*) FROM (SELECT * FROM public.orders WHERE total < 0) "
        "AS _drt_query_test"
    )
    assert check(0) is True  # 0 failing rows = pass
    assert check(1) is False
    assert check(500) is False


def test_query_table_template_renders() -> None:
    assert (
        render_query_test("SELECT * FROM {{ table }} WHERE x = 1", "public.orders")
        == "SELECT * FROM public.orders WHERE x = 1"
    )


def test_query_without_table_template_is_untouched() -> None:
    """Arbitrary SQL that never references {{ table }} is valid — the query
    type doesn't require it, only offers it."""
    sql = "SELECT * FROM other_schema.audit_log WHERE severity = 'critical'"
    assert render_query_test(sql, "public.orders") == sql


def test_query_type_table_name_still_validated() -> None:
    """{{ table }} renders the SAME _safe_table-validated value every other
    test type gets — no new injection surface via the table argument."""
    t = SyncTest(query="SELECT * FROM {{ table }}")
    with pytest.raises(ValueError, match="Invalid character"):
        build_test_query(t, "users; DROP TABLE")


def test_query_requires_non_empty_string() -> None:
    with pytest.raises(ValidationError):
        SyncTest(query="")


# ---------------------------------------------------------------------------
# build_failing_rows_query (#779 --store-failures) — must never drift from
# the COUNT(*) predicate build_test_query uses for the same test definition.
# ---------------------------------------------------------------------------


def test_failing_rows_row_count_is_none() -> None:
    """row_count is a whole-table aggregate — no per-row failure concept."""
    t = SyncTest(row_count=RowCountTest(min=1))
    assert build_failing_rows_query(t, "users") is None


@pytest.mark.parametrize(
    "make_test",
    [
        lambda: SyncTest(not_null=NotNullTest(columns=["id", "email"])),
        lambda: SyncTest(freshness=FreshnessTest(column="updated_at", max_age="7 days")),
        lambda: SyncTest(unique=UniqueTest(columns=["id"])),
        lambda: SyncTest(
            accepted_values=AcceptedValuesTest(column="status", values=["active"])
        ),
        lambda: SyncTest(query="SELECT * FROM {{ table }} WHERE total < 0"),
    ],
)
def test_failing_rows_predicate_matches_count_predicate(
    make_test, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The COUNT(*) check must be exactly a COUNT(*) wrap of the failing-rows
    sample query — single source of truth, no drift risk (#779).

    build_test_query(), called WITHOUT a precomputed ``failing_rows_query``,
    computes the rows query internally by calling build_failing_rows_query()
    itself — so a standalone build_failing_rows_query() call here (mimicking
    a caller who, unlike --store-failures, never threads the precomputed
    query through) still independently recomputes the predicate.
    freshness's predicate embeds ``datetime.now()``, read fresh on every
    call — so these two independent computations can observe a different
    instant and produce a microseconds-apart timestamp (this exact drift
    flaked in CI before the fix). Freeze the clock so both computations share
    an instant — the OTHER 4 types have no time dependency, so freezing is a
    no-op for them. Production code (drt/cli/commands/test.py) never takes
    this two-independent-calls path; see
    test_build_test_query_freshness_shares_single_clock_read for the actual
    shared-computation contract it relies on.
    """
    import drt.engine.test_runner as test_runner_module

    class _FrozenDatetime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):  # noqa: ANN001 — matches datetime.now's signature
            return cls(2026, 1, 1, tzinfo=tz)

    monkeypatch.setattr(test_runner_module, "datetime", _FrozenDatetime)

    test = make_test()
    table = "users"
    count_query, _ = build_test_query(test, table)
    rows_query = build_failing_rows_query(test, table)
    assert rows_query is not None
    alias = "_drt_query_test" if test.query is not None else "_drt_row_test"
    assert count_query == f"SELECT COUNT(*) FROM ({rows_query}) AS {alias}"


def test_failing_rows_query_selects_star() -> None:
    t = SyncTest(not_null=NotNullTest(columns=["email"]))
    rows_query = build_failing_rows_query(t, "users")
    assert rows_query is not None
    assert rows_query.startswith("SELECT * FROM users WHERE")


# ---------------------------------------------------------------------------
# Shared predicate computation (#779, masukai's CI review) — build_test_query
# derives its COUNT(*) by wrapping build_failing_rows_query's OWN output,
# rather than rebuilding the WHERE condition a second time. This makes the
# freshness now()-drift structurally impossible instead of merely unlikely:
# there is only ever one place the predicate gets computed per call.
# ---------------------------------------------------------------------------


def test_build_test_query_computes_freshness_condition_only_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression guard: a single build_test_query() call must invoke the
    time-relative condition builder exactly once, never twice internally."""
    import drt.engine.test_runner as test_runner_module

    calls: list[None] = []
    original = test_runner_module._freshness_condition

    def _counting(fresh):
        calls.append(None)
        return original(fresh)

    monkeypatch.setattr(test_runner_module, "_freshness_condition", _counting)

    t = SyncTest(freshness=FreshnessTest(column="updated_at", max_age="1 hour"))
    build_test_query(t, "users")
    assert len(calls) == 1


def test_build_test_query_reuses_provided_failing_rows_query() -> None:
    """When the caller already computed the rows query (--store-failures),
    build_test_query must wrap that exact string verbatim rather than
    recomputing the predicate — proven here with a string that deliberately
    does NOT match what build_failing_rows_query would generate, so a silent
    recomputation would make this assertion fail."""
    t = SyncTest(not_null=NotNullTest(columns=["email"]))
    precomputed = "SELECT * FROM users WHERE some_other_column IS NULL"
    query, check = build_test_query(t, "users", failing_rows_query=precomputed)
    assert query == f"SELECT COUNT(*) FROM ({precomputed}) AS _drt_row_test"
    assert check(0) is True
    assert check(2) is False


def test_build_test_query_freshness_shares_single_clock_read() -> None:
    """Compute build_failing_rows_query for freshness ONCE (as --store-failures
    now does in drt/cli/commands/test.py), then confirm build_test_query
    wraps that exact string — no second datetime.now() read, so the count
    check and the stored failure sample can never disagree (#779)."""
    t = SyncTest(freshness=FreshnessTest(column="updated_at", max_age="1 hour"))
    rows_query = build_failing_rows_query(t, "users")
    assert rows_query is not None
    query, check = build_test_query(t, "users", failing_rows_query=rows_query)
    assert query == f"SELECT COUNT(*) FROM ({rows_query}) AS _drt_row_test"
    assert check(0) is True


# ---------------------------------------------------------------------------
# test_display_name — query type + #400's "the map must cover every type"
# ---------------------------------------------------------------------------


def test_display_name_query_with_explicit_name() -> None:
    t = SyncTest(query="SELECT 1", name="no_negative_totals")
    assert get_test_display_name(t) == "query(no_negative_totals)"


def test_display_name_query_without_name_previews_sql() -> None:
    t = SyncTest(query="SELECT * FROM t WHERE total < 0")
    assert get_test_display_name(t) == "query(SELECT * FROM t WHERE total < 0)"


def test_display_name_query_preview_truncates_long_sql() -> None:
    long_sql = "SELECT * FROM t WHERE " + "x = 1 AND " * 10 + "y = 2"
    name = get_test_display_name(SyncTest(query=long_sql))
    assert name.startswith("query(")
    assert name.endswith("…)")
    assert len(name) < len(long_sql)


def test_display_name_never_unknown_for_a_real_test() -> None:
    """Every SyncTest instance actually constructible today must get a real
    label, never the 'unknown' fallback (that fallback exists only for the
    theoretical case of a SyncTest with a type slot the map hasn't caught up
    to — #400)."""
    assert get_test_display_name(SyncTest(query="SELECT 1")) != "unknown"


# Structural drift guard (#400): every SyncTest field that selects a test TYPE
# (i.e. every field except the two modifiers `name`/`severity`) must appear
# here. If someone adds a new type field without updating this set (and
# test_display_name), this test fails loudly instead of the display silently
# falling through to "unknown" in production — the exact #400 bug shape.
_EXPECTED_TEST_TYPE_FIELDS = frozenset(
    {"row_count", "not_null", "freshness", "unique", "accepted_values", "query"}
)


def test_display_name_covers_all_test_types() -> None:
    actual_fields = frozenset(SyncTest.model_fields) - {"name", "severity"}
    assert actual_fields == _EXPECTED_TEST_TYPE_FIELDS, (
        "SyncTest gained/lost a test-type field without updating "
        "_EXPECTED_TEST_TYPE_FIELDS here — update this set AND "
        "test_display_name()/build_test_query()/build_failing_rows_query() "
        "in drt/engine/test_runner.py (the #400 drift class)."
    )

    examples = {
        "row_count": SyncTest(row_count=RowCountTest(min=1)),
        "not_null": SyncTest(not_null=NotNullTest(columns=["id"])),
        "freshness": SyncTest(freshness=FreshnessTest(column="updated_at", max_age="1 day")),
        "unique": SyncTest(unique=UniqueTest(columns=["id"])),
        "accepted_values": SyncTest(
            accepted_values=AcceptedValuesTest(column="status", values=["active"])
        ),
        "query": SyncTest(query="SELECT 1"),
    }
    assert set(examples) == _EXPECTED_TEST_TYPE_FIELDS  # the fixture itself stays exhaustive
    for field_name, test in examples.items():
        name = get_test_display_name(test)
        assert name != "unknown", f"{field_name} has no test_display_name() branch"


# ---------------------------------------------------------------------------
# severity (#779) — default + validation
# ---------------------------------------------------------------------------


def test_severity_defaults_to_error() -> None:
    assert SyncTest(row_count=RowCountTest(min=1)).severity == "error"


def test_severity_accepts_warn() -> None:
    assert SyncTest(row_count=RowCountTest(min=1), severity="warn").severity == "warn"


def test_severity_rejects_unknown_value() -> None:
    with pytest.raises(ValidationError):
        SyncTest(row_count=RowCountTest(min=1), severity="critical")
