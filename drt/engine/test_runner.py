"""Test runner — builds validation queries for drt test."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from jinja2 import BaseLoader, Environment, select_autoescape

from drt.config.models import (
    AcceptedValuesTest,
    FreshnessTest,
    NotNullTest,
    SyncTest,
    UniqueTest,
)


@dataclass
class TestResult:
    test_name: str
    passed: bool
    message: str


def test_display_name(test_def: SyncTest) -> str:
    """Human-readable label for a SyncTest definition.

    Shared between the ``drt test`` CLI and the ``drt_run_test`` MCP tool so
    both render identical labels.

    Every test type must have a branch here — a type with no branch silently
    falls through to ``"unknown"`` (#400 was exactly this drift: a new type
    landed without a matching display-name branch). Adding a type? Add a
    branch to both this function's name lookup below AND the fallback
    if-chain, and extend the coverage set in
    ``tests/unit/test_test_runner.py::test_display_name_covers_all_test_types``.

    An explicit ``name:`` (#779) takes priority for EVERY type, not just
    ``query`` — ``not_null(warn_one)`` rather than the auto-generated
    ``not_null(a, b)`` — so a test the operator bothered to name is always
    shown by that name.
    """
    if test_def.name:
        for field_name, type_word in _TEST_TYPE_WORDS:
            if getattr(test_def, field_name) is not None:
                return f"{type_word}({test_def.name})"
        return "unknown"
    if test_def.row_count is not None:
        parts = []
        if test_def.row_count.min is not None:
            parts.append(f"min={test_def.row_count.min}")
        if test_def.row_count.max is not None:
            parts.append(f"max={test_def.row_count.max}")
        return f"row_count({', '.join(parts)})"
    if test_def.not_null is not None:
        cols = ", ".join(test_def.not_null.columns)
        return f"not_null({cols})"
    if test_def.freshness is not None:
        return f"freshness({test_def.freshness.column}, max_age={test_def.freshness.max_age})"
    if test_def.unique is not None:
        cols = ", ".join(test_def.unique.columns)
        return f"unique({cols})"
    if test_def.accepted_values is not None:
        vals = ", ".join(test_def.accepted_values.values)
        return f"accepted_values({test_def.accepted_values.column}: {vals})"
    if test_def.query is not None:
        preview = " ".join(test_def.query.split())  # collapse newlines/indentation
        if len(preview) > 40:
            preview = preview[:39] + "…"
        return f"query({preview})"
    return "unknown"


# field name -> display type-word, in the same order as the if-chain above.
# Used only to resolve `type(name)` when an explicit `name:` is given.
_TEST_TYPE_WORDS: tuple[tuple[str, str], ...] = (
    ("row_count", "row_count"),
    ("not_null", "not_null"),
    ("freshness", "freshness"),
    ("unique", "unique"),
    ("accepted_values", "accepted_values"),
    ("query", "query"),
)


def _safe_table(table: str) -> str:
    """Basic table name validation."""
    for ch in table:
        if not (ch.isalnum() or ch in "._"):
            raise ValueError(f"Invalid character in table name: {ch!r}")
    return table


def _safe_column(column: str) -> str:
    """Basic column name validation."""
    for ch in column:
        if not (ch.isalnum() or ch in "._"):
            raise ValueError(f"Invalid character in column name: {ch!r}")
    return column


def _parse_max_age(max_age_str: str) -> timedelta:
    """Parse max_age string like '7 days', '1 hour', etc."""
    parts = max_age_str.strip().split()
    if len(parts) != 2:
        msg = (
            f"Invalid max_age format: {max_age_str!r}. "
            "Use format like '7 days' or '1 hour'"
        )
        raise ValueError(msg)

    value_str, unit = parts
    try:
        value = int(value_str)
    except ValueError:
        raise ValueError(f"Invalid max_age value: {value_str!r}. Must be an integer.")
    if value <= 0:
        raise ValueError(
            f"Invalid max_age value: {value_str!r}. Must be a positive integer."
        )

    unit_lower = unit.lower()
    if unit_lower in ("day", "days"):
        return timedelta(days=value)
    elif unit_lower in ("hour", "hours"):
        return timedelta(hours=value)
    elif unit_lower in ("minute", "minutes"):
        return timedelta(minutes=value)
    elif unit_lower in ("second", "seconds"):
        return timedelta(seconds=value)
    elif unit_lower in ("week", "weeks"):
        return timedelta(weeks=value)
    else:
        msg = (
            f"Unknown time unit: {unit!r}. "
            "Supported: days, hours, minutes, seconds, weeks"
        )
        raise ValueError(msg)


# ---------------------------------------------------------------------------
# WHERE-predicate builders — one per test type, each used by BOTH the
# COUNT(*) check (build_test_query) and the failing-rows sample
# (build_failing_rows_query, #779's --store-failures). Single source of truth
# per predicate so the two can never drift apart.
# ---------------------------------------------------------------------------


def _not_null_condition(nn: NotNullTest) -> str:
    safe_cols = [_safe_column(col) for col in nn.columns]
    return " OR ".join(f"{col} IS NULL" for col in safe_cols)


def _freshness_condition(fresh: FreshnessTest) -> str:
    safe_col = _safe_column(fresh.column)
    max_age_delta = _parse_max_age(fresh.max_age)
    threshold = datetime.now(timezone.utc) - max_age_delta
    return f"{safe_col} < '{threshold.isoformat()}'"


def _unique_duplicate_condition(uniq: UniqueTest, safe_table: str) -> str:
    cols = ", ".join(_safe_column(col) for col in uniq.columns)
    # Use portable GROUP BY + HAVING pattern (works on PostgreSQL, MySQL, BigQuery, ClickHouse)
    return (
        f"({cols}) IN ("
        f"  SELECT {cols} FROM {safe_table} "
        f"  GROUP BY {cols} HAVING COUNT(*) > 1"
        f")"
    )


def _accepted_values_condition(av: AcceptedValuesTest) -> str:
    safe_col = _safe_column(av.column)
    # Escape single quotes in values to prevent SQL injection
    escaped_values = [val.replace("'", "''") for val in av.values]
    placeholders = ", ".join(f"'{val}'" for val in escaped_values)
    return f"{safe_col} NOT IN ({placeholders})"


# `{{ table }}` only — intentionally not the full `drt.config.vars` surface
# (no `var()`): keeps this template deliberately tiny, matching the SQL
# template surface elsewhere in the engine. select_autoescape (not a bare
# `autoescape=False`) renders this SQL/text-safe without tripping the
# py/jinja2/autoescape-false CodeQL rule.
_QUERY_TEST_ENV = Environment(
    loader=BaseLoader(),
    autoescape=select_autoescape(default_for_string=False, default=False),
)


def render_query_test(query: str, table: str) -> str:
    """Render a `query:` test's `{{ table }}` placeholder to the qualified table.

    ``table`` must already be validated (``_safe_table``) by the caller — this
    function only renders, it does not itself guard against an unsafe table
    name. The query text is project config the operator wrote (same trust
    model as ``model:`` SQL, which is already rendered the same unsandboxed
    way elsewhere in the engine) — not runtime user input.
    """
    return _QUERY_TEST_ENV.from_string(query).render(table=table)


def build_failing_rows_query(test: SyncTest, table: str) -> str | None:
    """Return the SELECT that returns the FAILING rows for *test*, or ``None``
    when the type has no per-row failure concept.

    ``row_count`` is a whole-table aggregate — an out-of-range count has no
    single offending row, so there is nothing to sample there. Every other
    type (including ``query``) returns the same predicate text
    ``build_test_query`` wraps in ``COUNT(*)`` — this is the row-level view of
    that same check, used for ``--store-failures`` (#779).
    """
    safe_table = _safe_table(table)

    if test.not_null is not None:
        return f"SELECT * FROM {safe_table} WHERE {_not_null_condition(test.not_null)}"
    if test.freshness is not None:
        return f"SELECT * FROM {safe_table} WHERE {_freshness_condition(test.freshness)}"
    if test.unique is not None:
        return (
            f"SELECT * FROM {safe_table} "
            f"WHERE {_unique_duplicate_condition(test.unique, safe_table)}"
        )
    if test.accepted_values is not None:
        condition = _accepted_values_condition(test.accepted_values)
        return f"SELECT * FROM {safe_table} WHERE {condition}"
    if test.query is not None:
        return render_query_test(test.query, safe_table)
    return None  # row_count — aggregate check, no per-row failure query


def build_test_query(test: SyncTest, table: str) -> tuple[str, Callable[[int], bool]]:
    """Return (SQL query, check_function) for a test.

    The query returns a single integer.
    The check function returns True if the test passes.
    """
    safe_table = _safe_table(table)

    if test.row_count is not None:
        rc = test.row_count
        query = f"SELECT COUNT(*) FROM {safe_table}"

        def check_row_count(val: int) -> bool:
            if rc.min is not None and val < rc.min:
                return False
            if rc.max is not None and val > rc.max:
                return False
            return True

        return query, check_row_count

    if test.not_null is not None:
        query = f"SELECT COUNT(*) FROM {safe_table} WHERE {_not_null_condition(test.not_null)}"

        def check_not_null(val: int) -> bool:
            return val == 0

        return query, check_not_null

    if test.freshness is not None:
        query = f"SELECT COUNT(*) FROM {safe_table} WHERE {_freshness_condition(test.freshness)}"

        def check_freshness(val: int) -> bool:
            # Test passes if there are no stale rows
            return val == 0

        return query, check_freshness

    if test.unique is not None:
        query = (
            f"SELECT COUNT(*) FROM {safe_table} "
            f"WHERE {_unique_duplicate_condition(test.unique, safe_table)}"
        )

        def check_unique(val: int) -> bool:
            # Test passes if no duplicate rows exist
            return val == 0

        return query, check_unique

    if test.accepted_values is not None:
        query = (
            f"SELECT COUNT(*) FROM {safe_table} "
            f"WHERE {_accepted_values_condition(test.accepted_values)}"
        )

        def check_accepted_values(val: int) -> bool:
            # Test passes if there are no invalid values
            return val == 0

        return query, check_accepted_values

    if test.query is not None:
        # Contract (#779): the user's query returns the FAILING rows; 0 rows =
        # pass. Wrap in COUNT(*) to preserve the existing single-int execution
        # path (execute_test_query) unchanged — mirrors exactly how the
        # incremental cursor predicate is subquery-wrapped in
        # engine/resolver.py's `_drt_base`, right down to the `_drt_` alias
        # convention. A stray `;` in the user's SQL becomes a syntax error
        # inside this parenthesized subquery, rather than a second statement.
        rendered = render_query_test(test.query, safe_table)
        query = f"SELECT COUNT(*) FROM ({rendered}) AS _drt_query_test"

        def check_query(val: int) -> bool:
            return val == 0

        return query, check_query

    raise ValueError("No test type defined in SyncTest.")
