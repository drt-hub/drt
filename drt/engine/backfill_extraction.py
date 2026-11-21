"""Bounded source-extraction helpers for ADR 0021 slice 2.

The sequential window loop intentionally does not live here yet. These
helpers prepare one bounded query or one fixed numeric ceiling for that later
loop while the slice-1 execution gate keeps ``run_sync`` fail-closed.
"""

from __future__ import annotations

from drt.config.profiles import ProfileConfigLike
from drt.engine.backfill import MAX_INT64
from drt.engine.resolver import build_numeric_ceiling_query, is_sql_source_profile
from drt.sources.base import Source

_MISSING = object()


def snapshot_numeric_ceiling(
    source: Source,
    resolved_model: str,
    profile: ProfileConfigLike,
    cursor_field: str,
    *,
    query_tags: dict[str, str] | None = None,
) -> int | None:
    """Snapshot ``MAX(cursor_field) + 1`` once for an integral SQL cursor.

    ``None`` means the model is empty (``MAX`` returned SQL NULL), so there is
    no numeric window to certify. API-shaped sources cannot infer a safe
    successor and must use an explicit ``sync.backfill.end`` instead.
    """
    if not is_sql_source_profile(profile):
        raise ValueError(
            "Numeric sync.backfill on a non-SQL source requires an explicit "
            "sync.backfill.end; drt cannot infer an exclusive numeric ceiling."
        )

    query = build_numeric_ceiling_query(resolved_model, profile, cursor_field)
    rows = iter(source.extract(query, profile, query_tags=query_tags))
    row = next(rows, None)
    if row is None:
        raise RuntimeError("Numeric sync.backfill MAX query returned no result row.")

    value = row.get("_drt_cursor_max", _MISSING)
    if value is _MISSING:
        # Some drivers case-fold unquoted result keys despite the quoted alias.
        value = next(
            (candidate for key, candidate in row.items() if key.casefold() == "_drt_cursor_max"),
            _MISSING,
        )
    if value is _MISSING:
        raise RuntimeError(
            "Numeric sync.backfill MAX query did not return the expected "
            "_drt_cursor_max result column."
        )
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(
            "Numeric sync.backfill requires an integral cursor_field; "
            f"MAX({cursor_field}) returned {value!r}."
        )
    if value >= MAX_INT64:
        raise ValueError(
            "Numeric sync.backfill cursor is at the 64-bit limit; set an explicit backfill.end."
        )
    return int(value) + 1
