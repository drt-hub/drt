"""Destination lookup — resolve FK values by querying the destination DB.

When syncing related tables via reverse ETL, child tables often reference
a parent table's auto-increment ID that the source warehouse doesn't know.
This module queries the destination DB once per lookup to build an in-memory
mapping, then enriches each source row with the resolved value.

Example YAML::

    destination:
      type: mysql
      table: profile_source_sessions
      upsert_key: [interviewer_profile_id, candidate_interview_id]
      lookups:
        interviewer_profile_id:
          table: interviewer_profiles
          match: { user_id: user_id }
          select: id
          on_miss: skip
"""

from __future__ import annotations

import json
from typing import Any

from drt.config.models import DestinationConfig, LookupConfig
from drt.destinations.query import fetch_rows
from drt.destinations.row_errors import RowError


def build_lookup_map(
    config: DestinationConfig,
    lookup: LookupConfig,
) -> dict[tuple[Any, ...], Any]:
    """Query the destination DB and return a mapping for FK resolution.

    Executes a single SELECT to fetch all rows from the lookup table,
    then builds ``{(match_col_values,): select_value}`` in memory.

    When ``lookup.check_only`` is ``True``, only the match columns are
    selected and the mapping values are ``None`` — only key membership
    is meaningful (existence-only filtering).
    """
    dest_match_cols = list(lookup.match.keys())
    if lookup.check_only:
        select_cols = list(dest_match_cols)
    else:
        assert lookup.select is not None  # enforced by LookupConfig validator
        select_cols = dest_match_cols + [lookup.select]
    cols_str = ", ".join(select_cols)
    query = f"SELECT {cols_str} FROM {lookup.table}"  # noqa: S608

    rows = fetch_rows(config, query, select_cols)

    mapping: dict[tuple[Any, ...], Any] = {}
    for row in rows:
        key = tuple(row[c] for c in dest_match_cols)
        if lookup.check_only:
            mapping[key] = None
        else:
            assert lookup.select is not None
            mapping[key] = row[lookup.select]
    return mapping


def detect_ambiguous_lookup_ordering(
    lookups: dict[str, LookupConfig],
) -> list[str]:
    """Detect lookups whose evaluation order changes row fate (#453).

    When multiple lookups share the same source column but have different
    ``on_miss`` policies, ``apply_lookups`` evaluates them in YAML
    insertion order and the **first miss wins** — its policy decides the
    row's fate, and the remaining lookups for that row are not evaluated.

    This means a YAML key reorder can flip the row's outcome (e.g. from
    ``fail`` to ``skip``) without any other config change, which is
    surprising. This helper returns one human-readable warning per
    ambiguous source column so callers can log at sync startup.

    Args:
        lookups: The destination's ``lookups`` mapping (``target_col -> LookupConfig``).

    Returns:
        One warning string per source column that participates in
        multiple lookups with differing policies. Empty when no ambiguity
        is detected.
    """
    if not lookups or len(lookups) < 2:
        return []

    # Row fate on miss is determined solely by on_miss — check_only only
    # affects the HIT path (whether the resolved value is written). So
    # `{skip, skip+check_only}` produces identical outcomes and should not
    # warn; `{skip, fail}` does.
    by_source: dict[str, list[tuple[str, str]]] = {}
    for target_col, lk in lookups.items():
        for source_col in lk.match.values():
            by_source.setdefault(source_col, []).append((target_col, lk.on_miss))

    warnings: list[str] = []
    for source_col, entries in by_source.items():
        if len(entries) < 2:
            continue
        on_miss_policies = {on_miss for _, on_miss in entries}
        if len(on_miss_policies) > 1:
            targets = [t for t, _ in entries]
            warnings.append(
                f"Lookups {targets} all match on source column '{source_col}' "
                f"but have differing on_miss policies "
                f"({sorted(on_miss_policies)}). "
                f"apply_lookups uses first-miss-wins in YAML insertion order, "
                f"so reordering these keys can change row fate. "
                f"Place check_only lookups first when in doubt — see issue #453."
            )
    return warnings


def apply_lookups(
    records: list[dict[str, Any]],
    lookup_maps: dict[str, tuple[LookupConfig, dict[tuple[Any, ...], Any]]],
    on_error: str,
) -> tuple[list[dict[str, Any]], list[RowError]]:
    """Enrich records with resolved lookup values.

    **Ordering invariant:** lookups are evaluated in YAML insertion order
    (preserved by ``dict``). For a row that misses on more than one
    lookup, the **first miss wins** — that lookup's ``on_miss`` policy
    decides the row's fate, and remaining lookups are not evaluated.
    When multiple lookups share a source column with different policies,
    reordering keys can therefore flip outcomes (see #453); use
    :func:`detect_ambiguous_lookup_ordering` at config-load time to warn.

    Args:
        records: Source rows to enrich.
        lookup_maps: ``{target_col: (LookupConfig, mapping)}``
        on_error: Sync-level error handling (``"skip"`` or ``"fail"``).

    Returns:
        Tuple of (enriched records, row-level errors).
        Skipped rows are excluded from the returned list.
    """
    enriched: list[dict[str, Any]] = []
    errors: list[RowError] = []

    for i, record in enumerate(records):
        skip = False
        fail = False

        for target_col, (lk_config, mapping) in lookup_maps.items():
            source_cols = list(lk_config.match.values())
            key = tuple(record.get(c) for c in source_cols)

            if key in mapping:
                if not lk_config.check_only:
                    record[target_col] = mapping[key]
            elif lk_config.on_miss == "null":
                # check_only + on_miss=null is rejected at config-load time
                # by LookupConfig._check_on_miss_consistency, so target_col
                # here always corresponds to a real value-resolving lookup.
                record[target_col] = None
            elif lk_config.on_miss == "fail":
                errors.append(
                    RowError(
                        batch_index=i,
                        record_preview=json.dumps(record, default=str)[:200],
                        http_status=None,
                        error_message=(
                            f"Lookup miss: {target_col} (table={lk_config.table}, key={key})"
                        ),
                    )
                )
                fail = True
                break
            else:
                # on_miss == "skip" (default)
                errors.append(
                    RowError(
                        batch_index=i,
                        record_preview=json.dumps(record, default=str)[:200],
                        http_status=None,
                        error_message=(
                            f"Lookup miss (skipped): {target_col} "
                            f"(table={lk_config.table}, key={key})"
                        ),
                    )
                )
                skip = True
                break

        if fail:
            if on_error == "fail":
                return enriched, errors
            continue
        if skip:
            continue

        # Drop match columns that were only used for lookup resolution.
        # check_only lookups are filter-only — their target name is just a label,
        # so they neither contribute drops nor block other lookups' drops.
        cols_to_drop: set[str] = set()
        all_target_cols = {
            tc for tc, (lk, _) in lookup_maps.items() if not lk.check_only
        }
        for target_col, (lk_config, _mapping) in lookup_maps.items():
            if lk_config.check_only:
                continue
            if lk_config.drop_match_columns:
                for source_col in lk_config.match.values():
                    if source_col != target_col and source_col not in all_target_cols:
                        cols_to_drop.add(source_col)
        for col in cols_to_drop:
            record.pop(col, None)

        enriched.append(record)

    return enriched, errors
