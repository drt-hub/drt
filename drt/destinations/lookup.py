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
    """
    dest_match_cols = list(lookup.match.keys())
    select_cols = dest_match_cols + [lookup.select]
    cols_str = ", ".join(select_cols)
    query = f"SELECT {cols_str} FROM {lookup.table}"  # noqa: S608

    rows = fetch_rows(config, query, select_cols)

    mapping: dict[tuple[Any, ...], Any] = {}
    for row in rows:
        key = tuple(row[c] for c in dest_match_cols)
        mapping[key] = row[lookup.select]
    return mapping


def apply_lookups(
    records: list[dict[str, Any]],
    lookup_maps: dict[str, tuple[LookupConfig, dict[tuple[Any, ...], Any]]],
    on_error: str,
) -> tuple[list[dict[str, Any]], list[RowError]]:
    """Enrich records with resolved lookup values.

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
                record[target_col] = mapping[key]
            elif lk_config.on_miss == "null":
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

        enriched.append(record)

    return enriched, errors
