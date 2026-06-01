"""Record-level diff for ``drt run --dry-run --diff`` (#413).

For queryable destinations (Postgres / MySQL / ClickHouse), computes a
true add/update/delete diff between the extracted source records and the
current destination state, keyed on ``upsert_key``.

For non-queryable destinations (REST API, Slack, HubSpot, etc.), falls
back to "sample mode" — shows the first ``limit`` records that would
be sent. Same flag, different depth.

Out of scope (tracked separately):
- Snowflake queryability (#468)
- Protocol method abstraction over hardcoded ``_QUERYABLE_TYPES`` (#469)
- Batch ``WHERE id IN (...)`` query optimisation (#470)
- ``--diff-fields`` column filter (#471)
- API-based diff for upsert-keyed SaaS destinations (#472)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from drt.config.models import DestinationConfig, SyncOptions
from drt.destinations.query import fetch_rows, get_table_name, is_queryable


@dataclass
class DiffResult:
    """Result of a record-level diff between source records and destination state.

    For queryable destinations, ``added`` / ``updated`` / ``deleted`` reflect
    the real comparison. For non-queryable destinations, only ``sample`` is
    populated (with ``supported=False`` and ``fallback_reason`` set).

    Lists are bounded by the ``limit`` parameter passed to :func:`compute_diff`;
    ``truncated`` is set when at least one list was capped.
    """

    # True-diff fields (queryable destinations)
    added: list[dict[str, Any]] = field(default_factory=list)
    updated: list[tuple[dict[str, Any], dict[str, Any]]] = field(default_factory=list)
    deleted: list[dict[str, Any]] = field(default_factory=list)

    # Fallback fields (non-queryable destinations)
    sample: list[dict[str, Any]] = field(default_factory=list)

    # Metadata
    total_source_rows: int = 0
    total_destination_rows: int = 0  # only meaningful when supported
    truncated: bool = False
    supported: bool = True
    fallback_reason: str | None = None

    @staticmethod
    def changed_fields(old: dict[str, Any], new: dict[str, Any]) -> dict[str, tuple[Any, Any]]:
        """Return the columns that differ between *old* and *new* as
        ``{col: (old_value, new_value)}``. Equal columns are omitted.

        Used by the renderer to show ``score: 0.5 → 0.95`` rather than
        every column on every updated row.
        """
        return {
            col: (old.get(col), new.get(col))
            for col in set(old) | set(new)
            if old.get(col) != new.get(col)
        }


def compute_diff(
    records: list[dict[str, Any]],
    config: DestinationConfig,
    sync_options: SyncOptions,
    limit: int = 20,
) -> DiffResult:
    """Compute a record-level diff for the given source records and destination.

    Args:
        records: Source records about to be written.
        config: Destination configuration.
        sync_options: Sync options (used to read ``mode`` for delete semantics).
        limit: Maximum number of records to include per category
            (added / updated / deleted / sample). Truncation is reported
            via :attr:`DiffResult.truncated`.

    Returns:
        :class:`DiffResult` populated with either a true diff (queryable
        destinations) or a sample of the source records (non-queryable).
    """
    # Non-queryable → sample mode
    if not is_queryable(config):
        sample = list(records[:limit])
        return DiffResult(
            sample=sample,
            total_source_rows=len(records),
            truncated=len(records) > limit,
            supported=False,
            fallback_reason=(
                f"True diff not available for destination type '{config.type}' "
                f"— showing a sample of records that would be sent."
            ),
        )

    # Queryable → true diff
    upsert_key: list[str] | None = getattr(config, "upsert_key", None)
    if not upsert_key:
        # Queryable but no upsert_key — can't key the diff. Treat as sample.
        sample = list(records[:limit])
        return DiffResult(
            sample=sample,
            total_source_rows=len(records),
            truncated=len(records) > limit,
            supported=False,
            fallback_reason=(
                f"upsert_key not configured for destination '{config.type}' "
                f"— showing a sample of records that would be written."
            ),
        )

    table = get_table_name(config)
    # Fetch the entire destination state (#470 may optimise this later).
    select_query = f"SELECT * FROM {table}"  # noqa: S608 — table from trusted config
    try:
        dest_rows = fetch_rows(config, select_query, columns=[])
    except Exception as e:
        return DiffResult(
            sample=list(records[:limit]),
            total_source_rows=len(records),
            truncated=len(records) > limit,
            supported=False,
            fallback_reason=f"Could not query destination ({type(e).__name__}): {e}",
        )

    # Build dest lookup keyed on upsert_key tuple
    dest_by_key: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in dest_rows:
        key = tuple(row.get(c) for c in upsert_key)
        dest_by_key[key] = row

    source_keys: set[tuple[Any, ...]] = set()
    added: list[dict[str, Any]] = []
    updated: list[tuple[dict[str, Any], dict[str, Any]]] = []

    for record in records:
        key = tuple(record.get(c) for c in upsert_key)
        source_keys.add(key)
        existing = dest_by_key.get(key)
        if existing is None:
            added.append(record)
        elif DiffResult.changed_fields(existing, record):
            updated.append((existing, record))
        # else: row matches destination exactly — no entry

    # Deleted is meaningful only when the engine would actually drop rows.
    # In replace mode, the destination table is rebuilt; rows that aren't
    # in the source effectively disappear. In full / incremental upsert
    # modes, dest-only rows are preserved, so reporting "deleted" would
    # be misleading.
    deleted: list[dict[str, Any]] = []
    if sync_options.mode == "replace":
        deleted = [row for key, row in dest_by_key.items() if key not in source_keys]

    truncated = len(added) > limit or len(updated) > limit or len(deleted) > limit

    return DiffResult(
        added=added[:limit],
        updated=updated[:limit],
        deleted=deleted[:limit],
        total_source_rows=len(records),
        total_destination_rows=len(dest_rows),
        truncated=truncated,
        supported=True,
    )
