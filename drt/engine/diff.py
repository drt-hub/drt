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
from drt.destinations._mirror_state import diff_keys
from drt.destinations.query import (
    fetch_rows,
    fetch_rows_by_keys,
    fetch_tracked_state,
    get_table_name,
    is_queryable,
)


@dataclass
class DiffResult:
    """Result of a record-level diff between source records and destination state.

    For queryable destinations, ``added`` / ``updated`` / ``deleted`` reflect
    the real comparison. For non-queryable destinations, only ``sample`` is
    populated (with ``supported=False`` and ``fallback_reason`` set).

    ``deleted`` rows carry full destination columns in ``replace`` mode; for the
    tracked-mirror preview (#693) they carry the ``upsert_key`` columns only,
    since the state table records keys rather than rows.

    ``delete_reason`` names *why* those rows go away, because the two cases are
    not equally alarming: ``"replace"`` rows vanish as a side effect of
    rebuilding the table, while ``"mirror"`` rows are removed by explicit
    DELETE statements from a table the destination otherwise keeps. It stays
    ``None`` when nothing would be deleted.

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
    # Provenance of ``deleted``: "replace" | "mirror" | None (#693).
    # Defaults to None so pre-existing callers keep the legacy rendering.
    delete_reason: str | None = None

    @staticmethod
    def changed_fields(
        old: dict[str, Any], new: dict[str, Any]
    ) -> dict[str, tuple[Any, Any]]:
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


def _is_tracked_mirror(sync_options: SyncOptions) -> bool:
    """True for ``mode: mirror`` with ``mirror.strategy: tracked`` (#686).

    The ``destination`` strategy needs a full destination read to preview
    (tracked separately); only the tracked strategy is previewable read-only.
    """
    return (
        sync_options.mode == "mirror"
        and sync_options.mirror is not None
        and sync_options.mirror.strategy == "tracked"
    )


def _preview_tracked_mirror_deletes(
    config: DestinationConfig,
    sync_options: SyncOptions,
    upsert_key: list[str],
    source_keys: set[tuple[Any, ...]],
) -> list[dict[str, Any]]:
    """Read-only preview of the rows tracked mirror would DELETE (#693).

    Reads the drt-managed ``_drt_synced_keys`` state for this sync and returns
    ``previous - current`` as key-column dicts. Rows are key-only: the state
    table stores keys, not full rows, and re-reading the destination for the
    other columns would cost a second scan for a preview.

    No prior state (first run / absent table) means no deletes, matching the
    baseline semantics of ``BaseSqlDestination._finalize_mirror_tracked``. A
    failed state read degrades to "nothing to preview" rather than failing the
    diff — the same tone as the query-failure fallback above, but narrower: only
    the delete preview is lost, the add/update diff stands.
    """
    # Same derivation as ``_finalize_mirror_tracked``: the injected sync name,
    # falling back to the target table when the options were built standalone.
    sync_name = str(sync_options._sync_name or getattr(config, "table", "") or "")
    try:
        previous = fetch_tracked_state(config, sync_name)
    except Exception:
        return []
    if not previous:
        return []
    return [
        dict(zip(upsert_key, key)) for key in diff_keys(previous, list(source_keys))
    ]


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

    # Pre-compute the source key set. In non-replace modes this lets us fetch
    # only the destination rows whose key is in the source (#470), avoiding a
    # full-table scan on large destinations. In replace mode we still need the
    # whole table (deleted = dest rows whose key is NOT in the source), which a
    # keyed fetch can never see — so that path keeps the full ``SELECT *`` scan.
    source_keys: set[tuple[Any, ...]] = set()
    for record in records:
        source_keys.add(tuple(record.get(c) for c in upsert_key))

    # keyed fetch is sound only when ``deleted`` is not the source-key
    # complement (mode != "replace") and there are records to key on (we read
    # the column set from records[0]).
    use_keyed_fetch = sync_options.mode != "replace" and bool(records)
    try:
        if use_keyed_fetch:
            # Explicit columns (never []) so returned dicts are keyed — this
            # also sidesteps the fetch_rows(columns=[]) empty-dict trap.
            columns = list(records[0].keys())
            try:
                dest_rows = fetch_rows_by_keys(
                    config,
                    upsert_key,
                    list(source_keys),
                    columns=columns,
                )
            except NotImplementedError:
                # ClickHouse (different paramstyle) — fall back to full scan.
                # keyed fetch is an optimisation, never a correctness need.
                select_query = f"SELECT * FROM {table}"  # noqa: S608 — table from trusted config
                dest_rows = fetch_rows(config, select_query, columns=[])
        else:
            select_query = f"SELECT * FROM {table}"  # noqa: S608 — table from trusted config
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

    added: list[dict[str, Any]] = []
    updated: list[tuple[dict[str, Any], dict[str, Any]]] = []

    for record in records:
        key = tuple(record.get(c) for c in upsert_key)
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
    #
    # Mirror mode is the exception (#693): the engine *does* drop rows, but the
    # keyed fetch above structurally cannot see them (it only returns dest rows
    # whose key IS in the source), so the delete set comes from a separate
    # read — for the ``tracked`` strategy, the drt-managed state table.
    deleted: list[dict[str, Any]] = []
    delete_reason: str | None = None
    if sync_options.mode == "replace":
        deleted = [
            row for key, row in dest_by_key.items() if key not in source_keys
        ]
        delete_reason = "replace"
    elif _is_tracked_mirror(sync_options):
        deleted = _preview_tracked_mirror_deletes(
            config, sync_options, upsert_key, source_keys
        )
        delete_reason = "mirror"

    truncated = (
        len(added) > limit or len(updated) > limit or len(deleted) > limit
    )

    return DiffResult(
        added=added[:limit],
        updated=updated[:limit],
        deleted=deleted[:limit],
        total_source_rows=len(records),
        total_destination_rows=len(dest_rows),
        truncated=truncated,
        supported=True,
        # Only claim a reason when there is something to explain — an empty
        # delete set in replace mode is not a "replace deletion".
        delete_reason=delete_reason if deleted else None,
    )
