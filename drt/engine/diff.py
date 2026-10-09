"""Record-level diff for ``drt run --dry-run --diff`` (#413).

For queryable destinations (Postgres / MySQL / Snowflake / ClickHouse /
Databricks), computes a
true add/update/delete diff between the extracted source records and the
current destination state, keyed on ``upsert_key``.

For non-queryable destinations (REST API, Slack, HubSpot, etc.), falls
back to "sample mode" — shows the first ``limit`` records that would
be sent. Same flag, different depth.

Out of scope (tracked separately):
- ``--diff-fields`` column filter (#471)
- API-based diff for upsert-keyed SaaS destinations (#472)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from drt.config.models import ClickHouseDestinationConfig, DestinationConfig, SyncOptions
from drt.destinations._mirror_state import decode_key, diff_keys
from drt.destinations.query import (
    fetch_all_keys,
    fetch_rows,
    fetch_rows_by_keys,
    fetch_tracked_state,
    get_table_name,
    is_queryable,
)


class _ResetToDestinationDefault:
    """Sentinel for a replace-mode column whose destination default is unknown.

    A record that omits a column during ``sync.mode: replace`` rebuilds that
    row from the source record. The resulting value is the destination's
    actual default, which the diff engine does not introspect; it is not
    necessarily ``None``.
    """

    def __repr__(self) -> str:
        return "<default>"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _ResetToDestinationDefault)

    def __hash__(self) -> int:
        return hash(_ResetToDestinationDefault)


RESET_TO_DESTINATION_DEFAULT = _ResetToDestinationDefault()


@dataclass
class DiffResult:
    """Result of a record-level diff between source records and destination state.

    For queryable destinations, ``added`` / ``updated`` / ``replaced`` /
    ``inserted`` / ``deleted`` reflect the real write shape. ``added`` is a
    new key from an upsert-style write; ``inserted`` is an append-only physical
    insert, even when its key already exists. For non-queryable destinations,
    only ``sample`` is populated (with ``supported=False`` and
    ``fallback_reason`` set).

    ``deleted`` rows carry full destination columns in ``replace`` mode; for both
    mirror previews (#693) they carry the ``upsert_key`` columns only, since those
    previews read keys rather than rows.

    ``delete_reason`` names *why* those rows go away, because the cases are not
    equally alarming — nor equally cheap to find out:

    - ``"replace"`` — rows vanish as a side effect of rebuilding the table.
    - ``"mirror"`` — the tracked strategy's explicit DELETEs, computed from
      drt's own state table, from a table the destination otherwise keeps.
    - ``"mirror_scan"`` — the destination strategy's explicit DELETEs. Same
      blast radius as ``"mirror"``, but establishing it required an *extra read
      of the destination's key set*, which the tracked preview never pays for.
      Naming it separately keeps that cost visible in both renderers instead of
      hiding a per-preview round trip behind an identical label.

    It stays ``None`` when nothing would be deleted.

    ``destination_keys_scanned`` records the extra key-only read needed by a
    destination-strategy mirror preview.  This is separate from
    ``total_destination_rows``: append-only writes do not fetch matching target
    rows, but their mirror delete preview can still scan target keys.  It remains
    true when that scan finds no deletion candidates, so the cost is not hidden.

    ``delete_preview_unavailable_reason`` is set when a mirror delete read
    fails. This is deliberately separate from ``deleted=[]``: the latter means
    the read succeeded and found no rows to remove, while the former means the
    add/update diff is still valid but the DELETE set is unknown.

    Lists are bounded by the ``limit`` parameter passed to :func:`compute_diff`;
    ``truncated`` is set when at least one list was capped.
    """

    # True-diff fields (queryable destinations)
    added: list[dict[str, Any]] = field(default_factory=list)
    updated: list[tuple[dict[str, Any], dict[str, Any]]] = field(default_factory=list)
    deleted: list[dict[str, Any]] = field(default_factory=list)
    # Kept after the legacy lists so existing positional constructors retain
    # their original ``added, updated, deleted`` ordering.
    replaced: list[tuple[dict[str, Any], dict[str, Any]]] = field(default_factory=list)
    inserted: list[dict[str, Any]] = field(default_factory=list)

    # Fallback fields (non-queryable destinations)
    sample: list[dict[str, Any]] = field(default_factory=list)

    # Metadata
    total_source_rows: int = 0
    # ``None`` means append-only rows did not need a matching-row read.  Their
    # mirror delete preview may still do a separate key-only scan below.
    total_destination_rows: int | None = 0
    truncated: bool = False
    supported: bool = True
    fallback_reason: str | None = None
    # Provenance of ``deleted``: "replace" | "mirror" | None (#693).
    # Defaults to None so pre-existing callers keep the legacy rendering.
    delete_reason: str | None = None
    delete_preview_unavailable_reason: str | None = None
    # A replace write rebuilds each existing row from its source record.
    # Omitted fields are therefore resets, unlike a partial update.
    writes_full_row: bool = False
    # Kept last so existing positional ``DiffResult`` constructors retain their
    # field ordering.  See the class docstring for why this can be true while
    # ``total_destination_rows`` remains ``None``.
    destination_keys_scanned: bool = False
    # How many rows the delete pass looked at: the denominator for a delete
    # percentage (#1218). ``None`` when the strategy cannot report it.
    delete_baseline: int | None = None

    @staticmethod
    def changed_fields(
        old: dict[str, Any], new: dict[str, Any], *, include_removed: bool = False
    ) -> dict[str, tuple[Any, Any]]:
        """Return the columns that differ between *old* and *new* as
        ``{col: (old_value, new_value)}``. Equal columns are omitted.

        Used by the renderer to show ``score: 0.5 → 0.95`` rather than
        every column on every updated row.

        The default compares the source record's own keys only. In a partial
        update, a target column omitted by this record remains untouched even
        when another heterogeneous batch record caused it to be fetched.
        Replace mode passes ``include_removed=True`` because it rebuilds the
        whole row and an omitted target column resets to its destination
        default.
        """
        changed = {col: (old.get(col), new[col]) for col in new if old.get(col) != new[col]}
        if include_removed:
            for col in old:
                if col not in new:
                    changed[col] = (old[col], RESET_TO_DESTINATION_DEFAULT)
        return changed


def _writes_full_row(config: DestinationConfig, sync_options: SyncOptions) -> bool:
    """Whether a matched record is rebuilt from source fields alone.

    Append-only writes intentionally do not qualify: they create a separate
    physical row (or fail a uniqueness constraint), not reset an existing row.
    """
    del config
    return sync_options.mode == "replace"


def _is_append_only(config: DestinationConfig, sync_options: SyncOptions) -> bool:
    """Whether this run physically appends records without matching target rows."""
    if sync_options.mode == "replace":
        return False
    if isinstance(config, ClickHouseDestinationConfig):
        return True
    # Snowflake and Databricks force MERGE for sync.mode: mirror even when
    # their destination config says mode: insert.
    return sync_options.mode != "mirror" and getattr(config, "mode", None) == "insert"


def _is_tracked_mirror(sync_options: SyncOptions) -> bool:
    """True for ``mode: mirror`` with ``mirror.strategy: tracked`` (#686).

    Previewable from drt's own state table, without touching the target rows.
    """
    return (
        sync_options.mode == "mirror"
        and sync_options.mirror is not None
        and sync_options.mirror.strategy == "tracked"
    )


def _is_destination_mirror(sync_options: SyncOptions) -> bool:
    """True for ``mode: mirror`` on the ``destination`` strategy (#340).

    That is the default: an omitted ``mirror:`` block, or an explicit
    ``strategy: destination``. ``mirror.scope`` (#687) is *not* a third strategy —
    it narrows this same path — so it is deliberately not part of the predicate.
    """
    return sync_options.mode == "mirror" and (
        sync_options.mirror is None or sync_options.mirror.strategy == "destination"
    )


def _is_diff_mirror(sync_options: SyncOptions) -> bool:
    """True for ``mode: mirror`` with ``mirror.strategy: diff`` (#1110).

    Previewable directly from ``sync_options._diff_removed_keys`` — no
    destination read needed at all, unlike ``tracked``/``destination``: the
    engine's ``incremental_strategy: diff`` extraction already computed the
    exact removed-key set before this function ever runs (see
    ``drt/engine/sync.py``), for both real and dry runs alike.
    """
    return (
        sync_options.mode == "mirror"
        and sync_options.mirror is not None
        and sync_options.mirror.strategy == "diff"
    )


def _observed_scopes(records: list[dict[str, Any]], scope_cols: list[str]) -> list[tuple[Any, ...]]:
    """The distinct ``mirror.scope`` value tuples these records would produce.

    Recomputed from the source records rather than read from
    ``BaseSqlDestination._mirror_scopes``: that set is accumulated inside
    ``_accumulate_mirror_state`` during ``load()``, and a dry run never calls
    ``load()``, so it would be empty here — which reads as "no scope observed"
    and would silently drop the scope narrowing. The derivation below is the same
    one ``_accumulate_mirror_state`` uses (``record.get(c)`` per scope column, so
    a missing column contributes ``None``); the real run rejects a missing scope
    column earlier via ``_validate_mirror_scope``.

    Deduped like the real path's ``set``, since the values only feed an ``IN``.
    """
    return list({tuple(record.get(c) for c in scope_cols) for record in records})


def _preview_destination_mirror_deletes(
    config: DestinationConfig,
    sync_options: SyncOptions,
    upsert_key: list[str],
    source_keys: set[tuple[Any, ...]],
    records: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], str | None, int | None]:
    """Read-only preview of the rows destination-strategy mirror would DELETE.

    The real pass is ``_build_mirror_delete(..., negate=True)``: "DELETE the rows
    whose key the source did not produce", optionally prefixed with a scope
    clause. So the preview reads the destination's key set (narrowed by the same
    scope clause) and returns ``dest_keys - source_keys`` as key-column dicts.

    This is the one strategy that costs an extra destination round trip — the
    ``NOT IN`` complement is invisible to #470's keyed fetch by construction — and
    that cost is surfaced to the user as ``delete_reason="mirror_scan"``.

    Rows are key-only for the same reason as the tracked preview: only keys are
    read, and re-reading full rows for a preview would cost a second scan.

    Any failure keeps the add/update comparison usable but returns an explicit
    unavailable reason. It must not collapse to a successful zero-delete
    preview: those two outcomes drive different deployment decisions.

    ClickHouse keys are compared as their string forms (caught in review,
    #1060, then narrowed to ClickHouse-only after a second review pass
    caught the first version comparing every dialect this way): the real
    ClickHouse DELETE (``_build_mirror_delete``) does the same on both the
    destination column and the bound source keys, via ``toString()`` — a
    typed column (e.g. UUID) fetched raw would otherwise compare unequal to
    the source's plain value and misreport a live row as a preview
    deletion. Coercing every dialect this way is wrong, not just
    unnecessary: a destination ``Decimal('1.00')`` and a source int ``1``
    are the same row under every other dialect's native comparison, but
    ``"1.00" != "1"`` as strings — that would have created a false
    deletion on a currently-correct path.
    """
    scope_cols = sync_options.mirror.scope if sync_options.mirror else None
    scopes = _observed_scopes(records, scope_cols) if scope_cols else None
    try:
        dest_keys = fetch_all_keys(config, upsert_key, scope_cols, scopes)
    except Exception as error:
        return [], f"{type(error).__name__}: {error}", None
    if isinstance(config, ClickHouseDestinationConfig):
        comparison_keys = {tuple(str(v) for v in key) for key in source_keys}

        def _normalize(key: tuple[Any, ...]) -> tuple[Any, ...]:
            return tuple(str(v) for v in key)
    else:
        comparison_keys = source_keys

        def _normalize(key: tuple[Any, ...]) -> tuple[Any, ...]:
            return key

    return (
        [dict(zip(upsert_key, key)) for key in dest_keys if _normalize(key) not in comparison_keys],
        None,
        len(dest_keys),
    )


def _preview_tracked_mirror_deletes(
    config: DestinationConfig,
    sync_options: SyncOptions,
    upsert_key: list[str],
    source_keys: set[tuple[Any, ...]],
    records: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], str | None, int | None]:
    """Read-only preview of the rows tracked mirror would DELETE (#693).

    Reads the drt-managed ``_drt_synced_keys`` state for this sync and returns
    ``previous - current`` as key-column dicts. With ``mirror.scope``, previous
    keys are first narrowed to the scope values observed in this run, matching
    ``BaseSqlDestination._finalize_mirror_tracked``: a run touching one scope
    must not preview deletions from another. Rows are key-only: the state table
    stores keys, not full rows, and re-reading the destination for the other
    columns would cost a second scan for a preview.

    No prior state (first run / absent table) means no deletes, matching the
    baseline semantics of ``BaseSqlDestination._finalize_mirror_tracked``. A
    failed state read leaves the add/update diff intact but returns an explicit
    unavailable reason, distinct from the successful first-run/empty-state case.
    """
    # Same derivation as ``_finalize_mirror_tracked``: the injected sync name,
    # falling back to the target table when the options were built standalone.
    sync_name = str(sync_options._sync_name or getattr(config, "table", "") or "")
    try:
        previous = fetch_tracked_state(config, sync_name)
        if not previous:
            return [], None, 0

        # Scope filtering (and the final diff) stay inside this same guard
        # (caught in review, #1061): decode_key()/diff_keys() can raise on
        # malformed or pre-#687 legacy key_json, and a mirror.scope column
        # that isn't actually a upsert_key member raises ValueError from
        # .index() — none of that should crash --dry-run --diff when the
        # add/update comparison is otherwise perfectly usable.
        scope_cols = sync_options.mirror.scope if sync_options.mirror else None
        if scope_cols:
            scope_positions = [upsert_key.index(column) for column in scope_cols]
            observed_scopes = set(_observed_scopes(records, scope_cols))
            previous = {
                key_hash: key_json
                for key_hash, key_json in previous.items()
                if tuple(decode_key(key_json)[position] for position in scope_positions)
                in observed_scopes
            }
        deleted_keys = diff_keys(previous, list(source_keys))
    except Exception as error:
        return [], f"{type(error).__name__}: {error}", None

    return (
        [dict(zip(upsert_key, key)) for key in deleted_keys],
        None,
        len(previous),
    )


def _append_only_diff(
    records: list[dict[str, Any]],
    config: DestinationConfig,
    sync_options: SyncOptions,
    upsert_key: list[str] | None,
    limit: int,
) -> DiffResult:
    """Preview append-only writes without comparing source keys to target rows.

    ClickHouse always appends. Snowflake and Databricks append when their
    destination ``mode`` is ``insert``. A duplicate key is still a new physical
    write in each of those cases, not an in-place update. ClickHouse mirror
    writes additionally need their independent DELETE preview, but that key
    read must not turn the appended source rows into matches.
    """
    source_keys = (
        {tuple(record.get(column) for column in upsert_key) for record in records}
        if upsert_key
        else set()
    )
    deleted: list[dict[str, Any]] = []
    delete_reason: str | None = None
    delete_preview_unavailable_reason: str | None = None
    destination_keys_scanned = False
    delete_baseline: int | None = None

    if sync_options.mode == "mirror":
        assert upsert_key is not None
        if _is_tracked_mirror(sync_options) and records:
            (
                deleted,
                delete_preview_unavailable_reason,
                delete_baseline,
            ) = _preview_tracked_mirror_deletes(
                config, sync_options, upsert_key, source_keys, records
            )
            delete_reason = "mirror"
        elif _is_destination_mirror(sync_options) and records:
            destination_keys_scanned = True
            (
                deleted,
                delete_preview_unavailable_reason,
                delete_baseline,
            ) = _preview_destination_mirror_deletes(
                config, sync_options, upsert_key, source_keys, records
            )
            delete_reason = "mirror_scan"
        elif _is_diff_mirror(sync_options):
            deleted = list(getattr(sync_options, "_diff_removed_keys", None) or [])
            delete_reason = "mirror"

    truncated = len(records) > limit or len(deleted) > limit
    return DiffResult(
        inserted=list(records[:limit]),
        deleted=deleted[:limit],
        total_source_rows=len(records),
        total_destination_rows=None,
        destination_keys_scanned=destination_keys_scanned,
        truncated=truncated,
        supported=True,
        delete_reason=delete_reason if deleted else None,
        delete_preview_unavailable_reason=delete_preview_unavailable_reason,
        delete_baseline=delete_baseline,
    )


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
        limit: Maximum number of records to include per category. Truncation
            is reported via :attr:`DiffResult.truncated`.

    Returns:
        :class:`DiffResult` populated with either a true diff (queryable
        destinations) or a sample of the source records (non-queryable).
    """
    upsert_key: list[str] | None = getattr(config, "upsert_key", None)
    # An append-only destination never looks up a matching target row: the
    # write creates a physical row even if the incoming key already exists.
    # Mirror still requires an upsert key for its separate DELETE preview, so
    # preserve the configuration fallback below when that key is absent.
    if _is_append_only(config, sync_options) and (
        sync_options.mode != "mirror" or upsert_key is not None
    ):
        return _append_only_diff(records, config, sync_options, upsert_key, limit)

    # Non-queryable → sample mode. This deliberately comes after the
    # append-only branch: BigQuery's streaming ``mode: insert`` does not
    # implement target reads, but its write shape is still known exactly.
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
    # #1064: hint every known source field, not just upsert_key — a
    # non-key column can suffer the identical case-folding collapse as a
    # key column on the dialects fetch_rows() reconciles for (Snowflake).
    # Built from every record's keys, not just records[0] (caught in Codex
    # review): dry_run_records accumulates across the whole run, and source
    # rows can legitimately have heterogeneous optional fields, so an
    # earlier version that only read records[0] missed a field that first
    # appeared in a later row — always includes upsert_key too, so an
    # uppercase-configured key is hinted even if the very first record
    # happened to omit it.
    field_hint = sorted({*upsert_key, *(k for record in records for k in record)})
    try:
        if use_keyed_fetch:
            # This is a read-only column projection, so fetch the batch-wide
            # field union. The real write groups heterogeneous records by
            # signature; ``changed_fields()`` below uses only the current
            # record's keys for partial updates, avoiding the rejected
            # union-based *comparison* that would report omitted fields as
            # ``value → None``.
            columns = field_hint
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
                dest_rows = fetch_rows(config, select_query, columns=[], field_hint=field_hint)
        else:
            select_query = f"SELECT * FROM {table}"  # noqa: S608 — table from trusted config
            dest_rows = fetch_rows(config, select_query, columns=[], field_hint=field_hint)
    except Exception as e:
        return DiffResult(
            sample=list(records[:limit]),
            total_source_rows=len(records),
            truncated=len(records) > limit,
            supported=False,
            # Exception class name only, not str(e) -- #778 review: this
            # reaches --dry-run --diff's persisted run_results.json
            # artifact today, and a driver error routinely embeds a DSN,
            # host, or credential in its message.
            fallback_reason=f"Could not query destination ({type(e).__name__})",
        )

    # Build dest lookup keyed on upsert_key tuple
    dest_by_key: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in dest_rows:
        key = tuple(row.get(c) for c in upsert_key)
        dest_by_key[key] = row

    added: list[dict[str, Any]] = []
    updated: list[tuple[dict[str, Any], dict[str, Any]]] = []
    replaced: list[tuple[dict[str, Any], dict[str, Any]]] = []
    writes_full_row = _writes_full_row(config, sync_options)

    for record in records:
        key = tuple(record.get(c) for c in upsert_key)
        existing = dest_by_key.get(key)
        if existing is None:
            added.append(record)
        elif DiffResult.changed_fields(existing, record, include_removed=writes_full_row):
            if writes_full_row:
                replaced.append((existing, record))
            else:
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
    # whose key IS in the source), so the delete set comes from a separate read —
    # the drt-managed state table for the ``tracked`` strategy, the destination's
    # own key set for the ``destination`` strategy (incl. ``mirror.scope``).
    deleted: list[dict[str, Any]] = []
    delete_reason: str | None = None
    delete_preview_unavailable_reason: str | None = None
    destination_keys_scanned = False
    delete_baseline = None
    if sync_options.mode == "replace":
        deleted = [row for key, row in dest_by_key.items() if key not in source_keys]
        delete_reason = "replace"
        delete_baseline = len(dest_by_key)
    # ``and records`` on both mirror legs: ``_finalize_mirror`` returns early
    # when no key was observed (``if not self._mirror_keys: return None``), and
    # that guard sits *above* the tracked dispatch — so a transient empty source
    # deletes nothing, on either strategy. Previewing a full wipe would tell the
    # operator the opposite of what the run would do.
    elif _is_tracked_mirror(sync_options) and records:
        (
            deleted,
            delete_preview_unavailable_reason,
            delete_baseline,
        ) = _preview_tracked_mirror_deletes(config, sync_options, upsert_key, source_keys, records)
        delete_reason = "mirror"
    elif _is_destination_mirror(sync_options) and records:
        destination_keys_scanned = True
        (
            deleted,
            delete_preview_unavailable_reason,
            delete_baseline,
        ) = _preview_destination_mirror_deletes(
            config, sync_options, upsert_key, source_keys, records
        )
        delete_reason = "mirror_scan"
    elif _is_diff_mirror(sync_options):
        # #1110, caught in Codex review: unlike tracked/destination, this
        # never reads the destination at all — sync.incremental_strategy:
        # diff already computed the exact removed-row set during extraction
        # (drt/engine/sync.py smuggles it onto sync_options._diff_removed_keys
        # the same way for both dry and real runs), so the preview is just
        # what's already there, not a separate read.
        deleted = list(getattr(sync_options, "_diff_removed_keys", None) or [])
        delete_reason = "mirror"

    truncated = (
        len(added) > limit or len(updated) > limit or len(replaced) > limit or len(deleted) > limit
    )

    return DiffResult(
        added=added[:limit],
        updated=updated[:limit],
        replaced=replaced[:limit],
        deleted=deleted[:limit],
        total_source_rows=len(records),
        total_destination_rows=len(dest_rows),
        destination_keys_scanned=destination_keys_scanned,
        truncated=truncated,
        supported=True,
        # Only claim a reason when there is something to explain — an empty
        # delete set in replace mode is not a "replace deletion".
        delete_reason=delete_reason if deleted else None,
        delete_preview_unavailable_reason=delete_preview_unavailable_reason,
        writes_full_row=writes_full_row,
        delete_baseline=delete_baseline,
    )
