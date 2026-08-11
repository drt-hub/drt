"""Opt-in engine-injected metadata columns for the sync engine (#762).

Unlike ``computed_fields`` (user Jinja templates over source data),
``metadata_columns`` writes engine-owned facts about the run itself —
``synced_at`` / ``run_id`` / ``sync_name`` — so there is nothing to render
and nothing that can fail: no I/O, no observer side effects, no error path.

**Pipeline position: last.** Applied after ``computed_fields``,
``field_mappings``, and ``mask`` (see ``engine/sync.py``), because the
column names here are already destination-facing — chosen directly in
``MetadataColumnsConfig``, not derived from a source column — and the
values are drt's own bookkeeping, not source data that a rename or masking
rule configured for source fields should ever touch.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from drt.config.sync_options import MetadataColumnsConfig


def apply_metadata_columns(
    records: list[dict[str, Any]],
    config: MetadataColumnsConfig | None,
    *,
    synced_at: str,
    run_id: str | None,
    sync_name: str,
) -> list[dict[str, Any]]:
    """Add the configured engine metadata columns to each record, in place.

    Args:
        records: Records after computed_fields, field_mappings, and mask.
        config: ``None`` means the feature is off (no columns added).
        synced_at: The run's UTC start timestamp — one value per
            ``run_sync()`` call, shared by every record it writes.
        run_id: The CLI-invocation-level id, or ``None`` for library callers
            that didn't pass one (same nullability as ``SyncResult.run_id``).
        sync_name: The sync's own name.

    Returns:
        ``records``, mutated in place and returned for chaining symmetry
        with the other transform steps.
    """
    if config is None:
        return records

    values: dict[str, Any] = {}
    if config.synced_at:
        values[config.synced_at] = synced_at
    if config.run_id:
        values[config.run_id] = run_id
    if config.sync_name:
        values[config.sync_name] = sync_name

    if not values:
        return records

    for record in records:
        record.update(values)

    return records
