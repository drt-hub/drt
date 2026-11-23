"""Static destination-key safety checks shared by plan and backfill."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DestinationKeyOverlaps:
    """Destination-facing upsert-key columns written by transforms."""

    run_varying_metadata: tuple[str, ...] = ()
    computed_fields: tuple[str, ...] = ()


def destination_key_overlaps(options: Any, upsert_key: list[str] | None) -> DestinationKeyOverlaps:
    """Find unsafe transform outputs in a destination-facing upsert key.

    ``metadata_columns`` targets are already destination-facing. Computed
    field names are source-facing because they are created before
    ``field_mappings``, so resolve their one-pass rename before comparing.

    ``sync_run_id`` is included defensively even though the current public
    ``MetadataColumnsConfig`` exposes only ``synced_at``, ``run_id``, and the
    stable ``sync_name``. This keeps the safety gate correct if the existing
    per-sync correlation id becomes an injectable metadata field later.
    """
    keys = set(upsert_key or ())
    if not keys:
        return DestinationKeyOverlaps()

    metadata = getattr(options, "metadata_columns", None)
    metadata_targets = {
        target
        for name in ("synced_at", "run_id", "sync_run_id")
        if metadata is not None
        and isinstance((target := getattr(metadata, name, None)), str)
        and target
    }

    mappings = getattr(options, "field_mappings", None) or {}
    computed = getattr(options, "computed_fields", None) or {}
    computed_targets = {mappings.get(name, name) for name in computed}

    return DestinationKeyOverlaps(
        run_varying_metadata=tuple(sorted(keys & metadata_targets)),
        computed_fields=tuple(sorted(keys & computed_targets)),
    )


def backfill_key_safety_error(sync: Any) -> str | None:
    """Return a fail-closed operator error for an unsafe backfill key."""
    upsert_key = getattr(sync.destination, "upsert_key", None)
    overlaps = destination_key_overlaps(sync.sync, upsert_key)
    if not overlaps.run_varying_metadata and not overlaps.computed_fields:
        return None

    reasons: list[str] = []
    if overlaps.run_varying_metadata:
        reasons.append(
            "run-varying sync.metadata_columns target(s) "
            f"{list(overlaps.run_varying_metadata)!r}; a retry uses new run metadata"
        )
    if overlaps.computed_fields:
        reasons.append(
            "computed_fields destination column(s) "
            f"{list(overlaps.computed_fields)!r}; drt cannot prove a computed key "
            "is stable across extraction retries"
        )

    return (
        f"Windowed backfill sync {sync.name!r} cannot replay safely: "
        "destination.upsert_key overlaps "
        + " and ".join(reasons)
        + ". Use destination-facing key columns derived directly from stable source "
        "identity (alias them in model SQL if needed); refusing before extraction."
    )
