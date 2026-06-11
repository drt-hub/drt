"""Declarative column renaming for the sync engine (#415).

``field_mappings`` lets a sync rename source columns to destination field
names without aliasing in the source SQL (``SELECT foo AS bar``). This
decouples the mapping from the query so the same query can feed multiple
syncs with different destination shapes — the same first-class feature
Census / Hightouch / Polytomic expose.

The transform is **pure** (no I/O, no observer side effects), so it lives
outside ``engine/sync.py``'s observability boundary and is unit-testable
in isolation. It runs late in the pipeline — after cursor tracking and
lookups, which both operate on *source* column names — so the rename is
the last thing that happens before a record reaches the destination.
"""

from __future__ import annotations

from typing import Any


def apply_field_mappings(
    records: list[dict[str, Any]],
    field_mappings: dict[str, str] | None,
) -> list[dict[str, Any]]:
    """Rename record keys per ``field_mappings`` (source column → dest field).

    Each record's keys are remapped in a **single pass**: a key present in
    ``field_mappings`` is emitted under its mapped name, any other key is
    kept as-is. Single-pass remapping is deliberately order-independent —
    it never chains (a mapping ``{a: b, b: c}`` renames ``a→b`` and
    ``b→c`` from the *original* keys, it does not turn ``a`` into ``c``).

    Best-effort by design: a mapping whose source column is absent from a
    record is simply not applied to that record (the source query may
    legitimately omit it). ``drt validate`` surfaces mappings that can
    never match (see ``config`` validation) so genuine typos are caught
    at config time rather than silently dropped here.

    Collision note: if two source columns map to the same destination
    name, or a mapped name collides with an unmapped key, the later key in
    record-insertion order wins (last-write-wins, as with any dict
    comprehension). Such a config is almost always a mistake; keep
    ``field_mappings`` targets distinct.

    Args:
        records: The batch of source rows (post-lookup).
        field_mappings: ``{source_column: destination_field}``. ``None``
            or empty is a no-op that returns the input list unchanged.

    Returns:
        A new list of new dicts with keys remapped. The input is not
        mutated (the engine may still hold references to the originals —
        e.g. for cursor tracking already done upstream).
    """
    if not field_mappings:
        return records
    return [
        {field_mappings.get(key, key): value for key, value in record.items()} for record in records
    ]


def unmapped_source_columns(
    field_mappings: dict[str, str] | None,
    available_columns: set[str] | None,
) -> list[str]:
    """Return mapping source columns that can never match the source schema.

    Best-effort config-time check for ``drt validate``: given the columns
    a sync's source query is known to produce (when introspectable),
    report any ``field_mappings`` key that isn't among them — those are
    almost certainly typos, since the rename would silently never apply.

    Args:
        field_mappings: ``{source_column: destination_field}`` or ``None``.
        available_columns: The set of column names the source produces, or
            ``None`` when the schema can't be introspected (in which case
            no warning is emitted — absence of schema is not evidence of a
            typo).

    Returns:
        Sorted list of source-column keys absent from ``available_columns``.
        Empty when there's nothing to warn about (or no schema to check
        against).
    """
    if not field_mappings or available_columns is None:
        return []
    return sorted(src for src in field_mappings if src not in available_columns)
