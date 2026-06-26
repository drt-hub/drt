"""Declarative PII masking for the sync engine (#427).

``mask`` hashes or redacts named fields after extraction and before the records
reach the destination, so personal data (email, phone, name) can be obscured
without rewriting the source SQL.

The transform is **pure** (no I/O, no observer side effects), so it lives outside
``engine/sync.py``'s observability boundary and is unit-testable in isolation. It
runs as the **last** transform — right after ``field_mappings`` — so ``mask`` keys
reference the field name as it leaves drt (the post-rename, destination-facing name).

v1 is intentionally flat: each value is ``"hash"`` or ``"redact"``. Param-bearing
strategies (e.g. ``truncate``) land later as a backwards-compatible object form (#660).
"""

from __future__ import annotations

import hashlib
from typing import Any, Literal

MaskStrategy = Literal["hash", "redact"]

_REDACTED = "[REDACTED]"


def _mask_value(value: Any, strategy: MaskStrategy) -> Any:
    """Apply one masking strategy to one value.

    ``None`` passes through unchanged — a null carries no PII, and masking it
    would only hide that the source value was absent. Non-string values are
    stringified before hashing, so an integer phone number hashes the same way
    its text form would.
    """
    if value is None:
        return None
    if strategy == "hash":
        return hashlib.sha256(str(value).encode("utf-8")).hexdigest()
    return _REDACTED  # "redact"


def apply_mask(
    records: list[dict[str, Any]],
    mask: dict[str, MaskStrategy] | None,
) -> list[dict[str, Any]]:
    """Return records with ``mask`` fields hashed or redacted per their strategy.

    Best-effort by design: a configured field absent from a record is simply not
    masked for that record (the source query may legitimately omit it). The input
    is not mutated — a new list of new dicts is returned, mirroring
    ``apply_field_mappings`` so the engine can apply the two in sequence.

    Args:
        records: The batch of rows (post-lookup, post-field-mapping).
        mask: ``{field_name: "hash" | "redact"}``. ``None`` or empty is a no-op
            that returns the input list unchanged.

    Returns:
        A new list of new dicts with the configured fields masked.
    """
    if not mask:
        return records
    return [
        {
            key: (_mask_value(value, mask[key]) if key in mask else value)
            for key, value in record.items()
        }
        for record in records
    ]
