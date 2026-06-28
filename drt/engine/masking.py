"""Declarative PII masking for the sync engine (#427, #660).

``mask`` hashes, redacts, or truncates named fields after extraction and before the
records reach the destination, so personal data (email, phone, name) can be obscured
without rewriting the source SQL.

The transform is **pure** (no I/O, no observer side effects), so it lives outside
``engine/sync.py``'s observability boundary and is unit-testable in isolation. It
runs as the **last** transform — right after ``field_mappings`` — so ``mask`` keys
reference the field name as it leaves drt (the post-rename, destination-facing name).

Two config forms (see ``MaskSpec`` in ``config/models.py``):

* flat: ``field: "hash" | "redact"`` — parameter-less strategies.
* object: ``field: {strategy: "truncate", length: N}`` — strategies that take a
  parameter. The flat form keeps working unchanged.
"""

from __future__ import annotations

import hashlib
from typing import Any

from drt.config.models import MaskRule, MaskSpec

_REDACTED = "[REDACTED]"


def _mask_value(value: Any, rule: MaskSpec) -> Any:
    """Apply one masking rule to one value.

    ``None`` passes through unchanged — a null carries no PII, and masking it would
    only hide that the source value was absent. Non-string values are stringified
    first, so an integer is masked the same way its text form would be.
    """
    if value is None:
        return None

    if isinstance(rule, MaskRule):
        strategy, length = rule.strategy, rule.length
    else:
        strategy, length = rule, None

    if strategy == "hash":
        return hashlib.sha256(str(value).encode("utf-8")).hexdigest()
    if strategy == "truncate":
        text = str(value)
        # length is guaranteed non-negative for truncate by MaskRule validation;
        # the None guard keeps this total for the type checker.
        return text if length is None else text[:length]
    return _REDACTED  # "redact"


def apply_mask(
    records: list[dict[str, Any]],
    mask: dict[str, MaskSpec] | None,
) -> list[dict[str, Any]]:
    """Return records with ``mask`` fields obscured per their strategy.

    Best-effort by design: a configured field absent from a record is simply not
    masked for that record (the source query may legitimately omit it). The input
    is not mutated — a new list of new dicts is returned, mirroring
    ``apply_field_mappings`` so the engine can apply the two in sequence.

    Args:
        records: The batch of rows (post-lookup, post-field-mapping).
        mask: ``{field_name: spec}`` where spec is ``"hash"`` / ``"redact"`` or a
            ``MaskRule`` (e.g. ``{strategy: "truncate", length: 2}``). ``None`` or
            empty is a no-op that returns the input list unchanged.

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
