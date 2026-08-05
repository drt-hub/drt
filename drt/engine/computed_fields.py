"""Declarative derived columns for the sync engine (#763).

``computed_fields`` fills the gap between ``field_mappings`` (rename) and
``mask`` (obscure): drt could reshape *which* columns reach a destination and
*how much* of them, but not derive a new one. The workarounds were to push
destination-specific formatting into a shared dbt mart — one mart per
destination — or to use the REST destination's ``body_template``, which the
other 33 structured destinations do not have.

The transform is **pure** (no I/O, no observer side effects), so like
``field_mappings`` and ``mask`` it lives outside ``engine/sync.py``'s
observability boundary and is unit-testable in isolation.

Two guarantees worth stating up front, because both are choices:

**Order-independent.** Every template is evaluated against the record *as it
arrived*, so a computed field can never read another computed field. Chaining
would make the result depend on YAML key order, which ``field_mappings``
explicitly rejected for the same reason (see its module docstring). A user who
wants a two-step derivation writes the composed expression.

**Pipeline position: first.** ``computed_fields`` runs before
``field_mappings`` and ``mask``, so templates reference **source** column names
— matching how ``cursor_field`` and ``lookups`` already read — while the
rename and the mask then apply to the computed result. Note this is *not* the
order sketched in #763, which had ``mask`` before ``field_mappings``; that
would have inverted the shipped meaning of every existing ``mask:`` key (they
reference post-rename names today) and silently stopped masking the fields
users had configured.
"""

from __future__ import annotations

from typing import Any

from drt.destinations.row_errors import RowError, record_preview
from drt.templates.renderer import render_value


def apply_computed_fields(
    records: list[dict[str, Any]],
    computed_fields: dict[str, str] | None,
    on_error: str,
) -> tuple[list[dict[str, Any]], list[RowError]]:
    """Add derived fields to each record by rendering their templates.

    Args:
        records: Source rows, after lookups and before ``field_mappings``.
        computed_fields: ``{field_name: jinja_template}``. Templates read the
            row as ``{{ row.column }}``; a single-expression template keeps the
            value's Python type (see :func:`drt.templates.renderer.render_value`).
        on_error: Sync-level error handling (``"skip"`` or ``"fail"``).

    Returns:
        Tuple of (records with the computed fields added, row-level errors).
        Rows dropped under ``on_error="skip"`` are excluded from the list.

    Raises:
        ValueError: under ``on_error="fail"``, naming the field and the row.
            A template failure is nearly always a config defect — a mistyped
            column, a filter used wrongly — which affects every row alike, so
            failing names the cause once instead of reporting N skipped rows
            and zero synced. This is a deliberate divergence from
            ``apply_lookups``, where the same setting only stops the batch: a
            lookup miss is a statement about *data*, and missing a referenced
            row is a legitimate thing for data to do.
    """
    if not computed_fields:
        return records, []

    kept: list[dict[str, Any]] = []
    errors: list[RowError] = []

    for i, record in enumerate(records):
        # Rendered against `record` and only assigned once every field
        # succeeded, so no computed field can observe another and a row is
        # never left half-derived — see the module docstring.
        derived: dict[str, Any] = {}
        failure: str | None = None

        for name, template in computed_fields.items():
            try:
                derived[name] = render_value(template, record)
            except Exception as e:
                # Deliberately broad. A missing column arrives as ValueError
                # (render_value converts Jinja's UndefinedError), but arithmetic
                # and filters raise whatever Python raises — ZeroDivisionError
                # for `{{ row.a / row.b }}`, TypeError for a filter handed the
                # wrong type. Catching only ValueError would let those bypass
                # `on_error` entirely and abort a run that asked to skip bad
                # rows. BaseException (KeyboardInterrupt, SystemExit) is
                # correctly not caught — graceful shutdown depends on it.
                # First failure wins: the rest would report the same config
                # defect against the same row.
                failure = f"computed_fields['{name}']: {type(e).__name__}: {e}"
                break

        if failure is not None:
            errors.append(
                RowError(
                    batch_index=i,
                    record_preview=record_preview(record),
                    http_status=None,
                    error_message=failure,
                )
            )
            if on_error == "fail":
                raise ValueError(failure)
            continue

        record.update(derived)
        kept.append(record)

    return kept, errors
