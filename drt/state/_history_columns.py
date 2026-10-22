"""Additive, optional columns of the warehouse-backed ``_drt_history`` tables (#1264).

``HistoryEntry`` grows additive fields over time (``guards`` first, lineage
fields later). Local and object-store history are JSONL, so a new field is just
a new key. The warehouse-backed tables have a fixed column list, and they are
the *user's* tables: they may predate the field, and the role drt connects with
may be allowed to INSERT and SELECT but not to ALTER (the pre-provisioning
escape hatch of #695 / #960). So a new field is handled as an *optional column*:

* If the column is present, it is written and read.
* If it is absent, drt tries ``ALTER TABLE ... ADD COLUMN`` once (only when
  appending; a read never changes the schema).
* If the column cannot be added, drt logs one warning and carries on without
  it. History is best-effort by contract; a missing optional column must never
  fail the sync it records, nor break ``drt status --history``.

Values are stored as text (JSON for structured fields such as ``guards``, plain
text for scalar ones such as ``approved_by``), the one type every
warehouse here treats the same way.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterable, Sequence
from typing import Any

from drt.state.history import HistoryEntry

logger = logging.getLogger(__name__)

# Order matters: it is the order of the columns appended to INSERT / SELECT.
OPTIONAL_HISTORY_COLUMNS: tuple[str, ...] = ("guards", "plan_id", "approved_by")

# Only structured fields are JSON text; scalar fields are stored as plain text so
# they stay queryable (``WHERE approved_by = 'alice'``).
_JSON_COLUMNS = frozenset({"guards"})


def encode_optional(entry: HistoryEntry, present: Sequence[str]) -> list[str | None]:
    """Cell values for the optional columns that exist, in ``present`` order."""
    cells: list[str | None] = []
    for name in present:
        value = getattr(entry, name, None)
        if value is None:
            cells.append(None)
        elif name in _JSON_COLUMNS:
            cells.append(json.dumps(value, default=str))
        else:
            cells.append(str(value))
    return cells


def decode_optional(present: Sequence[str], cells: Sequence[Any]) -> dict[str, Any]:
    """``HistoryEntry`` keyword arguments from the optional cells of a row."""
    kwargs: dict[str, Any] = {}
    for name, cell in zip(present, cells, strict=True):
        if cell is None or cell == "":
            continue
        if name not in _JSON_COLUMNS:
            kwargs[name] = str(cell)
        elif isinstance(cell, str):
            try:
                kwargs[name] = json.loads(cell)
            except ValueError:
                logger.warning("history column %r holds non-JSON text; ignoring it", name)
        else:
            # A driver that already parsed a JSON-typed column.
            kwargs[name] = cell
    return kwargs


def column_list_sql(present: Sequence[str]) -> str:
    """``", guards"`` — appended to a base column list (empty when none exist)."""
    return "".join(f", {name}" for name in present)


def resolve_optional_columns(
    list_columns: Callable[[], Iterable[str]],
    add_column: Callable[[str], None] | None,
    *,
    table: str,
) -> tuple[str, ...]:
    """Which optional history columns the table has, adding the missing ones.

    ``list_columns`` returns the table's current column names. ``add_column`` adds
    one text column, or is ``None`` for a read (a read never alters a schema).
    A failure to list columns means the table is treated as legacy: no optional
    column is touched.
    """
    try:
        existing = {str(name).lower() for name in list_columns()}
    except Exception as exc:  # noqa: BLE001 — history is best-effort
        logger.warning(
            "could not inspect columns of %s (%s); optional history fields skipped", table, exc
        )
        return ()

    present = [name for name in OPTIONAL_HISTORY_COLUMNS if name in existing]
    missing = [name for name in OPTIONAL_HISTORY_COLUMNS if name not in existing]
    if add_column is None or not missing:
        return tuple(present)

    for name in missing:
        try:
            add_column(name)
        except Exception as exc:  # noqa: BLE001 — e.g. no ALTER privilege
            # Another session may have added it between the probe and the ALTER.
            try:
                if name in {str(c).lower() for c in list_columns()}:
                    present.append(name)
                    continue
            except Exception:  # noqa: BLE001
                pass
            logger.warning(
                "history column %r could not be added to %s (%s); this table will not "
                "record it. Grant ALTER, or add a TEXT/STRING column named %r yourself.",
                name,
                table,
                exc,
                name,
            )
        else:
            present.append(name)
    return tuple(name for name in OPTIONAL_HISTORY_COLUMNS if name in present)
