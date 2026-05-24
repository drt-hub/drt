"""Shared JSON-column serialization for SQL destinations.

Postgres, MySQL, and (in v0.8) other SQL-family destinations all need to
decide what to do with ``dict`` and ``list`` values flowing in from
upstream sources — wrap them with a driver-native JSON adapter, encode
them via ``json.dumps``, or pass them through to a typed ARRAY column.

The decision logic (validate against the user-declared ``json_columns``
allowlist, raise early on unlisted complex types) is dialect-agnostic.
Only the encoding step differs:

- **Postgres** (psycopg2): ``Json(value)`` wrapper for dicts;
  lists pass through to the driver's ARRAY adapter.
- **MySQL** (pymysql): ``json.dumps(value)`` for both dicts and lists,
  since pymysql has no native JSON adapter.

This module centralises the decision logic and lets each dialect inject
its own ``dict_encoder`` / ``list_encoder``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

# Encoder signature: takes a value, returns the wire-format representation.
Encoder = Callable[[Any], Any]


def serialize_complex_value(
    value: Any,
    column: str | None,
    json_columns: list[str] | None,
    *,
    dict_encoder: Encoder,
    list_encoder: Encoder | None = None,
) -> Any:
    """Serialize ``dict`` / ``list`` values, validating against ``json_columns``.

    Args:
        value: The cell value from the source row.
        column: Name of the column this value belongs to (None when unknown —
            tests, ad-hoc serialization).
        json_columns: User-declared allowlist of columns permitted to hold
            JSON-encoded complex values. When ``None``, all dict/list values
            are encoded (back-compat with pre-#316 behaviour).
        dict_encoder: Wire-format encoder for ``dict`` values (e.g.
            ``psycopg2.extras.Json`` or ``json.dumps``).
        list_encoder: Wire-format encoder for ``list`` values. When ``None``
            (Postgres-style), lists pass through unchanged — the driver
            handles them via its native ARRAY adapter. When supplied
            (MySQL-style), lists are encoded.

    Returns:
        Encoded value for dict/list inputs, raw value for everything else.

    Raises:
        ValueError: When ``json_columns`` is set and ``column`` is not in
            the allowlist — fail early with a pointing error rather than
            letting a "can't adapt type 'dict'" surface deep in the driver.
    """
    if isinstance(value, dict):
        if _column_allowed(column, json_columns):
            return dict_encoder(value)
        raise ValueError(_unlisted_error(column, value, json_columns))

    if isinstance(value, list):
        if _column_allowed(column, json_columns):
            return list_encoder(value) if list_encoder is not None else value
        raise ValueError(_unlisted_error(column, value, json_columns))

    return value


def _column_allowed(column: str | None, json_columns: list[str] | None) -> bool:
    """Return True when a complex value is permitted in ``column``.

    Two cases say yes:
    1. ``json_columns`` is ``None`` (back-compat — no allowlist enforced).
    2. ``column`` appears in the allowlist.
    """
    if json_columns is None:
        return True
    return column is not None and column in json_columns


def _unlisted_error(column: str | None, value: Any, json_columns: list[str] | None) -> str:
    return (
        f"Column '{column}' contains a {type(value).__name__} value but "
        f"is not listed in json_columns={json_columns}. "
        f"Add '{column}' to json_columns or remove the value."
    )
