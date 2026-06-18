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
    schema: dict[str, str] | None = None,
) -> Any:
    """Serialize ``dict`` / ``list`` values for an SQL destination.

    Three layers of decision, in precedence order:

    1. **Explicit ``json_columns`` (Layer 2, #316)** — when set, it is an
       allowlist: a complex value in a listed column is encoded, in an
       unlisted column raises early. The user's declaration always wins.
    2. **Schema introspection (Layer 3, #317)** — when ``json_columns`` is
       ``None`` and a ``schema`` map is supplied, route by the column's real
       type: a ``json`` column encodes (dict *and* list), an ``array`` column
       passes the list through to the driver's native adapter.
    3. **Back-compat (pre-#316)** — when neither applies, every dict/list is
       encoded. ``schema=None`` makes this function behave **exactly** as
       before Layer 3, so existing callers are unaffected.

    Args:
        value: The cell value from the source row.
        column: Name of the column this value belongs to (None when unknown —
            tests, ad-hoc serialization).
        json_columns: User-declared allowlist (Layer 2). ``None`` = no allowlist.
        dict_encoder: Wire-format encoder for JSON values (``psycopg2.extras.Json``
            or ``json.dumps``); also used to encode a ``list`` into a JSON column.
        list_encoder: Wire-format encoder for ``list`` values in the back-compat
            path. ``None`` (Postgres-style) passes lists through to the driver's
            ARRAY adapter; supplied (MySQL-style) encodes them.
        schema: ``{column: category}`` from :func:`drt.destinations.schema.describe_columns`,
            where category is ``"json" | "array" | "scalar"``. ``None`` disables
            Layer 3 (introspection unavailable / opted out).

    Returns:
        Encoded value for dict/list inputs, raw value for everything else.

    Raises:
        ValueError: When ``json_columns`` is set and ``column`` is not in it.
    """
    if not isinstance(value, (dict, list)):
        return value

    # Layer 2: explicit json_columns allowlist always wins.
    if json_columns is not None:
        if _column_allowed(column, json_columns):
            return _encode(value, dict_encoder, list_encoder)
        raise ValueError(_unlisted_error(column, value, json_columns))

    # Layer 3: route by the destination column's real type.
    if schema is not None and column is not None:
        category = schema.get(column)
        if category == "json":
            # A JSON/JSONB column takes both dicts and lists as encoded JSON —
            # this is what resolves the list→JSONB-vs-ARRAY ambiguity.
            return dict_encoder(value)
        if category == "array":
            # Native array column — hand the list to the driver's adapter.
            return value
        # scalar / unknown / not in schema → fall through to back-compat.

    # Back-compat (and the Layer-3 fall-through): encode everything.
    return _encode(value, dict_encoder, list_encoder)


def _encode(value: Any, dict_encoder: Encoder, list_encoder: Encoder | None) -> Any:
    if isinstance(value, dict):
        return dict_encoder(value)
    return list_encoder(value) if list_encoder is not None else value


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
