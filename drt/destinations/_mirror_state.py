"""Key canonicalisation for tracked mirror (#686).

Tracked mirror (``sync.mirror.strategy: tracked``) persists the set of
``upsert_key`` tuples drt has successfully synced in a drt-managed side
table (``_drt_synced_keys``) in the destination, so the mirror DELETE pass
only ever removes rows drt itself wrote — never rows the application (or
another pipeline) inserted. This module holds the destination-agnostic
pieces: the canonical JSON encoding of a key tuple, its sha256 identity,
and the previous-minus-current diff. The SQL (DDL + state read + rewrite)
lives in each destination, using its own driver and quoting helpers.

``diff_keys`` stays in use by the dry-run ``--diff`` preview
(``engine/diff.py``, #693) — a read-only, best-effort, human-triggered path
where loading the previous key set into Python is an acceptable, bounded
cost. The real execution path (``_finalize_mirror_tracked`` in each
destination) no longer calls it: #694 part 2 replaced its Python-side
``SELECT`` + set-diff with a SQL-side ``NOT EXISTS`` join against a staged
table of this run's keys, so a state table with millions of rows never
gets read into memory just to compute a typically-small diff. ``decode_key``
is the shared piece that survived that move — turning a diffed row's
``key_json`` back into a key tuple is still needed on both paths.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

STATE_TABLE = "_drt_synced_keys"

# Scratch table name for staging this run's current keys during the SQL-side
# diff (#694 part 2). Unqualified and unquoted: every dialect's tracked-mirror
# scratch table is either a genuine session-scoped TEMPORARY table (Postgres,
# MySQL, Snowflake — no schema-qualification possible or needed) or a
# real table addressed in the target's own schema (ClickHouse, Databricks —
# same reasoning ``_delete_via_staged_keys`` already documents), never a
# user-configured identifier, so it never needs Composable-safe quoting.
DIFF_STAGING_TABLE = "__drt_mirror_diff_keys"


def decode_key(key_json_str: str) -> tuple[Any, ...]:
    """Inverse of ``key_json`` — a diffed state row's JSON back to a key tuple."""
    return tuple(json.loads(key_json_str))


def key_json(key: tuple[Any, ...]) -> str:
    """Canonical JSON for an ``upsert_key`` tuple.

    int/str values (the real-world key case) round-trip exactly through
    the state table; non-JSON-native values (datetime, Decimal, UUID) are
    stringified via ``default=str``, so deletes for such keys bind the
    string form — a documented tracked-mirror limitation.
    """
    return json.dumps(list(key), default=str, separators=(",", ":"))


def key_hash(key: tuple[Any, ...]) -> str:
    """sha256 hex identity of a key tuple — the state table's key column."""
    return hashlib.sha256(key_json(key).encode()).hexdigest()


def diff_keys(previous: dict[str, str], current: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    """``previous`` (hash -> key_json) minus ``current`` -> key tuples to delete."""
    current_hashes = {key_hash(k) for k in current}
    return [decode_key(kj) for h, kj in previous.items() if h not in current_hashes]
