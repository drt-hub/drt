"""Key canonicalisation for tracked mirror (#686).

Tracked mirror (``sync.mirror.strategy: tracked``) persists the set of
``upsert_key`` tuples drt has successfully synced in a drt-managed side
table (``_drt_synced_keys``) in the destination, so the mirror DELETE pass
only ever removes rows drt itself wrote — never rows the application (or
another pipeline) inserted. This module holds the destination-agnostic
pieces: the canonical JSON encoding of a key tuple, its sha256 identity,
and the previous-minus-current diff. The SQL (DDL + state read + rewrite)
lives in each destination, using its own driver and quoting helpers.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

STATE_TABLE = "_drt_synced_keys"


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


def diff_keys(
    previous: dict[str, str], current: list[tuple[Any, ...]]
) -> list[tuple[Any, ...]]:
    """``previous`` (hash -> key_json) minus ``current`` -> key tuples to delete."""
    current_hashes = {key_hash(k) for k in current}
    return [
        tuple(json.loads(kj))
        for h, kj in previous.items()
        if h not in current_hashes
    ]
