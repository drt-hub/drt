"""Unit tests for tracked-mirror key canonicalisation helpers (#686).

Tracked mirror persists the set of upsert_key tuples drt has synced in a
drt-managed ``_drt_synced_keys`` table in the destination. These helpers
are the destination-agnostic pieces: the canonical JSON encoding of a key
tuple, its sha256 identity, and the previous-minus-current diff. The SQL
lives in each destination and is tested in the per-destination
``test_*_mirror_mode.py`` suites.
"""

from __future__ import annotations

import hashlib
from datetime import date

from drt.destinations._mirror_state import (
    STATE_TABLE,
    diff_keys,
    key_hash,
    key_json,
)


def test_state_table_name() -> None:
    assert STATE_TABLE == "_drt_synced_keys"


def test_key_json_is_compact_and_deterministic() -> None:
    assert key_json((1, "a")) == '[1,"a"]'


def test_key_json_round_trips_int_and_str() -> None:
    import json

    assert json.loads(key_json((42, "user-7"))) == [42, "user-7"]


def test_key_json_stringifies_non_json_types() -> None:
    assert key_json((date(2026, 1, 1),)) == '["2026-01-01"]'


def test_key_hash_is_sha256_of_key_json() -> None:
    assert key_hash((1,)) == hashlib.sha256(b"[1]").hexdigest()


def test_diff_keys_returns_previous_minus_current() -> None:
    previous = {key_hash((k,)): key_json((k,)) for k in (1, 2, 3)}
    to_delete = diff_keys(previous, [(1,), (2,)])
    assert to_delete == [(3,)]


def test_diff_keys_empty_previous_deletes_nothing() -> None:
    assert diff_keys({}, [(1,)]) == []


def test_diff_keys_composite_keys_round_trip() -> None:
    previous = {
        key_hash((1, "a")): key_json((1, "a")),
        key_hash((2, "b")): key_json((2, "b")),
    }
    assert diff_keys(previous, [(1, "a")]) == [(2, "b")]
