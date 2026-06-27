"""Tests for mask — flat PII masking before load (#427)."""

from __future__ import annotations

import hashlib

import pytest
from pydantic import ValidationError

from drt.config.models import SyncOptions
from drt.engine.masking import apply_mask

# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


class TestMaskConfig:
    def test_accepts_hash_and_redact(self) -> None:
        opts = SyncOptions(mask={"email": "hash", "phone": "redact"})
        assert opts.mask == {"email": "hash", "phone": "redact"}

    def test_rejects_unknown_strategy(self) -> None:
        with pytest.raises(ValidationError):
            SyncOptions(mask={"email": "encrypt"})

    def test_rejects_object_form(self) -> None:
        # v1 is flat only; the object form (truncate etc.) is a follow-up (#660).
        with pytest.raises(ValidationError):
            SyncOptions(mask={"name": {"strategy": "redact"}})

    def test_defaults_to_none(self) -> None:
        assert SyncOptions().mask is None


# ---------------------------------------------------------------------------
# apply_mask
# ---------------------------------------------------------------------------


class TestApplyMask:
    def test_none_is_noop(self) -> None:
        records = [{"email": "a@b.com"}]
        assert apply_mask(records, None) is records

    def test_empty_is_noop(self) -> None:
        records = [{"email": "a@b.com"}]
        assert apply_mask(records, {}) is records

    def test_hash_is_sha256_hex(self) -> None:
        expected = hashlib.sha256(b"a@b.com").hexdigest()
        out = apply_mask([{"email": "a@b.com"}], {"email": "hash"})
        assert out == [{"email": expected}]

    def test_hash_is_deterministic(self) -> None:
        a = apply_mask([{"email": "x@y.com"}], {"email": "hash"})
        b = apply_mask([{"email": "x@y.com"}], {"email": "hash"})
        assert a == b

    def test_redact_uses_placeholder(self) -> None:
        out = apply_mask([{"phone": "07700900123"}], {"phone": "redact"})
        assert out == [{"phone": "[REDACTED]"}]

    def test_none_value_passes_through(self) -> None:
        out = apply_mask([{"email": None}], {"email": "hash"})
        assert out == [{"email": None}]

    def test_non_string_is_stringified_before_hashing(self) -> None:
        out = apply_mask([{"id": 12345}], {"id": "hash"})
        assert out == [{"id": hashlib.sha256(b"12345").hexdigest()}]

    def test_field_absent_from_record_is_skipped(self) -> None:
        out = apply_mask([{"id": 1}], {"email": "hash"})
        assert out == [{"id": 1}]

    def test_unmasked_fields_untouched(self) -> None:
        out = apply_mask([{"id": 1, "email": "a@b.com"}], {"email": "redact"})
        assert out == [{"id": 1, "email": "[REDACTED]"}]

    def test_input_not_mutated(self) -> None:
        records = [{"email": "a@b.com"}]
        apply_mask(records, {"email": "hash"})
        assert records == [{"email": "a@b.com"}]

    def test_multiple_fields_and_rows(self) -> None:
        out = apply_mask(
            [
                {"email": "a@b.com", "name": "Bob", "id": 1},
                {"email": "c@d.com", "name": "Sue", "id": 2},
            ],
            {"email": "hash", "name": "redact"},
        )
        assert out[0]["name"] == "[REDACTED]"
        assert out[1]["name"] == "[REDACTED]"
        assert out[0]["id"] == 1
        assert out[0]["email"] == hashlib.sha256(b"a@b.com").hexdigest()
