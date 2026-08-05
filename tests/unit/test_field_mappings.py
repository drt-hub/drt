"""Unit tests for the field_mappings transform (#415)."""

from __future__ import annotations

from drt.engine.field_mappings import apply_field_mappings, unmapped_source_columns

# ---------------------------------------------------------------------------
# apply_field_mappings
# ---------------------------------------------------------------------------


class TestApplyFieldMappings:
    def test_none_is_noop_returns_same_list(self) -> None:
        records = [{"id": 1, "name": "a"}]
        result = apply_field_mappings(records, None)
        assert result is records  # identity — no copy when nothing to do

    def test_empty_mapping_is_noop(self) -> None:
        records = [{"id": 1}]
        result = apply_field_mappings(records, {})
        assert result is records

    def test_renames_mapped_keys_keeps_others(self) -> None:
        records = [{"user_id": 1, "full_name": "Alice", "extra": "keep"}]
        result = apply_field_mappings(
            records, {"user_id": "id", "full_name": "name"}
        )
        assert result == [{"id": 1, "name": "Alice", "extra": "keep"}]

    def test_does_not_mutate_input(self) -> None:
        records = [{"user_id": 1}]
        apply_field_mappings(records, {"user_id": "id"})
        assert records == [{"user_id": 1}]  # original untouched

    def test_absent_source_column_is_skipped_per_record(self) -> None:
        """A mapping whose source column is missing from a given record is
        simply not applied to that record (best-effort)."""
        records = [
            {"user_id": 1, "full_name": "Alice"},
            {"user_id": 2},  # no full_name on this row
        ]
        result = apply_field_mappings(
            records, {"user_id": "id", "full_name": "name"}
        )
        assert result == [
            {"id": 1, "name": "Alice"},
            {"id": 2},
        ]

    def test_single_pass_does_not_chain(self) -> None:
        """`{a: b, b: c}` renames from the ORIGINAL keys — a→b and b→c —
        it does not chain a into c."""
        records = [{"a": 1, "b": 2}]
        result = apply_field_mappings(records, {"a": "b", "b": "c"})
        # a→b and b→c, both from the original record. Insertion order:
        # 'a' first yields key 'b', then 'b' yields key 'c'.
        assert result == [{"b": 1, "c": 2}]

    def test_value_types_preserved(self) -> None:
        records = [{"ts": None, "n": 0, "flag": False, "obj": {"x": 1}}]
        result = apply_field_mappings(records, {"ts": "created_at"})
        assert result == [
            {"created_at": None, "n": 0, "flag": False, "obj": {"x": 1}}
        ]

    def test_empty_records_list(self) -> None:
        assert apply_field_mappings([], {"a": "b"}) == []

    def test_collision_last_write_wins(self) -> None:
        """Two source columns mapping to the same destination name →
        last key in insertion order wins (documented footgun)."""
        records = [{"first": "A", "second": "B"}]
        result = apply_field_mappings(records, {"first": "name", "second": "name"})
        assert result == [{"name": "B"}]


# ---------------------------------------------------------------------------
# unmapped_source_columns (validate helper)
# ---------------------------------------------------------------------------


class TestUnmappedSourceColumns:
    def test_none_mapping_returns_empty(self) -> None:
        assert unmapped_source_columns(None, {"a", "b"}) == []

    def test_none_schema_returns_empty(self) -> None:
        """No introspectable schema → no warning (absence of schema is not
        evidence of a typo)."""
        assert unmapped_source_columns({"a": "x"}, None) == []

    def test_all_present_returns_empty(self) -> None:
        assert unmapped_source_columns({"a": "x", "b": "y"}, {"a", "b", "c"}) == []

    def test_reports_absent_keys_sorted(self) -> None:
        result = unmapped_source_columns(
            {"zzz": "x", "aaa": "y", "present": "z"}, {"present", "other"}
        )
        assert result == ["aaa", "zzz"]

    def test_empty_mapping_returns_empty(self) -> None:
        assert unmapped_source_columns({}, {"a"}) == []
