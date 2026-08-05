"""Unit tests for declarative derived columns (#763)."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from drt.config.models import SyncOptions
from drt.engine.computed_fields import apply_computed_fields


def _rows() -> list[dict[str, Any]]:
    return [
        {"first": "Ada", "last": "Lovelace", "n": 5, "phone": "090-1234-5678"},
        {"first": "Alan", "last": "Turing", "n": 7, "phone": "080-0000-1111"},
    ]


class TestApplyComputedFields:
    def test_none_is_noop_returns_same_list(self) -> None:
        rows = _rows()
        out, errors = apply_computed_fields(rows, None, "fail")
        assert out is rows
        assert errors == []

    def test_empty_mapping_is_noop(self) -> None:
        rows = _rows()
        out, errors = apply_computed_fields(rows, {}, "fail")
        assert out is rows
        assert errors == []

    def test_empty_records_list(self) -> None:
        out, errors = apply_computed_fields([], {"x": "{{ row.a }}"}, "fail")
        assert out == []
        assert errors == []

    def test_derives_from_source_columns(self) -> None:
        out, errors = apply_computed_fields(
            _rows(), {"full_name": "{{ row.first }} {{ row.last }}"}, "fail"
        )
        assert [r["full_name"] for r in out] == ["Ada Lovelace", "Alan Turing"]
        assert errors == []

    def test_constant_needs_no_expression(self) -> None:
        out, _ = apply_computed_fields(_rows(), {"source_system": "drt"}, "fail")
        assert {r["source_system"] for r in out} == {"drt"}

    def test_single_expression_keeps_its_type(self) -> None:
        """The reason render_value exists: a typed destination gets a number."""
        out, _ = apply_computed_fields(
            _rows(), {"n_thousands": "{{ row.n * 1000 }}", "n_text": "n={{ row.n }}"}, "fail"
        )
        assert out[0]["n_thousands"] == 5000
        assert isinstance(out[0]["n_thousands"], int)
        assert out[0]["n_text"] == "n=5"

    def test_existing_column_can_be_replaced_in_place(self) -> None:
        """The normalisation case — phone -> E.164 without inventing a name."""
        out, _ = apply_computed_fields(
            _rows(), {"phone": "+81{{ row.phone | replace('-', '') }}"}, "fail"
        )
        assert out[0]["phone"] == "+8109012345678"

    def test_unrelated_columns_are_preserved(self) -> None:
        out, _ = apply_computed_fields(_rows(), {"x": "{{ row.n }}"}, "fail")
        assert out[0]["first"] == "Ada"
        assert out[0]["phone"] == "090-1234-5678"


class TestNoChaining:
    """Templates read the record as it arrived, never another computed field.

    Chaining would make results depend on YAML key order, which
    ``field_mappings`` rejected for the same reason.
    """

    def test_a_computed_field_cannot_read_another(self) -> None:
        fields = {"a": "{{ row.n }}", "b": "{{ row.a }}"}
        with pytest.raises(ValueError, match=r"computed_fields\['b'\]"):
            apply_computed_fields(_rows(), fields, "fail")

    def test_result_is_independent_of_key_order(self) -> None:
        forward = {"x": "{{ row.n }}", "y": "{{ row.first }}"}
        reverse = {"y": "{{ row.first }}", "x": "{{ row.n }}"}
        out_a, _ = apply_computed_fields(_rows(), forward, "fail")
        out_b, _ = apply_computed_fields(_rows(), reverse, "fail")
        assert out_a == out_b

    def test_replacing_a_column_reads_the_original_value(self) -> None:
        """Self-reference reads the source value, not a partially-updated one."""
        out, _ = apply_computed_fields(
            [{"n": 2}], {"n": "{{ row.n * 10 }}", "m": "{{ row.n }}"}, "fail"
        )
        assert out[0] == {"n": 20, "m": 2}


class TestErrorHandling:
    def test_fail_raises_and_names_the_field(self) -> None:
        with pytest.raises(ValueError, match=r"computed_fields\['bad'\]"):
            apply_computed_fields(_rows(), {"bad": "{{ row.nope }}"}, "fail")

    def test_skip_drops_the_row_and_records_an_error(self) -> None:
        rows = [{"n": 1}, {"n": 2, "extra": "here"}]
        out, errors = apply_computed_fields(rows, {"x": "{{ row.extra }}"}, "skip")

        assert [r["n"] for r in out] == [2], "the row that could be computed must survive"
        assert len(errors) == 1
        assert errors[0].batch_index == 0
        assert "computed_fields['x']" in errors[0].error_message
        assert errors[0].http_status is None

    def test_skipped_row_is_never_half_derived(self) -> None:
        """A row failing on the second field must not keep the first one.

        It is dropped, so nothing downstream can see it — but if the
        assignment happened per field instead of once at the end, a caller
        holding the original list would observe the partial mutation.
        """
        rows = [{"n": 1}]
        out, errors = apply_computed_fields(
            rows, {"ok": "{{ row.n }}", "bad": "{{ row.nope }}"}, "skip"
        )
        assert out == []
        assert len(errors) == 1
        assert "ok" not in rows[0], "the first field leaked onto a dropped row"

    def test_first_failure_wins(self) -> None:
        """One config defect reports once per row, not once per broken field."""
        out, errors = apply_computed_fields(
            _rows(), {"a": "{{ row.nope }}", "b": "{{ row.also_nope }}"}, "skip"
        )
        assert out == []
        assert len(errors) == len(_rows())
        assert all("computed_fields['a']" in e.error_message for e in errors)

    def test_error_preview_is_truncated(self) -> None:
        big = [{"blob": "x" * 5000}]
        _, errors = apply_computed_fields(big, {"c": "{{ row.nope }}"}, "skip")
        assert len(errors[0].record_preview) <= 200

    def test_non_value_error_still_honours_on_error(self) -> None:
        """Only a missing column surfaces as ValueError; the rest raise natively.

        `{{ row.a / row.b }}` over a zero raises ZeroDivisionError straight out
        of the rendered template. Catching only ValueError here would let that
        abort a run that had explicitly asked to skip bad rows.
        """
        rows = [{"a": 10, "b": 2}, {"a": 10, "b": 0}]
        out, errors = apply_computed_fields(rows, {"ratio": "{{ row.a / row.b }}"}, "skip")

        assert [r["ratio"] for r in out] == [5.0]
        assert len(errors) == 1
        assert "ZeroDivisionError" in errors[0].error_message

    def test_non_value_error_fails_when_asked_to(self) -> None:
        with pytest.raises(ValueError, match="ZeroDivisionError"):
            apply_computed_fields([{"a": 1, "b": 0}], {"r": "{{ row.a / row.b }}"}, "fail")

    def test_null_through_a_filter_renders_the_string_none(self) -> None:
        """Pinned, not endorsed — Jinja stringifies None before a filter runs.

        `{{ row.phone | replace('-', '') }}` over a NULL phone yields the
        literal "None", not an error and not a null. This is plain Jinja
        behaviour, shared with the REST destination's `body_template`, so
        computed_fields deliberately does not special-case it — diverging would
        mean two templating semantics in one tool. The documented workaround is
        an explicit default: `{{ row.phone or '' | replace('-', '') }}`.
        """
        out, errors = apply_computed_fields(
            [{"phone": None}], {"p": "{{ row.phone | replace('-', '') }}"}, "skip"
        )
        assert out[0]["p"] == "None"
        assert errors == []

    def test_a_bare_null_column_stays_null(self) -> None:
        """The single-node path preserves it, which is what makes the above odd."""
        out, _ = apply_computed_fields([{"phone": None}], {"p": "{{ row.phone }}"}, "fail")
        assert out[0]["p"] is None


class TestValueTypes:
    @pytest.mark.parametrize(
        "value",
        [5, True, None, Decimal("1.50"), "123", "01"],
    )
    def test_lone_expression_round_trips_the_value(self, value: Any) -> None:
        out, _ = apply_computed_fields([{"v": value}], {"copy": "{{ row.v }}"}, "fail")
        assert out[0]["copy"] == value
        assert type(out[0]["copy"]) is type(value)


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


class TestComputedFieldsConfig:
    def test_accepts_templates(self) -> None:
        opts = SyncOptions(computed_fields={"full_name": "{{ row.a }} {{ row.b }}"})
        assert opts.computed_fields == {"full_name": "{{ row.a }} {{ row.b }}"}

    def test_rejects_unparseable_template_at_config_time(self) -> None:
        """A malformed template is a YAML typo — it should never reach a run."""
        with pytest.raises(ValidationError, match=r"computed_fields\['broken'\]"):
            SyncOptions(computed_fields={"broken": "{% if row.a %}unclosed"})

    def test_rejects_an_empty_field_name(self) -> None:
        with pytest.raises(ValidationError, match="non-empty field names"):
            SyncOptions(computed_fields={"  ": "{{ row.a }}"})

    def test_an_unknown_column_is_not_a_config_error(self) -> None:
        """Only syntax is decidable here; the query decides what columns exist."""
        SyncOptions(computed_fields={"x": "{{ row.whatever }}"})


# ---------------------------------------------------------------------------
# Engine integration — position in the transform pipeline
# ---------------------------------------------------------------------------


class TestPipelineOrder:
    """computed_fields -> field_mappings -> mask, verified end to end.

    The ordering is load-bearing in both directions: templates must see source
    column names, and `mask` must keep seeing post-rename names (its shipped
    meaning). #763 sketched `computed_fields -> mask -> field_mappings`, which
    would have silently unmasked every existing configuration.
    """

    def _run(self, sync_options: dict[str, Any], row: dict[str, Any]) -> list[dict[str, Any]]:
        from collections.abc import Iterator

        from drt.config.credentials import BigQueryProfile
        from drt.config.models import DestinationConfig, SyncConfig
        from drt.destinations.base import SyncResult
        from drt.engine.sync import run_sync

        captured: list[dict[str, Any]] = []

        class FakeSource:
            def extract(
                self, query: str, config: object, *, query_tags: dict[str, str] | None = None
            ) -> Iterator[dict]:
                yield dict(row)

            def test_connection(self, config: object) -> bool:
                return True

        class CapturingDestination:
            def load(
                self,
                records: list[dict],
                config: DestinationConfig,
                sync_options: SyncOptions,
            ) -> SyncResult:
                captured.extend(records)
                result = SyncResult()
                result.success = len(records)
                return result

        sync = SyncConfig.model_validate(
            {
                "name": "computed_demo",
                "model": "ref('users')",
                "destination": {"type": "rest_api", "url": "https://example.com"},
                "sync": {"batch_size": 10, **sync_options},
            }
        )
        run_sync(
            sync,
            FakeSource(),
            CapturingDestination(),
            BigQueryProfile(type="bigquery", project="p", dataset="d"),
            Path("."),
        )
        return captured

    def test_computed_then_renamed_then_masked(self) -> None:
        loaded = self._run(
            {
                "computed_fields": {"full_name": "{{ row.first }} {{ row.last }}"},
                "field_mappings": {"full_name": "name"},
                "mask": {"name": "redact"},
            },
            {"first": "Ada", "last": "Lovelace"},
        )
        # Computed from source names, renamed to the destination name, then
        # masked under that destination name.
        assert loaded[0]["name"] == "[REDACTED]"
        assert "full_name" not in loaded[0]

    def test_templates_read_source_names_not_renamed_ones(self) -> None:
        loaded = self._run(
            {
                "computed_fields": {"greeting": "hi {{ row.first }}"},
                "field_mappings": {"first": "given_name"},
            },
            {"first": "Ada"},
        )
        assert loaded[0]["greeting"] == "hi Ada"
        assert loaded[0]["given_name"] == "Ada"

    def test_skipped_rows_are_counted(self) -> None:
        from collections.abc import Iterator

        from drt.config.credentials import BigQueryProfile
        from drt.config.models import DestinationConfig, SyncConfig
        from drt.destinations.base import SyncResult
        from drt.engine.sync import run_sync

        class FakeSource:
            def extract(
                self, query: str, config: object, *, query_tags: dict[str, str] | None = None
            ) -> Iterator[dict]:
                yield {"a": 1}
                yield {"b": 2}

            def test_connection(self, config: object) -> bool:
                return True

        class FakeDestination:
            def load(
                self,
                records: list[dict],
                config: DestinationConfig,
                sync_options: SyncOptions,
            ) -> SyncResult:
                result = SyncResult()
                result.success = len(records)
                return result

        sync = SyncConfig.model_validate(
            {
                "name": "computed_skip",
                "model": "ref('users')",
                "destination": {"type": "rest_api", "url": "https://example.com"},
                "sync": {
                    "batch_size": 10,
                    "on_error": "skip",
                    "computed_fields": {"doubled": "{{ row.a * 2 }}"},
                },
            }
        )
        result = run_sync(
            sync,
            FakeSource(),
            FakeDestination(),
            BigQueryProfile(type="bigquery", project="p", dataset="d"),
            Path("."),
        )
        assert result.success == 1
        assert result.skipped == 1
        assert len(result.row_errors) == 1
