"""Tests for template renderer."""

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from drt.templates.renderer import _compile_value, render_template, render_value, tojson_safe


def test_render_simple(sample_row: dict) -> None:
    result = render_template('{"text": "Hello {{ row.name }}"}', sample_row)
    assert result == '{"text": "Hello Alice"}'


def test_render_simple_with_named_row_context(sample_row: dict) -> None:
    result = render_template('{"text": "Hello {{ row.name }}"}', row=sample_row)
    assert result == '{"text": "Hello Alice"}'


def test_render_missing_variable(sample_row: dict) -> None:
    with pytest.raises(ValueError, match="Template error"):
        render_template("{{ row.missing_field }}", sample_row)


def test_tojson_safe_datetime() -> None:
    dt = datetime(2026, 5, 28, 12, 34, 56, tzinfo=timezone.utc)
    assert tojson_safe(dt) == '"2026-05-28T12:34:56+00:00"'


def test_tojson_safe_date_and_time() -> None:
    assert tojson_safe(date(2026, 5, 28)) == '"2026-05-28"'
    assert tojson_safe(time(12, 34, 56)) == '"12:34:56"'


def test_tojson_safe_decimal_and_uuid() -> None:
    assert tojson_safe(Decimal("3.14")) == '"3.14"'
    u = UUID("12345678-1234-5678-1234-567812345678")
    assert tojson_safe(u) == '"12345678-1234-5678-1234-567812345678"'


def test_tojson_safe_nested_row() -> None:
    row = {
        "name": "Alice",
        "annotated_at": datetime(2026, 5, 28, 12, 0, 0, tzinfo=timezone.utc),
        "price": Decimal("9.99"),
        "uid": UUID("12345678-1234-5678-1234-567812345678"),
        "note": None,
    }
    rendered = render_template("{{ row | tojson_safe }}", row)
    parsed = json.loads(rendered)
    assert parsed["name"] == "Alice"
    assert parsed["annotated_at"] == "2026-05-28T12:00:00+00:00"
    assert parsed["price"] == "9.99"
    assert parsed["uid"] == "12345678-1234-5678-1234-567812345678"
    assert parsed["note"] is None


def test_render_template_accepts_named_rows_context() -> None:
    rows = [{"id": 1}, {"id": 2}]

    rendered = render_template("{{ rows | tojson_safe }}", rows=rows)

    assert json.loads(rendered) == rows


def test_tojson_safe_ensure_ascii_false() -> None:
    assert tojson_safe({"msg": "こんにちは"}) == '{"msg": "こんにちは"}'


def test_tojson_safe_unknown_type_raises() -> None:
    class Custom:
        pass

    with pytest.raises(TypeError, match="not JSON serializable"):
        tojson_safe(Custom())


def test_tojson_strict_still_fails_on_datetime() -> None:
    """Standard `tojson` is unchanged — non-breaking guarantee."""
    row = {"ts": datetime(2026, 5, 28, tzinfo=timezone.utc)}
    with pytest.raises(TypeError, match="not JSON serializable"):
        render_template("{{ row.ts | tojson }}", row)


# --- render_value: single output node keeps its type (#763) ------------------


class TestRenderValueKeepsType:
    """One output node → that node's value. Anything else → a string."""

    @pytest.mark.parametrize(
        "template,expected",
        [
            ("{{ row.n }}", 5),
            ("{{ row.n * 1000 }}", 5000),
            ("{{ row.flag }}", True),
            ("{{ row.nothing }}", None),
            ("{{ row.amount }}", Decimal("1.50")),
            ("{{ row.ts }}", datetime(2026, 5, 28, tzinfo=timezone.utc)),
        ],
    )
    def test_lone_expression_returns_the_python_value(
        self, template: str, expected: object
    ) -> None:
        row = {
            "n": 5,
            "flag": True,
            "nothing": None,
            "amount": Decimal("1.50"),
            "ts": datetime(2026, 5, 28, tzinfo=timezone.utc),
        }
        result = render_value(template, row)
        assert result == expected
        assert type(result) is type(expected)

    @pytest.mark.parametrize("raw", ["123", "01", "1_000", "True", "None", "[1, 2]"])
    def test_a_string_column_stays_a_string(self, raw: str) -> None:
        """The reason Jinja's own native_concat is not used.

        native_concat runs `literal_eval` over the rendered result, so a column
        holding "123" would reach the destination as the integer 123 while a
        column holding "01" stayed a string — the same config behaving
        differently per row, decided by the data. Every value here is
        `literal_eval`-able (or deceptively close to it) and must survive as
        the string it was.
        """
        result = render_value("{{ row.value }}", {"value": raw})
        assert result == raw
        assert isinstance(result, str)

    @pytest.mark.parametrize(
        "template,expected",
        [
            ("{{ row.a }}-{{ row.b }}", "x-y"),  # two expressions
            ("+81{{ row.a }}", "+81x"),  # literal text plus an expression
            ("{{ row.n }} ", "5 "),  # even one trailing space is a second node
            ("drt-prod", "drt-prod"),  # no expression at all
            ("01", "01"),  # ...and a literal is never literal_eval'd
            ("", ""),  # nothing to render is the empty string, not None
        ],
    )
    def test_anything_but_a_lone_node_renders_as_text(self, template: str, expected: str) -> None:
        result = render_value(template, {"a": "x", "b": "y", "n": 5})
        assert result == expected
        assert isinstance(result, str)

    def test_undefined_still_raises_for_a_lone_expression(self) -> None:
        """The trap the native code generator opens up.

        It never stringifies an output node, so StrictUndefined has nothing to
        trip over: without an explicit check the Undefined object itself is
        returned and gets written into the record. A typo'd column name has to
        fail, not produce a mystery value.
        """
        with pytest.raises(ValueError, match="Template error"):
            render_value("{{ row.missing_field }}", {"present": 1})

    def test_undefined_still_raises_with_surrounding_text(self) -> None:
        with pytest.raises(ValueError, match="Template error"):
            render_value("prefix-{{ row.missing_field }}", {"present": 1})

    def test_filters_are_available(self) -> None:
        """Same environment as render_template — one templating story."""
        row = {"ts": datetime(2026, 5, 28, tzinfo=timezone.utc)}
        assert render_value("{{ row.ts | tojson_safe }}", row) == '"2026-05-28T00:00:00+00:00"'


class TestRenderValueReuse:
    def test_templates_are_compiled_once(self) -> None:
        """Every caller renders per record; re-parsing per row was the cost."""
        template = "{{ row.unique_to_this_test }}"
        render_value(template, {"unique_to_this_test": 1})
        before = _compile_value.cache_info().hits
        for _ in range(5):
            render_value(template, {"unique_to_this_test": 2})
        assert _compile_value.cache_info().hits == before + 5

    def test_concurrent_renders_do_not_share_state(self) -> None:
        """`drt run --threads N` renders from several threads through one cache."""
        template = "{{ row.i }}"
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda i: render_value(template, {"i": i}), range(200)))
        assert results == list(range(200))
