"""Tests for template renderer."""

import json
from datetime import date, datetime, time, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from drt.templates.renderer import render_template, tojson_safe


def test_render_simple(sample_row: dict) -> None:
    result = render_template('{"text": "Hello {{ row.name }}"}', sample_row)
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
