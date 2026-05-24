"""Tests for drt.destinations._serializer.serialize_complex_value.

Covers the dialect-agnostic decision matrix in one place so future SQL
destinations (Snowflake, ClickHouse, etc.) just need to supply their own
``dict_encoder`` / ``list_encoder`` and inherit the validation logic
automatically.

The dialect-specific wrappers (``postgres._serialize_value``,
``mysql._serialize_value``) are exercised by the existing
``test_postgres_destination.py`` / ``test_mysql_destination.py`` suites —
they delegate here.
"""

from __future__ import annotations

import json

import pytest

from drt.destinations._serializer import serialize_complex_value

# ---------------------------------------------------------------------------
# Encoders used throughout the matrix
# ---------------------------------------------------------------------------


def _tag_encoder(value: object) -> tuple[str, object]:
    """A tagging encoder so tests can distinguish encoded values from passthrough."""
    return ("encoded", value)


def _json_encoder(value: object) -> str:
    return json.dumps(value, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Scalars and None pass through unchanged regardless of config
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [None, 0, 1, -1, 3.14, True, False, "", "hello", b"bytes"],
)
def test_scalar_values_pass_through(value: object) -> None:
    out = serialize_complex_value(
        value,
        column="anything",
        json_columns=["something"],
        dict_encoder=_tag_encoder,
        list_encoder=_json_encoder,
    )
    assert out == value


# ---------------------------------------------------------------------------
# dict handling
# ---------------------------------------------------------------------------


def test_dict_encoded_when_json_columns_is_none_backcompat() -> None:
    """Pre-#316 behaviour: ``json_columns=None`` encodes every dict."""
    out = serialize_complex_value(
        {"k": "v"},
        column="profile",
        json_columns=None,
        dict_encoder=_tag_encoder,
    )
    assert out == ("encoded", {"k": "v"})


def test_dict_encoded_when_column_in_allowlist() -> None:
    out = serialize_complex_value(
        {"k": "v"},
        column="profile",
        json_columns=["profile"],
        dict_encoder=_tag_encoder,
    )
    assert out == ("encoded", {"k": "v"})


def test_dict_raises_when_column_not_in_allowlist() -> None:
    with pytest.raises(ValueError, match="Column 'profile' contains a dict"):
        serialize_complex_value(
            {"k": "v"},
            column="profile",
            json_columns=["other"],
            dict_encoder=_tag_encoder,
        )


def test_dict_raises_when_column_is_none_and_allowlist_set() -> None:
    """Allowlist is enforced even when column metadata is missing —
    safest default, otherwise an unnamed dict would slip past validation."""
    with pytest.raises(ValueError, match="contains a dict"):
        serialize_complex_value(
            {"k": "v"},
            column=None,
            json_columns=["something"],
            dict_encoder=_tag_encoder,
        )


# ---------------------------------------------------------------------------
# list handling — depends on whether the dialect supplies a list_encoder
# ---------------------------------------------------------------------------


def test_list_passes_through_when_no_list_encoder_postgres_style() -> None:
    """Postgres ARRAY adapter handles lists natively → pass-through."""
    out = serialize_complex_value(
        [1, 2, 3],
        column="tags",
        json_columns=["tags"],
        dict_encoder=_tag_encoder,
        list_encoder=None,
    )
    assert out == [1, 2, 3]


def test_list_passes_through_when_json_columns_is_none_postgres_style() -> None:
    out = serialize_complex_value(
        [1, 2, 3],
        column="tags",
        json_columns=None,
        dict_encoder=_tag_encoder,
        list_encoder=None,
    )
    assert out == [1, 2, 3]


def test_list_encoded_when_list_encoder_present_mysql_style() -> None:
    """MySQL has no ARRAY type → encode lists as JSON strings."""
    out = serialize_complex_value(
        [1, 2, 3],
        column="tags",
        json_columns=["tags"],
        dict_encoder=_json_encoder,
        list_encoder=_json_encoder,
    )
    assert out == "[1, 2, 3]"


def test_list_encoded_with_no_json_columns_mysql_style() -> None:
    """MySQL back-compat: no json_columns → encode all lists."""
    out = serialize_complex_value(
        [1, 2, 3],
        column="tags",
        json_columns=None,
        dict_encoder=_json_encoder,
        list_encoder=_json_encoder,
    )
    assert out == "[1, 2, 3]"


def test_list_raises_when_column_not_in_allowlist_postgres() -> None:
    """Even with pass-through encoder, an unlisted list column still fails fast."""
    with pytest.raises(ValueError, match="Column 'tags' contains a list"):
        serialize_complex_value(
            [1, 2, 3],
            column="tags",
            json_columns=["other"],
            dict_encoder=_tag_encoder,
            list_encoder=None,
        )


def test_list_raises_when_column_not_in_allowlist_mysql() -> None:
    with pytest.raises(ValueError, match="Column 'tags' contains a list"):
        serialize_complex_value(
            [1, 2, 3],
            column="tags",
            json_columns=["other"],
            dict_encoder=_json_encoder,
            list_encoder=_json_encoder,
        )


# ---------------------------------------------------------------------------
# Error message shape — locks the contract for #538 / #317 follow-ups
# ---------------------------------------------------------------------------


def test_error_message_names_column_value_type_and_allowlist() -> None:
    """The pointing error must surface enough context to be self-fixing."""
    with pytest.raises(ValueError) as exc:
        serialize_complex_value(
            {"k": "v"},
            column="profile",
            json_columns=["a", "b"],
            dict_encoder=_tag_encoder,
        )
    msg = str(exc.value)
    assert "'profile'" in msg
    assert "dict" in msg
    assert "json_columns=['a', 'b']" in msg
    # Concrete remediation
    assert "Add 'profile' to json_columns" in msg
