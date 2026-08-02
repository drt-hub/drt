"""Unit tests for the query-tagging payload builder (#768)."""

from __future__ import annotations

import re

from drt.config.query_tags import (
    build_query_tags,
    new_run_id,
    normalize_bigquery_label,
    render_comment_header,
)


def test_new_run_id_is_lowercase_hex() -> None:
    run_id = new_run_id()
    assert re.fullmatch(r"[0-9a-f]{12}", run_id)


def test_new_run_id_is_unique_per_call() -> None:
    assert new_run_id() != new_run_id()


def test_build_query_tags_includes_app_sync_run_id() -> None:
    tags = build_query_tags("users_to_hubspot", "abc123", {})
    assert tags == {"app": "drt", "sync": "users_to_hubspot", "run_id": "abc123"}


def test_build_query_tags_extra_can_override_defaults() -> None:
    tags = build_query_tags("s", "r", {"app": "custom", "team": "growth"})
    assert tags == {"app": "custom", "sync": "s", "run_id": "r", "team": "growth"}


def test_render_comment_header_shape() -> None:
    header = render_comment_header({"app": "drt", "sync": "users", "run_id": "abc123"})
    assert header == "/* drt app=drt sync=users run_id=abc123 */"


def test_render_comment_header_strips_comment_terminator() -> None:
    """A value containing ``*/`` must not be able to close the comment early
    and let the rest of the payload land in live SQL."""
    header = render_comment_header({"sync": "evil*/DROP TABLE x;--"})
    assert "*/" not in header[:-3]  # not before the header's own closing */
    assert header.endswith("*/")


def test_render_comment_header_strips_newlines() -> None:
    header = render_comment_header({"sync": "a\nb\rc"})
    assert "\n" not in header
    assert "\r" not in header


def test_normalize_bigquery_label_lowercases_and_substitutes() -> None:
    assert normalize_bigquery_label("Users -> HubSpot") == "users----hubspot"


def test_normalize_bigquery_label_truncates_to_63_chars() -> None:
    assert len(normalize_bigquery_label("x" * 100)) == 63


def test_normalize_bigquery_label_leaves_valid_input_untouched() -> None:
    assert normalize_bigquery_label("users_to_hubspot-v2") == "users_to_hubspot-v2"
