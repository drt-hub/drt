"""Tests for ``scripts/check_changelog_monotonic.py``."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

# Load the script as a module — it lives outside ``drt/`` so an import path
# rewrite is the simplest way to make it testable.
_SCRIPT = Path(__file__).parent.parent.parent / "scripts" / "check_changelog_monotonic.py"
_spec = importlib.util.spec_from_file_location("check_changelog_monotonic", _SCRIPT)
assert _spec is not None and _spec.loader is not None
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)


# ---------------------------------------------------------------------------
# parse_versions_from_changelog
# ---------------------------------------------------------------------------


def test_parse_changelog_extracts_h2_version_headings() -> None:
    text = """# Changelog

## [Unreleased]

## [0.7.5] - 2026-05-25
some text

## [0.7.4] - 2026-05-23
"""
    assert mod.parse_versions_from_changelog(text) == {"0.7.5", "0.7.4"}


def test_parse_changelog_empty_content_returns_empty_set() -> None:
    assert mod.parse_versions_from_changelog("") == set()


def test_parse_changelog_ignores_unreleased_heading() -> None:
    """``## [Unreleased]`` is not a version section — must not be returned."""
    assert mod.parse_versions_from_changelog("## [Unreleased]\n") == set()


def test_parse_changelog_ignores_h3_dagster_drt_headings() -> None:
    """``### [X.Y.Z]`` is the dagster-drt sub-section format; not for drt-core."""
    text = """## dagster-drt

### [0.2.0] - 2026-04-04 (dagster-drt)
### [0.1.0] - 2026-04-01 (dagster-drt)

## drt-core
"""
    assert mod.parse_versions_from_changelog(text) == set()


# ---------------------------------------------------------------------------
# parse_versions_from_tags
# ---------------------------------------------------------------------------


def test_parse_tags_extracts_drt_core_versions() -> None:
    text = "v0.7.5\nv0.7.4\nv0.1.0\n"
    assert mod.parse_versions_from_tags(text) == {"0.7.5", "0.7.4", "0.1.0"}


def test_parse_tags_ignores_dagster_drt_prefix() -> None:
    """``dagster-drt-v0.2.0`` is a separate package; do not validate here."""
    text = "v0.7.5\ndagster-drt-v0.2.0\nv0.7.4\n"
    assert mod.parse_versions_from_tags(text) == {"0.7.5", "0.7.4"}


def test_parse_tags_ignores_malformed_lines() -> None:
    text = "v0.7.5\nnot-a-tag\n\nv\nvX.Y.Z\n"
    assert mod.parse_versions_from_tags(text) == {"0.7.5"}


# ---------------------------------------------------------------------------
# check() — the function the CI step calls
# ---------------------------------------------------------------------------


def test_check_passes_when_every_tag_has_a_changelog_section() -> None:
    changelog = "## [0.7.5] - 2026-05-25\n## [0.7.4] - 2026-05-23\n"
    tags = "v0.7.5\nv0.7.4\n"
    ok, message = mod.check(changelog, tags)
    assert ok is True
    assert "2" in message  # mentions the count


def test_check_fails_when_a_tag_is_missing_from_changelog() -> None:
    """The headline accident the guard exists to prevent (#492 / #567)."""
    changelog = "## [0.7.5] - 2026-05-25\n"  # 0.7.4 section accidentally removed
    tags = "v0.7.5\nv0.7.4\n"
    ok, message = mod.check(changelog, tags)
    assert ok is False
    assert "0.7.4" in message
    assert "missing" in message.lower()


def test_check_respects_the_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    """Versions in ALLOWLISTED_MISSING_VERSIONS must not trigger a failure.

    Injected via ``monkeypatch`` so the test stays valid regardless of the
    current module-level allowlist contents (which is now empty after the
    v0.5.2 / v0.5.3 CHANGELOG backfill).
    """
    monkeypatch.setattr(mod, "ALLOWLISTED_MISSING_VERSIONS", frozenset({"9.9.9"}))
    changelog = "## [0.7.5] - 2026-05-25\n"
    tags = "v0.7.5\nv9.9.9\n"
    ok, message = mod.check(changelog, tags)
    assert ok is True, f"Allowlisted version should not fail; got: {message}"


def test_check_returns_friendly_message_when_no_tags_exist() -> None:
    ok, message = mod.check("", "")
    assert ok is True
    assert "no" in message.lower() or "nothing" in message.lower()


def test_check_failure_message_includes_restore_hint() -> None:
    """The error must include a copy-pasteable command for restoring the section."""
    changelog = "## [0.7.5] - 2026-05-25\n"
    tags = "v0.7.5\nv0.7.4\n"
    ok, message = mod.check(changelog, tags)
    assert ok is False
    assert "git show v0.7.4:CHANGELOG.md" in message


# ---------------------------------------------------------------------------
# Version-sort key — protects against 0.7.10 < 0.7.9 lexical ordering
# ---------------------------------------------------------------------------


def test_version_sort_key_is_numeric_not_lexical() -> None:
    """Tomorrow's 0.7.10 must sort AFTER 0.7.9, not between 0.7.1 and 0.7.2."""
    versions = ["0.7.9", "0.7.10", "0.7.2", "0.7.1"]
    versions.sort(key=mod._version_sort_key)
    assert versions == ["0.7.1", "0.7.2", "0.7.9", "0.7.10"]


# ---------------------------------------------------------------------------
# Allowlist is documented (not silently bypassing things forever)
# ---------------------------------------------------------------------------


def test_allowlist_is_a_frozenset() -> None:
    """The allowlist must be a frozenset so the script + tests can rely on
    immutability. Currently empty (post-v0.5.2 / v0.5.3 backfill) — the
    guard is in full-strict mode. Any future additions should come with
    an inline comment explaining the historical context.
    """
    assert isinstance(mod.ALLOWLISTED_MISSING_VERSIONS, frozenset)
