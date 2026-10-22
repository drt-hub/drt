"""The declared plan coverage must agree with what ``drt plan`` actually does (#1223)."""

from __future__ import annotations

import typing
from typing import Any
from unittest.mock import patch

import pytest

import drt.connectors  # noqa: F401 — registers the built-in destinations
from drt.config.models import SyncOptions
from drt.connectors import registry
from drt.destinations import query
from drt.engine.diff import _is_append_only
from drt.engine.plan_coverage import PLAN_COVERAGE, PLAN_STATUSES, plan_coverage


def _is_builtin(destination_type: str) -> bool:
    _config_class, impl = registry._destination_registry[destination_type]
    return impl.__module__.startswith("drt.")


# Installed third-party plugins are registered too; the matrix covers built-ins only.
_BUILTIN = sorted(t for t in registry.registered_destination_types() if _is_builtin(t))


def _config(destination_type: str, **fields: Any) -> Any:
    config_class, _impl = registry._destination_registry[destination_type]
    return config_class.model_construct(type=destination_type, **fields)


def _reads_target(destination_type: str) -> bool:
    """Does the real row-reader dispatch accept this destination's config?"""
    config = _config(destination_type)
    readers = [name for name in dir(query) if name.startswith("_fetch_rows_")]
    with patch.multiple(query, **{name: lambda *a, **k: [] for name in readers}):
        try:
            query.fetch_rows(config, "SELECT 1", columns=[])
        except TypeError:
            return False
    return True


def _can_choose_insert(destination_type: str) -> bool:
    """Whether the config model accepts ``mode: insert`` at all (not just any string)."""
    config_class, _impl = registry._destination_registry[destination_type]
    field = config_class.model_fields.get("mode")
    return field is not None and "insert" in typing.get_args(field.annotation)


def _append_only(destination_type: str) -> bool:
    config = _config(destination_type, mode="insert")
    always = _is_append_only(_config(destination_type), SyncOptions(mode="full"))
    chosen = _can_choose_insert(destination_type) and _is_append_only(
        config, SyncOptions(mode="full")
    )
    return always or chosen


def _derived_group(destination_type: str) -> str:
    if _reads_target(destination_type):
        return "available"
    if _append_only(destination_type):
        return "partial"
    return "not-planned"


def test_every_builtin_destination_declares_its_plan_status() -> None:
    assert sorted(set(_BUILTIN) - set(PLAN_COVERAGE)) == []
    registered = set(registry.registered_destination_types())
    assert sorted(set(PLAN_COVERAGE) - registered) == [], "declared but not registered"


@pytest.mark.parametrize("destination_type", _BUILTIN)
def test_the_declaration_agrees_with_the_code_paths(destination_type: str) -> None:
    declared = PLAN_COVERAGE[destination_type]
    group = _derived_group(destination_type)

    assert declared.status in PLAN_STATUSES
    if group == "not-planned":
        assert declared.status in {"needs-api-diff", "unavailable"}, (
            f"{destination_type} cannot be planned today, so it cannot be declared "
            f"{declared.status!r}"
        )
    else:
        assert declared.status == group, (
            f"{destination_type} is {group} according to the code, declared {declared.status!r}"
        )


@pytest.mark.parametrize("destination_type", _BUILTIN)
def test_anything_short_of_available_says_why(destination_type: str) -> None:
    declared = PLAN_COVERAGE[destination_type]
    if declared.status != "available":
        assert len(declared.reason.strip()) > 10


def test_an_undeclared_type_has_no_declaration_rather_than_a_wrong_one() -> None:
    assert plan_coverage("some_third_party_destination") is None
    assert plan_coverage("postgres") is PLAN_COVERAGE["postgres"]


def test_the_generated_coverage_page_is_current() -> None:
    import importlib.util
    from pathlib import Path

    script = Path(__file__).resolve().parents[2] / "scripts" / "gen_plan_coverage.py"
    spec = importlib.util.spec_from_file_location("gen_plan_coverage", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.DOC.read_text() == module.render(), (
        "docs/guides/plan-coverage.md is stale; run python scripts/gen_plan_coverage.py"
    )
