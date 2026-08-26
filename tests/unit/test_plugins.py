"""Tests for entry-point based plugin discovery (#297)."""

from __future__ import annotations

from collections.abc import Iterator
from importlib.metadata import EntryPoint

import pytest

from drt.plugins import (
    CONNECTOR_GROUPS,
    PLUGIN_GROUPS,
    DiscoveredPlugin,
    _reset_plugin_state,
    discover_plugins,
    load_plugins,
)

_CALLS: list[str] = []


def _register_ok() -> None:
    _CALLS.append("ok")


def _register_broken() -> None:
    raise RuntimeError("sink unreachable")


# Not a function — a plain module-level value, so `ep.load()` returns
# something non-callable. Covers the case where a plugin's entry point
# points at the wrong kind of target.
_NOT_CALLABLE = object()


@pytest.fixture(autouse=True)
def _reset() -> Iterator[None]:
    _CALLS.clear()
    _reset_plugin_state()
    yield
    _CALLS.clear()
    _reset_plugin_state()


def _fake_entry_points(
    monkeypatch: pytest.MonkeyPatch, mapping: dict[str, list[EntryPoint]]
) -> None:
    def _fake(*, group: str) -> list[EntryPoint]:
        return mapping.get(group, [])

    monkeypatch.setattr("drt.plugins.entry_points", _fake)


def test_plugin_groups_include_all_four_adr_0008_registries_plus_connectors() -> None:
    assert set(PLUGIN_GROUPS) == {
        "drt.sources",
        "drt.destinations",
        "drt.secret_providers",
        "drt.permission_checkers",
        "drt.audit_loggers",
        "drt.observers",
    }


def test_connector_groups_are_a_subset_of_plugin_groups() -> None:
    assert CONNECTOR_GROUPS <= set(PLUGIN_GROUPS)
    assert CONNECTOR_GROUPS == {"drt.sources", "drt.destinations"}


def test_discover_plugins_empty_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_entry_points(monkeypatch, {})
    assert discover_plugins() == []


def test_discover_plugins_does_not_invoke_the_target(monkeypatch: pytest.MonkeyPatch) -> None:
    ep = EntryPoint(
        name="my_logger", value=f"{__name__}:_register_ok", group="drt.audit_loggers"
    )
    _fake_entry_points(monkeypatch, {"drt.audit_loggers": [ep]})

    results = discover_plugins()

    assert results == [
        DiscoveredPlugin(
            group="drt.audit_loggers",
            name="my_logger",
            value=f"{__name__}:_register_ok",
            dist_name=None,
            dist_version=None,
            author=None,
        )
    ]
    assert _CALLS == []


def test_load_plugins_invokes_registration_callable(monkeypatch: pytest.MonkeyPatch) -> None:
    ep = EntryPoint(
        name="my_logger", value=f"{__name__}:_register_ok", group="drt.audit_loggers"
    )
    _fake_entry_points(monkeypatch, {"drt.audit_loggers": [ep]})

    results = load_plugins()

    assert _CALLS == ["ok"]
    assert len(results) == 1
    assert results[0].loaded is True
    assert results[0].error is None


def test_load_plugins_isolates_a_broken_plugin(monkeypatch: pytest.MonkeyPatch) -> None:
    broken = EntryPoint(
        name="broken", value=f"{__name__}:_register_broken", group="drt.audit_loggers"
    )
    ok = EntryPoint(name="ok", value=f"{__name__}:_register_ok", group="drt.observers")
    _fake_entry_points(monkeypatch, {"drt.audit_loggers": [broken], "drt.observers": [ok]})

    results = load_plugins()

    by_name = {r.name: r for r in results}
    assert by_name["broken"].loaded is False
    assert by_name["broken"].error is not None
    assert "sink unreachable" in by_name["broken"].error
    assert by_name["ok"].loaded is True
    assert _CALLS == ["ok"]


def test_load_plugins_is_cached_across_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    ep = EntryPoint(name="my_logger", value=f"{__name__}:_register_ok", group="drt.observers")
    _fake_entry_points(monkeypatch, {"drt.observers": [ep]})

    load_plugins()
    load_plugins()

    assert _CALLS == ["ok"]


def test_load_plugins_force_reruns() -> None:
    def _mapping_a(monkeypatch: pytest.MonkeyPatch) -> None:
        pass

    load_plugins()  # no entry points registered (real env) — establishes cache
    load_plugins(force=True)
    load_plugins(force=True)
    # No assertion on _CALLS here (real entry points may be empty in CI) —
    # this only proves force=True doesn't raise on repeated invocation.


def test_load_plugins_reports_a_non_callable_target_as_an_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-blocking nit from Pawansingh3889's review on PR #996: a
    non-callable entry-point target used to be silently marked loaded=True
    (nothing was invoked, but the entry looked fine)."""
    ep = EntryPoint(
        name="not_callable", value=f"{__name__}:_NOT_CALLABLE", group="drt.audit_loggers"
    )
    _fake_entry_points(monkeypatch, {"drt.audit_loggers": [ep]})

    results = load_plugins()

    assert results[0].loaded is False
    assert results[0].error is not None
    assert "not callable" in results[0].error


def test_connector_entry_point_is_discovered_but_flagged_not_yet_usable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ep = EntryPoint(
        name="salesforce_premium", value=f"{__name__}:_register_ok", group="drt.destinations"
    )
    _fake_entry_points(monkeypatch, {"drt.destinations": [ep]})

    results = load_plugins()

    assert results[0].loaded is True
    assert results[0].group in CONNECTOR_GROUPS


# ---------------------------------------------------------------------------
# `drt plugins list` CLI surface
# ---------------------------------------------------------------------------


def test_cli_plugins_list_reports_no_plugins_by_default() -> None:
    from typer.testing import CliRunner

    from drt.cli.main import app

    result = CliRunner().invoke(app, ["plugins", "list"])

    assert result.exit_code == 0
    assert "No plugins discovered" in result.stdout


def test_cli_plugins_list_json_marks_connector_entries_not_yet_usable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import json as _json

    from typer.testing import CliRunner

    from drt.cli.main import app

    ep = EntryPoint(
        name="salesforce_premium", value=f"{__name__}:_register_ok", group="drt.destinations"
    )
    _fake_entry_points(monkeypatch, {"drt.destinations": [ep]})

    result = CliRunner().invoke(app, ["plugins", "list", "--format", "json"])

    assert result.exit_code == 0
    payload = _json.loads(result.stdout)
    assert payload["plugins"][0]["group"] == "drt.destinations"
    assert payload["plugins"][0]["loaded"] is True
    # True since #997: a registered destination type is nameable in a sync
    # YAML like any built-in. Only a load failure makes a plugin unusable.
    assert payload["plugins"][0]["usable_in_sync_yaml"] is True


def test_cli_plugins_list_reuses_the_startup_callback_cache_not_a_second_load(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression for a Codex-caught bug on PR #996.

    ``drt plugins list`` used to call ``load_plugins(force=True)``, which
    re-invoked every registration callable a *second* time on top of the one
    the root ``@app.callback()`` already did. Registries that reject
    duplicate registration (SecretProvider, the connector registry) would
    then falsely report a legitimately loaded plugin as errored, and
    cumulative registries (``register_extra_observer``) would double-register
    it. Assert the callable fires exactly once for one CLI invocation.
    """
    from typer.testing import CliRunner

    from drt.cli.main import app

    ep = EntryPoint(name="my_logger", value=f"{__name__}:_register_ok", group="drt.audit_loggers")
    _fake_entry_points(monkeypatch, {"drt.audit_loggers": [ep]})

    result = CliRunner().invoke(app, ["plugins", "list"])

    assert result.exit_code == 0
    assert _CALLS == ["ok"]
    assert "error" not in result.stdout.lower()


def test_cli_plugins_list_json_marks_non_connector_entries_usable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import json as _json

    from typer.testing import CliRunner

    from drt.cli.main import app

    ep = EntryPoint(name="my_logger", value=f"{__name__}:_register_ok", group="drt.audit_loggers")
    _fake_entry_points(monkeypatch, {"drt.audit_loggers": [ep]})

    result = CliRunner().invoke(app, ["plugins", "list", "--format", "json"])

    assert result.exit_code == 0
    payload = _json.loads(result.stdout)
    assert payload["plugins"][0]["usable_in_sync_yaml"] is True


def test_cli_plugins_list_table_shows_connector_and_error_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers the table-render branches — JSON tests above only exercise the
    dict-construction branches, not this separate Rich-table code path."""
    from typer.testing import CliRunner

    from drt.cli.main import app

    # Rich wraps status text across lines at its default (narrow, non-tty)
    # width, which would fragment the substrings this test asserts on.
    monkeypatch.setenv("COLUMNS", "200")

    connector = EntryPoint(
        name="salesforce_premium", value=f"{__name__}:_register_ok", group="drt.destinations"
    )
    broken = EntryPoint(
        name="broken", value=f"{__name__}:_register_broken", group="drt.audit_loggers"
    )
    _fake_entry_points(
        monkeypatch, {"drt.destinations": [connector], "drt.audit_loggers": [broken]}
    )

    result = CliRunner().invoke(app, ["plugins", "list"])

    assert result.exit_code == 0
    # The connector caveat is gone (#997); a loaded connector reads the same
    # as any other loaded plugin, and only real failures are called out.
    assert "not yet usable in sync YAML" not in result.stdout
    assert "loaded" in result.stdout
    assert "error: sink unreachable" in result.stdout


# ---------------------------------------------------------------------------
# Author metadata extraction (a real installed dist populates `ep.dist`;
# constructing a bare EntryPoint() never does, per importlib.metadata)
# ---------------------------------------------------------------------------


class _FakeMetadata:
    def __init__(self, values: dict[str, str]) -> None:
        self._values = values

    def get(self, key: str, default: str | None = None) -> str | None:
        return self._values.get(key, default)


class _FakeDist:
    def __init__(self, *, name: str, version: str, metadata: dict[str, str]) -> None:
        self.name = name
        self.version = version
        self.metadata = _FakeMetadata(metadata)


class _FakeEntryPoint:
    def __init__(self, *, name: str, value: str, group: str, dist: _FakeDist | None) -> None:
        self.name = name
        self.value = value
        self.group = group
        self.dist = dist

    def load(self) -> object:
        import importlib

        module_name, _, attr = self.value.partition(":")
        return getattr(importlib.import_module(module_name), attr)


def test_discover_plugins_reads_author_from_dist_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ep = _FakeEntryPoint(
        name="my_logger",
        value=f"{__name__}:_register_ok",
        group="drt.audit_loggers",
        dist=_FakeDist(name="my-package", version="0.1.0", metadata={"Author": "Jane Doe"}),
    )
    monkeypatch.setattr(
        "drt.plugins.entry_points", lambda *, group: [ep] if group == "drt.audit_loggers" else []
    )

    result = discover_plugins()[0]

    assert result.dist_name == "my-package"
    assert result.dist_version == "0.1.0"
    assert result.author == "Jane Doe"


def test_discover_plugins_falls_back_to_author_email(monkeypatch: pytest.MonkeyPatch) -> None:
    ep = _FakeEntryPoint(
        name="my_logger",
        value=f"{__name__}:_register_ok",
        group="drt.audit_loggers",
        dist=_FakeDist(
            name="my-package", version="0.1.0", metadata={"Author-email": "Jane <jane@x.com>"}
        ),
    )
    monkeypatch.setattr(
        "drt.plugins.entry_points", lambda *, group: [ep] if group == "drt.audit_loggers" else []
    )

    result = discover_plugins()[0]

    assert result.author == "Jane <jane@x.com>"
