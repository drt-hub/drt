"""Third-party connector types survive config parsing (#997).

ADR 0009 recorded the blocker these tests close: a plugin could register a
connector and still never be nameable in a sync YAML, because
``SyncConfig.destination`` and ``load_profile()`` both rejected an unrecognized
``type`` *before* the connector registry was consulted.

The two halves are asymmetric on purpose and are tested as such. Destinations go
through a pydantic union, so the fix is a callable discriminator plus a
catch-all member. Profiles are plain dataclasses with no validator to hook, so
the fix is a registry lookup after the hand-written dispatch chain.

Most of what follows guards the *negative* space — the built-ins whose behaviour
must not have moved — because that is what an extensible union risks.
"""

from __future__ import annotations

import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

import drt.connectors.registry as registry
from drt.config.base import GenericDestinationConfig
from drt.config.credentials import load_profile
from drt.config.models import SlackDestinationConfig, SyncConfig
from drt.config.sync_options import _BUILTIN_DESTINATION_TAGS

PLUGIN_DESTINATION = "salesforce_premium"
PLUGIN_SOURCE = "salesforce_premium_src"


@pytest.fixture
def clean_registry():
    """Snapshot and restore the module-global connector registries.

    Registration is a process-wide side effect and ``register_destination()``
    refuses a duplicate name, so without this a test that registers a fake
    connector would leak into every later test in the session.
    """
    destinations = dict(registry._destination_registry)
    sources = dict(registry._source_registry)
    try:
        yield registry
    finally:
        registry._destination_registry.clear()
        registry._destination_registry.update(destinations)
        registry._source_registry.clear()
        registry._source_registry.update(sources)


class _PluginDestination:
    """Stand-in for a destination class shipped by a third-party package."""


class _PluginSource:
    """Stand-in for a source class shipped by a third-party package."""


@dataclass
class _PluginProfile:
    type: str
    instance_url: str
    api_key: str | None = None
    options: dict[str, Any] = field(default_factory=dict)

    def describe(self) -> str:
        return f"{self.type} ({self.instance_url})"


def _sync(destination: dict[str, Any]) -> Any:
    """Parse a minimal sync YAML payload and hand back its destination."""
    return SyncConfig.model_validate(
        {"name": "s", "model": "m", "destination": destination}
    ).destination


def _write_profiles(tmp_path: Path, body: str) -> Path:
    (tmp_path / "profiles.yml").write_text(textwrap.dedent(body))
    return tmp_path


# ---------------------------------------------------------------------------
# Destinations — built-ins must be untouched
# ---------------------------------------------------------------------------


def test_builtin_destination_still_parses_to_its_concrete_class() -> None:
    """The union still narrows to the exact model, not to the catch-all."""
    dest = _sync({"type": "slack", "webhook_url_env": "SLACK_WEBHOOK_URL"})
    assert type(dest) is SlackDestinationConfig
    assert not isinstance(dest, GenericDestinationConfig)


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"type": "slack", "webhook_url_env": "W"}, "SlackDestinationConfig"),
        ({"type": "file", "path": "/tmp/out.jsonl"}, "FileDestinationConfig"),
        (
            {"type": "rest_api", "url": "https://api.example.com/v1/things"},
            "RestApiDestinationConfig",
        ),
    ],
)
def test_representative_builtin_paths_unchanged(payload: dict[str, Any], expected: str) -> None:
    assert type(_sync(payload)).__name__ == expected


def test_invalid_builtin_still_raises_a_strict_per_field_error() -> None:
    """The catch-all must not swallow a built-in's own validation.

    ``postgres`` requires ``upsert_key``; the error has to stay a per-field
    ``missing`` located inside the postgres member, not degrade into a
    union-level complaint or — far worse — parse as a generic config.
    """
    with pytest.raises(ValidationError) as exc:
        _sync({"type": "postgres", "host": "h", "dbname": "d", "user": "u", "table": "t"})
    errors = exc.value.errors()
    assert any(
        err["type"] == "missing" and err["loc"] == ("destination", "postgres", "upsert_key")
        for err in errors
    ), errors


def test_typoed_builtin_type_is_still_rejected_at_parse_time() -> None:
    """``postgress`` must not quietly become a plugin config.

    This is the regression the registry check in ``_destination_tag`` exists to
    prevent: routing every unknown type to the catch-all would make ``drt
    validate`` pass on a typo and fail much later at connector resolution.
    """
    with pytest.raises(ValidationError) as exc:
        _sync({"type": "postgress", "host": "h"})
    err = exc.value.errors()[0]
    assert err["type"] == "union_tag_invalid"
    assert err["loc"] == ("destination",)


def test_missing_type_is_still_union_tag_not_found() -> None:
    with pytest.raises(ValidationError) as exc:
        _sync({"webhook_url_env": "W"})
    assert exc.value.errors()[0]["type"] == "union_tag_not_found"


def test_unregistered_third_party_type_is_rejected() -> None:
    """Nothing registered it, so it is indistinguishable from a typo."""
    with pytest.raises(ValidationError) as exc:
        _sync({"type": PLUGIN_DESTINATION, "instance_url": "https://x"})
    assert exc.value.errors()[0]["type"] == "union_tag_invalid"


def test_builtin_tags_match_the_connector_registry() -> None:
    """Every built-in union member is registered, and vice versa.

    The tags are hand-written (mypy needs them literal), so this is the guard
    that a new destination cannot be added to the registry while silently
    routing through the catch-all and losing its own validation.
    """
    assert _BUILTIN_DESTINATION_TAGS == set(registry._destination_registry)


# ---------------------------------------------------------------------------
# Destinations — the new third-party path
# ---------------------------------------------------------------------------


def test_registered_third_party_destination_parses_and_resolves(clean_registry) -> None:
    """The end-to-end scenario #297 promised and ADR 0009 said was blocked."""
    clean_registry.register_destination(PLUGIN_DESTINATION, _PluginProfile, _PluginDestination)

    dest = _sync(
        {
            "type": PLUGIN_DESTINATION,
            "instance_url": "https://acme.my.salesforce.com",
            "api_key": "sekrit",
        }
    )

    assert isinstance(dest, GenericDestinationConfig)
    assert dest.type == PLUGIN_DESTINATION
    # The plugin's own fields survive verbatim — its destination implementation
    # reads them off the config it is handed.
    assert dest.model_extra == {
        "instance_url": "https://acme.my.salesforce.com",
        "api_key": "sekrit",
    }
    # And the config now reaches the registry, which is the point.
    assert isinstance(registry.get_destination(dest), _PluginDestination)


def test_generic_config_supports_the_shared_destination_api(clean_registry) -> None:
    """``describe``/``describe_safe``/``rate_limit_key`` are called generically.

    ``drt/cli/output.py`` and ``drt/docs/builder.py`` call them on whatever
    ``sync.destination`` is, so a plugin config missing them would crash at
    render time rather than at parse time.
    """
    clean_registry.register_destination(PLUGIN_DESTINATION, _PluginProfile, _PluginDestination)
    dest = _sync({"type": PLUGIN_DESTINATION, "instance_url": "https://acme.example"})

    # Type-only on purpose (#696): arbitrary plugin fields must never leak into
    # a hosted docs site through a label.
    assert dest.describe() == PLUGIN_DESTINATION
    assert dest.describe_safe() == PLUGIN_DESTINATION
    assert "acme.example" not in dest.describe()
    assert dest.rate_limit_key() == PLUGIN_DESTINATION


def test_generic_config_carries_retry_and_rate_limit(clean_registry) -> None:
    """Shared machinery (`resolve_retry`, the limiter registry) still applies."""
    clean_registry.register_destination(PLUGIN_DESTINATION, _PluginProfile, _PluginDestination)
    dest = _sync(
        {
            "type": PLUGIN_DESTINATION,
            "instance_url": "https://x",
            "retry": {"max_attempts": 7},
            "rate_limit": {"requests_per_second": 2.5},
        }
    )
    assert dest.retry is not None and dest.retry.max_attempts == 7
    assert dest.rate_limit is not None and dest.rate_limit.requests_per_second == 2.5


def test_generic_config_round_trips_through_model_dump(clean_registry) -> None:
    clean_registry.register_destination(PLUGIN_DESTINATION, _PluginProfile, _PluginDestination)
    payload = {"type": PLUGIN_DESTINATION, "instance_url": "https://x", "batch_size": 250}
    dumped = _sync(payload).model_dump()
    assert dumped["type"] == PLUGIN_DESTINATION
    assert dumped["instance_url"] == "https://x"
    assert dumped["batch_size"] == 250
    # Re-parsing the dump yields the same config.
    assert _sync(dumped).model_dump() == dumped


def test_unknown_plugin_fields_are_accepted_not_rejected(clean_registry) -> None:
    """Documents the trade ADR 0009 named for this option.

    drt-core does not know the plugin's schema, so a typo'd *plugin* field is
    carried rather than flagged. Tightening this against the registry's stored
    ``config_class`` is the deliberately deferred second pass — asserted here so
    the behaviour is a recorded decision rather than an accident.
    """
    clean_registry.register_destination(PLUGIN_DESTINATION, _PluginProfile, _PluginDestination)
    dest = _sync({"type": PLUGIN_DESTINATION, "instanse_url": "typo'd on purpose"})
    assert dest.model_extra == {"instanse_url": "typo'd on purpose"}


def test_plugin_cannot_shadow_a_builtin_destination(clean_registry) -> None:
    """A built-in type keeps its concrete model even if re-registered.

    ``register_destination`` already refuses duplicates; this pins the parsing
    side of that guarantee — the discriminator checks the built-in set first.
    """
    with pytest.raises(ValueError, match="already registered"):
        clean_registry.register_destination("slack", _PluginProfile, _PluginDestination)
    assert type(_sync({"type": "slack", "webhook_url_env": "W"})) is SlackDestinationConfig


# ---------------------------------------------------------------------------
# Profiles / sources
# ---------------------------------------------------------------------------


def test_builtin_source_profile_is_unchanged(tmp_path: Path) -> None:
    d = _write_profiles(
        tmp_path,
        """
        local:
          type: duckdb
          database: ./analytics.db
    """,
    )
    profile = load_profile("local", config_dir=d)
    assert type(profile).__name__ == "DuckDBProfile"
    assert profile.database == "./analytics.db"


def test_registered_third_party_source_loads(tmp_path: Path, clean_registry) -> None:
    clean_registry.register_source(PLUGIN_SOURCE, _PluginProfile, _PluginSource)
    d = _write_profiles(
        tmp_path,
        f"""
        premium:
          type: {PLUGIN_SOURCE}
          instance_url: https://acme.my.salesforce.com
          api_key: sekrit
    """,
    )

    profile = load_profile("premium", config_dir=d)

    assert isinstance(profile, _PluginProfile)
    assert profile.type == PLUGIN_SOURCE
    assert profile.instance_url == "https://acme.my.salesforce.com"
    assert isinstance(registry.get_source(profile), _PluginSource)


def test_unregistered_source_still_raises_with_a_useful_message(tmp_path: Path) -> None:
    d = _write_profiles(
        tmp_path,
        """
        nope:
          type: totally_unknown
    """,
    )
    with pytest.raises(ValueError, match="Unsupported source type 'totally_unknown'"):
        load_profile("nope", config_dir=d)


def test_typoed_builtin_source_is_still_rejected(tmp_path: Path) -> None:
    d = _write_profiles(
        tmp_path,
        """
        typo:
          type: duckdbb
          database: ./x.db
    """,
    )
    with pytest.raises(ValueError, match="Unsupported source type 'duckdbb'"):
        load_profile("typo", config_dir=d)


def test_plugin_profile_with_an_unknown_field_names_the_plugin(
    tmp_path: Path, clean_registry
) -> None:
    """A dataclass raises a bare TypeError; the message has to be better."""
    clean_registry.register_source(PLUGIN_SOURCE, _PluginProfile, _PluginSource)
    d = _write_profiles(
        tmp_path,
        f"""
        bad:
          type: {PLUGIN_SOURCE}
          instance_url: https://x
          nonsense_field: 1
    """,
    )
    with pytest.raises(ValueError, match="does not match the .* profile registered by its plugin"):
        load_profile("bad", config_dir=d)


def test_builtin_source_dispatch_wins_over_the_registry(tmp_path: Path, clean_registry) -> None:
    """The fallback is reached only after the hand-written chain runs out.

    ``duckdb`` is both hand-dispatched and registered, so this pins the order:
    the built-in construction (with its per-type defaults) must still be what
    runs, not a generic reconstruction from the registry.
    """
    d = _write_profiles(
        tmp_path,
        """
        local:
          type: duckdb
    """,
    )
    profile = load_profile("local", config_dir=d)
    # The hand-written branch supplies this default; a registry-built profile
    # would not have applied it.
    assert profile.database == ":memory:"
