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
import warnings
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel, Tag, ValidationError

import drt.connectors.registry as registry
from drt.config.base import GenericDestinationConfig
from drt.config.credentials import (
    _DISPATCHED_SOURCE_TYPES,
    RestApiProfile,
    load_profile,
    save_profile,
)
from drt.config.models import SlackDestinationConfig, SyncConfig
from drt.config.profiles import ProfileConfigLike
from drt.config.sync_options import _BUILTIN_DESTINATION_TAGS, GENERIC_DESTINATION_TAG
from drt.sources.base import Source

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

    def extract(
        self,
        query: str,
        config: ProfileConfigLike,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        assert isinstance(config, _PluginProfile)
        yield {
            "instance_url": config.instance_url,
            "query": query,
            "query_tags": query_tags,
        }

    def test_connection(self, config: ProfileConfigLike) -> bool:
        assert isinstance(config, _PluginProfile)
        return bool(config.instance_url)


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

    # Set equality alone cannot catch a Tag attached to the wrong class, or a
    # tag and its registration mistyped the same way. Pin each tag to the
    # Literal on the model it annotates — the correspondence pydantic used to
    # derive for free from Field(discriminator="type").
    from typing import get_args as _get_args

    from drt.config.models import DestinationConfig

    for member in _get_args(_get_args(DestinationConfig)[0]):
        model, *meta = _get_args(member)
        tag = next(m.tag for m in meta if isinstance(m, Tag))
        if tag == GENERIC_DESTINATION_TAG:
            continue
        literal = _get_args(model.model_fields["type"].annotation)[0]
        assert tag == literal, f"{model.__name__} is tagged {tag!r} but its type is {literal!r}"


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


def test_registered_third_party_source_extracts_through_source_protocol(
    tmp_path: Path, clean_registry
) -> None:
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
    source: Source = registry.get_source(profile)
    assert isinstance(source, Source)
    assert list(source.extract("SELECT leads", profile, query_tags={"sync": "leads"})) == [
        {
            "instance_url": "https://acme.my.salesforce.com",
            "query": "SELECT leads",
            "query_tags": {"sync": "leads"},
        }
    ]
    assert source.test_connection(profile) is True


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
    """A signature mismatch must name the class, not blame profiles.yml vaguely.

    Deliberately does not say "its plugin" any more: the same path is reached by
    any registered profile class, and calling a first-party one a plugin was
    exactly the rest_api wart this fixes.
    """
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
    with pytest.raises(ValueError, match="does not match the .* profile registered by"):
        load_profile("bad", config_dir=d)


def test_builtin_source_dispatch_wins_over_the_registry(tmp_path: Path) -> None:
    """The fallback is reached only after the hand-written chain runs out.

    Asserted on ``port``, not ``database``: the earlier version of this test
    checked that duckdb yields ``":memory:"``, which the dataclass default
    supplies either way, so it passed whichever path ran and proved nothing.
    ``postgres`` coerces ``port`` to ``int`` *in the branch*; a registry-built
    profile would pass the raw YAML string straight through.
    """
    d = _write_profiles(tmp_path, """
        pg:
          type: postgres
          host: db.example
          dbname: analytics
          user: analyst
          port: "5432"
    """)
    profile = load_profile("pg", config_dir=d)
    assert profile.port == 5432
    assert isinstance(profile.port, int)


def test_every_registered_source_is_dispatched_or_reachable() -> None:
    """``_DISPATCHED_SOURCE_TYPES`` must not drift from the chain or the registry.

    ``rest_api`` was registered and in the ``ProfileConfig`` union with no branch
    in the chain, so it fell through to the plugin fallback and was reported to
    users as third-party. Nothing caught that.
    """
    assert _DISPATCHED_SOURCE_TYPES == set(registry._source_registry), (
        "load_profile()'s hand-written chain and the connector registry disagree; "
        "a built-in with no branch is reported to users as a plugin."
    )


def test_unknown_source_message_does_not_advertise_builtins_as_plugins(tmp_path: Path) -> None:
    """No plugins installed means no 'Also registered' tail."""
    d = _write_profiles(tmp_path, """
        nope:
          type: totally_unknown
    """)
    with pytest.raises(ValueError) as exc:
        load_profile("nope", config_dir=d)
    assert "Also registered" not in str(exc.value)


def test_plugin_profile_must_implement_describe(tmp_path: Path, clean_registry) -> None:
    """drt-core calls describe() on whatever load_profile() returns.

    ``drt/cli/output.py`` has no hasattr guard, so without this check a plugin
    profile lacking ``describe()`` crashed with AttributeError several layers
    into ``drt run --dry-run``.
    """

    @dataclass
    class NoDescribe:
        type: str
        url: str

    clean_registry.register_source(PLUGIN_SOURCE, NoDescribe, _PluginSource)
    d = _write_profiles(tmp_path, f"""
        p:
          type: {PLUGIN_SOURCE}
          url: https://x
    """)
    with pytest.raises(ValueError, match="does not implement describe"):
        load_profile("p", config_dir=d)


def test_plugin_profile_must_satisfy_profile_config_like(
    tmp_path: Path, clean_registry
) -> None:
    """A plugin profile that accepts ``type=`` but doesn't expose it as a
    readable attribute fails the ProfileConfigLike structural check (#1034),
    not just the narrower describe()-callable one. Deliberately not a
    dataclass -- a dataclass field named ``type`` would satisfy the Protocol
    automatically, so this stores it privately instead.
    """

    class NoType:
        def __init__(self, type: str, url: str) -> None:
            self._type = type
            self.url = url

        def describe(self) -> str:
            return "no-type"

    clean_registry.register_source(PLUGIN_SOURCE, NoType, _PluginSource)
    d = _write_profiles(tmp_path, f"""
        p:
          type: {PLUGIN_SOURCE}
          url: https://x
    """)
    with pytest.raises(ValueError, match="ProfileConfigLike"):
        load_profile("p", config_dir=d)


def test_plugin_internal_type_error_is_not_blamed_on_the_profile(
    tmp_path: Path, clean_registry
) -> None:
    """A TypeError from inside the plugin's own __post_init__ is its bug, not the user's."""

    @dataclass
    class Exploding:
        type: str
        port: int | None = None

        def __post_init__(self) -> None:
            int(self.port)  # raises TypeError on None

        def describe(self) -> str:
            return self.type

    clean_registry.register_source(PLUGIN_SOURCE, Exploding, _PluginSource)
    d = _write_profiles(tmp_path, f"""
        p:
          type: {PLUGIN_SOURCE}
          port: null
    """)
    # Propagates as the plugin's own TypeError rather than being rewritten into
    # "your profile does not match".
    with pytest.raises(TypeError):
        load_profile("p", config_dir=d)


def test_registered_plugin_profile_round_trips_through_save(tmp_path: Path, clean_registry) -> None:
    """load_profile() and save_profile() must stay symmetric (#997)."""
    clean_registry.register_source(PLUGIN_SOURCE, _PluginProfile, _PluginSource)
    save_profile(
        "p",
        _PluginProfile(type=PLUGIN_SOURCE, instance_url="https://acme", api_key="k"),
        config_dir=tmp_path,
    )
    loaded = load_profile("p", config_dir=tmp_path)
    assert isinstance(loaded, _PluginProfile)
    assert loaded.instance_url == "https://acme"


def test_sentinel_tag_is_not_a_usable_destination_type() -> None:
    """`type: __plugin__` must not reach the catch-all.

    The sentinel is a real tag in the union, so returning it verbatim from the
    discriminator let a user name it and parse cleanly, deferring the failure to
    `get_destination()` — the exact thing the registry check exists to prevent.
    """
    with pytest.raises(ValidationError) as exc:
        _sync({"type": GENERIC_DESTINATION_TAG, "anything": 1})
    assert exc.value.errors()[0]["type"] == "union_tag_not_found"


def test_non_dict_mappings_are_accepted() -> None:
    """pydantic takes any Mapping as model input; the discriminator must too."""
    from types import MappingProxyType

    dest = _sync(MappingProxyType({"type": "slack", "webhook_url_env": "W"}))
    assert type(dest) is SlackDestinationConfig


def test_plugin_config_class_is_used_when_it_is_a_pydantic_model(clean_registry) -> None:
    """The registered config_class parses the payload, not the catch-all.

    Without this a plugin's own defaults were lost and the `assert isinstance(
    config, MyConfig)` that docs/guides/building-a-destination.md tells authors
    to write in `load()` failed.
    """

    class AcmeConfig(GenericDestinationConfig):
        instance_url: str
        batch_size: int = 500

        def describe(self) -> str:
            return f"{self.type} ({self.instance_url})"

    clean_registry.register_destination(PLUGIN_DESTINATION, AcmeConfig, _PluginDestination)
    dest = _sync({"type": PLUGIN_DESTINATION, "instance_url": "https://acme"})

    assert isinstance(dest, AcmeConfig)
    assert dest.batch_size == 500  # the plugin's default, not lost
    assert dest.describe() == f"{PLUGIN_DESTINATION} (https://acme)"


def test_plugin_config_class_enforces_its_own_required_fields(clean_registry) -> None:
    class AcmeConfig(GenericDestinationConfig):
        instance_url: str

    clean_registry.register_destination(PLUGIN_DESTINATION, AcmeConfig, _PluginDestination)
    with pytest.raises(ValidationError):
        _sync({"type": PLUGIN_DESTINATION})


def test_distinct_plugin_destinations_stay_distinct_in_docs(clean_registry) -> None:
    """drt/docs/builder.py keys nodes on f"{type}|{describe()}".

    A type-only describe() collapsed two plugin destinations pointing at
    different systems into a single lineage node.
    """

    class AcmeConfig(GenericDestinationConfig):
        instance_url: str

        def describe(self) -> str:
            return f"{self.type} ({self.instance_url})"

    clean_registry.register_destination(PLUGIN_DESTINATION, AcmeConfig, _PluginDestination)
    one = _sync({"type": PLUGIN_DESTINATION, "instance_url": "https://one"})
    two = _sync({"type": PLUGIN_DESTINATION, "instance_url": "https://two"})
    assert f"{one.type}|{one.describe()}" != f"{two.type}|{two.describe()}"


def test_fallback_refuses_extras_drt_core_reads_itself(clean_registry) -> None:
    """`lookups`/`table` on the permissive path used to crash mid-run.

    Registered with a non-pydantic config_class, so the payload lands on
    GenericDestinationConfig rather than a plugin model.
    """
    clean_registry.register_destination(PLUGIN_DESTINATION, object, _PluginDestination)
    with pytest.raises(ValidationError, match="drt-core reads off a destination config"):
        _sync({"type": PLUGIN_DESTINATION, "lookups": {"a": {"table": "t"}}})


def test_config_class_must_subclass_the_catch_all_to_take_over(clean_registry) -> None:
    """A plugin model outside the union is not delegated to.

    `SyncConfig.destination` is a closed discriminated union; parsing an
    arbitrary BaseModel there left the field holding a value outside its own
    declared type, so dumping the *container* fell back to pydantic's
    warning-driven best-effort path and any operator-set retry/rate_limit was
    dropped before it could reach resolve_retry().
    """

    class Outside(BaseModel):
        type: str
        instance_url: str

    clean_registry.register_destination(PLUGIN_DESTINATION, Outside, _PluginDestination)
    dest = _sync(
        {"type": PLUGIN_DESTINATION, "instance_url": "https://x", "retry": {"max_attempts": 7}}
    )

    assert isinstance(dest, GenericDestinationConfig)
    assert dest.retry is not None and dest.retry.max_attempts == 7
    assert dest.model_extra == {"instance_url": "https://x"}


def test_subclassed_config_class_keeps_retry_and_serializes_its_own_fields(clean_registry) -> None:
    """The documented contract: subclass, and you get both halves."""

    class AcmeConfig(GenericDestinationConfig):
        instance_url: str

    clean_registry.register_destination(PLUGIN_DESTINATION, AcmeConfig, _PluginDestination)
    sync = SyncConfig.model_validate(
        {
            "name": "s",
            "model": "m",
            "destination": {
                "type": PLUGIN_DESTINATION,
                "instance_url": "https://acme",
                "retry": {"max_attempts": 7},
            },
        }
    )
    assert isinstance(sync.destination, AcmeConfig)
    assert sync.destination.retry is not None and sync.destination.retry.max_attempts == 7

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        dumped = sync.model_dump()
    # No pydantic serializer warnings, and SerializeAsAny keeps the subclass's
    # own declared field in the dump rather than the union member's schema.
    assert [w for w in caught if "serializer" in str(w.message).lower()] == []
    assert dumped["destination"]["instance_url"] == "https://acme"


def test_save_profile_refuses_a_literal_rest_api_token(tmp_path: Path) -> None:
    """rest_api must stay on the *_env-only path every other built-in uses.

    Without its own branch it fell into the dataclass fallback #997 added, and
    asdict() wrote the raw token straight into profiles.yml.
    """
    with pytest.raises(ValueError, match="stores env var names, not secrets"):
        save_profile(
            "myrest",
            RestApiProfile(
                type="rest_api",
                url="https://api.example.com/v1",
                auth={"type": "bearer", "token": "sk-live-SECRET-12345"},
            ),
            config_dir=tmp_path,
        )


def test_save_profile_writes_rest_api_env_var_form(tmp_path: Path) -> None:
    save_profile(
        "ok",
        RestApiProfile(
            type="rest_api", url="https://x", auth={"type": "bearer", "token_env": "TOK"}
        ),
        config_dir=tmp_path,
    )
    written = (tmp_path / "profiles.yml").read_text()
    assert "token_env: TOK" in written
    assert "sk-live" not in written
    assert isinstance(load_profile("ok", config_dir=tmp_path), RestApiProfile)
